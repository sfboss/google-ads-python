#!/usr/bin/env python3
"""
YouTube2Text - Topic-based YouTube Keyword Generator

Takes a topic as input, searches YouTube for related videos, extracts content
(titles, descriptions, transcripts, tags), and generates keyword arrays for
consumption by other tools.

Similar to serp2text.py but uses YouTube as the content source.
"""

import argparse
import asyncio
import json
import logging
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from urllib.parse import quote

import yake
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, BarColumn, TimeElapsedColumn
from rich.table import Table

try:
    import nltk
    from nltk.tokenize import word_tokenize
    from nltk.corpus import stopwords
    from nltk.stem import WordNetLemmatizer
    NLTK_AVAILABLE = True
except ImportError:
    NLTK_AVAILABLE = False

# --------------------------------------------------------------------------- #
# Globals
# --------------------------------------------------------------------------- #
console = Console()
logger = logging.getLogger("youtube2text")

DEFAULT_CFG = {
    "num_videos": 10,
    "top_n_keywords": 100,
    "lang": "en",
    "timeout": 60,
    "min_characters": 200,
    "extract_transcripts": True,
    "extract_comments": False,  # Comments can be rate-limited
    "dedupe_keywords": True,
    "include_phrases": True,
    "max_phrase_length": 3,
}

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def init_logging(log_level: str, log_file: Path):
    """Initialize logging with rich console and file output."""
    log_file.parent.mkdir(parents=True, exist_ok=True)
    handlers = [RichHandler(rich_tracebacks=True, console=console)]
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            "%Y-%m-%d %H:%M:%S",
        )
    )
    handlers.append(file_handler)
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        handlers=handlers,
    )


def slugify(text: str) -> str:
    """Convert text to filesystem-safe slug."""
    return "".join(c if c.isalnum() else "_" for c in text.lower())[:60]


def ensure_yt_dlp():
    """Check if yt-dlp is available."""
    try:
        subprocess.run(["yt-dlp", "--version"], capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        console.print("[bold red]Error: yt-dlp is not installed or not in PATH[/bold red]")
        console.print("Install it with: [bold cyan]pip install yt-dlp[/bold cyan]")
        return False


# --------------------------------------------------------------------------- #
# YouTube Search and Content Extraction
# --------------------------------------------------------------------------- #
class YouTubeExtractor:
    """Extract content from YouTube videos based on search topics."""
    
    def __init__(self, output_dir: Path, timeout: int = 60):
        self.output_dir = output_dir
        self.timeout = timeout
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize NLTK if available
        if NLTK_AVAILABLE:
            self._init_nltk()
    
    def _init_nltk(self):
        """Initialize NLTK components."""
        try:
            self.stop_words = set(stopwords.words('english'))
            self.lemmatizer = WordNetLemmatizer()
        except LookupError:
            # Download required NLTK data
            nltk.download('punkt', quiet=True)
            nltk.download('stopwords', quiet=True)
            nltk.download('wordnet', quiet=True)
            nltk.download('averaged_perceptron_tagger', quiet=True)
            self.stop_words = set(stopwords.words('english'))
            self.lemmatizer = WordNetLemmatizer()
    
    def search_youtube_videos(self, topic: str, num_videos: int) -> List[str]:
        """Search YouTube for videos related to the topic."""
        logger.info(f"Searching YouTube for '{topic}' - expecting {num_videos} videos")
        
        # Use yt-dlp to search YouTube
        search_query = f"ytsearch{num_videos}:{topic}"
        
        command = [
            "yt-dlp",
            "--get-id",
            "--no-download",
            search_query
        ]
        
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=self.timeout,
                check=True
            )
            
            video_ids = [vid_id.strip() for vid_id in result.stdout.strip().split('\n') if vid_id.strip()]
            video_urls = [f"https://www.youtube.com/watch?v={vid_id}" for vid_id in video_ids]
            
            logger.info(f"Found {len(video_urls)} video URLs")
            return video_urls
            
        except subprocess.TimeoutExpired:
            logger.error(f"YouTube search timed out after {self.timeout} seconds")
            return []
        except subprocess.CalledProcessError as e:
            logger.error(f"YouTube search failed: {e.stderr}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error during YouTube search: {e}")
            return []
    
    def extract_video_metadata(self, video_url: str) -> Optional[Dict]:
        """Extract metadata from a single video."""
        command = [
            "yt-dlp",
            "--dump-json",
            "--no-download",
            video_url
        ]
        
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=self.timeout,
                check=True
            )
            
            return json.loads(result.stdout)
            
        except (subprocess.CalledProcessError, json.JSONDecodeError, subprocess.TimeoutExpired) as e:
            logger.warning(f"Failed to extract metadata from {video_url}: {e}")
            return None
    
    def extract_video_subtitles(self, video_url: str) -> Optional[str]:
        """Extract English subtitles from a video."""
        temp_dir = self.output_dir / "temp_subs"
        temp_dir.mkdir(exist_ok=True)
        
        command = [
            "yt-dlp",
            "--write-subs",
            "--write-auto-subs",
            "--sub-langs", "en",
            "--skip-download",
            "--output", str(temp_dir / "%(id)s.%(ext)s"),
            video_url
        ]
        
        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=self.timeout,
                check=True
            )
            
            # Look for subtitle files
            for subtitle_file in temp_dir.glob("*.vtt"):
                try:
                    with open(subtitle_file, 'r', encoding='utf-8') as f:
                        content = f.read()
                        # Clean VTT format
                        lines = content.split('\\n')
                        transcript_lines = []
                        for line in lines:
                            line = line.strip()
                            if (line and 
                                not line.startswith('WEBVTT') and 
                                not line.startswith('NOTE') and
                                not '-->' in line and
                                not line.isdigit()):
                                transcript_lines.append(line)
                        
                        # Clean up temp file
                        subtitle_file.unlink()
                        
                        return ' '.join(transcript_lines)
                except Exception as e:
                    logger.debug(f"Error processing subtitle file {subtitle_file}: {e}")
                    
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
            logger.debug(f"Failed to extract subtitles from {video_url}: {e}")
        
        # Clean up temp directory
        try:
            temp_dir.rmdir()
        except:
            pass
            
        return None
    
    def extract_content_from_videos(self, video_urls: List[str], 
                                  extract_transcripts: bool = True) -> List[Dict]:
        """Extract content from multiple videos with progress tracking."""
        content_data = []
        
        with Progress(
            SpinnerColumn(),
            "{task.description}",
            BarColumn(),
            "{task.completed}/{task.total}",
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task_id = progress.add_task("Extracting video content", total=len(video_urls))
            
            for video_url in video_urls:
                try:
                    # Extract metadata
                    metadata = self.extract_video_metadata(video_url)
                    if not metadata:
                        progress.advance(task_id, 1)
                        continue
                    
                    video_content = {
                        "url": video_url,
                        "id": metadata.get("id"),
                        "title": metadata.get("title", ""),
                        "description": metadata.get("description", ""),
                        "tags": metadata.get("tags", []),
                        "categories": metadata.get("categories", []),
                        "uploader": metadata.get("uploader", ""),
                        "view_count": metadata.get("view_count", 0),
                        "duration": metadata.get("duration", 0),
                        "transcript": ""
                    }
                    
                    # Extract transcript if requested
                    if extract_transcripts:
                        transcript = self.extract_video_subtitles(video_url)
                        if transcript:
                            video_content["transcript"] = transcript
                    
                    content_data.append(video_content)
                    
                except Exception as e:
                    logger.warning(f"Error processing {video_url}: {e}")
                
                progress.advance(task_id, 1)
        
        logger.info(f"Successfully extracted content from {len(content_data)} videos")
        return content_data


# --------------------------------------------------------------------------- #
# Keyword Extraction
# --------------------------------------------------------------------------- #
class KeywordExtractor:
    """Extract keywords from YouTube video content."""
    
    def __init__(self, lang: str = "en", top_n: int = 100):
        self.lang = lang
        self.top_n = top_n
        
        # Initialize YAKE extractor
        self.yake_extractor = yake.KeywordExtractor(
            lan=lang,
            n=3,  # Maximum number of words in keyphrase
            dedupLim=0.7,  # Deduplication threshold
            top=top_n * 2,  # Extract more initially, then filter
            features=None
        )
        
        # Initialize NLTK if available
        if NLTK_AVAILABLE:
            try:
                self.stop_words = set(stopwords.words('english'))
                self.lemmatizer = WordNetLemmatizer()
            except LookupError:
                self.stop_words = set()
                self.lemmatizer = None
        else:
            self.stop_words = set()
            self.lemmatizer = None
    
    def clean_text(self, text: str) -> str:
        """Clean and normalize text for keyword extraction."""
        if not text:
            return ""
        
        # Remove URLs
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
        
        # Remove email addresses
        text = re.sub(r'\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}\\b', '', text)
        
        # Remove excessive whitespace
        text = re.sub(r'\\s+', ' ', text)
        
        # Remove special characters but keep spaces and basic punctuation
        text = re.sub(r'[^a-zA-Z0-9\\s.,!?-]', ' ', text)
        
        return text.strip()
    
    def extract_keywords_from_content(self, content_data: List[Dict], 
                                    include_phrases: bool = True) -> List[Tuple[str, float]]:
        """Extract keywords from video content using multiple methods."""
        logger.info("Extracting keywords from video content")
        
        # Aggregate all text content
        all_text_parts = []
        
        for video in content_data:
            # Collect text from different sources with different weights
            title = self.clean_text(video.get("title", ""))
            if title:
                # Titles are important, so add them multiple times
                all_text_parts.extend([title] * 3)
            
            description = self.clean_text(video.get("description", ""))
            if description and len(description) > 50:  # Only substantial descriptions
                all_text_parts.append(description)
            
            # Tags are already keywords, so treat them specially
            tags = video.get("tags", [])
            if tags:
                tag_text = " ".join([self.clean_text(tag) for tag in tags])
                if tag_text:
                    # Tags are very important for keywords
                    all_text_parts.extend([tag_text] * 2)
            
            transcript = self.clean_text(video.get("transcript", ""))
            if transcript and len(transcript) > 100:  # Only substantial transcripts
                all_text_parts.append(transcript)
        
        if not all_text_parts:
            logger.warning("No text content found for keyword extraction")
            return []
        
        # Combine all text
        combined_text = " ".join(all_text_parts)
        
        # Extract keywords using YAKE
        keywords = self.yake_extractor.extract_keywords(combined_text)
        
        # Clean and filter keywords
        filtered_keywords = []
        seen_keywords = set()
        
        for keyword, score in keywords:
            # Clean the keyword
            clean_keyword = keyword.strip().lower()
            
            # Skip if too short, too long, or already seen
            if (len(clean_keyword) < 3 or 
                len(clean_keyword) > 50 or 
                clean_keyword in seen_keywords):
                continue
            
            # Skip common stop words and meaningless terms
            if clean_keyword in self.stop_words:
                continue
            
            # Skip if it's just numbers or single characters
            if clean_keyword.isdigit() or len(clean_keyword.replace(' ', '')) < 3:
                continue
            
            seen_keywords.add(clean_keyword)
            filtered_keywords.append((clean_keyword, score))
        
        # Sort by score (lower is better in YAKE)
        filtered_keywords.sort(key=lambda x: x[1])
        
        # Return top N keywords
        return filtered_keywords[:self.top_n]
    
    def extract_tag_keywords(self, content_data: List[Dict]) -> List[str]:
        """Extract unique keywords from video tags."""
        all_tags = []
        for video in content_data:
            tags = video.get("tags", [])
            for tag in tags:
                clean_tag = self.clean_text(tag).lower()
                if clean_tag and len(clean_tag) >= 3:
                    all_tags.append(clean_tag)
        
        # Count frequency and return most common
        from collections import Counter
        tag_counter = Counter(all_tags)
        return [tag for tag, count in tag_counter.most_common(50)]


# --------------------------------------------------------------------------- #
# Main Functions
# --------------------------------------------------------------------------- #
def save_keywords_json(keywords: List[Tuple[str, float]], output_path: Path):
    """Save keywords in the same format as serp2text.py."""
    data = [{"keyword": k, "score": s} for k, s in keywords]
    output_path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def save_keywords_array(keywords: List[Tuple[str, float]], output_path: Path):
    """Save keywords as a simple array of strings for easy consumption."""
    keyword_array = [k for k, s in keywords]
    output_path.write_text(json.dumps(keyword_array, indent=2), encoding="utf-8")


def save_content_data(content_data: List[Dict], output_path: Path):
    """Save extracted video content data."""
    output_path.write_text(json.dumps(content_data, indent=2, ensure_ascii=False), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(
        description="YouTube2Text - Topic-based YouTube keyword generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate keywords for "machine learning"
  python youtube2text.py "machine learning" --num-videos 15
  
  # Generate keywords with transcripts disabled (faster)
  python youtube2text.py "cooking recipes" --no-transcripts
  
  # Save to custom directory
  python youtube2text.py "fitness training" --outdir custom_results
  
  # Show top keywords in a table
  python youtube2text.py "python programming" --show
        """
    )
    
    parser.add_argument(
        "topic",
        help="Topic to search for on YouTube"
    )
    
    parser.add_argument(
        "--num-videos",
        type=int,
        default=DEFAULT_CFG["num_videos"],
        help=f"Number of videos to analyze (default: {DEFAULT_CFG['num_videos']})"
    )
    
    parser.add_argument(
        "--top-n",
        type=int,
        default=DEFAULT_CFG["top_n_keywords"],
        help=f"Number of top keywords to extract (default: {DEFAULT_CFG['top_n_keywords']})"
    )
    
    parser.add_argument(
        "--lang",
        default=DEFAULT_CFG["lang"],
        help=f"Language for keyword extraction (default: {DEFAULT_CFG['lang']})"
    )
    
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_CFG["timeout"],
        help=f"Timeout for video processing (default: {DEFAULT_CFG['timeout']}s)"
    )
    
    parser.add_argument(
        "--no-transcripts",
        action="store_true",
        help="Skip transcript extraction (faster but fewer keywords)"
    )
    
    parser.add_argument(
        "--outdir",
        default="results",
        help="Output directory (default: results)"
    )
    
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level"
    )
    
    parser.add_argument(
        "--show",
        action="store_true",
        help="Display keyword table after extraction"
    )
    
    args = parser.parse_args()
    
    # Check dependencies
    if not ensure_yt_dlp():
        sys.exit(1)
    
    # Setup output directory
    topic_slug = slugify(args.topic)
    output_dir = Path(args.outdir) / topic_slug
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Initialize logging
    init_logging(args.log_level, output_dir / "youtube2text.log")
    
    logger.info(f"Starting YouTube2Text for topic: '{args.topic}'")
    logger.info(f"Output directory: {output_dir}")
    
    try:
        # Initialize extractor
        extractor = YouTubeExtractor(output_dir, timeout=args.timeout)
        
        # Search for videos
        video_urls = extractor.search_youtube_videos(args.topic, args.num_videos)
        if not video_urls:
            console.print("[bold red]No videos found for the given topic[/bold red]")
            sys.exit(1)
        
        # Save video URLs
        urls_file = output_dir / "video_urls.txt"
        urls_file.write_text("\\n".join(video_urls), encoding="utf-8")
        logger.info(f"Saved {len(video_urls)} video URLs to {urls_file}")
        
        # Extract content from videos
        extract_transcripts = not args.no_transcripts
        content_data = extractor.extract_content_from_videos(video_urls, extract_transcripts)
        
        if not content_data:
            console.print("[bold red]No content could be extracted from videos[/bold red]")
            sys.exit(1)
        
        # Save content data
        content_file = output_dir / "content_data.json"
        save_content_data(content_data, content_file)
        logger.info(f"Saved content data to {content_file}")
        
        # Extract keywords
        keyword_extractor = KeywordExtractor(lang=args.lang, top_n=args.top_n)
        keywords = keyword_extractor.extract_keywords_from_content(content_data)
        
        if not keywords:
            console.print("[bold red]No keywords could be extracted[/bold red]")
            sys.exit(1)
        
        # Save keywords in multiple formats
        keywords_json = output_dir / "keywords.json"
        save_keywords_json(keywords, keywords_json)
        logger.info(f"Saved keywords to {keywords_json}")
        
        # Save as simple array for easy consumption
        keywords_array = output_dir / "keywords_array.json"
        save_keywords_array(keywords, keywords_array)
        logger.info(f"Saved keyword array to {keywords_array}")
        
        # Extract tag-based keywords
        tag_keywords = keyword_extractor.extract_tag_keywords(content_data)
        if tag_keywords:
            tags_file = output_dir / "tag_keywords.json"
            tags_file.write_text(json.dumps(tag_keywords, indent=2), encoding="utf-8")
            logger.info(f"Saved tag keywords to {tags_file}")
        
        # Display results
        console.print(f"\\n[bold green]✅ Keyword extraction complete![/bold green]")
        console.print(f"📊 Analyzed {len(content_data)} videos")
        console.print(f"🔑 Extracted {len(keywords)} keywords")
        console.print(f"📁 Results saved to: {output_dir}")
        
        if args.show and keywords:
            table = Table(
                title=f"Top keywords for '{args.topic}'",
                show_lines=False
            )
            table.add_column("Rank", justify="right", width=6)
            table.add_column("Keyword", overflow="fold")
            table.add_column("Score", justify="right", width=10)
            
            for i, (keyword, score) in enumerate(keywords[:20], 1):  # Show top 20
                table.add_row(str(i), keyword, f"{score:.4f}")
            
            console.print(table)
        
        # Print file locations for easy consumption
        console.print(f"\\n📋 [bold]Output Files:[/bold]")
        console.print(f"   • Keywords (detailed): {keywords_json}")
        console.print(f"   • Keywords (array): {keywords_array}")
        console.print(f"   • Tag keywords: {output_dir / 'tag_keywords.json'}")
        console.print(f"   • Content data: {content_file}")
        console.print(f"   • Video URLs: {urls_file}")
        
    except KeyboardInterrupt:
        console.print("\\n[bold red]Interrupted by user[/bold red]")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        console.print(f"[bold red]Error: {e}[/bold red]")
        sys.exit(1)


if __name__ == "__main__":
    main()
