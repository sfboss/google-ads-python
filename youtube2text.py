#!/usr/bin/env python3
"""
YouTube2Text - Complete Video Data Extractor
Extracts all available metadata, transcripts, comments, and content from YouTube videos
"""

import os
import sys
import json
import subprocess
import argparse
import re
from pathlib import Path
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class YouTube2Text:
    def __init__(self, output_dir="youtube_extracts"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
    def sanitize_filename(self, filename):
        """Sanitize filename for filesystem compatibility"""
        # Remove or replace problematic characters
        sanitized = re.sub(r'[<>:"/\\|?*]', '_', filename)
        # Limit length
        if len(sanitized) > 100:
            sanitized = sanitized[:100]
        return sanitized
    
    def get_video_id(self, url):
        """Extract video ID from YouTube URL"""
        patterns = [
            r'(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/embed/)([a-zA-Z0-9_-]{11})',
            r'youtube\.com/watch\?.*v=([a-zA-Z0-9_-]{11})'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        
        raise ValueError(f"Could not extract video ID from URL: {url}")
    
    def run_yt_dlp_command(self, command, description=""):
        """Run yt-dlp command with error handling"""
        try:
            logger.info(f"Running: {description}")
            logger.debug(f"Command: {' '.join(command)}")
            
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=True
            )
            
            if result.stdout:
                logger.debug(f"stdout: {result.stdout}")
            if result.stderr:
                logger.debug(f"stderr: {result.stderr}")
                
            return result
        except subprocess.CalledProcessError as e:
            logger.error(f"Error running {description}: {e}")
            logger.error(f"stderr: {e.stderr}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error running {description}: {e}")
            return None
    
    def extract_metadata(self, url, video_folder):
        """Extract comprehensive metadata using yt-dlp"""
        metadata_file = video_folder / "metadata.json"
        
        command = [
            "yt-dlp",
            "--dump-json",
            "--no-download",
            "--write-info-json",
            "--write-description",
            "--write-thumbnail",
            "--write-all-thumbnails",
            "--output", str(video_folder / "%(title)s.%(ext)s"),
            "--cookies", "/workspaces/google-ads-python/cookies.txt",
            url
        ]
        
        result = self.run_yt_dlp_command(command, "Extracting metadata")
        
        if result and result.stdout:
            try:
                metadata = json.loads(result.stdout)
                with open(metadata_file, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, indent=2, ensure_ascii=False)
                logger.info(f"Metadata saved to: {metadata_file}")
                return metadata
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing metadata JSON: {e}")
                return None
        return None
    
    def extract_subtitles(self, url, video_folder):
        """Extract all available subtitles"""
        subtitles_folder = video_folder / "subtitles"
        subtitles_folder.mkdir(exist_ok=True)
        
        command = [
            "yt-dlp",
            "--write-subs",
            "--write-auto-subs",
            "--sub-langs", "all",
            "--skip-download",
            "--output", str(subtitles_folder / "%(title)s.%(ext)s"),
            "--cookies", "/workspaces/google-ads-python/cookies.txt",
            url
        ]
        
        result = self.run_yt_dlp_command(command, "Extracting subtitles")
        
        if result:
            logger.info(f"Subtitles extracted to: {subtitles_folder}")
            return True
        return False
    
    def extract_comments(self, url, video_folder):
        """Extract comments using yt-dlp"""
        comments_file = video_folder / "comments.json"
        
        command = [
            "yt-dlp",
            "--write-comments",
            "--skip-download",
            "--output", str(video_folder / "%(title)s.%(ext)s"),
            "--cookies", "/workspaces/google-ads-python/cookies.txt",
            url
        ]
        
        result = self.run_yt_dlp_command(command, "Extracting comments")
        
        if result:
            # Look for the generated comments file
            for file in video_folder.glob("*.info.json"):
                try:
                    with open(file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        if 'comments' in data:
                            with open(comments_file, 'w', encoding='utf-8') as cf:
                                json.dump(data['comments'], cf, indent=2, ensure_ascii=False)
                            logger.info(f"Comments saved to: {comments_file}")
                            return True
                except Exception as e:
                    logger.error(f"Error processing comments from {file}: {e}")
        
        return False
    
    def extract_thumbnails(self, url, video_folder):
        """Extract all available thumbnails"""
        thumbnails_folder = video_folder / "thumbnails"
        thumbnails_folder.mkdir(exist_ok=True)
        
        command = [
            "yt-dlp",
            "--write-thumbnail",
            "--write-all-thumbnails",
            "--skip-download",
            "--output", str(thumbnails_folder / "%(title)s.%(ext)s"),
            "--cookies", "/workspaces/google-ads-python/cookies.txt",
            url
        ]
        
        result = self.run_yt_dlp_command(command, "Extracting thumbnails")
        
        if result:
            logger.info(f"Thumbnails extracted to: {thumbnails_folder}")
            return True
        return False
    
    def create_corpus_summary(self, video_folder, metadata):
        """Create a comprehensive corpus summary"""
        corpus_file = video_folder / "corpus_summary.txt"
        
        try:
            with open(corpus_file, 'w', encoding='utf-8') as f:
                f.write("=" * 80 + "\n")
                f.write("YOUTUBE VIDEO CORPUS SUMMARY\n")
                f.write("=" * 80 + "\n\n")
                
                # Basic video info
                f.write(f"Title: {metadata.get('title', 'N/A')}\n")
                f.write(f"Channel: {metadata.get('channel', 'N/A')}\n")
                f.write(f"Upload Date: {metadata.get('upload_date', 'N/A')}\n")
                f.write(f"Duration: {metadata.get('duration_string', 'N/A')}\n")
                f.write(f"View Count: {metadata.get('view_count', 'N/A')}\n")
                f.write(f"Like Count: {metadata.get('like_count', 'N/A')}\n")
                f.write(f"Comment Count: {metadata.get('comment_count', 'N/A')}\n")
                f.write(f"Video ID: {metadata.get('id', 'N/A')}\n")
                f.write(f"URL: {metadata.get('webpage_url', 'N/A')}\n\n")
                
                # Description
                if metadata.get('description'):
                    f.write("-" * 40 + "\n")
                    f.write("DESCRIPTION\n")
                    f.write("-" * 40 + "\n")
                    f.write(metadata['description'] + "\n\n")
                
                # Tags
                if metadata.get('tags'):
                    f.write("-" * 40 + "\n")
                    f.write("TAGS\n")
                    f.write("-" * 40 + "\n")
                    f.write(", ".join(metadata['tags']) + "\n\n")
                
                # Categories
                if metadata.get('categories'):
                    f.write("-" * 40 + "\n")
                    f.write("CATEGORIES\n")
                    f.write("-" * 40 + "\n")
                    f.write(", ".join(metadata['categories']) + "\n\n")
                
                # Available files summary
                f.write("-" * 40 + "\n")
                f.write("EXTRACTED FILES\n")
                f.write("-" * 40 + "\n")
                
                for item in video_folder.rglob("*"):
                    if item.is_file():
                        relative_path = item.relative_to(video_folder)
                        f.write(f"- {relative_path}\n")
                
                f.write("\n" + "=" * 80 + "\n")
                f.write("END OF CORPUS SUMMARY\n")
                f.write("=" * 80 + "\n")
                
            logger.info(f"Corpus summary created: {corpus_file}")
            
        except Exception as e:
            logger.error(f"Error creating corpus summary: {e}")
    
    def process_video(self, url):
        """Main processing function"""
        try:
            # Get video ID and create folder
            video_id = self.get_video_id(url)
            logger.info(f"Processing video ID: {video_id}")
            
            # Create timestamped folder name
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            folder_name = f"{video_id}_{timestamp}"
            video_folder = self.output_dir / folder_name
            video_folder.mkdir(exist_ok=True)
            
            logger.info(f"Created output folder: {video_folder}")
            
            # Extract metadata first (needed for other operations)
            logger.info("Step 1: Extracting metadata...")
            metadata = self.extract_metadata(url, video_folder)
            
            if not metadata:
                logger.error("Failed to extract metadata. Aborting.")
                return False
            
            # Extract subtitles
            logger.info("Step 2: Extracting subtitles...")
            self.extract_subtitles(url, video_folder)
            
            # Extract comments
            logger.info("Step 3: Extracting comments...")
            self.extract_comments(url, video_folder)
            
            # Extract thumbnails
            logger.info("Step 4: Extracting thumbnails...")
            self.extract_thumbnails(url, video_folder)
            
            # Create corpus summary
            logger.info("Step 5: Creating corpus summary...")
            self.create_corpus_summary(video_folder, metadata)
            
            logger.info(f"Processing complete! All data saved to: {video_folder}")
            
            # Print summary
            print(f"\n{'='*60}")
            print(f"EXTRACTION COMPLETE")
            print(f"{'='*60}")
            print(f"Video: {metadata.get('title', 'Unknown')}")
            print(f"Channel: {metadata.get('channel', 'Unknown')}")
            print(f"Output folder: {video_folder}")
            print(f"{'='*60}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing video: {e}")
            return False

def main():
    parser = argparse.ArgumentParser(
        description="YouTube2Text - Extract all data from YouTube videos",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python youtube2text.py https://www.youtube.com/watch?v=dQw4w9WgXcQ
  python youtube2text.py https://youtu.be/dQw4w9WgXcQ --output /path/to/output
  python youtube2text.py https://www.youtube.com/watch?v=dQw4w9WgXcQ --verbose
        """
    )
    
    parser.add_argument(
        "url",
        help="YouTube video URL"
    )
    
    parser.add_argument(
        "-o", "--output",
        default="youtube_extracts",
        help="Output directory (default: youtube_extracts)"
    )
    
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Check if yt-dlp is installed
    try:
        subprocess.run(["yt-dlp", "--version"], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("Error: yt-dlp is not installed or not in PATH")
        print("Install it with: pip install yt-dlp")
        sys.exit(1)
    
    # Create extractor and process video
    extractor = YouTube2Text(args.output)
    success = extractor.process_video(args.url)
    
    if success:
        print("\n✅ Extraction completed successfully!")
        print("You can now analyze the extracted corpus for keywords and insights.")
    else:
        print("\n❌ Extraction failed. Check the logs for details.")
        sys.exit(1)

if __name__ == "__main__":
    main()