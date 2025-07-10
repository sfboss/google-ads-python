#!/usr/bin/env python
# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Modular Google Ads Keyword Research Service

This service provides a convenient CLI interface for retrieving keyword data
in structured JSON format, isolating the functionality for keyword generation
and historical volume metrics.
"""

import argparse
import json
import sys
import re
import os
from collections import Counter
from typing import List, Dict, Any, Optional
from datetime import datetime

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

try:
    import nltk
    from nltk.tokenize import word_tokenize, sent_tokenize
    from nltk.corpus import stopwords
    from nltk.stem import WordNetLemmatizer
    from nltk.tag import pos_tag
    NLTK_AVAILABLE = True
except ImportError:
    NLTK_AVAILABLE = False


class AdWordsKeywordService:
    """Service for Google Ads keyword research operations with JSON output and YouTube content integration."""
    
    def __init__(self, client: GoogleAdsClient):
        """Initialize the service with a Google Ads client.
        
        Args:
            client: An initialized GoogleAdsClient instance.
        """
        self.client = client
        self.keyword_plan_idea_service = client.get_service("KeywordPlanIdeaService")
        self.googleads_service = client.get_service("GoogleAdsService")
        
        # Initialize NLTK components if available
        if NLTK_AVAILABLE:
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
        else:
            self.stop_words = set()
            self.lemmatizer = None
    
    def extract_youtube_content_data(self, youtube_extracts_dir: str = "/workspaces/google-ads-python/youtube_extracts") -> Dict[str, Any]:
        """Extract and aggregate content from YouTube video folders.
        
        Args:
            youtube_extracts_dir: Path to the YouTube extracts directory.
            
        Returns:
            Dictionary containing aggregated YouTube content data.
        """
        aggregated_data = {
            "videos": [],
            "all_titles": [],
            "all_descriptions": [],
            "all_tags": [],
            "all_categories": [],
            "all_transcripts": [],
            "total_views": 0,
            "total_videos": 0
        }
        
        if not os.path.exists(youtube_extracts_dir):
            return aggregated_data
        
        for folder_name in os.listdir(youtube_extracts_dir):
            folder_path = os.path.join(youtube_extracts_dir, folder_name)
            if not os.path.isdir(folder_path):
                continue
                
            video_data = self._extract_single_video_data(folder_path)
            if video_data:
                aggregated_data["videos"].append(video_data)
                
                # Aggregate content
                if video_data.get("title"):
                    aggregated_data["all_titles"].append(video_data["title"])
                if video_data.get("description"):
                    aggregated_data["all_descriptions"].append(video_data["description"])
                if video_data.get("tags"):
                    aggregated_data["all_tags"].extend(video_data["tags"])
                if video_data.get("categories"):
                    aggregated_data["all_categories"].extend(video_data["categories"])
                if video_data.get("transcript"):
                    aggregated_data["all_transcripts"].append(video_data["transcript"])
                if video_data.get("view_count"):
                    aggregated_data["total_views"] += video_data["view_count"]
                
                aggregated_data["total_videos"] += 1
        
        return aggregated_data
    
    def _extract_single_video_data(self, folder_path: str) -> Optional[Dict[str, Any]]:
        """Extract data from a single YouTube video folder.
        
        Args:
            folder_path: Path to the video folder.
            
        Returns:
            Dictionary containing video data or None if no data found.
        """
        video_data = {}
        
        # Extract metadata
        metadata_path = os.path.join(folder_path, "metadata.json")
        if os.path.exists(metadata_path):
            try:
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
                    video_data.update({
                        "id": metadata.get("id"),
                        "title": metadata.get("title"),
                        "description": metadata.get("description"),
                        "tags": metadata.get("tags", []),
                        "categories": metadata.get("categories", []),
                        "view_count": metadata.get("view_count", 0),
                        "duration": metadata.get("duration"),
                        "uploader": metadata.get("uploader"),
                        "upload_date": metadata.get("upload_date")
                    })
            except (json.JSONDecodeError, IOError):
                pass
        
        # Extract transcript from subtitles
        subtitles_dir = os.path.join(folder_path, "subtitles")
        if os.path.exists(subtitles_dir):
            transcript = self._extract_english_transcript(subtitles_dir)
            if transcript:
                video_data["transcript"] = transcript
        
        return video_data if video_data else None
    
    def _extract_english_transcript(self, subtitles_dir: str) -> Optional[str]:
        """Extract English transcript from subtitle files.
        
        Args:
            subtitles_dir: Path to the subtitles directory.
            
        Returns:
            Extracted transcript text or None.
        """
        for filename in os.listdir(subtitles_dir):
            if ".en." in filename and filename.endswith(".vtt"):
                file_path = os.path.join(subtitles_dir, filename)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        # Extract text from VTT format
                        lines = content.split('\n')
                        transcript_lines = []
                        for line in lines:
                            line = line.strip()
                            # Skip VTT headers, timestamps, and empty lines
                            if (line and 
                                not line.startswith('WEBVTT') and 
                                not line.startswith('NOTE') and
                                not '-->' in line and
                                not line.isdigit()):
                                transcript_lines.append(line)
                        return ' '.join(transcript_lines)
                except (IOError, UnicodeDecodeError):
                    continue
        return None
    
    def tokenize_youtube_content(self, youtube_data: Dict[str, Any], min_word_length: int = 3, max_keywords: int = 100) -> Dict[str, Any]:
        """Tokenize YouTube content data and extract potential keywords.
        
        Args:
            youtube_data: Aggregated YouTube content data.
            min_word_length: Minimum length for keywords.
            max_keywords: Maximum number of keywords to return.
            
        Returns:
            Dictionary containing tokenized data and keyword suggestions.
        """
        if not NLTK_AVAILABLE:
            return {
                "error": "NLTK not available. Please install with: pip install nltk",
                "basic_keywords": self._basic_tokenization(youtube_data, min_word_length, max_keywords)
            }
        
        tokenized_data = {
            "title_keywords": [],
            "description_keywords": [],
            "tag_keywords": [],
            "transcript_keywords": [],
            "combined_keywords": [],
            "keyword_phrases": [],
            "content_analysis": {
                "total_videos": youtube_data.get("total_videos", 0),
                "total_views": youtube_data.get("total_views", 0),
                "categories": list(set(youtube_data.get("all_categories", []))),
                "top_tags": [],
                "content_themes": []
            }
        }
        
        # Tokenize titles
        all_titles = ' '.join(youtube_data.get("all_titles", []))
        if all_titles:
            tokenized_data["title_keywords"] = self._extract_keywords_from_text(all_titles, min_word_length)
        
        # Tokenize descriptions  
        all_descriptions = ' '.join(youtube_data.get("all_descriptions", []))
        if all_descriptions:
            tokenized_data["description_keywords"] = self._extract_keywords_from_text(all_descriptions, min_word_length)
        
        # Process tags
        all_tags = youtube_data.get("all_tags", [])
        if all_tags:
            tokenized_data["tag_keywords"] = self._process_tags(all_tags)
            tag_counter = Counter(all_tags)
            tokenized_data["content_analysis"]["top_tags"] = tag_counter.most_common(10)
        
        # Tokenize transcripts
        all_transcripts = ' '.join(youtube_data.get("all_transcripts", []))
        if all_transcripts:
            tokenized_data["transcript_keywords"] = self._extract_keywords_from_text(all_transcripts, min_word_length)
        
        # Combine all keywords
        all_keywords = (
            tokenized_data["title_keywords"] +
            tokenized_data["description_keywords"] +
            tokenized_data["tag_keywords"] +
            tokenized_data["transcript_keywords"]
        )
        
        # Count and rank keywords
        keyword_counter = Counter(all_keywords)
        tokenized_data["combined_keywords"] = [
            {"keyword": keyword, "frequency": count}
            for keyword, count in keyword_counter.most_common(max_keywords)
        ]
        
        # Extract multi-word phrases
        combined_text = f"{all_titles} {all_descriptions} {all_transcripts}"
        tokenized_data["keyword_phrases"] = self._extract_phrases(combined_text, min_word_length)
        
        # Content theme analysis
        tokenized_data["content_analysis"]["content_themes"] = self._identify_content_themes(
            tokenized_data["combined_keywords"][:20]  # Top 20 keywords for themes
        )
        
        return tokenized_data
    
    def _extract_keywords_from_text(self, text: str, min_word_length: int) -> List[str]:
        """Extract keywords from text using NLTK processing.
        
        Args:
            text: Input text to process.
            min_word_length: Minimum word length.
            
        Returns:
            List of extracted keywords.
        """
        if not text or not NLTK_AVAILABLE:
            return []
        
        # Clean and tokenize
        text = re.sub(r'[^\w\s]', ' ', text.lower())
        tokens = word_tokenize(text)
        
        # POS tagging and filtering
        pos_tags = pos_tag(tokens)
        keywords = []
        
        for word, pos in pos_tags:
            # Keep nouns, adjectives, and proper nouns
            if (pos.startswith(('NN', 'JJ', 'VB')) and 
                len(word) >= min_word_length and
                word not in self.stop_words and
                word.isalpha()):
                # Lemmatize the word
                lemmatized = self.lemmatizer.lemmatize(word, pos='n')
                keywords.append(lemmatized)
        
        return keywords
    
    def _process_tags(self, tags: List[str]) -> List[str]:
        """Process YouTube tags into keywords.
        
        Args:
            tags: List of YouTube tags.
            
        Returns:
            List of processed keywords from tags.
        """
        processed_tags = []
        for tag in tags:
            # Clean and split compound tags
            tag = tag.lower().strip()
            # Split on common separators
            tag_parts = re.split(r'[,\-_\s]+', tag)
            for part in tag_parts:
                if len(part) >= 3 and part.isalpha():
                    processed_tags.append(part)
        return processed_tags
    
    def _extract_phrases(self, text: str, min_word_length: int, max_phrases: int = 50) -> List[Dict[str, Any]]:
        """Extract multi-word phrases from text.
        
        Args:
            text: Input text.
            min_word_length: Minimum word length.
            max_phrases: Maximum number of phrases to return.
            
        Returns:
            List of phrase dictionaries with text and frequency.
        """
        if not text or not NLTK_AVAILABLE:
            return []
        
        # Extract 2-3 word phrases
        sentences = sent_tokenize(text.lower())
        phrases = []
        
        for sentence in sentences:
            words = word_tokenize(sentence)
            words = [w for w in words if w.isalpha() and len(w) >= min_word_length and w not in self.stop_words]
            
            # Extract 2-word phrases
            for i in range(len(words) - 1):
                phrase = f"{words[i]} {words[i+1]}"
                phrases.append(phrase)
            
            # Extract 3-word phrases
            for i in range(len(words) - 2):
                phrase = f"{words[i]} {words[i+1]} {words[i+2]}"
                phrases.append(phrase)
        
        # Count and return top phrases
        phrase_counter = Counter(phrases)
        return [
            {"phrase": phrase, "frequency": count}
            for phrase, count in phrase_counter.most_common(max_phrases)
            if count > 1  # Only phrases that appear more than once
        ]
    
    def _identify_content_themes(self, top_keywords: List[Dict[str, Any]]) -> List[str]:
        """Identify content themes from top keywords.
        
        Args:
            top_keywords: List of top keyword dictionaries.
            
        Returns:
            List of identified themes.
        """
        if not top_keywords:
            return []
        
        # Simple theme identification based on keyword patterns
        themes = []
        keywords = [kw["keyword"] for kw in top_keywords]
        
        # Technology themes
        tech_keywords = ["data", "ai", "software", "technology", "digital", "platform", "system"]
        if any(kw in keywords for kw in tech_keywords):
            themes.append("Technology")
        
        # Business themes
        business_keywords = ["business", "sales", "marketing", "customer", "service", "crm"]
        if any(kw in keywords for kw in business_keywords):
            themes.append("Business")
        
        # Entertainment themes
        entertainment_keywords = ["music", "video", "entertainment", "song", "artist"]
        if any(kw in keywords for kw in entertainment_keywords):
            themes.append("Entertainment")
        
        # Education themes
        education_keywords = ["learn", "training", "education", "tutorial", "guide"]
        if any(kw in keywords for kw in education_keywords):
            themes.append("Education")
        
        return themes if themes else ["General"]
    
    def _basic_tokenization(self, youtube_data: Dict[str, Any], min_word_length: int, max_keywords: int) -> List[str]:
        """Basic tokenization without NLTK for fallback.
        
        Args:
            youtube_data: YouTube content data.
            min_word_length: Minimum word length.
            max_keywords: Maximum keywords to return.
            
        Returns:
            List of basic keywords.
        """
        all_text = ' '.join([
            ' '.join(youtube_data.get("all_titles", [])),
            ' '.join(youtube_data.get("all_descriptions", [])),
            ' '.join(youtube_data.get("all_tags", [])),
            ' '.join(youtube_data.get("all_transcripts", []))
        ])
        
        # Basic word extraction
        words = re.findall(r'\b[a-zA-Z]{' + str(min_word_length) + ',}\b', all_text.lower())
        
        # Basic stop words (without NLTK)
        basic_stop_words = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by',
            'is', 'are', 'was', 'were', 'be', 'been', 'have', 'has', 'had', 'do', 'does', 'did',
            'will', 'would', 'could', 'should', 'this', 'that', 'these', 'those', 'you', 'your'
        }
        
        filtered_words = [w for w in words if w not in basic_stop_words]
        word_counter = Counter(filtered_words)
        
        return [word for word, count in word_counter.most_common(max_keywords)]

    def generate_keywords_from_youtube_content(
        self,
        customer_id: str,
        youtube_extracts_dir: str = "/workspaces/google-ads-python/youtube_extracts",
        location_ids: List[str] = ["1023191"],
        language_id: str = "1000",
        include_adult_keywords: bool = False,
        page_size: int = 1000
    ) -> Dict[str, Any]:
        """Generate keyword ideas using extracted YouTube content as seed keywords.
        
        Args:
            customer_id: Google Ads customer ID.
            youtube_extracts_dir: Path to YouTube extracts directory.
            location_ids: Location IDs for targeting.
            language_id: Language criterion ID.
            include_adult_keywords: Whether to include adult keywords.
            page_size: Number of results to return.
            
        Returns:
            Dictionary containing YouTube content analysis and Google Ads keyword suggestions.
        """
        # Extract YouTube content
        youtube_data = self.extract_youtube_content_data(youtube_extracts_dir)
        
        if youtube_data["total_videos"] == 0:
            return {
                "status": "error",
                "timestamp": datetime.now().isoformat(),
                "error": {
                    "message": "No YouTube content found in the specified directory",
                    "directory": youtube_extracts_dir
                }
            }
        
        # Tokenize content
        tokenized_data = self.tokenize_youtube_content(youtube_data)
        
        # Extract top keywords for Google Ads research
        seed_keywords = []
        if tokenized_data.get("combined_keywords"):
            # Use top 20 keywords as seeds
            seed_keywords = [kw["keyword"] for kw in tokenized_data["combined_keywords"][:20]]
        
        # Add top phrases as additional seeds
        if tokenized_data.get("keyword_phrases"):
            seed_keywords.extend([phrase["phrase"] for phrase in tokenized_data["keyword_phrases"][:10]])
        
        if not seed_keywords:
            return {
                "status": "error",
                "timestamp": datetime.now().isoformat(),
                "error": {
                    "message": "No keywords could be extracted from YouTube content"
                }
            }
        
        # Generate keyword ideas using Google Ads
        ads_result = self.generate_keyword_ideas(
            customer_id=customer_id,
            keyword_texts=seed_keywords,
            location_ids=location_ids,
            language_id=language_id,
            include_adult_keywords=include_adult_keywords,
            page_size=page_size
        )
        
        # Combine results
        return {
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "youtube_content_analysis": {
                "source_directory": youtube_extracts_dir,
                "videos_analyzed": youtube_data["total_videos"],
                "total_views": youtube_data["total_views"],
                "content_categories": list(set(youtube_data.get("all_categories", []))),
                "tokenization_results": tokenized_data
            },
            "seed_keywords_used": seed_keywords,
            "google_ads_research": ads_result
        }


def main():
    """CLI interface for the AdWords Keyword Service."""
    parser = argparse.ArgumentParser(
        description="Google Ads Keyword Research Service - JSON Output",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate keyword ideas from seed keywords
  python adwords_service.py keyword-ideas -c 1234567890 -k "python training" "data science"
  
  # Generate keyword ideas from URL
  python adwords_service.py keyword-ideas -c 1234567890 -p "https://example.com/training"
  
  # Get historical metrics for keywords
  python adwords_service.py historical-metrics -c 1234567890 -k "python training" "data science"
  
  # Generate keywords from YouTube content analysis
  python adwords_service.py youtube-keywords -c 1234567890 -d "/path/to/youtube_extracts"
  
  # Pretty print JSON output
  python adwords_service.py keyword-ideas -c 1234567890 -k "python" --pretty
        """
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Keyword Ideas command
    ideas_parser = subparsers.add_parser("keyword-ideas", help="Generate keyword ideas")
    ideas_parser.add_argument("-c", "--customer_id", required=True, help="Google Ads customer ID")
    ideas_parser.add_argument("-k", "--keywords", nargs="+", help="Seed keywords")
    ideas_parser.add_argument("-p", "--page_url", help="URL to generate ideas from")
    ideas_parser.add_argument("-l", "--location_ids", nargs="+", default=["1023191"], 
                             help="Location IDs (default: New York)")
    ideas_parser.add_argument("-i", "--language_id", default="1000", 
                             help="Language ID (default: English)")
    ideas_parser.add_argument("--include_adult", action="store_true", 
                             help="Include adult keywords")
    ideas_parser.add_argument("--page_size", type=int, default=1000, 
                             help="Number of results (default: 1000)")
    
    # Historical Metrics command
    metrics_parser = subparsers.add_parser("historical-metrics", help="Generate historical metrics")
    metrics_parser.add_argument("-c", "--customer_id", required=True, help="Google Ads customer ID")
    metrics_parser.add_argument("-k", "--keywords", nargs="+", required=True, help="Keywords for metrics")
    metrics_parser.add_argument("-l", "--location_ids", nargs="+", default=["2840"], 
                               help="Location IDs (default: USA)")
    metrics_parser.add_argument("-i", "--language_id", default="1000", 
                               help="Language ID (default: English)")
    metrics_parser.add_argument("--include_adult", action="store_true", 
                               help="Include adult keywords")
    metrics_parser.add_argument("-n", "--network", default="GOOGLE_SEARCH",
                               choices=["GOOGLE_SEARCH", "GOOGLE_SEARCH_AND_PARTNERS"],
                               help="Keyword plan network")
    
    # YouTube Content Integration command
    youtube_parser = subparsers.add_parser("youtube-keywords", help="Generate keywords from YouTube content")
    youtube_parser.add_argument("-c", "--customer_id", required=True, help="Google Ads customer ID")
    youtube_parser.add_argument("-d", "--youtube_dir", 
                               default="/workspaces/google-ads-python/youtube_extracts",
                               help="Path to YouTube extracts directory")
    youtube_parser.add_argument("-l", "--location_ids", nargs="+", default=["1023191"], 
                               help="Location IDs (default: New York)")
    youtube_parser.add_argument("-i", "--language_id", default="1000", 
                               help="Language ID (default: English)")
    youtube_parser.add_argument("--include_adult", action="store_true", 
                               help="Include adult keywords")
    youtube_parser.add_argument("--page_size", type=int, default=1000, 
                               help="Number of results (default: 1000)")
    
    # Global options
    parser.add_argument("--pretty", action="store_true", help="Pretty print JSON output")
    parser.add_argument("--config", default="/workspaces/google-ads-python/google-ads.yaml",
                       help="Path to Google Ads configuration file")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    try:
        # Initialize client
        client = GoogleAdsClient.load_from_storage(args.config, version="v19")
        service = AdWordsKeywordService(client)
        
        result = None
        
        if args.command == "keyword-ideas":
            result = service.generate_keyword_ideas(
                customer_id=args.customer_id,
                keyword_texts=args.keywords,
                page_url=args.page_url,
                location_ids=args.location_ids,
                language_id=args.language_id,
                include_adult_keywords=args.include_adult,
                page_size=args.page_size
            )
        
        elif args.command == "historical-metrics":
            result = service.generate_historical_metrics(
                customer_id=args.customer_id,
                keywords=args.keywords,
                location_ids=args.location_ids,
                language_id=args.language_id,
                include_adult_keywords=args.include_adult,
                keyword_plan_network=args.network
            )
        
        elif args.command == "youtube-keywords":
            result = service.generate_keywords_from_youtube_content(
                customer_id=args.customer_id,
                youtube_extracts_dir=args.youtube_dir,
                location_ids=args.location_ids,
                language_id=args.language_id,
                include_adult_keywords=args.include_adult,
                page_size=args.page_size
            )
        
        # Output JSON
        if args.pretty:
            print(json.dumps(result, indent=2, ensure_ascii=False))
        else:
            print(json.dumps(result, ensure_ascii=False))
            
        # Exit with error code if request failed
        if result.get("status") == "error":
            sys.exit(1)
    
    except Exception as ex:
        error_result = {
            "status": "error",
            "timestamp": datetime.now().isoformat(),
            "error": {
                "message": str(ex),
                "type": type(ex).__name__
            }
        }
        
        if args.pretty:
            print(json.dumps(error_result, indent=2, ensure_ascii=False))
        else:
            print(json.dumps(error_result, ensure_ascii=False))
        
        sys.exit(1)


if __name__ == "__main__":
    main()
