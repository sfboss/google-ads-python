#!/usr/bin/env python
"""
YouTube Content Tokenizer and Keyword Extractor

This standalone script extracts content from YouTube video folders and tokenizes it
for keyword research purposes. It processes metadata, transcripts, titles, descriptions,
tags, and categories to generate meaningful keywords and phrases.
"""

import argparse
import json
import os
import re
from collections import Counter
from typing import List, Dict, Any, Optional
from datetime import datetime

try:
    import nltk
    from nltk.tokenize import word_tokenize, sent_tokenize
    from nltk.corpus import stopwords
    from nltk.stem import WordNetLemmatizer
    from nltk.tag import pos_tag
    NLTK_AVAILABLE = True
except ImportError:
    NLTK_AVAILABLE = False


class YouTubeContentTokenizer:
    """Tokenizes YouTube content for keyword extraction and analysis."""
    
    def __init__(self):
        """Initialize the tokenizer with NLTK components if available."""
        if NLTK_AVAILABLE:
            self._ensure_nltk_data()
            try:
                self.stop_words = set(stopwords.words('english'))
                self.lemmatizer = WordNetLemmatizer()
            except Exception as e:
                print(f"⚠️  NLTK initialization failed: {e}")
                print("   Falling back to basic tokenization")
                self.stop_words = set()
                self.lemmatizer = None
        else:
            self.stop_words = set()
            self.lemmatizer = None
            print("⚠️  NLTK not available. Using basic tokenization.")
    
    def _ensure_nltk_data(self):
        """Ensure required NLTK data is downloaded."""
        required_data = ['punkt', 'stopwords', 'wordnet', 'averaged_perceptron_tagger']
        
        for dataset in required_data:
            try:
                # Try to access the data to see if it exists
                if dataset == 'punkt':
                    nltk.data.find('tokenizers/punkt')
                elif dataset == 'stopwords':
                    nltk.data.find('corpora/stopwords')
                elif dataset == 'wordnet':
                    nltk.data.find('corpora/wordnet')
                elif dataset == 'averaged_perceptron_tagger':
                    nltk.data.find('taggers/averaged_perceptron_tagger')
            except LookupError:
                print(f"📥 Downloading NLTK data: {dataset}")
                try:
                    nltk.download(dataset, quiet=True)
                except Exception as e:
                    print(f"❌ Failed to download {dataset}: {e}")
    
    def extract_youtube_content_data(self, youtube_extracts_dir: str) -> Dict[str, Any]:
        """Extract and aggregate content from YouTube video folders.
        
        Args:
            youtube_extracts_dir: Path to the YouTube extracts directory.
            
        Returns:
            Dictionary containing aggregated YouTube content data.
        """
        print(f"🔍 Scanning YouTube extracts directory: {youtube_extracts_dir}")
        
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
            print(f"❌ Directory not found: {youtube_extracts_dir}")
            return aggregated_data
        
        folders = [f for f in os.listdir(youtube_extracts_dir) 
                  if os.path.isdir(os.path.join(youtube_extracts_dir, f))]
        
        print(f"📁 Found {len(folders)} folders to analyze")
        
        for folder_name in folders:
            folder_path = os.path.join(youtube_extracts_dir, folder_name)
            print(f"\n🎥 Processing: {folder_name}")
            
            video_data = self._extract_single_video_data(folder_path)
            if video_data:
                aggregated_data["videos"].append(video_data)
                
                # Aggregate content
                if video_data.get("title"):
                    aggregated_data["all_titles"].append(video_data["title"])
                    print(f"   ✅ Title: {video_data['title'][:50]}...")
                
                if video_data.get("description"):
                    aggregated_data["all_descriptions"].append(video_data["description"])
                    print(f"   ✅ Description: {len(video_data['description'])} characters")
                
                if video_data.get("tags"):
                    aggregated_data["all_tags"].extend(video_data["tags"])
                    print(f"   ✅ Tags: {len(video_data['tags'])} tags")
                
                if video_data.get("categories"):
                    aggregated_data["all_categories"].extend(video_data["categories"])
                    print(f"   ✅ Categories: {video_data['categories']}")
                
                if video_data.get("transcript"):
                    aggregated_data["all_transcripts"].append(video_data["transcript"])
                    print(f"   ✅ Transcript: {len(video_data['transcript'])} characters")
                
                if video_data.get("view_count"):
                    aggregated_data["total_views"] += video_data["view_count"]
                    print(f"   ✅ Views: {video_data['view_count']:,}")
                
                aggregated_data["total_videos"] += 1
            else:
                print(f"   ❌ No extractable content found")
        
        print(f"\n🎉 Successfully processed {aggregated_data['total_videos']} videos")
        print(f"📊 Total views across all videos: {aggregated_data['total_views']:,}")
        
        return aggregated_data
    
    def _extract_single_video_data(self, folder_path: str) -> Optional[Dict[str, Any]]:
        """Extract data from a single YouTube video folder."""
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
        """Extract English transcript from subtitle files."""
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
    
    def tokenize_youtube_content(self, youtube_data: Dict[str, Any], 
                               min_word_length: int = 3, 
                               max_keywords: int = 100) -> Dict[str, Any]:
        """Tokenize YouTube content data and extract potential keywords."""
        print("\n🧠 Starting content tokenization and analysis...")
        
        if not NLTK_AVAILABLE:
            print("⚠️  Using basic tokenization (NLTK not available)")
            return {
                "error": "NLTK not available. Install with: pip install nltk",
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
        print("📝 Tokenizing video titles...")
        all_titles = ' '.join(youtube_data.get("all_titles", []))
        if all_titles:
            tokenized_data["title_keywords"] = self._extract_keywords_from_text(all_titles, min_word_length)
            print(f"   ✅ Extracted {len(tokenized_data['title_keywords'])} title keywords")
        
        # Tokenize descriptions  
        print("📄 Tokenizing video descriptions...")
        all_descriptions = ' '.join(youtube_data.get("all_descriptions", []))
        if all_descriptions:
            tokenized_data["description_keywords"] = self._extract_keywords_from_text(all_descriptions, min_word_length)
            print(f"   ✅ Extracted {len(tokenized_data['description_keywords'])} description keywords")
        
        # Process tags
        print("🏷️  Processing video tags...")
        all_tags = youtube_data.get("all_tags", [])
        if all_tags:
            tokenized_data["tag_keywords"] = self._process_tags(all_tags)
            tag_counter = Counter(all_tags)
            tokenized_data["content_analysis"]["top_tags"] = tag_counter.most_common(10)
            print(f"   ✅ Processed {len(all_tags)} total tags into {len(tokenized_data['tag_keywords'])} keywords")
        
        # Tokenize transcripts
        print("🎤 Tokenizing video transcripts...")
        all_transcripts = ' '.join(youtube_data.get("all_transcripts", []))
        if all_transcripts:
            tokenized_data["transcript_keywords"] = self._extract_keywords_from_text(all_transcripts, min_word_length)
            print(f"   ✅ Extracted {len(tokenized_data['transcript_keywords'])} transcript keywords")
        
        # Combine all keywords
        print("🔗 Combining and ranking all keywords...")
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
        print(f"   ✅ Generated {len(tokenized_data['combined_keywords'])} ranked keywords")
        
        # Extract multi-word phrases
        print("📖 Extracting multi-word phrases...")
        combined_text = f"{all_titles} {all_descriptions} {all_transcripts}"
        tokenized_data["keyword_phrases"] = self._extract_phrases(combined_text, min_word_length)
        print(f"   ✅ Extracted {len(tokenized_data['keyword_phrases'])} phrases")
        
        # Content theme analysis
        print("🎯 Identifying content themes...")
        tokenized_data["content_analysis"]["content_themes"] = self._identify_content_themes(
            tokenized_data["combined_keywords"][:20]  # Top 20 keywords for themes
        )
        print(f"   ✅ Identified themes: {', '.join(tokenized_data['content_analysis']['content_themes'])}")
        
        return tokenized_data
    
    def _extract_keywords_from_text(self, text: str, min_word_length: int) -> List[str]:
        """Extract keywords from text using NLTK processing."""
        if not text:
            return []
        
        if not NLTK_AVAILABLE or not self.lemmatizer:
            # Fall back to basic extraction
            return self._basic_keyword_extraction(text, min_word_length)
        
        try:
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
        except Exception as e:
            print(f"⚠️  NLTK processing failed: {e}, using basic extraction")
            return self._basic_keyword_extraction(text, min_word_length)
    
    def _basic_keyword_extraction(self, text: str, min_word_length: int) -> List[str]:
        """Basic keyword extraction without NLTK."""
        # Basic word extraction
        words = re.findall(r'\b[a-zA-Z]{' + str(min_word_length) + ',}\b', text.lower())
        
        # Basic stop words
        basic_stop_words = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by',
            'is', 'are', 'was', 'were', 'be', 'been', 'have', 'has', 'had', 'do', 'does', 'did',
            'will', 'would', 'could', 'should', 'this', 'that', 'these', 'those', 'you', 'your'
        }
        
        return [w for w in words if w not in basic_stop_words]
    
    def _process_tags(self, tags: List[str]) -> List[str]:
        """Process YouTube tags into keywords."""
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
        """Extract multi-word phrases from text."""
        if not text:
            return []
        
        if not NLTK_AVAILABLE or not self.lemmatizer:
            return self._basic_phrase_extraction(text, min_word_length, max_phrases)
        
        try:
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
        except Exception as e:
            print(f"⚠️  NLTK phrase extraction failed: {e}, using basic extraction")
            return self._basic_phrase_extraction(text, min_word_length, max_phrases)
    
    def _basic_phrase_extraction(self, text: str, min_word_length: int, max_phrases: int) -> List[Dict[str, Any]]:
        """Basic phrase extraction without NLTK."""
        # Split into sentences using simple punctuation
        sentences = re.split(r'[.!?]+', text.lower())
        phrases = []
        
        basic_stop_words = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by',
            'is', 'are', 'was', 'were', 'be', 'been', 'have', 'has', 'had', 'do', 'does', 'did'
        }
        
        for sentence in sentences:
            words = re.findall(r'\b[a-zA-Z]{' + str(min_word_length) + ',}\b', sentence)
            words = [w for w in words if w not in basic_stop_words]
            
            # Extract 2-word phrases
            for i in range(len(words) - 1):
                phrase = f"{words[i]} {words[i+1]}"
                phrases.append(phrase)
        
        # Count and return top phrases
        phrase_counter = Counter(phrases)
        return [
            {"phrase": phrase, "frequency": count}
            for phrase, count in phrase_counter.most_common(max_phrases)
            if count > 1
        ]
    
    def _identify_content_themes(self, top_keywords: List[Dict[str, Any]]) -> List[str]:
        """Identify content themes from top keywords."""
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
        """Basic tokenization without NLTK for fallback."""
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
    
    def analyze_and_save_results(self, youtube_extracts_dir: str, output_file: str = None) -> Dict[str, Any]:
        """Complete analysis pipeline that extracts, tokenizes, and saves results."""
        print("🚀 Starting YouTube Content Tokenization Pipeline")
        print("=" * 60)
        
        # Extract content
        youtube_data = self.extract_youtube_content_data(youtube_extracts_dir)
        
        if youtube_data["total_videos"] == 0:
            print("❌ No YouTube content found for analysis")
            return {
                "status": "error",
                "message": "No YouTube content found in the specified directory",
                "directory": youtube_extracts_dir
            }
        
        # Tokenize content
        tokenized_data = self.tokenize_youtube_content(youtube_data)
        
        # Prepare complete results
        results = {
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "analysis_summary": {
                "source_directory": youtube_extracts_dir,
                "videos_analyzed": youtube_data["total_videos"],
                "total_views": youtube_data["total_views"],
                "content_categories": list(set(youtube_data.get("all_categories", []))),
                "content_themes": tokenized_data.get("content_analysis", {}).get("content_themes", [])
            },
            "youtube_content_data": youtube_data,
            "tokenization_results": tokenized_data
        }
        
        # Save results if output file specified
        if output_file:
            try:
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(results, f, indent=2, ensure_ascii=False)
                print(f"\n💾 Results saved to: {output_file}")
            except IOError as e:
                print(f"❌ Error saving results: {e}")
        
        return results
    
    def print_summary(self, results: Dict[str, Any]):
        """Print a formatted summary of the tokenization results."""
        if results.get("status") != "success":
            print(f"❌ Analysis failed: {results.get('message', 'Unknown error')}")
            return
        
        summary = results.get("analysis_summary", {})
        tokenization = results.get("tokenization_results", {})
        
        print("\n" + "=" * 60)
        print("📊 YOUTUBE CONTENT TOKENIZATION SUMMARY")
        print("=" * 60)
        
        print(f"📁 Source Directory: {summary.get('source_directory')}")
        print(f"🎥 Videos Analyzed: {summary.get('videos_analyzed')}")
        print(f"👀 Total Views: {summary.get('total_views'):,}")
        print(f"🎯 Content Themes: {', '.join(summary.get('content_themes', []))}")
        print(f"📂 Categories: {', '.join(summary.get('content_categories', []))}")
        
        # Top keywords
        combined_keywords = tokenization.get("combined_keywords", [])
        if combined_keywords:
            print(f"\n🔑 TOP 10 KEYWORDS:")
            for i, kw in enumerate(combined_keywords[:10], 1):
                print(f"   {i:2d}. {kw['keyword']} (appears {kw['frequency']} times)")
        
        # Top phrases
        phrases = tokenization.get("keyword_phrases", [])
        if phrases:
            print(f"\n📖 TOP 5 PHRASES:")
            for i, phrase in enumerate(phrases[:5], 1):
                print(f"   {i}. \"{phrase['phrase']}\" (appears {phrase['frequency']} times)")
        
        # Content analysis
        content_analysis = tokenization.get("content_analysis", {})
        top_tags = content_analysis.get("top_tags", [])
        if top_tags:
            print(f"\n🏷️  TOP 5 TAGS:")
            for i, (tag, count) in enumerate(top_tags[:5], 1):
                print(f"   {i}. {tag} (used {count} times)")
        
        print("\n" + "=" * 60)


def main():
    """CLI interface for the YouTube Content Tokenizer."""
    parser = argparse.ArgumentParser(
        description="YouTube Content Tokenizer - Extract and tokenize YouTube video content",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze YouTube content and display summary
  python youtube_content_tokenizer.py /path/to/youtube_extracts
  
  # Analyze and save detailed results to JSON
  python youtube_content_tokenizer.py /path/to/youtube_extracts --output results.json
  
  # Use current directory's youtube_extracts folder
  python youtube_content_tokenizer.py
  
  # Adjust keyword parameters
  python youtube_content_tokenizer.py --min-word-length 4 --max-keywords 200
        """
    )
    
    parser.add_argument("youtube_dir", nargs="?", 
                       default="/workspaces/google-ads-python/youtube_extracts",
                       help="Path to YouTube extracts directory (default: ./youtube_extracts)")
    parser.add_argument("-o", "--output", help="Output JSON file for detailed results")
    parser.add_argument("--min-word-length", type=int, default=3,
                       help="Minimum word length for keywords (default: 3)")
    parser.add_argument("--max-keywords", type=int, default=100,
                       help="Maximum number of keywords to extract (default: 100)")
    parser.add_argument("--quiet", action="store_true",
                       help="Suppress detailed progress output")
    
    args = parser.parse_args()
    
    # Initialize tokenizer
    tokenizer = YouTubeContentTokenizer()
    
    # Run analysis
    try:
        results = tokenizer.analyze_and_save_results(
            youtube_extracts_dir=args.youtube_dir,
            output_file=args.output
        )
        
        if not args.quiet:
            tokenizer.print_summary(results)
        
        # Exit with error code if analysis failed
        if results.get("status") == "error":
            exit(1)
            
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        exit(1)


if __name__ == "__main__":
    main()
