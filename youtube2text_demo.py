#!/usr/bin/env python3
"""
YouTube2Text Demo - Generate keywords from existing YouTube extracts

This demonstrates how youtube2text works by processing the existing extracted
YouTube content and generating keyword arrays for consumption by other tools.
"""

import json
import re
from collections import Counter
from pathlib import Path
from typing import List, Dict, Tuple

try:
    import yake
    YAKE_AVAILABLE = True
except ImportError:
    YAKE_AVAILABLE = False

try:
    import nltk
    from nltk.tokenize import word_tokenize
    from nltk.corpus import stopwords
    NLTK_AVAILABLE = True
except ImportError:
    NLTK_AVAILABLE = False


class YouTubeKeywordDemo:
    """Demonstrate keyword extraction from existing YouTube data."""
    
    def __init__(self):
        self.stop_words = self._get_stop_words()
        if YAKE_AVAILABLE:
            self.yake_extractor = yake.KeywordExtractor(
                lan="en",
                n=3,
                dedupLim=0.7,
                top=200,
                features=None
            )
    
    def _get_stop_words(self):
        """Get stop words for filtering."""
        if NLTK_AVAILABLE:
            try:
                return set(stopwords.words('english'))
            except LookupError:
                nltk.download('stopwords', quiet=True)
                return set(stopwords.words('english'))
        else:
            # Basic stop words if NLTK not available
            return {
                'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by',
                'is', 'are', 'was', 'were', 'be', 'been', 'have', 'has', 'had', 'do', 'does', 'did',
                'will', 'would', 'could', 'should', 'this', 'that', 'these', 'those', 'you', 'your',
                'we', 'us', 'our', 'they', 'them', 'their', 'i', 'me', 'my', 'he', 'him', 'his', 'she',
                'her', 'it', 'its', 'can', 'may', 'might', 'must', 'shall', 'should', 'would', 'could'
            }
    
    def clean_text(self, text: str) -> str:
        """Clean text for keyword extraction."""
        if not text:
            return ""
        
        # Remove URLs
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
        
        # Remove excessive whitespace and special characters
        text = re.sub(r'[^a-zA-Z0-9\\s]', ' ', text)
        text = re.sub(r'\\s+', ' ', text)
        
        return text.strip()
    
    def extract_from_existing_data(self, extracts_dir: str = "/workspaces/google-ads-python/youtube_extracts") -> Dict:
        """Extract keywords from existing YouTube extracts data."""
        print("🎯 YOUTUBE2TEXT KEYWORD DEMO")
        print("=" * 50)
        print(f"📁 Processing: {extracts_dir}")
        
        # Load existing data from the tokenization results
        results_file = Path("/workspaces/google-ads-python/youtube_tokenization_results.json")
        
        if results_file.exists():
            print("✅ Using existing tokenization results")
            with open(results_file, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
            
            # Extract the content for demonstration
            youtube_data = existing_data.get("youtube_content_data", {})
            videos = youtube_data.get("videos", [])
            
            print(f"📊 Found {len(videos)} videos with content")
            
            # Process the videos to create keyword arrays
            content_analysis = self._analyze_video_content(videos)
            
        else:
            print("❌ No existing tokenization results found")
            print("   Run youtube_content_tokenizer.py first to generate data")
            return {}
        
        return content_analysis
    
    def _analyze_video_content(self, videos: List[Dict]) -> Dict:
        """Analyze video content and generate keyword arrays."""
        print("\\n🔍 ANALYZING VIDEO CONTENT FOR KEYWORDS")
        print("-" * 40)
        
        # Collect all text content
        all_titles = []
        all_descriptions = []
        all_tags = []
        all_transcripts = []
        
        for video in videos:
            if video.get("title"):
                all_titles.append(self.clean_text(video["title"]))
                print(f"📝 Title: {video['title'][:60]}...")
            
            if video.get("description"):
                all_descriptions.append(self.clean_text(video["description"]))
                print(f"📄 Description: {len(video['description'])} chars")
            
            if video.get("tags"):
                all_tags.extend([self.clean_text(tag) for tag in video["tags"]])
                print(f"🏷️  Tags: {len(video['tags'])} tags")
            
            if video.get("transcript"):
                all_transcripts.append(self.clean_text(video["transcript"]))
                print(f"🎤 Transcript: {len(video['transcript'])} chars")
        
        # Generate different types of keyword arrays
        keyword_arrays = {}
        
        # 1. Title-based keywords
        print("\\n🔑 EXTRACTING TITLE KEYWORDS...")
        title_keywords = self._extract_keywords_from_text_list(all_titles, max_keywords=20)
        keyword_arrays["title_keywords"] = title_keywords
        print(f"   ✅ {len(title_keywords)} title keywords")
        
        # 2. Tag-based keywords (already clean keywords)
        print("🏷️  PROCESSING TAG KEYWORDS...")
        tag_keywords = self._process_tags(all_tags)
        keyword_arrays["tag_keywords"] = tag_keywords
        print(f"   ✅ {len(tag_keywords)} tag keywords")
        
        # 3. Description keywords
        print("📄 EXTRACTING DESCRIPTION KEYWORDS...")
        desc_keywords = self._extract_keywords_from_text_list(all_descriptions, max_keywords=30)
        keyword_arrays["description_keywords"] = desc_keywords
        print(f"   ✅ {len(desc_keywords)} description keywords")
        
        # 4. Transcript keywords
        print("🎤 EXTRACTING TRANSCRIPT KEYWORDS...")
        transcript_keywords = self._extract_keywords_from_text_list(all_transcripts, max_keywords=50)
        keyword_arrays["transcript_keywords"] = transcript_keywords
        print(f"   ✅ {len(transcript_keywords)} transcript keywords")
        
        # 5. Combined keywords (most important)
        print("🎯 GENERATING COMBINED KEYWORD ARRAY...")
        combined_keywords = self._create_combined_keywords(keyword_arrays, max_keywords=100)
        keyword_arrays["combined_keywords"] = combined_keywords
        print(f"   ✅ {len(combined_keywords)} combined keywords")
        
        # 6. YAKE-extracted keywords (if available)
        if YAKE_AVAILABLE:
            print("🔬 EXTRACTING YAKE KEYWORDS...")
            all_text = " ".join(all_titles + all_descriptions + all_transcripts)
            yake_keywords = self._extract_yake_keywords(all_text, max_keywords=50)
            keyword_arrays["yake_keywords"] = yake_keywords
            print(f"   ✅ {len(yake_keywords)} YAKE keywords")
        
        return keyword_arrays
    
    def _extract_keywords_from_text_list(self, text_list: List[str], max_keywords: int = 50) -> List[str]:
        """Extract keywords from a list of text strings."""
        all_text = " ".join(text_list)
        if not all_text:
            return []
        
        # Simple word extraction and frequency counting
        words = re.findall(r'\\b[a-zA-Z]{3,}\\b', all_text.lower())
        
        # Filter out stop words and count frequency
        filtered_words = [word for word in words if word not in self.stop_words and len(word) >= 3]
        word_counts = Counter(filtered_words)
        
        # Return most common words
        return [word for word, count in word_counts.most_common(max_keywords)]
    
    def _process_tags(self, tags: List[str]) -> List[str]:
        """Process tags into clean keywords."""
        processed = []
        for tag in tags:
            if not tag:
                continue
            
            clean_tag = tag.lower().strip()
            
            # Split compound tags
            parts = re.split(r'[\\s_-]+', clean_tag)
            for part in parts:
                if len(part) >= 3 and part not in self.stop_words:
                    processed.append(part)
        
        # Remove duplicates and return most common
        tag_counts = Counter(processed)
        return [tag for tag, count in tag_counts.most_common(50)]
    
    def _create_combined_keywords(self, keyword_arrays: Dict, max_keywords: int = 100) -> List[str]:
        """Create a combined keyword array with weighted importance."""
        keyword_scores = Counter()
        
        # Weight different sources
        weights = {
            "title_keywords": 3,      # Titles are very important
            "tag_keywords": 4,        # Tags are most important (already curated)
            "description_keywords": 2, # Descriptions are moderately important
            "transcript_keywords": 1   # Transcripts provide volume but less targeted
        }
        
        for source, keywords in keyword_arrays.items():
            if source in weights and keywords:
                weight = weights[source]
                for i, keyword in enumerate(keywords):
                    # Give higher scores to keywords that appear earlier in lists
                    position_bonus = max(0, (len(keywords) - i) / len(keywords))
                    keyword_scores[keyword] += weight * (1 + position_bonus)
        
        # Return top keywords sorted by score
        return [keyword for keyword, score in keyword_scores.most_common(max_keywords)]
    
    def _extract_yake_keywords(self, text: str, max_keywords: int = 50) -> List[str]:
        """Extract keywords using YAKE algorithm."""
        if not YAKE_AVAILABLE or not text:
            return []
        
        try:
            keywords = self.yake_extractor.extract_keywords(text)
            # YAKE returns (keyword, score) tuples, lower score is better
            return [kw for kw, score in sorted(keywords, key=lambda x: x[1])[:max_keywords]]
        except Exception as e:
            print(f"   ⚠️  YAKE extraction failed: {e}")
            return []
    
    def save_keyword_arrays(self, keyword_arrays: Dict, output_dir: str = "youtube2text_demo_output"):
        """Save keyword arrays in multiple formats for consumption."""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        print(f"\\n💾 SAVING KEYWORD ARRAYS TO: {output_path}")
        print("-" * 40)
        
        # Save each keyword array type
        for array_name, keywords in keyword_arrays.items():
            if keywords:
                # Save as JSON array (most common format)
                json_file = output_path / f"{array_name}.json"
                with open(json_file, 'w', encoding='utf-8') as f:
                    json.dump(keywords, f, indent=2)
                print(f"✅ {json_file} ({len(keywords)} keywords)")
                
                # Save as simple text file (one keyword per line)
                txt_file = output_path / f"{array_name}.txt"
                with open(txt_file, 'w', encoding='utf-8') as f:
                    f.write("\\n".join(keywords))
                print(f"✅ {txt_file} ({len(keywords)} keywords)")
        
        # Save master combined file for easy consumption
        master_file = output_path / "all_keywords_combined.json"
        with open(master_file, 'w', encoding='utf-8') as f:
            json.dump(keyword_arrays, f, indent=2)
        print(f"✅ {master_file} (master file)")
        
        # Save simple array for direct consumption
        if keyword_arrays.get("combined_keywords"):
            simple_array = output_path / "keywords_simple_array.json"
            with open(simple_array, 'w', encoding='utf-8') as f:
                json.dump(keyword_arrays["combined_keywords"], f)
            print(f"✅ {simple_array} (simple array for concatenation)")
        
        return output_path
    
    def display_sample_output(self, keyword_arrays: Dict):
        """Display sample of the generated keyword arrays."""
        print("\\n📋 SAMPLE KEYWORD ARRAYS OUTPUT")
        print("=" * 50)
        
        for array_name, keywords in keyword_arrays.items():
            if keywords:
                print(f"\\n🔑 {array_name.replace('_', ' ').title()}:")
                # Show first 10 keywords
                sample = keywords[:10]
                for i, keyword in enumerate(sample, 1):
                    print(f"   {i:2d}. {keyword}")
                if len(keywords) > 10:
                    print(f"   ... and {len(keywords) - 10} more")
        
        # Show how it would be consumed by other tools
        if keyword_arrays.get("combined_keywords"):
            print("\\n🔗 FOR CONCATENATION WITH OTHER KEYWORD FILES:")
            print("-" * 40)
            print("Array format (JSON):")
            sample_array = keyword_arrays["combined_keywords"][:5]
            print(f'  {json.dumps(sample_array)}')
            print("\\nText format (one per line):")
            for kw in sample_array:
                print(f"  {kw}")


def main():
    """Run the YouTube2Text keyword generation demo."""
    demo = YouTubeKeywordDemo()
    
    # Extract keywords from existing data
    keyword_arrays = demo.extract_from_existing_data()
    
    if not keyword_arrays:
        print("❌ No keyword arrays generated")
        return
    
    # Save the keyword arrays
    output_dir = demo.save_keyword_arrays(keyword_arrays)
    
    # Display sample output
    demo.display_sample_output(keyword_arrays)
    
    print(f"\\n🎉 YOUTUBE2TEXT DEMO COMPLETE!")
    print("=" * 50)
    print(f"📁 Output saved to: {output_dir}")
    print(f"🔗 Files ready for keyword concatenation:")
    
    if keyword_arrays.get("combined_keywords"):
        print(f"   • keywords_simple_array.json - {len(keyword_arrays['combined_keywords'])} keywords")
        print(f"   • combined_keywords.json - {len(keyword_arrays['combined_keywords'])} keywords")
        print(f"   • combined_keywords.txt - {len(keyword_arrays['combined_keywords'])} keywords")
    
    print("\\n📋 USAGE EXAMPLES:")
    print("   # Load keywords in Python:")
    print("   import json")
    print("   with open('youtube2text_demo_output/keywords_simple_array.json') as f:")
    print("       keywords = json.load(f)")
    print("\\n   # Concatenate with other keyword files:")
    print("   all_keywords = keywords + serp_keywords + other_keywords")


if __name__ == "__main__":
    main()
