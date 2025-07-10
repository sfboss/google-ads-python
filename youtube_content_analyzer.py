#!/usr/bin/env python3
"""
YouTube Content Analyzer with NLTK and YAKE
Extracts and analyzes metadata, descriptions, transcripts, and subtitle data
"""

import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import logging

# Data processing
import pandas as pd
import numpy as np

# Text processing
import nltk
import yake
from textstat import flesch_reading_ease, flesch_kincaid_grade
from vaderSentiment import SentimentIntensityAnalyzer

# Visualization
import matplotlib.pyplot as plt
import seaborn as sns
from wordcloud import WordCloud

# Download required NLTK data
def download_nltk_data():
    """Download required NLTK datasets"""
    required_data = [
        'punkt',
        'stopwords',
        'averaged_perceptron_tagger',
        'punkt_tab',
        'averaged_perceptron_tagger_eng'
    ]
    
    for dataset in required_data:
        try:
            if dataset == 'punkt':
                nltk.data.find('tokenizers/punkt')
            elif dataset == 'punkt_tab':
                nltk.data.find('tokenizers/punkt_tab')
            elif dataset == 'stopwords':
                nltk.data.find('corpora/stopwords')
            elif dataset == 'averaged_perceptron_tagger':
                nltk.data.find('taggers/averaged_perceptron_tagger')
            elif dataset == 'averaged_perceptron_tagger_eng':
                nltk.data.find('taggers/averaged_perceptron_tagger_eng')
        except LookupError:
            print(f"Downloading NLTK data: {dataset}")
            nltk.download(dataset, quiet=True)

class YouTubeContentAnalyzer:
    """
    Comprehensive YouTube content analyzer that extracts and processes:
    - Video metadata (title, description, tags, categories)
    - Transcripts and subtitles
    - Comments (if available)
    - Applies NLTK and YAKE analysis for keyword extraction and sentiment
    """
    
    def __init__(self, extract_base_path: str):
        """
        Initialize the analyzer
        
        Args:
            extract_base_path: Path to the youtube_extracts directory
        """
        self.extract_base_path = Path(extract_base_path)
        self.analyzer = SentimentIntensityAnalyzer()
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Download NLTK data
        download_nltk_data()
        
        # NLTK components
        from nltk.corpus import stopwords
        from nltk.tokenize import word_tokenize, sent_tokenize
        from nltk.tag import pos_tag
        
        self.stopwords = set(stopwords.words('english'))
        self.word_tokenize = word_tokenize
        self.sent_tokenize = sent_tokenize
        self.pos_tag = pos_tag
        
    def extract_metadata_info(self, metadata_path: Path) -> Dict[str, Any]:
        """
        Extract comprehensive information from metadata.json
        
        Args:
            metadata_path: Path to metadata.json file
            
        Returns:
            Dictionary with extracted metadata information
        """
        try:
            with open(metadata_path, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            
            # Core video information
            video_info = {
                'video_id': metadata.get('id', ''),
                'title': metadata.get('title', ''),
                'description': metadata.get('description', ''),
                'duration': metadata.get('duration', 0),
                'view_count': metadata.get('view_count', 0),
                'age_limit': metadata.get('age_limit', 0),
                'upload_date': metadata.get('upload_date', ''),
                'webpage_url': metadata.get('webpage_url', ''),
                
                # Channel information
                'channel_id': metadata.get('channel_id', ''),
                'channel_url': metadata.get('channel_url', ''),
                'uploader': metadata.get('uploader', ''),
                
                # Content classification
                'categories': metadata.get('categories', []),
                'tags': metadata.get('tags', []),
                
                # Engagement metrics
                'like_count': metadata.get('like_count', 0),
                'dislike_count': metadata.get('dislike_count', 0),
                'comment_count': metadata.get('comment_count', 0),
                
                # Technical details
                'resolution': metadata.get('resolution', ''),
                'fps': metadata.get('fps', 0),
                'aspect_ratio': metadata.get('aspect_ratio', 0),
                
                # Content flags
                'live_status': metadata.get('live_status', ''),
                'playable_in_embed': metadata.get('playable_in_embed', False),
                'age_limit': metadata.get('age_limit', 0),
                
                # Thumbnails
                'thumbnail_count': len(metadata.get('thumbnails', [])),
                'thumbnail_url': metadata.get('thumbnail', ''),
                
                # Subtitles/Captions
                'has_automatic_captions': bool(metadata.get('automatic_captions')),
                'caption_languages': list(metadata.get('automatic_captions', {}).keys()) if metadata.get('automatic_captions') else [],
                'subtitles_available': bool(metadata.get('subtitles')),
                'subtitle_languages': list(metadata.get('subtitles', {}).keys()) if metadata.get('subtitles') else []
            }
            
            return video_info
            
        except Exception as e:
            self.logger.error(f"Error extracting metadata from {metadata_path}: {e}")
            return {}
    
    def extract_subtitle_text(self, subtitle_path: Path) -> str:
        """
        Extract clean text from VTT subtitle files
        
        Args:
            subtitle_path: Path to VTT subtitle file
            
        Returns:
            Clean text content without timestamps
        """
        try:
            with open(subtitle_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Remove VTT header
            content = re.sub(r'^WEBVTT.*?\n\n', '', content, flags=re.MULTILINE | re.DOTALL)
            
            # Remove timestamps and positioning info
            # Pattern matches: 00:00:00.300 --> 00:00:03.403 position:63% line:0%
            content = re.sub(r'\d{2}:\d{2}:\d{2}\.\d{3} --> \d{2}:\d{2}:\d{2}\.\d{3}.*?\n', '', content)
            
            # Remove any remaining positioning tags
            content = re.sub(r'position:\d+%.*?\n', '', content)
            content = re.sub(r'line:\d+%.*?\n', '', content)
            
            # Clean up extra whitespace and newlines
            content = re.sub(r'\n+', ' ', content)
            content = re.sub(r'\s+', ' ', content)
            
            return content.strip()
            
        except Exception as e:
            self.logger.error(f"Error extracting subtitle text from {subtitle_path}: {e}")
            return ""
    
    def analyze_text_with_nltk(self, text: str) -> Dict[str, Any]:
        """
        Perform comprehensive NLTK analysis on text
        
        Args:
            text: Text to analyze
            
        Returns:
            Dictionary with NLTK analysis results
        """
        if not text.strip():
            return {}
        
        try:
            # Tokenization
            sentences = self.sent_tokenize(text)
            words = self.word_tokenize(text.lower())
            words_no_stop = [w for w in words if w.isalpha() and w not in self.stopwords]
            
            # POS tagging
            pos_tags = self.pos_tag(words)
            
            # Extract different word types
            nouns = [word for word, pos in pos_tags if pos.startswith('NN')]
            verbs = [word for word, pos in pos_tags if pos.startswith('VB')]
            adjectives = [word for word, pos in pos_tags if pos.startswith('JJ')]
            
            # Word frequency analysis
            word_freq = nltk.FreqDist(words_no_stop)
            
            # Readability metrics
            flesch_score = flesch_reading_ease(text)
            fk_grade = flesch_kincaid_grade(text)
            
            # Sentiment analysis
            sentiment = self.analyzer.polarity_scores(text)
            
            analysis = {
                'sentence_count': len(sentences),
                'word_count': len(words),
                'unique_words': len(set(words_no_stop)),
                'avg_words_per_sentence': len(words) / len(sentences) if sentences else 0,
                'lexical_diversity': len(set(words_no_stop)) / len(words_no_stop) if words_no_stop else 0,
                
                # Word type counts
                'noun_count': len(nouns),
                'verb_count': len(verbs),
                'adjective_count': len(adjectives),
                
                # Most common words
                'most_common_words': word_freq.most_common(20),
                
                # Readability
                'flesch_reading_ease': flesch_score,
                'flesch_kincaid_grade': fk_grade,
                'reading_difficulty': self._classify_reading_difficulty(flesch_score),
                
                # Sentiment
                'sentiment_positive': sentiment['pos'],
                'sentiment_negative': sentiment['neg'],
                'sentiment_neutral': sentiment['neu'],
                'sentiment_compound': sentiment['compound'],
                'sentiment_label': self._classify_sentiment(sentiment['compound'])
            }
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Error in NLTK analysis: {e}")
            return {}
    
    def extract_keywords_with_yake(self, text: str, max_keywords: int = 20) -> List[Tuple[str, float]]:
        """
        Extract keywords using YAKE algorithm
        
        Args:
            text: Text to extract keywords from
            max_keywords: Maximum number of keywords to extract
            
        Returns:
            List of tuples (keyword, score) sorted by relevance
        """
        if not text.strip():
            return []
        
        try:
            # Configure YAKE
            kw_extractor = yake.KeywordExtractor(
                lan="en",                    # Language
                n=3,                        # Maximum number of words in keyphrase
                dedupLim=0.7,              # Deduplication threshold
                top=max_keywords,          # Number of keywords to extract
                features=None
            )
            
            keywords = kw_extractor.extract_keywords(text)
            
            # Sort by score (lower is better in YAKE)
            return sorted(keywords, key=lambda x: x[1])
            
        except Exception as e:
            self.logger.error(f"Error in YAKE keyword extraction: {e}")
            return []
    
    def _classify_reading_difficulty(self, flesch_score: float) -> str:
        """Classify reading difficulty based on Flesch score"""
        if flesch_score >= 90:
            return "Very Easy"
        elif flesch_score >= 80:
            return "Easy"
        elif flesch_score >= 70:
            return "Fairly Easy"
        elif flesch_score >= 60:
            return "Standard"
        elif flesch_score >= 50:
            return "Fairly Difficult"
        elif flesch_score >= 30:
            return "Difficult"
        else:
            return "Very Difficult"
    
    def _classify_sentiment(self, compound_score: float) -> str:
        """Classify sentiment based on compound score"""
        if compound_score >= 0.05:
            return "Positive"
        elif compound_score <= -0.05:
            return "Negative"
        else:
            return "Neutral"
    
    def analyze_video_folder(self, folder_path: Path) -> Dict[str, Any]:
        """
        Analyze a complete video extraction folder
        
        Args:
            folder_path: Path to video extraction folder
            
        Returns:
            Complete analysis results
        """
        results = {
            'folder_name': folder_path.name,
            'analysis_date': datetime.now().isoformat(),
            'metadata': {},
            'subtitle_analysis': {},
            'combined_text_analysis': {},
            'keywords': [],
            'files_processed': [],
            'has_content': False
        }
        
        # Extract metadata
        metadata_path = folder_path / 'metadata.json'
        if metadata_path.exists():
            results['metadata'] = self.extract_metadata_info(metadata_path)
            results['files_processed'].append(str(metadata_path))
            results['has_content'] = True
        
        # Extract and analyze subtitles
        subtitles_path = folder_path / 'subtitles'
        if subtitles_path.exists():
            subtitle_texts = {}
            
            for subtitle_file in subtitles_path.glob('*.vtt'):
                # Get language code from filename
                lang_match = re.search(r'\.([a-z]{2}(?:-[A-Z][a-z]+)?(?:-[a-z]{2})?)(?:-en)?\.vtt$', subtitle_file.name)
                lang = lang_match.group(1) if lang_match else 'en'
                
                text = self.extract_subtitle_text(subtitle_file)
                if text:
                    subtitle_texts[lang] = text
                    results['files_processed'].append(str(subtitle_file))
                    results['has_content'] = True
            
            # Analyze English subtitles if available
            if 'en' in subtitle_texts:
                results['subtitle_analysis'] = self.analyze_text_with_nltk(subtitle_texts['en'])
                results['keywords'] = self.extract_keywords_with_yake(subtitle_texts['en'])
            
            results['subtitle_texts'] = subtitle_texts
        
        # Combined text analysis (title + description + transcript)
        combined_text = ""
        if results['metadata']:
            combined_text += results['metadata'].get('title', '') + " "
            combined_text += results['metadata'].get('description', '') + " "
        
        if 'en' in results.get('subtitle_texts', {}):
            combined_text += results['subtitle_texts']['en']
        
        if combined_text.strip():
            results['combined_text_analysis'] = self.analyze_text_with_nltk(combined_text)
            if not results['keywords']:  # If no keywords from subtitles
                results['keywords'] = self.extract_keywords_with_yake(combined_text)
        
        return results
    
    def analyze_all_videos(self) -> List[Dict[str, Any]]:
        """
        Analyze all video folders in the extract directory
        
        Returns:
            List of analysis results for all videos
        """
        results = []
        
        if not self.extract_base_path.exists():
            self.logger.error(f"Extract directory not found: {self.extract_base_path}")
            return results
        
        # Find all video folders
        video_folders = [d for d in self.extract_base_path.iterdir() if d.is_dir()]
        
        self.logger.info(f"Found {len(video_folders)} video folders to analyze")
        
        for folder in video_folders:
            self.logger.info(f"Analyzing folder: {folder.name}")
            analysis = self.analyze_video_folder(folder)
            
            # Only include folders that have actual content
            if analysis.get('has_content', False):
                results.append(analysis)
            else:
                self.logger.info(f"Skipping empty folder: {folder.name}")
        
        return results
    
    def create_analysis_report(self, results: List[Dict[str, Any]], output_path: str = None) -> str:
        """
        Create a comprehensive analysis report
        
        Args:
            results: Analysis results from analyze_all_videos()
            output_path: Path to save the report (optional)
            
        Returns:
            Report content as string
        """
        if not results:
            return "No analysis results to report."
        
        report_lines = []
        report_lines.append("# YouTube Content Analysis Report")
        report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"Videos Analyzed: {len(results)}")
        report_lines.append("=" * 60)
        report_lines.append("")
        
        for i, result in enumerate(results, 1):
            report_lines.append(f"## Video {i}: {result.get('folder_name', 'Unknown')}")
            
            metadata = result.get('metadata', {})
            if metadata:
                report_lines.append(f"**Title:** {metadata.get('title', 'N/A')}")
                report_lines.append(f"**Video ID:** {metadata.get('video_id', 'N/A')}")
                report_lines.append(f"**Duration:** {metadata.get('duration', 0)} seconds")
                report_lines.append(f"**View Count:** {metadata.get('view_count', 0):,}")
                report_lines.append(f"**Categories:** {', '.join(metadata.get('categories', []))}")
                report_lines.append(f"**Tags:** {', '.join(metadata.get('tags', [])[:10])}...")  # First 10 tags
                report_lines.append("")
            
            # Text analysis
            analysis = result.get('combined_text_analysis', {})
            if analysis:
                report_lines.append("### Text Analysis")
                report_lines.append(f"- **Word Count:** {analysis.get('word_count', 0):,}")
                report_lines.append(f"- **Sentence Count:** {analysis.get('sentence_count', 0):,}")
                report_lines.append(f"- **Unique Words:** {analysis.get('unique_words', 0):,}")
                report_lines.append(f"- **Lexical Diversity:** {analysis.get('lexical_diversity', 0):.3f}")
                report_lines.append(f"- **Reading Difficulty:** {analysis.get('reading_difficulty', 'N/A')}")
                report_lines.append(f"- **Flesch Score:** {analysis.get('flesch_reading_ease', 0):.1f}")
                report_lines.append(f"- **Sentiment:** {analysis.get('sentiment_label', 'N/A')} ({analysis.get('sentiment_compound', 0):.3f})")
                report_lines.append("")
            
            # Keywords
            keywords = result.get('keywords', [])
            if keywords:
                report_lines.append("### Top Keywords (YAKE)")
                for j, (keyword, score) in enumerate(keywords[:10], 1):
                    report_lines.append(f"{j}. {keyword} (score: {score:.3f})")
                report_lines.append("")
            
            # Most common words
            if analysis and 'most_common_words' in analysis:
                report_lines.append("### Most Common Words")
                for j, (word, count) in enumerate(analysis['most_common_words'][:10], 1):
                    report_lines.append(f"{j}. {word}: {count}")
                report_lines.append("")
            
            report_lines.append("-" * 40)
            report_lines.append("")
        
        # Summary statistics
        report_lines.append("## Summary Statistics")
        
        total_views = sum(r.get('metadata', {}).get('view_count', 0) for r in results)
        total_duration = sum(r.get('metadata', {}).get('duration', 0) for r in results)
        avg_sentiment = np.mean([r.get('combined_text_analysis', {}).get('sentiment_compound', 0) for r in results if r.get('combined_text_analysis')])
        
        report_lines.append(f"- **Total Views:** {total_views:,}")
        report_lines.append(f"- **Total Duration:** {total_duration:,} seconds ({total_duration/3600:.1f} hours)")
        report_lines.append(f"- **Average Sentiment:** {avg_sentiment:.3f}")
        
        report_content = "\n".join(report_lines)
        
        if output_path:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(report_content)
            self.logger.info(f"Report saved to: {output_path}")
        
        return report_content
    
    def create_visualizations(self, results: List[Dict[str, Any]], output_dir: str = "analysis_charts"):
        """
        Create visualizations from analysis results
        
        Args:
            results: Analysis results
            output_dir: Directory to save charts
        """
        if not results:
            return
        
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # Set style
        plt.style.use('seaborn-v0_8')
        
        # 1. Sentiment Distribution
        sentiments = [r.get('combined_text_analysis', {}).get('sentiment_compound', 0) for r in results if r.get('combined_text_analysis')]
        if sentiments:
            plt.figure(figsize=(10, 6))
            plt.hist(sentiments, bins=20, alpha=0.7, color='skyblue', edgecolor='black')
            plt.title('Sentiment Distribution Across Videos')
            plt.xlabel('Sentiment Score')
            plt.ylabel('Number of Videos')
            plt.axvline(x=0, color='red', linestyle='--', alpha=0.7, label='Neutral')
            plt.legend()
            plt.tight_layout()
            plt.savefig(output_path / 'sentiment_distribution.png', dpi=300, bbox_inches='tight')
            plt.close()
        
        # 2. Word Cloud from all keywords
        all_keywords = []
        for result in results:
            keywords = result.get('keywords', [])
            all_keywords.extend([kw[0] for kw in keywords[:10]])  # Top 10 from each video
        
        if all_keywords:
            keyword_text = ' '.join(all_keywords)
            wordcloud = WordCloud(width=800, height=400, background_color='white').generate(keyword_text)
            
            plt.figure(figsize=(12, 6))
            plt.imshow(wordcloud, interpolation='bilinear')
            plt.axis('off')
            plt.title('Keywords Word Cloud')
            plt.tight_layout()
            plt.savefig(output_path / 'keywords_wordcloud.png', dpi=300, bbox_inches='tight')
            plt.close()
        
        # 3. View Count vs Sentiment
        view_counts = []
        sentiment_scores = []
        titles = []
        
        for result in results:
            metadata = result.get('metadata', {})
            analysis = result.get('combined_text_analysis', {})
            
            if metadata.get('view_count') and analysis.get('sentiment_compound') is not None:
                view_counts.append(metadata['view_count'])
                sentiment_scores.append(analysis['sentiment_compound'])
                titles.append(metadata.get('title', 'Unknown')[:30] + '...' if len(metadata.get('title', '')) > 30 else metadata.get('title', 'Unknown'))
        
        if view_counts and sentiment_scores:
            plt.figure(figsize=(12, 8))
            scatter = plt.scatter(sentiment_scores, view_counts, alpha=0.7, s=100)
            plt.xlabel('Sentiment Score')
            plt.ylabel('View Count')
            plt.title('View Count vs Sentiment Score')
            plt.yscale('log')
            
            # Add labels for each point
            for i, title in enumerate(titles):
                plt.annotate(title, (sentiment_scores[i], view_counts[i]), 
                           xytext=(5, 5), textcoords='offset points', fontsize=8, alpha=0.7)
            
            plt.tight_layout()
            plt.savefig(output_path / 'views_vs_sentiment.png', dpi=300, bbox_inches='tight')
            plt.close()
        
        self.logger.info(f"Visualizations saved to: {output_path}")

def main():
    """Main function to run the analyzer"""
    
    # Initialize analyzer
    extract_path = "/workspaces/google-ads-python/youtube_extracts"
    analyzer = YouTubeContentAnalyzer(extract_path)
    
    print("🚀 Starting YouTube Content Analysis...")
    print("📊 This will extract and analyze:")
    print("   ✓ Video metadata (title, description, tags, categories)")
    print("   ✓ Subtitle/transcript content")
    print("   ✓ Sentiment analysis with VADER")
    print("   ✓ Keyword extraction with YAKE")
    print("   ✓ Text statistics with NLTK")
    print("   ✓ Readability metrics")
    print()
    
    # Analyze all videos
    results = analyzer.analyze_all_videos()
    
    if not results:
        print("❌ No videos found to analyze. Make sure the extract directory contains video folders.")
        return
    
    print(f"✅ Successfully analyzed {len(results)} videos!")
    print()
    
    # Create report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = f"/workspaces/google-ads-python/youtube_analysis_report_{timestamp}.md"
    
    print("📝 Generating comprehensive report...")
    report = analyzer.create_analysis_report(results, report_path)
    
    # Create visualizations
    print("📊 Creating visualizations...")
    analyzer.create_visualizations(results, f"/workspaces/google-ads-python/analysis_charts_{timestamp}")
    
    # Save detailed results as JSON
    json_path = f"/workspaces/google-ads-python/youtube_analysis_data_{timestamp}.json"
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print("🎉 Analysis Complete!")
    print(f"📄 Report saved to: {report_path}")
    print(f"📊 Charts saved to: analysis_charts_{timestamp}/")
    print(f"💾 Raw data saved to: {json_path}")
    print()
    
    # Show quick summary
    print("📋 Quick Summary:")
    for i, result in enumerate(results, 1):
        metadata = result.get('metadata', {})
        analysis = result.get('combined_text_analysis', {})
        keywords = result.get('keywords', [])
        
        print(f"  {i}. {metadata.get('title', 'Unknown Title')}")
        print(f"     Views: {metadata.get('view_count', 0):,} | "
              f"Duration: {metadata.get('duration', 0)}s | "
              f"Sentiment: {analysis.get('sentiment_label', 'N/A')}")
        if keywords:
            top_keywords = [kw[0] for kw in keywords[:5]]
            print(f"     Top Keywords: {', '.join(top_keywords)}")
        print()

if __name__ == "__main__":
    main()
