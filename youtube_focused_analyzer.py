#!/usr/bin/env python3
"""
Focused YouTube Data Extractor and Analyzer
Demonstrates how to ensure complete data extraction and NLTK/YAKE analysis
"""

import json
import os
import re
from pathlib import Path
from typing import Dict, List, Any
import nltk
import yake
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import pandas as pd

def setup_nltk():
    """Setup NLTK with required data"""
    required_data = ['punkt', 'stopwords', 'averaged_perceptron_tagger', 'averaged_perceptron_tagger_eng']
    for dataset in required_data:
        try:
            if 'punkt' in dataset:
                nltk.data.find('tokenizers/punkt')
            elif 'stopwords' in dataset:
                nltk.data.find('corpora/stopwords')
            elif 'tagger' in dataset:
                nltk.data.find(f'taggers/{dataset}')
        except LookupError:
            print(f"Downloading {dataset}...")
            nltk.download(dataset, quiet=True)

def extract_all_metadata_fields(metadata_path: Path) -> Dict[str, Any]:
    """Extract ALL possible fields from metadata.json"""
    print(f"\n🔍 Analyzing metadata: {metadata_path}")
    
    if not metadata_path.exists():
        print("❌ No metadata.json found")
        return {}
    
    with open(metadata_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Core video information
    extracted = {
        'id': data.get('id', ''),
        'title': data.get('title', ''),
        'description': data.get('description', ''),
        'duration': data.get('duration', 0),
        'view_count': data.get('view_count', 0),
        'upload_date': data.get('upload_date', ''),
        'webpage_url': data.get('webpage_url', ''),
        
        # Channel info
        'channel_id': data.get('channel_id', ''),
        'channel_url': data.get('channel_url', ''),
        'uploader': data.get('uploader', ''),
        
        # Content classification  
        'categories': data.get('categories', []),
        'tags': data.get('tags', []),
        
        # Engagement
        'like_count': data.get('like_count', 0),
        'comment_count': data.get('comment_count', 0),
        
        # Technical
        'resolution': data.get('resolution', ''),
        'fps': data.get('fps', 0),
        'aspect_ratio': data.get('aspect_ratio', 0),
        
        # Content flags
        'live_status': data.get('live_status', ''),
        'age_limit': data.get('age_limit', 0),
        'playable_in_embed': data.get('playable_in_embed', False),
        
        # Captions and subtitles
        'automatic_captions_available': bool(data.get('automatic_captions', {})),
        'available_caption_languages': list(data.get('automatic_captions', {}).keys()),
        'subtitles_available': bool(data.get('subtitles', {})),
        'available_subtitle_languages': list(data.get('subtitles', {}).keys()),
        
        # Thumbnails
        'thumbnail_count': len(data.get('thumbnails', [])),
        'best_thumbnail': data.get('thumbnail', ''),
    }
    
    print(f"✅ Title: {extracted['title']}")
    print(f"✅ Duration: {extracted['duration']} seconds")
    print(f"✅ Views: {extracted['view_count']:,}")
    print(f"✅ Categories: {extracted['categories']}")
    print(f"✅ Tags: {len(extracted['tags'])} tags")
    print(f"✅ Description: {len(extracted['description'])} characters")
    print(f"✅ Captions: {len(extracted['available_caption_languages'])} languages")
    
    return extracted

def extract_english_transcript(subtitles_dir: Path) -> str:
    """Extract clean English transcript from subtitle files"""
    print(f"\n📝 Extracting transcript from: {subtitles_dir}")
    
    if not subtitles_dir.exists():
        print("❌ No subtitles directory found")
        return ""
    
    # Look for English subtitle files
    english_files = []
    for file in subtitles_dir.glob("*.vtt"):
        if ".en.vtt" in file.name or ".en-" in file.name:
            english_files.append(file)
    
    if not english_files:
        print("❌ No English subtitle files found")
        # Show available files
        available = list(subtitles_dir.glob("*.vtt"))
        print(f"Available subtitle files: {[f.name for f in available[:5]]}")
        return ""
    
    # Use the first English file found
    subtitle_file = english_files[0]
    print(f"✅ Using: {subtitle_file.name}")
    
    with open(subtitle_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Clean VTT content
    # Remove header
    content = re.sub(r'^WEBVTT.*?\n\n', '', content, flags=re.MULTILINE | re.DOTALL)
    
    # Remove timestamps and positioning
    content = re.sub(r'\d{2}:\d{2}:\d{2}\.\d{3} --> \d{2}:\d{2}:\d{2}\.\d{3}.*?\n', '', content)
    content = re.sub(r'position:\d+%.*?\n', '', content)
    content = re.sub(r'line:\d+%.*?\n', '', content)
    
    # Clean whitespace
    content = re.sub(r'\n+', ' ', content)
    content = re.sub(r'\s+', ' ', content)
    
    transcript = content.strip()
    print(f"✅ Extracted {len(transcript)} characters of transcript")
    
    return transcript

def analyze_with_nltk_and_yake(text: str, title: str = "") -> Dict[str, Any]:
    """Comprehensive text analysis with NLTK and YAKE"""
    print(f"\n🧠 Analyzing text: '{title[:50]}...'")
    
    if not text.strip():
        print("❌ No text to analyze")
        return {}
    
    setup_nltk()
    
    from nltk.corpus import stopwords
    from nltk.tokenize import word_tokenize, sent_tokenize
    from nltk.tag import pos_tag
    from textstat import flesch_reading_ease, flesch_kincaid_grade
    
    # NLTK Analysis
    stopwords_en = set(stopwords.words('english'))
    sentences = sent_tokenize(text)
    words = word_tokenize(text.lower())
    words_clean = [w for w in words if w.isalpha() and w not in stopwords_en]
    
    # POS tagging
    pos_tags = pos_tag(words)
    nouns = [word for word, pos in pos_tags if pos.startswith('NN')]
    verbs = [word for word, pos in pos_tags if pos.startswith('VB')]
    adjectives = [word for word, pos in pos_tags if pos.startswith('JJ')]
    
    # Word frequency
    word_freq = nltk.FreqDist(words_clean)
    
    # YAKE keyword extraction
    kw_extractor = yake.KeywordExtractor(
        lan="en",
        n=3,  # max phrase length
        dedupLim=0.7,
        top=20
    )
    keywords = kw_extractor.extract_keywords(text)
    
    # Sentiment analysis
    analyzer = SentimentIntensityAnalyzer()
    sentiment = analyzer.polarity_scores(text)
    
    # Readability
    flesch_score = flesch_reading_ease(text)
    fk_grade = flesch_kincaid_grade(text)
    
    def classify_sentiment(score):
        if score >= 0.05:
            return "Positive"
        elif score <= -0.05:
            return "Negative" 
        else:
            return "Neutral"
    
    def classify_reading_difficulty(score):
        if score >= 90: return "Very Easy"
        elif score >= 80: return "Easy"
        elif score >= 70: return "Fairly Easy"
        elif score >= 60: return "Standard"
        elif score >= 50: return "Fairly Difficult"
        elif score >= 30: return "Difficult"
        else: return "Very Difficult"
    
    analysis = {
        # Basic stats
        'word_count': len(words),
        'unique_words': len(set(words_clean)),
        'sentence_count': len(sentences),
        'avg_words_per_sentence': len(words) / len(sentences) if sentences else 0,
        'lexical_diversity': len(set(words_clean)) / len(words_clean) if words_clean else 0,
        
        # POS counts
        'noun_count': len(nouns),
        'verb_count': len(verbs),
        'adjective_count': len(adjectives),
        
        # Top words
        'most_common_words': word_freq.most_common(15),
        
        # Keywords (YAKE)
        'keywords': keywords,
        'top_keywords': [kw[0] for kw in keywords[:10]],
        
        # Sentiment (VADER)
        'sentiment_positive': sentiment['pos'],
        'sentiment_negative': sentiment['neg'],
        'sentiment_neutral': sentiment['neu'],
        'sentiment_compound': sentiment['compound'],
        'sentiment_label': classify_sentiment(sentiment['compound']),
        
        # Readability
        'flesch_score': flesch_score,
        'flesch_kincaid_grade': fk_grade,
        'reading_difficulty': classify_reading_difficulty(flesch_score)
    }
    
    print(f"✅ Words: {analysis['word_count']:,}")
    print(f"✅ Sentences: {analysis['sentence_count']:,}")
    print(f"✅ Unique words: {analysis['unique_words']:,}")
    print(f"✅ Sentiment: {analysis['sentiment_label']} ({analysis['sentiment_compound']:.3f})")
    print(f"✅ Reading level: {analysis['reading_difficulty']}")
    print(f"✅ Top keywords: {', '.join(analysis['top_keywords'][:5])}")
    
    return analysis

def analyze_youtube_video(video_folder: Path) -> Dict[str, Any]:
    """Complete analysis of a single YouTube video folder"""
    print(f"\n🎥 ANALYZING VIDEO FOLDER: {video_folder.name}")
    print("=" * 60)
    
    # 1. Extract metadata
    metadata = extract_all_metadata_fields(video_folder / "metadata.json")
    
    # 2. Extract transcript
    transcript = extract_english_transcript(video_folder / "subtitles")
    
    # 3. Combine all text for analysis
    combined_text = ""
    if metadata.get('title'):
        combined_text += metadata['title'] + ". "
    if metadata.get('description'):
        combined_text += metadata['description'] + " "
    if transcript:
        combined_text += transcript
    
    # 4. Analyze with NLTK and YAKE
    analysis = {}
    if combined_text.strip():
        analysis = analyze_with_nltk_and_yake(combined_text, metadata.get('title', ''))
    
    # 5. Create comprehensive result
    result = {
        'folder_name': video_folder.name,
        'metadata': metadata,
        'transcript': transcript,
        'transcript_length': len(transcript),
        'combined_text_length': len(combined_text),
        'analysis': analysis,
        'has_content': bool(metadata or transcript)
    }
    
    return result

def create_summary_report(results: List[Dict[str, Any]]) -> str:
    """Create a summary report of all analyses"""
    
    report = ["# YouTube Content Analysis Summary", ""]
    report.append(f"**Total Videos Analyzed:** {len(results)}")
    report.append("")
    
    for i, result in enumerate(results, 1):
        meta = result.get('metadata', {})
        analysis = result.get('analysis', {})
        
        report.append(f"## {i}. {meta.get('title', 'Unknown Title')}")
        report.append("")
        
        # Metadata summary
        report.append("### 📊 Metadata")
        report.append(f"- **Video ID:** {meta.get('id', 'N/A')}")
        report.append(f"- **Duration:** {meta.get('duration', 0)} seconds")
        report.append(f"- **Views:** {meta.get('view_count', 0):,}")
        report.append(f"- **Categories:** {', '.join(meta.get('categories', []))}")
        report.append(f"- **Tags:** {len(meta.get('tags', []))} tags")
        report.append(f"- **Description Length:** {len(meta.get('description', ''))} characters")
        report.append("")
        
        # Content analysis
        if analysis:
            report.append("### 🧠 Text Analysis")
            report.append(f"- **Total Words:** {analysis.get('word_count', 0):,}")
            report.append(f"- **Unique Words:** {analysis.get('unique_words', 0):,}")
            report.append(f"- **Sentences:** {analysis.get('sentence_count', 0):,}")
            report.append(f"- **Lexical Diversity:** {analysis.get('lexical_diversity', 0):.3f}")
            report.append(f"- **Reading Level:** {analysis.get('reading_difficulty', 'N/A')}")
            report.append(f"- **Sentiment:** {analysis.get('sentiment_label', 'N/A')} ({analysis.get('sentiment_compound', 0):.3f})")
            report.append("")
            
            # Keywords
            if analysis.get('top_keywords'):
                report.append("### 🔑 Top Keywords (YAKE)")
                for j, keyword in enumerate(analysis['top_keywords'][:10], 1):
                    report.append(f"{j}. {keyword}")
                report.append("")
            
            # Most common words
            if analysis.get('most_common_words'):
                report.append("### 📈 Most Common Words")
                for j, (word, count) in enumerate(analysis['most_common_words'][:10], 1):
                    report.append(f"{j}. {word}: {count}")
                report.append("")
        
        report.append("---")
        report.append("")
    
    return "\n".join(report)

def main():
    """Main demonstration function"""
    print("🚀 YouTube Content Extractor & Analyzer")
    print("📊 Ensures complete extraction of:")
    print("   ✓ Video metadata (title, description, tags, categories)")
    print("   ✓ English transcripts from subtitles") 
    print("   ✓ NLTK text analysis (POS, word frequency, readability)")
    print("   ✓ YAKE keyword extraction")
    print("   ✓ VADER sentiment analysis")
    print()
    
    # Analyze all video folders
    extract_dir = Path("/workspaces/google-ads-python/youtube_extracts")
    results = []
    
    if not extract_dir.exists():
        print(f"❌ Directory not found: {extract_dir}")
        return
    
    video_folders = [d for d in extract_dir.iterdir() if d.is_dir()]
    print(f"📁 Found {len(video_folders)} folders to check")
    print()
    
    for folder in video_folders:
        result = analyze_youtube_video(folder)
        if result['has_content']:
            results.append(result)
    
    print(f"\n🎉 Successfully analyzed {len(results)} videos with content!")
    
    if results:
        # Create summary report
        report = create_summary_report(results)
        
        # Save report
        report_file = "/workspaces/google-ads-python/youtube_comprehensive_analysis.md"
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report)
        
        # Save detailed data
        data_file = "/workspaces/google-ads-python/youtube_detailed_data.json"
        with open(data_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        print(f"📄 Report saved: {report_file}")
        print(f"💾 Data saved: {data_file}")
        
        # Show quick overview
        print("\n📋 QUICK OVERVIEW:")
        for i, result in enumerate(results, 1):
            meta = result['metadata']
            analysis = result['analysis']
            print(f"{i}. {meta.get('title', 'Unknown')}")
            print(f"   Views: {meta.get('view_count', 0):,} | Words: {analysis.get('word_count', 0):,} | Sentiment: {analysis.get('sentiment_label', 'N/A')}")
            if analysis.get('top_keywords'):
                print(f"   Keywords: {', '.join(analysis['top_keywords'][:3])}")
        
    else:
        print("❌ No videos with content found!")

if __name__ == "__main__":
    main()
