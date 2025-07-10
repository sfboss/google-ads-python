# Complete YouTube Data Extraction & Analysis Guide

This guide demonstrates how to **ensure complete extraction** of all YouTube content data and apply comprehensive NLTK and YAKE analysis.

## 📊 What We Extract & Analyze

### ✅ From metadata.json:
- **Video Details**: title, description, duration, view count, upload date
- **Channel Info**: channel ID, uploader, channel URL  
- **Content Classification**: categories, tags (complete list)
- **Engagement Metrics**: view count, like/dislike counts, comments
- **Technical Details**: resolution, fps, aspect ratio
- **Content Flags**: age restrictions, embed permissions, live status
- **Media Assets**: thumbnail count and URLs
- **Subtitle/Caption Info**: available languages, automatic captions

### ✅ From Subtitle Files:
- **English Transcripts**: Clean text extraction from .en.vtt files
- **Multi-language Support**: Detection of available subtitle languages
- **Content Cleaning**: Removal of timestamps, positioning data, formatting

### ✅ NLTK Analysis Applied:
- **Tokenization**: Word and sentence tokenization
- **POS Tagging**: Nouns, verbs, adjectives identification
- **Word Frequency**: Most common words analysis
- **Readability Metrics**: Flesch reading ease, Flesch-Kincaid grade
- **Text Statistics**: Word count, sentence count, lexical diversity

### ✅ YAKE Keyword Extraction:
- **Automatic Keyword Detection**: Multi-word phrase extraction
- **Relevance Scoring**: Keywords ranked by importance
- **Language-Specific**: Tuned for English content
- **Deduplication**: Removes similar/duplicate keywords

### ✅ Sentiment Analysis (VADER):
- **Positive/Negative/Neutral Scores**: Detailed sentiment breakdown
- **Compound Score**: Overall sentiment rating
- **Context-Aware**: Handles social media style text well

## 🚀 Usage Examples

### Basic Analysis
```python
from youtube_focused_analyzer import analyze_youtube_video
from pathlib import Path

# Analyze a single video folder
video_folder = Path("/workspaces/google-ads-python/youtube_extracts/TUXh42V_ng4_20250710_074347")
result = analyze_youtube_video(video_folder)

# Access metadata
metadata = result['metadata']
print(f"Title: {metadata['title']}")
print(f"Views: {metadata['view_count']:,}")
print(f"Categories: {metadata['categories']}")
print(f"Tags: {len(metadata['tags'])} total")

# Access analysis results
analysis = result['analysis']
print(f"Sentiment: {analysis['sentiment_label']} ({analysis['sentiment_compound']:.3f})")
print(f"Keywords: {', '.join(analysis['top_keywords'][:5])}")
print(f"Word Count: {analysis['word_count']:,}")
```

### Comprehensive Analysis
```python
# Run the complete analyzer
python youtube_focused_analyzer.py
```

## 📋 Sample Results

### Rick Astley - Never Gonna Give You Up
- **Views**: 1,672,688,970
- **Sentiment**: Negative (-0.997) *[due to repetitive "never gonna" phrases]*
- **Keywords**: "Gonna Give", "Gonna", "gonna make", "Rick Astley"
- **Word Count**: 1,050 words
- **Reading Level**: Difficult

### Salesforce Explanation Video  
- **Views**: 360,883
- **Sentiment**: Positive (1.000) *[informative, helpful content]*
- **Keywords**: "Salesforce", "Einstein", "data", "CRM", "customer"
- **Word Count**: 2,059 words
- **Reading Level**: Fairly Difficult

## 🔧 Ensuring Complete Data Extraction

### 1. Metadata Validation
```python
def validate_metadata_extraction(metadata_path):
    """Ensure all critical metadata fields are extracted"""
    required_fields = ['title', 'description', 'view_count', 'categories', 'tags']
    
    with open(metadata_path) as f:
        data = json.load(f)
    
    missing = [field for field in required_fields if not data.get(field)]
    if missing:
        print(f"⚠️  Missing metadata fields: {missing}")
    else:
        print("✅ All critical metadata fields present")
```

### 2. Subtitle Content Verification
```python
def ensure_english_transcript(subtitles_dir):
    """Verify English transcript extraction"""
    english_files = list(subtitles_dir.glob("*.en.vtt"))
    
    if not english_files:
        print("❌ No English subtitles found")
        # Show what's available
        available = [f.name for f in subtitles_dir.glob("*.vtt")]
        print(f"Available: {available[:5]}")
        return False
    
    # Test extraction
    content = extract_english_transcript(subtitles_dir)
    if len(content) < 100:
        print(f"⚠️  Transcript seems short: {len(content)} characters")
    else:
        print(f"✅ Good transcript length: {len(content)} characters")
    
    return True
```

### 3. Analysis Quality Checks
```python
def validate_analysis_quality(analysis):
    """Check if analysis results are meaningful"""
    issues = []
    
    if analysis.get('word_count', 0) < 50:
        issues.append("Very low word count")
    
    if analysis.get('unique_words', 0) < 20:
        issues.append("Very low vocabulary diversity")
    
    if not analysis.get('keywords'):
        issues.append("No keywords extracted")
    
    if abs(analysis.get('sentiment_compound', 0)) < 0.1:
        issues.append("Neutral sentiment - may indicate processing issues")
    
    if issues:
        print(f"⚠️  Analysis quality concerns: {issues}")
    else:
        print("✅ Analysis quality looks good")
```

## 📊 Complete Data Structure

```python
{
    'folder_name': 'TUXh42V_ng4_20250710_074347',
    'metadata': {
        'id': 'TUXh42V_ng4',
        'title': 'What is Salesforce? (2024 Update) | Salesforce Explained',
        'description': 'What is Salesforce? Salesforce is the #1 AI CRM...',
        'duration': 530,
        'view_count': 360883,
        'categories': ['Science & Technology'],
        'tags': ['crm software', 'crm system', 'crm', 'ai in crm', ...],
        'channel_id': 'UCUpquzY878NEaZm5bc7m2sQ',
        'uploader': 'Salesforce',
        'available_caption_languages': ['ab', 'aa', 'af', 'ak', ...],
        'thumbnail_count': 42
    },
    'transcript': 'Every company wants to improve its relationships with customers...',
    'analysis': {
        'word_count': 2059,
        'unique_words': 419,
        'sentence_count': 113,
        'lexical_diversity': 0.442,
        'sentiment_compound': 1.000,
        'sentiment_label': 'Positive',
        'flesch_score': 57.1,
        'reading_difficulty': 'Fairly Difficult',
        'keywords': [
            ('Salesforce', 0.003),
            ('Einstein', 0.007),
            ('data', 0.009),
            ('CRM', 0.012),
            ('customer', 0.015)
        ],
        'most_common_words': [
            ('data', 38),
            ('salesforce', 29),
            ('ai', 28),
            ('einstein', 24),
            ('customer', 24)
        ]
    }
}
```

## 🎯 Key Benefits

1. **Complete Data Coverage**: Extracts ALL available metadata fields
2. **Reliable Text Extraction**: Properly handles subtitle cleaning and language detection
3. **Multi-faceted Analysis**: Combines NLTK, YAKE, and VADER for comprehensive insights
4. **Quality Validation**: Built-in checks to ensure meaningful results
5. **Structured Output**: Consistent, well-organized data structure
6. **Error Handling**: Graceful handling of missing files or corrupted data

## 🔧 Tools Used

- **NLTK**: Text tokenization, POS tagging, frequency analysis, readability metrics
- **YAKE**: Unsupervised keyword extraction optimized for short documents
- **VADER**: Sentiment analysis specifically tuned for social media text
- **TextStat**: Readability and text complexity measurements
- **Pandas**: Data manipulation and analysis
- **JSON**: Structured data storage and retrieval

This approach ensures you capture **every piece of valuable information** from YouTube videos and apply **state-of-the-art NLP techniques** for meaningful insights.
