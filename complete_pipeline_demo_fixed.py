#!/usr/bin/env python
"""
Complete YouTube Content to Keyword Research Pipeline Demo

This script demonstrates the complete pipeline from YouTube content extraction
through tokenization to Google Ads keyword research preparation.
"""

import json
import os
from datetime import datetime
from youtube_content_tokenizer import YouTubeContentTokenizer


def demo_complete_pipeline():
    """Demonstrate the complete YouTube to keyword research pipeline."""
    print("🚀 COMPLETE YOUTUBE CONTENT TO KEYWORD RESEARCH PIPELINE")
    print("=" * 70)
    
    # Initialize tokenizer
    tokenizer = YouTubeContentTokenizer()
    
    # Step 1: Extract and tokenize YouTube content
    print("\n📥 STEP 1: EXTRACTING & TOKENIZING YOUTUBE CONTENT")
    print("-" * 50)
    
    results = tokenizer.analyze_and_save_results(
        youtube_extracts_dir="/workspaces/google-ads-python/youtube_extracts",
        output_file="complete_pipeline_results.json"
    )
    
    if results.get("status") != "success":
        print("❌ Pipeline failed during content extraction")
        return
    
    # Step 2: Generate keyword seeds for Google Ads
    print("\n🎯 STEP 2: GENERATING KEYWORD SEEDS FOR GOOGLE ADS")
    print("-" * 50)
    
    tokenization = results.get("tokenization_results", {})
    
    # Extract top keywords as seeds
    seed_keywords = []
    combined_keywords = tokenization.get("combined_keywords", [])
    if combined_keywords:
        seed_keywords = [kw["keyword"] for kw in combined_keywords[:20]]
        print(f"✅ Extracted {len(seed_keywords)} top keywords as seeds:")
        for i, keyword in enumerate(seed_keywords[:10], 1):
            print(f"   {i:2d}. {keyword}")
        if len(seed_keywords) > 10:
            print(f"   ... and {len(seed_keywords) - 10} more")
    
    # Extract top phrases as additional seeds
    seed_phrases = []
    phrases = tokenization.get("keyword_phrases", [])
    if phrases:
        seed_phrases = [phrase["phrase"] for phrase in phrases[:10]]
        print(f"\n✅ Extracted {len(seed_phrases)} top phrases as additional seeds:")
        for i, phrase in enumerate(seed_phrases[:5], 1):
            print(f"   {i}. \"{phrase}\"")
        if len(seed_phrases) > 5:
            print(f"   ... and {len(seed_phrases) - 5} more")
    
    all_seeds = seed_keywords + seed_phrases
    
    # Step 3: Prepare Google Ads research command
    print(f"\n📊 STEP 3: GOOGLE ADS KEYWORD RESEARCH PREPARATION")
    print("-" * 50)
    
    print(f"✅ Total seeds prepared: {len(all_seeds)}")
    print("\n🛠️  To run Google Ads keyword research, use this command:")
    print("   (Replace YOUR_CUSTOMER_ID with your actual Google Ads customer ID)\n")
    
    # Create command with first 10 seeds (to keep command manageable)
    sample_seeds = all_seeds[:10]
    seeds_str = ' '.join([f'"{seed}"' for seed in sample_seeds])
    
    command = f"python adwords_service.py youtube-keywords -c YOUR_CUSTOMER_ID --pretty"
    print(f"   {command}\n")
    
    print("   Or use the traditional keyword-ideas command with extracted seeds:\n")
    command_traditional = f"python adwords_service.py keyword-ideas -c YOUR_CUSTOMER_ID -k {seeds_str} --pretty"
    print(f"   {command_traditional}\n")
    
    # Step 4: Content analysis summary
    print("\n📈 STEP 4: CONTENT ANALYSIS SUMMARY")
    print("-" * 50)
    
    analysis_summary = results.get("analysis_summary", {})
    content_analysis = tokenization.get("content_analysis", {})
    
    print(f"📁 Source: {analysis_summary.get('source_directory')}")
    print(f"🎥 Videos Analyzed: {analysis_summary.get('videos_analyzed')}")
    print(f"👀 Total Views: {analysis_summary.get('total_views'):,}")
    print(f"🎯 Content Themes: {', '.join(analysis_summary.get('content_themes', []))}")
    print(f"📂 Categories: {', '.join(analysis_summary.get('content_categories', []))}")
    
    # Top keywords summary
    if combined_keywords:
        print(f"\n🔑 TOP 5 KEYWORDS FOR ADS TARGETING:")
        for i, kw in enumerate(combined_keywords[:5], 1):
            print(f"   {i}. {kw['keyword']} (frequency: {kw['frequency']})")
    
    # Top phrases summary
    if phrases:
        print(f"\n📖 TOP 3 PHRASES FOR LONG-TAIL KEYWORDS:")
        for i, phrase in enumerate(phrases[:3], 1):
            print(f"   {i}. \"{phrase['phrase']}\" (frequency: {phrase['frequency']})")
    
    # Step 5: Save targeting suggestions
    print(f"\n💾 STEP 5: SAVING GOOGLE ADS TARGETING SUGGESTIONS")
    print("-" * 50)
    
    targeting_suggestions = {
        "timestamp": datetime.now().isoformat(),
        "source_analysis": {
            "videos_analyzed": analysis_summary.get('videos_analyzed'),
            "total_views": analysis_summary.get('total_views'),
            "content_themes": analysis_summary.get('content_themes', []),
            "content_categories": analysis_summary.get('content_categories', [])
        },
        "keyword_targeting": {
            "primary_keywords": seed_keywords[:15],
            "long_tail_phrases": seed_phrases[:10],
            "all_seed_keywords": all_seeds
        },
        "google_ads_commands": {
            "youtube_integration": command,
            "traditional_research": command_traditional
        },
        "targeting_recommendations": {
            "audience_interests": content_analysis.get("content_themes", []),
            "content_categories": analysis_summary.get('content_categories', []),
            "high_value_keywords": [kw["keyword"] for kw in combined_keywords[:10]] if combined_keywords else [],
            "demographic_insights": {
                "total_potential_reach": analysis_summary.get('total_views', 0),
                "content_mix": analysis_summary.get('content_categories', [])
            }
        }
    }
    
    # Save targeting suggestions
    try:
        with open("google_ads_targeting_suggestions.json", 'w', encoding='utf-8') as f:
            json.dump(targeting_suggestions, f, indent=2, ensure_ascii=False)
        print("✅ Targeting suggestions saved to: google_ads_targeting_suggestions.json")
    except IOError as e:
        print(f"❌ Error saving targeting suggestions: {e}")
    
    print(f"\n🎉 PIPELINE COMPLETE!")
    print("=" * 70)
    print("\n📋 SUMMARY:")
    print(f"   • Analyzed {analysis_summary.get('videos_analyzed')} YouTube videos")
    print(f"   • Extracted {len(all_seeds)} potential keyword seeds")
    print(f"   • Identified {len(analysis_summary.get('content_themes', []))} content themes")
    print(f"   • Prepared Google Ads research commands")
    print(f"   • Saved targeting suggestions for campaign setup")
    
    print("\n🚀 NEXT STEPS:")
    print("   1. Set up Google Ads API credentials in google-ads.yaml")
    print("   2. Replace YOUR_CUSTOMER_ID with your actual customer ID")
    print("   3. Run the generated commands to get keyword volumes and competition")
    print("   4. Use the results to build targeted ad campaigns")
    
    return targeting_suggestions


if __name__ == "__main__":
    # Run the complete pipeline
    targeting_suggestions = demo_complete_pipeline()
    
    print("\n" + "=" * 70)
    print("🎯 PIPELINE DEMONSTRATION COMPLETE!")
    print("Check the generated files for detailed results and suggestions.")
    print("=" * 70)
