#!/usr/bin/env python
"""
Simple Keyword Enrichment Script
Processes keywords from master list with Google Ads API
"""

import pandas as pd
import numpy as np
import json
import time
import subprocess
from datetime import datetime
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
CUSTOMER_ID = "3399365278"
BATCH_SIZE = 500
API_DELAY = 2.5

def load_keywords(file_path="keyword_master_list.txt"):
    """Load keywords from master list."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            keywords = [line.strip() for line in f if line.strip()]
        
        # Remove duplicates
        seen = set()
        unique_keywords = []
        for kw in keywords:
            if kw not in seen:
                seen.add(kw)
                unique_keywords.append(kw)
        
        logger.info(f"Loaded {len(unique_keywords)} unique keywords")
        return unique_keywords
    except FileNotFoundError:
        logger.error(f"Could not find {file_path}")
        return []

def chunk_keywords(keywords, chunk_size):
    """Split keywords into chunks."""
    for i in range(0, len(keywords), chunk_size):
        yield keywords[i:i + chunk_size]

def call_google_ads_api(keywords):
    """Call Google Ads Historical Metrics API."""
    start_time = time.time()
    
    cmd = [
        "python", "adwords_service.py", "historical-metrics",
        "-c", CUSTOMER_ID
    ]
    
    for keyword in keywords:
        cmd.extend(["-k", keyword])
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        response_time = time.time() - start_time
        
        if result.stdout:
            try:
                data = json.loads(result.stdout)
                return data, response_time
            except json.JSONDecodeError:
                return {"status": "error", "error": "JSON decode error"}, response_time
        else:
            return {"status": "error", "error": result.stderr}, response_time
            
    except subprocess.TimeoutExpired:
        return {"status": "error", "error": "Timeout"}, 300.0
    except Exception as e:
        return {"status": "error", "error": str(e)}, time.time() - start_time

def calculate_trend_coefficient(monthly_volumes):
    """Calculate trend coefficient from monthly data."""
    if not monthly_volumes or len(monthly_volumes) < 3:
        return 0.0
    
    try:
        # Sort by year/month
        sorted_volumes = sorted(monthly_volumes, key=lambda x: (x['year'], x['month']))
        volumes = [vol.get('monthly_searches', 0) for vol in sorted_volumes]
        
        # Simple linear regression
        n = len(volumes)
        x = np.arange(n)
        y = np.array(volumes)
        
        x_mean = np.mean(x)
        y_mean = np.mean(y)
        
        numerator = np.sum((x - x_mean) * (y - y_mean))
        denominator = np.sum((x - x_mean) ** 2)
        
        if denominator == 0:
            return 0.0
        
        slope = numerator / denominator
        
        # Normalize relative to mean
        if y_mean > 0:
            return round(slope / y_mean, 6)
        else:
            return 0.0
            
    except Exception as e:
        logger.error(f"Error calculating trend: {e}")
        return 0.0

def calculate_seasonality_score(monthly_volumes):
    """Calculate seasonality score."""
    if not monthly_volumes or len(monthly_volumes) < 12:
        return 0.0
    
    try:
        # Group by month
        month_averages = {}
        for vol in monthly_volumes:
            month = vol.get('month', 'UNKNOWN')
            searches = vol.get('monthly_searches', 0)
            
            if month not in month_averages:
                month_averages[month] = []
            month_averages[month].append(searches)
        
        # Calculate monthly averages
        monthly_avgs = {month: np.mean(values) for month, values in month_averages.items()}
        
        if len(monthly_avgs) < 3:
            return 0.0
        
        # Coefficient of variation
        values = list(monthly_avgs.values())
        mean_val = np.mean(values)
        std_val = np.std(values)
        
        if mean_val > 0:
            return round(std_val / mean_val, 4)
        else:
            return 0.0
            
    except Exception as e:
        logger.error(f"Error calculating seasonality: {e}")
        return 0.0

def create_sparkline_data(monthly_volumes):
    """Create sparkline data string."""
    if not monthly_volumes:
        return ""
    
    try:
        sorted_volumes = sorted(monthly_volumes, key=lambda x: (x['year'], x['month']))
        volumes = [vol.get('monthly_searches', 0) for vol in sorted_volumes[-12:]]
        return ",".join(map(str, volumes))
    except Exception:
        return ""

def enrich_keyword_data(keyword_data):
    """Enrich single keyword with all metrics."""
    enriched = {
        "keyword": keyword_data.get("text", ""),
        "avg_monthly_searches": keyword_data.get("avg_monthly_searches", 0),
        "competition": keyword_data.get("competition", "UNKNOWN"),
        "competition_index": keyword_data.get("competition_index", 0),
        "low_top_of_page_bid_micros": keyword_data.get("low_top_of_page_bid_micros", 0),
        "high_top_of_page_bid_micros": keyword_data.get("high_top_of_page_bid_micros", 0),
        "low_top_of_page_bid_usd": keyword_data.get("low_top_of_page_bid_micros", 0) / 1_000_000,
        "high_top_of_page_bid_usd": keyword_data.get("high_top_of_page_bid_micros", 0) / 1_000_000,
    }
    
    # Process monthly volumes
    monthly_volumes = keyword_data.get("monthly_search_volumes", [])
    
    if monthly_volumes:
        enriched.update({
            "volume_trend_coef": calculate_trend_coefficient(monthly_volumes),
            "seasonality_score": calculate_seasonality_score(monthly_volumes),
            "latest_volume": monthly_volumes[-1].get("monthly_searches", 0) if monthly_volumes else 0,
            "sparkline_data": create_sparkline_data(monthly_volumes),
            "monthly_data_points": len(monthly_volumes),
            "has_historical_data": True
        })
        
        # Additional stats
        volumes = [vol.get("monthly_searches", 0) for vol in monthly_volumes]
        if volumes:
            enriched.update({
                "min_volume": min(volumes),
                "max_volume": max(volumes),
                "volume_range": max(volumes) - min(volumes),
                "volume_std": round(np.std(volumes), 2)
            })
    else:
        enriched.update({
            "volume_trend_coef": 0.0,
            "seasonality_score": 0.0,
            "latest_volume": 0,
            "sparkline_data": "",
            "monthly_data_points": 0,
            "has_historical_data": False,
            "min_volume": 0,
            "max_volume": 0,
            "volume_range": 0,
            "volume_std": 0
        })
    
    return enriched

def process_batch_results(api_response, batch_keywords):
    """Process API batch results."""
    enriched_keywords = []
    
    if api_response.get("status") != "success":
        # Create error entries
        for keyword in batch_keywords:
            enriched_keywords.append({
                "keyword": keyword,
                "data_source": "error",
                "error_message": api_response.get("error", {}).get("message", "Unknown error"),
                "avg_monthly_searches": 0,
                "competition": "UNKNOWN",
                "competition_index": 0,
                "low_top_of_page_bid_micros": 0,
                "high_top_of_page_bid_micros": 0,
                "volume_trend_coef": 0.0,
                "seasonality_score": 0.0,
                "latest_volume": 0,
                "sparkline_data": "",
                "has_historical_data": False
            })
        return enriched_keywords
    
    # Process successful response
    historical_metrics = api_response.get("historical_metrics", [])
    api_results = {result.get("text", "").lower(): result for result in historical_metrics}
    
    # Enrich each keyword
    for keyword in batch_keywords:
        api_data = api_results.get(keyword.lower())
        
        if api_data:
            enriched = enrich_keyword_data(api_data)
            enriched["data_source"] = "google_ads_api"
            enriched["api_match"] = True
        else:
            enriched = {
                "keyword": keyword,
                "data_source": "no_match",
                "api_match": False,
                "avg_monthly_searches": 0,
                "competition": "UNKNOWN",
                "competition_index": 0,
                "low_top_of_page_bid_micros": 0,
                "high_top_of_page_bid_micros": 0,
                "volume_trend_coef": 0.0,
                "seasonality_score": 0.0,
                "latest_volume": 0,
                "sparkline_data": "",
                "has_historical_data": False
            }
        
        enriched_keywords.append(enriched)
    
    return enriched_keywords

def main():
    """Main enrichment process."""
    print("🚀 Starting Keyword Enrichment with Google Ads Data")
    print("=" * 60)
    
    # Load keywords
    keywords = load_keywords()
    if not keywords:
        print("❌ No keywords loaded. Exiting.")
        return
    
    # Process in batches
    all_enriched_data = []
    batches = list(chunk_keywords(keywords, BATCH_SIZE))
    
    print(f"📊 Processing {len(keywords)} keywords in {len(batches)} batches of {BATCH_SIZE}")
    print("-" * 60)
    
    for i, batch in enumerate(batches):
        print(f"🔄 Processing batch {i+1}/{len(batches)} ({len(batch)} keywords)...")
        
        # Call API
        api_response, response_time = call_google_ads_api(batch)
        
        # Process results
        batch_results = process_batch_results(api_response, batch)
        all_enriched_data.extend(batch_results)
        
        # Log results
        successful_matches = sum(1 for r in batch_results if r.get("api_match", False))
        print(f"   ✅ {successful_matches}/{len(batch)} matches in {response_time:.2f}s")
        
        # Handle rate limiting
        if "429" in str(api_response.get("error", "")):
            print("   ⏳ Rate limit detected. Waiting longer...")
            time.sleep(API_DELAY * 2)
        elif i < len(batches) - 1:
            print(f"   ⏱️  Waiting {API_DELAY}s before next batch...")
            time.sleep(API_DELAY)
    
    # Convert to DataFrame
    df = pd.DataFrame(all_enriched_data)
    
    # Calculate summary
    total_matches = df['api_match'].sum() if 'api_match' in df.columns else 0
    match_rate = total_matches / len(keywords) * 100 if keywords else 0
    
    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"results/enriched_keywords_{timestamp}.csv"
    
    # Create results directory
    Path("results").mkdir(exist_ok=True)
    
    df.to_csv(output_file, index=False)
    
    print("\n" + "=" * 60)
    print("🎉 ENRICHMENT COMPLETE!")
    print("=" * 60)
    print(f"📁 Output file: {output_file}")
    print(f"📊 Total keywords: {len(keywords):,}")
    print(f"✅ Successful matches: {total_matches:,} ({match_rate:.1f}%)")
    print(f"📈 Keywords with historical data: {df['has_historical_data'].sum() if 'has_historical_data' in df.columns else 0:,}")
    
    if 'avg_monthly_searches' in df.columns:
        print(f"📊 Average monthly searches: {df['avg_monthly_searches'].mean():.0f}")
    
    print("=" * 60)
    
    return df

if __name__ == "__main__":
    main()
