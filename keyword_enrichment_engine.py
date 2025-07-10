#!/usr/bin/env python
"""
Advanced Keyword Enrichment System with Google Ads Integration
Processes keywords in optimal batches with trend analysis, sparklines, and custom metrics
"""

import pandas as pd
import numpy as np
import json
import time
import subprocess
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
import logging
from pathlib import Path
import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import base64
from io import BytesIO

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KeywordEnrichmentEngine:
    """Advanced keyword enrichment with Google Ads data and trend analysis."""
    
    def __init__(self, customer_id: str = "3399365278"):
        self.customer_id = customer_id
        self.optimal_batch_size = 500  # Based on scaling analysis
        self.api_delay = 2.5  # Seconds between requests
        self.results_dir = Path("results/keyword_enrichment")
        self.results_dir.mkdir(parents=True, exist_ok=True)
        
    def load_keyword_master_list(self, file_path: str = "keyword_master_list.txt") -> List[str]:
        """Load keywords from master list file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                keywords = [line.strip() for line in f if line.strip()]
            
            # Remove duplicates while preserving order
            seen = set()
            unique_keywords = []
            for kw in keywords:
                if kw not in seen:
                    seen.add(kw)
                    unique_keywords.append(kw)
            
            logger.info(f"Loaded {len(unique_keywords)} unique keywords from {file_path}")
            return unique_keywords
        except FileNotFoundError:
            logger.error(f"Could not find {file_path}")
            return []
    
    def chunk_keywords(self, keywords: List[str], chunk_size: int) -> List[List[str]]:
        """Split keywords into optimal batch sizes."""
        for i in range(0, len(keywords), chunk_size):
            yield keywords[i:i + chunk_size]
    
    def call_google_ads_api(self, keywords: List[str], api_type: str = "historical-metrics") -> Tuple[Dict[str, Any], float]:
        """Call Google Ads API with error handling and timing."""
        start_time = time.time()
        
        cmd = [
            "python", "adwords_service.py", api_type,
            "-c", self.customer_id
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
    
    def calculate_trend_coefficient(self, monthly_volumes: List[Dict]) -> float:
        """Calculate volume trend coefficient from monthly search data."""
        if not monthly_volumes or len(monthly_volumes) < 3:
            return 0.0
        
        try:
            # Sort by year/month
            sorted_volumes = sorted(monthly_volumes, key=lambda x: (x['year'], x['month']))
            
            # Extract search volumes
            volumes = [vol.get('monthly_searches', 0) for vol in sorted_volumes]
            
            # Calculate linear regression coefficient
            n = len(volumes)
            x = np.arange(n)
            y = np.array(volumes)
            
            # Linear regression: y = mx + b
            x_mean = np.mean(x)
            y_mean = np.mean(y)
            
            numerator = np.sum((x - x_mean) * (y - y_mean))
            denominator = np.sum((x - x_mean) ** 2)
            
            if denominator == 0:
                return 0.0
            
            slope = numerator / denominator
            
            # Normalize coefficient relative to mean volume
            if y_mean > 0:
                normalized_coef = slope / y_mean
            else:
                normalized_coef = 0.0
            
            return round(normalized_coef, 6)
            
        except Exception as e:
            logger.error(f"Error calculating trend coefficient: {e}")
            return 0.0
    
    def calculate_seasonality_score(self, monthly_volumes: List[Dict]) -> float:
        """Calculate seasonality score based on monthly variation."""
        if not monthly_volumes or len(monthly_volumes) < 12:
            return 0.0
        
        try:
            # Group by month to find seasonal patterns
            month_averages = {}
            for vol in monthly_volumes:
                month = vol.get('month', 'UNKNOWN')
                searches = vol.get('monthly_searches', 0)
                
                if month not in month_averages:
                    month_averages[month] = []
                month_averages[month].append(searches)
            
            # Calculate average for each month
            monthly_avgs = {}
            for month, values in month_averages.items():
                monthly_avgs[month] = np.mean(values)
            
            if len(monthly_avgs) < 3:
                return 0.0
            
            # Calculate coefficient of variation
            values = list(monthly_avgs.values())
            mean_val = np.mean(values)
            std_val = np.std(values)
            
            if mean_val > 0:
                cv = std_val / mean_val
                return round(cv, 4)
            else:
                return 0.0
                
        except Exception as e:
            logger.error(f"Error calculating seasonality score: {e}")
            return 0.0
    
    def calculate_volatility_index(self, monthly_volumes: List[Dict]) -> float:
        """Calculate volatility index based on month-to-month changes."""
        if not monthly_volumes or len(monthly_volumes) < 3:
            return 0.0
        
        try:
            # Sort by year/month
            sorted_volumes = sorted(monthly_volumes, key=lambda x: (x['year'], x['month']))
            volumes = [vol.get('monthly_searches', 0) for vol in sorted_volumes]
            
            # Calculate month-to-month percentage changes
            changes = []
            for i in range(1, len(volumes)):
                if volumes[i-1] > 0:
                    change = abs(volumes[i] - volumes[i-1]) / volumes[i-1]
                    changes.append(change)
            
            if not changes:
                return 0.0
            
            # Return average absolute change
            return round(np.mean(changes), 4)
            
        except Exception as e:
            logger.error(f"Error calculating volatility index: {e}")
            return 0.0
    
    def create_sparkline_data(self, monthly_volumes: List[Dict]) -> str:
        """Create sparkline data string for visualization."""
        if not monthly_volumes:
            return ""
        
        try:
            # Sort by year/month
            sorted_volumes = sorted(monthly_volumes, key=lambda x: (x['year'], x['month']))
            volumes = [vol.get('monthly_searches', 0) for vol in sorted_volumes[-12:]]  # Last 12 months
            
            # Create simple comma-separated values for sparkline
            return ",".join(map(str, volumes))
            
        except Exception as e:
            logger.error(f"Error creating sparkline data: {e}")
            return ""
    
    def enrich_keyword_data(self, keyword_data: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich single keyword data with advanced metrics."""
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
        
        # Process monthly volumes if available
        monthly_volumes = keyword_data.get("monthly_search_volumes", [])
        
        if monthly_volumes:
            enriched.update({
                "volume_trend_coef": self.calculate_trend_coefficient(monthly_volumes),
                "seasonality_score": self.calculate_seasonality_score(monthly_volumes),
                "volatility_index": self.calculate_volatility_index(monthly_volumes),
                "latest_volume": monthly_volumes[-1].get("monthly_searches", 0) if monthly_volumes else 0,
                "sparkline_data": self.create_sparkline_data(monthly_volumes),
                "monthly_data_points": len(monthly_volumes),
                "has_historical_data": True
            })
            
            # Calculate additional metrics
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
                "volatility_index": 0.0,
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
    
    def process_batch_results(self, api_response: Dict[str, Any], batch_keywords: List[str]) -> List[Dict[str, Any]]:
        """Process API response and enrich all keywords in batch."""
        enriched_keywords = []
        
        if api_response.get("status") != "success":
            # Create empty entries for failed batch
            for keyword in batch_keywords:
                enriched_keywords.append({
                    "keyword": keyword,
                    "data_source": "error",
                    "error_message": api_response.get("error", {}).get("message", "Unknown error"),
                    **{k: 0 for k in ["avg_monthly_searches", "competition_index", "low_top_of_page_bid_micros", 
                                     "high_top_of_page_bid_micros", "volume_trend_coef", "seasonality_score", 
                                     "volatility_index", "latest_volume"]}
                })
            return enriched_keywords
        
        # Process successful response
        historical_metrics = api_response.get("historical_metrics", [])
        
        # Create lookup for API results
        api_results = {result.get("text", "").lower(): result for result in historical_metrics}
        
        # Enrich each keyword
        for keyword in batch_keywords:
            api_data = api_results.get(keyword.lower())
            
            if api_data:
                enriched = self.enrich_keyword_data(api_data)
                enriched["data_source"] = "google_ads_api"
                enriched["api_match"] = True
            else:
                # Create entry with default values
                enriched = {
                    "keyword": keyword,
                    "data_source": "no_match",
                    "api_match": False,
                    **{k: 0 for k in ["avg_monthly_searches", "competition_index", "low_top_of_page_bid_micros", 
                                     "high_top_of_page_bid_micros", "volume_trend_coef", "seasonality_score", 
                                     "volatility_index", "latest_volume"]}
                }
            
            enriched_keywords.append(enriched)
        
        return enriched_keywords
    
    def enrich_keywords_bulk(self, keywords: List[str], progress_callback=None) -> pd.DataFrame:
        """Enrich all keywords using optimal batch processing."""
        all_enriched_data = []
        batches = list(self.chunk_keywords(keywords, self.optimal_batch_size))
        
        logger.info(f"Processing {len(keywords)} keywords in {len(batches)} batches of {self.optimal_batch_size}")
        
        for i, batch in enumerate(batches):
            batch_start_time = time.time()
            
            logger.info(f"Processing batch {i+1}/{len(batches)} ({len(batch)} keywords)")
            
            # Call Google Ads API
            api_response, response_time = self.call_google_ads_api(batch, "historical-metrics")
            
            # Process results
            batch_results = self.process_batch_results(api_response, batch)
            all_enriched_data.extend(batch_results)
            
            # Progress reporting
            if progress_callback:
                progress_callback(i + 1, len(batches), response_time, api_response.get("status"))
            
            # Log batch summary
            successful_matches = sum(1 for r in batch_results if r.get("api_match", False))
            logger.info(f"Batch {i+1} complete: {successful_matches}/{len(batch)} matches, {response_time:.2f}s")
            
            # Rate limiting delay
            if i < len(batches) - 1:
                time.sleep(self.api_delay)
        
        # Convert to DataFrame
        df = pd.DataFrame(all_enriched_data)
        
        # Add summary statistics
        total_matches = df['api_match'].sum() if 'api_match' in df.columns else 0
        logger.info(f"Enrichment complete: {total_matches}/{len(keywords)} keywords matched ({total_matches/len(keywords)*100:.1f}%)")
        
        return df
    
    def save_enriched_data(self, df: pd.DataFrame, filename_suffix: str = "") -> str:
        """Save enriched data to CSV with timestamp."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"enriched_keywords_{timestamp}{filename_suffix}.csv"
        filepath = self.results_dir / filename
        
        df.to_csv(filepath, index=False)
        logger.info(f"Enriched data saved to: {filepath}")
        
        return str(filepath)
    
    def generate_enrichment_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate summary statistics for enriched data."""
        summary = {
            "total_keywords": len(df),
            "successful_matches": df['api_match'].sum() if 'api_match' in df.columns else 0,
            "match_rate": (df['api_match'].sum() / len(df) * 100) if 'api_match' in df.columns else 0,
            "keywords_with_historical_data": df['has_historical_data'].sum() if 'has_historical_data' in df.columns else 0,
            "avg_monthly_searches": {
                "mean": df['avg_monthly_searches'].mean(),
                "median": df['avg_monthly_searches'].median(),
                "max": df['avg_monthly_searches'].max(),
                "min": df['avg_monthly_searches'].min()
            },
            "competition_distribution": df['competition'].value_counts().to_dict() if 'competition' in df.columns else {},
            "trend_analysis": {
                "positive_trends": (df['volume_trend_coef'] > 0.01).sum() if 'volume_trend_coef' in df.columns else 0,
                "negative_trends": (df['volume_trend_coef'] < -0.01).sum() if 'volume_trend_coef' in df.columns else 0,
                "stable_trends": ((df['volume_trend_coef'] >= -0.01) & (df['volume_trend_coef'] <= 0.01)).sum() if 'volume_trend_coef' in df.columns else 0
            }
        }
        
        return summary

def main():
    """Main function to run keyword enrichment."""
    enricher = KeywordEnrichmentEngine()
    
    # Load keywords
    keywords = enricher.load_keyword_master_list()
    if not keywords:
        logger.error("No keywords loaded. Exiting.")
        return
    
    print(f"Starting enrichment of {len(keywords)} keywords...")
    
    # Define progress callback
    def progress_callback(batch_num, total_batches, response_time, status):
        print(f"Progress: {batch_num}/{total_batches} batches complete ({response_time:.2f}s, {status})")
    
    # Enrich keywords
    enriched_df = enricher.enrich_keywords_bulk(keywords, progress_callback)
    
    # Save results
    output_file = enricher.save_enriched_data(enriched_df)
    
    # Generate summary
    summary = enricher.generate_enrichment_summary(enriched_df)
    
    print("\n" + "="*60)
    print("KEYWORD ENRICHMENT SUMMARY")
    print("="*60)
    print(f"Total keywords processed: {summary['total_keywords']:,}")
    print(f"Successful API matches: {summary['successful_matches']:,} ({summary['match_rate']:.1f}%)")
    print(f"Keywords with historical data: {summary['keywords_with_historical_data']:,}")
    print(f"Average monthly searches: {summary['avg_monthly_searches']['mean']:.0f}")
    print(f"Trend analysis:")
    print(f"  - Positive trends: {summary['trend_analysis']['positive_trends']:,}")
    print(f"  - Negative trends: {summary['trend_analysis']['negative_trends']:,}")
    print(f"  - Stable trends: {summary['trend_analysis']['stable_trends']:,}")
    print(f"\nOutput file: {output_file}")
    print("="*60)

if __name__ == "__main__":
    main()
