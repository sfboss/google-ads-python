#!/usr/bin/env python
"""
Progress Monitor for Keyword Enrichment
"""

import time
import pandas as pd
from pathlib import Path
import glob

def monitor_progress():
    """Monitor enrichment progress."""
    print("📊 Keyword Enrichment Progress Monitor")
    print("=" * 50)
    
    # Check for output files
    results_dir = Path("results")
    if results_dir.exists():
        csv_files = list(results_dir.glob("enriched_keywords_*.csv"))
        
        if csv_files:
            # Get latest file
            latest_file = max(csv_files, key=lambda f: f.stat().st_mtime)
            
            try:
                df = pd.read_csv(latest_file)
                total_keywords = len(df)
                matches = df['api_match'].sum() if 'api_match' in df.columns else 0
                match_rate = matches / total_keywords * 100 if total_keywords > 0 else 0
                
                print(f"📁 Latest file: {latest_file.name}")
                print(f"📊 Keywords processed: {total_keywords:,}")
                print(f"✅ Successful matches: {matches:,} ({match_rate:.1f}%)")
                
                if 'has_historical_data' in df.columns:
                    historical_data = df['has_historical_data'].sum()
                    print(f"📈 With historical data: {historical_data:,}")
                
                if 'avg_monthly_searches' in df.columns:
                    avg_volume = df['avg_monthly_searches'].mean()
                    print(f"📊 Avg monthly searches: {avg_volume:.0f}")
                
                # Sample of high-volume keywords
                if 'avg_monthly_searches' in df.columns:
                    top_keywords = df.nlargest(5, 'avg_monthly_searches')[['keyword', 'avg_monthly_searches']]
                    print("\n🔥 Top Volume Keywords:")
                    for _, row in top_keywords.iterrows():
                        print(f"   • {row['keyword']}: {row['avg_monthly_searches']:,}")
                        
            except Exception as e:
                print(f"❌ Error reading file: {e}")
        else:
            print("⏳ No output files found yet...")
    else:
        print("⏳ Results directory not found yet...")

if __name__ == "__main__":
    monitor_progress()
