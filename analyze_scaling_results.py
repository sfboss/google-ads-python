#!/usr/bin/env python
"""
Keyword Scaling Test Results Analyzer

This script analyzes the results from the keyword scaling test and generates
visualizations and insights about API performance with different batch sizes.
"""

import json
import csv
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import argparse
import glob

def load_test_results(results_dir: str) -> dict:
    """Load the most recent test results from the results directory."""
    results_path = Path(results_dir)
    
    # Find the most recent detailed results file
    json_files = list(results_path.glob("scaling_test_detailed_*.json"))
    if not json_files:
        raise FileNotFoundError(f"No test results found in {results_dir}")
    
    latest_file = max(json_files, key=lambda x: x.stat().st_mtime)
    print(f"Loading results from: {latest_file}")
    
    with open(latest_file, 'r', encoding='utf-8') as f:
        return json.load(f)

def load_summary_csv(results_dir: str) -> pd.DataFrame:
    """Load the most recent summary CSV."""
    results_path = Path(results_dir)
    
    # Find the most recent summary CSV file
    csv_files = list(results_path.glob("scaling_test_summary_*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No summary CSV found in {results_dir}")
    
    latest_file = max(csv_files, key=lambda x: x.stat().st_mtime)
    print(f"Loading summary from: {latest_file}")
    
    return pd.read_csv(latest_file)

def create_visualizations(df: pd.DataFrame, results_dir: str):
    """Create visualizations of the test results."""
    plt.style.use('seaborn-v0_8')
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle('Google Ads Keyword API Scaling Analysis', fontsize=16, fontweight='bold')
    
    # Response Time Analysis
    axes[0, 0].plot(df['batch_size'], df['ideas_avg_response_time'], 'o-', label='Keyword Ideas', linewidth=2)
    axes[0, 0].plot(df['batch_size'], df['metrics_avg_response_time'], 's-', label='Historical Metrics', linewidth=2)
    axes[0, 0].set_xlabel('Batch Size (Number of Keywords)')
    axes[0, 0].set_ylabel('Average Response Time (seconds)')
    axes[0, 0].set_title('Response Time vs Batch Size')
    axes[0, 0].legend()
    axes[0, 0].grid(True, alpha=0.3)
    
    # Success Rate Analysis
    axes[0, 1].plot(df['batch_size'], df['ideas_success_rate'] * 100, 'o-', label='Keyword Ideas', linewidth=2)
    axes[0, 1].plot(df['batch_size'], df['metrics_success_rate'] * 100, 's-', label='Historical Metrics', linewidth=2)
    axes[0, 1].set_xlabel('Batch Size (Number of Keywords)')
    axes[0, 1].set_ylabel('Success Rate (%)')
    axes[0, 1].set_title('API Success Rate vs Batch Size')
    axes[0, 1].legend()
    axes[0, 1].grid(True, alpha=0.3)
    axes[0, 1].set_ylim(0, 105)
    
    # Results Quantity Analysis
    axes[0, 2].plot(df['batch_size'], df['ideas_avg_total_results'], 'o-', label='Keyword Ideas', linewidth=2)
    axes[0, 2].plot(df['batch_size'], df['metrics_avg_total_results'], 's-', label='Historical Metrics', linewidth=2)
    axes[0, 2].set_xlabel('Batch Size (Number of Keywords)')
    axes[0, 2].set_ylabel('Average Number of Results')
    axes[0, 2].set_title('Result Quantity vs Batch Size')
    axes[0, 2].legend()
    axes[0, 2].grid(True, alpha=0.3)
    
    # Data Quality - Monthly Data Availability
    axes[1, 0].plot(df['batch_size'], df['ideas_monthly_data_ratio'] * 100, 'o-', label='Keyword Ideas', linewidth=2)
    axes[1, 0].plot(df['batch_size'], df['metrics_monthly_data_ratio'] * 100, 's-', label='Historical Metrics', linewidth=2)
    axes[1, 0].set_xlabel('Batch Size (Number of Keywords)')
    axes[1, 0].set_ylabel('Monthly Data Availability (%)')
    axes[1, 0].set_title('Data Quality vs Batch Size')
    axes[1, 0].legend()
    axes[1, 0].grid(True, alpha=0.3)
    
    # Efficiency Analysis (Results per Second)
    df['ideas_efficiency'] = df['ideas_avg_total_results'] / df['ideas_avg_response_time']
    df['metrics_efficiency'] = df['metrics_avg_total_results'] / df['metrics_avg_response_time']
    
    axes[1, 1].plot(df['batch_size'], df['ideas_efficiency'], 'o-', label='Keyword Ideas', linewidth=2)
    axes[1, 1].plot(df['batch_size'], df['metrics_efficiency'], 's-', label='Historical Metrics', linewidth=2)
    axes[1, 1].set_xlabel('Batch Size (Number of Keywords)')
    axes[1, 1].set_ylabel('Results per Second')
    axes[1, 1].set_title('API Efficiency vs Batch Size')
    axes[1, 1].legend()
    axes[1, 1].grid(True, alpha=0.3)
    
    # Unique Suggestions (for Keyword Ideas only)
    axes[1, 2].plot(df['batch_size'], df['ideas_avg_unique_suggestions'], 'o-', color='blue', linewidth=2)
    axes[1, 2].set_xlabel('Batch Size (Number of Keywords)')
    axes[1, 2].set_ylabel('Average Unique Suggestions')
    axes[1, 2].set_title('Unique Keyword Suggestions vs Batch Size')
    axes[1, 2].grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # Save the plot
    plot_filename = Path(results_dir) / 'scaling_analysis_charts.png'
    plt.savefig(plot_filename, dpi=300, bbox_inches='tight')
    print(f"Visualizations saved to: {plot_filename}")
    
    plt.show()

def generate_insights(df: pd.DataFrame) -> str:
    """Generate textual insights from the analysis."""
    insights = []
    insights.append("GOOGLE ADS KEYWORD API SCALING ANALYSIS INSIGHTS")
    insights.append("=" * 60)
    
    # Response time analysis
    max_response_ideas = df.loc[df['ideas_avg_response_time'].idxmax()]
    max_response_metrics = df.loc[df['metrics_avg_response_time'].idxmax()]
    
    insights.append(f"\n📊 RESPONSE TIME ANALYSIS:")
    insights.append(f"• Keyword Ideas API: Peak response time {max_response_ideas['ideas_avg_response_time']:.2f}s at batch size {max_response_ideas['batch_size']}")
    insights.append(f"• Historical Metrics API: Peak response time {max_response_metrics['metrics_avg_response_time']:.2f}s at batch size {max_response_metrics['batch_size']}")
    
    # Calculate response time growth rate
    min_batch = df['batch_size'].min()
    max_batch = df['batch_size'].max()
    ideas_growth = ((df[df['batch_size'] == max_batch]['ideas_avg_response_time'].iloc[0] / 
                    df[df['batch_size'] == min_batch]['ideas_avg_response_time'].iloc[0]) - 1) * 100
    
    insights.append(f"• Response time growth from {min_batch} to {max_batch} keywords: {ideas_growth:.1f}%")
    
    # Success rate analysis
    min_success_ideas = df['ideas_success_rate'].min()
    min_success_metrics = df['metrics_success_rate'].min()
    
    insights.append(f"\n✅ SUCCESS RATE ANALYSIS:")
    insights.append(f"• Keyword Ideas API: Minimum success rate {min_success_ideas:.1%}")
    insights.append(f"• Historical Metrics API: Minimum success rate {min_success_metrics:.1%}")
    
    if min_success_ideas < 1.0 or min_success_metrics < 1.0:
        insights.append("⚠️  WARNING: Some batch sizes show reduced success rates!")
    
    # Data quality analysis
    avg_monthly_data_ideas = df['ideas_monthly_data_ratio'].mean()
    avg_monthly_data_metrics = df['metrics_monthly_data_ratio'].mean()
    
    insights.append(f"\n📈 DATA QUALITY ANALYSIS:")
    insights.append(f"• Average monthly data availability (Ideas): {avg_monthly_data_ideas:.1%}")
    insights.append(f"• Average monthly data availability (Metrics): {avg_monthly_data_metrics:.1%}")
    
    # Find optimal batch size
    df['ideas_efficiency'] = df['ideas_avg_total_results'] / df['ideas_avg_response_time']
    optimal_batch = df.loc[df['ideas_efficiency'].idxmax()]
    
    insights.append(f"\n🎯 OPTIMIZATION RECOMMENDATIONS:")
    insights.append(f"• Most efficient batch size: {optimal_batch['batch_size']} keywords")
    insights.append(f"  - Response time: {optimal_batch['ideas_avg_response_time']:.2f}s")
    insights.append(f"  - Results per second: {optimal_batch['ideas_efficiency']:.1f}")
    insights.append(f"  - Success rate: {optimal_batch['ideas_success_rate']:.1%}")
    
    # Diminishing returns analysis
    if len(df) > 1:
        # Calculate efficiency drop-off
        max_efficiency = df['ideas_efficiency'].max()
        efficiency_threshold = max_efficiency * 0.8  # 80% of peak efficiency
        
        efficient_batches = df[df['ideas_efficiency'] >= efficiency_threshold]
        max_efficient_batch = efficient_batches['batch_size'].max()
        
        insights.append(f"• Diminishing returns threshold: ~{max_efficient_batch} keywords")
        insights.append(f"  (Beyond this, efficiency drops below 80% of peak)")
    
    # Rate limiting detection
    timeout_batches = df[df['ideas_avg_response_time'] > 10]  # More than 10 seconds
    if not timeout_batches.empty:
        insights.append(f"\n⏱️  PERFORMANCE WARNINGS:")
        insights.append(f"• Slow response times (>10s) detected at batch sizes: {list(timeout_batches['batch_size'])}")
    
    return "\n".join(insights)

def main():
    """Main function to analyze keyword scaling test results."""
    parser = argparse.ArgumentParser(description="Analyze Google Ads Keyword Scaling Test Results")
    parser.add_argument("--results-dir", default="results/keyword_scaling_test",
                       help="Directory containing test results")
    parser.add_argument("--no-charts", action="store_true",
                       help="Skip generating charts")
    
    args = parser.parse_args()
    
    try:
        # Load results
        print("Loading test results...")
        df = load_summary_csv(args.results_dir)
        
        print(f"Loaded data for {len(df)} batch sizes")
        print(f"Batch size range: {df['batch_size'].min()} - {df['batch_size'].max()}")
        
        # Generate insights
        insights = generate_insights(df)
        print("\n" + insights)
        
        # Save insights to file
        insights_file = Path(args.results_dir) / "analysis_insights.txt"
        with open(insights_file, 'w', encoding='utf-8') as f:
            f.write(insights)
        print(f"\nInsights saved to: {insights_file}")
        
        # Generate visualizations
        if not args.no_charts:
            try:
                print("\nGenerating visualizations...")
                create_visualizations(df, args.results_dir)
            except ImportError as e:
                print(f"Visualization libraries not available: {e}")
                print("Install matplotlib and seaborn to generate charts")
        
    except Exception as e:
        print(f"Error analyzing results: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
