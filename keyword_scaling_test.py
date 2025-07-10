#!/usr/bin/env python
"""
Google Ads Keyword Scaling Test

This script tests the Google Ads Keyword Planner API behavior with increasing
numbers of seed keywords to analyze:
1. Response time degradation
2. Quality/quantity of keyword suggestions
3. Historical data completeness
4. API rate limiting or service quality degradation

The test uses keywords from keyword_master_list.txt and sends requests with
progressively larger batches to identify optimal batch sizes and potential
diminishing returns.
"""

import json
import time
import csv
import statistics
from datetime import datetime
from typing import List, Dict, Any, Tuple
import subprocess
import random
import os

# Configuration
CUSTOMER_ID = "3399365278"
KEYWORD_LIST_FILE = "keyword_master_list.txt"
RESULTS_DIR = "results/keyword_scaling_test"
MAX_KEYWORDS_PER_TEST = 800  # Maximum keywords to test (Google's limit is around 1000)
BATCH_SIZES = [1, 3, 5, 10, 20, 30, 50, 75, 100, 150, 200, 300, 400, 500, 600, 700, 800]
ITERATIONS_PER_BATCH = 3  # Number of times to test each batch size for averaging
DELAY_BETWEEN_REQUESTS = 2  # Seconds to wait between API calls

class KeywordScalingTester:
    """Test class for analyzing Google Ads Keyword API scaling behavior."""
    
    def __init__(self):
        """Initialize the tester with configuration and result storage."""
        self.results = []
        self.keyword_pool = []
        self.ensure_results_directory()
        
    def ensure_results_directory(self):
        """Create results directory if it doesn't exist."""
        os.makedirs(RESULTS_DIR, exist_ok=True)
        
    def load_keywords(self) -> List[str]:
        """Load keywords from the master list file.
        
        Returns:
            List of unique keywords from the file.
        """
        keywords = []
        try:
            with open(KEYWORD_LIST_FILE, 'r', encoding='utf-8') as f:
                keywords = [line.strip() for line in f if line.strip()]
            
            # Remove duplicates while preserving order
            seen = set()
            unique_keywords = []
            for kw in keywords:
                if kw not in seen:
                    seen.add(kw)
                    unique_keywords.append(kw)
            
            print(f"Loaded {len(unique_keywords)} unique keywords from {KEYWORD_LIST_FILE}")
            return unique_keywords
            
        except FileNotFoundError:
            print(f"Error: Could not find {KEYWORD_LIST_FILE}")
            return []
        except Exception as e:
            print(f"Error loading keywords: {e}")
            return []
    
    def call_keyword_ideas_api(self, keywords: List[str]) -> Tuple[Dict[str, Any], float]:
        """Call the Google Ads Keyword Ideas API with the given keywords.
        
        Args:
            keywords: List of seed keywords to send to the API.
            
        Returns:
            Tuple of (API response dict, response time in seconds).
        """
        start_time = time.time()
        
        # Build command
        cmd = [
            "python", "adwords_service.py", "keyword-ideas",
            "-c", CUSTOMER_ID
        ]
        
        # Add keywords
        for keyword in keywords:
            cmd.extend(["-k", keyword])
        
        try:
            # Execute API call
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)  # 5 min timeout
            response_time = time.time() - start_time
            
            # Parse JSON response
            if result.stdout:
                try:
                    data = json.loads(result.stdout)
                    return data, response_time
                except json.JSONDecodeError as e:
                    return {
                        "status": "error", 
                        "error": {"message": f"JSON decode error: {e}", "stdout": result.stdout}
                    }, response_time
            else:
                return {
                    "status": "error", 
                    "error": {"message": "No output from API", "stderr": result.stderr}
                }, response_time
                
        except subprocess.TimeoutExpired:
            return {
                "status": "error", 
                "error": {"message": "API call timeout (>5 minutes)"}
            }, 300.0
        except Exception as e:
            return {
                "status": "error", 
                "error": {"message": f"Subprocess error: {e}"}
            }, time.time() - start_time
    
    def call_historical_metrics_api(self, keywords: List[str]) -> Tuple[Dict[str, Any], float]:
        """Call the Google Ads Historical Metrics API with the given keywords.
        
        Args:
            keywords: List of keywords to get historical metrics for.
            
        Returns:
            Tuple of (API response dict, response time in seconds).
        """
        start_time = time.time()
        
        # Build command
        cmd = [
            "python", "adwords_service.py", "historical-metrics",
            "-c", CUSTOMER_ID
        ]
        
        # Add keywords
        for keyword in keywords:
            cmd.extend(["-k", keyword])
        
        try:
            # Execute API call
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)  # 5 min timeout
            response_time = time.time() - start_time
            
            # Parse JSON response
            if result.stdout:
                try:
                    data = json.loads(result.stdout)
                    return data, response_time
                except json.JSONDecodeError as e:
                    return {
                        "status": "error", 
                        "error": {"message": f"JSON decode error: {e}", "stdout": result.stdout}
                    }, response_time
            else:
                return {
                    "status": "error", 
                    "error": {"message": "No output from API", "stderr": result.stderr}
                }, response_time
                
        except subprocess.TimeoutExpired:
            return {
                "status": "error", 
                "error": {"message": "API call timeout (>5 minutes)"}
            }, 300.0
        except Exception as e:
            return {
                "status": "error", 
                "error": {"message": f"Subprocess error: {e}"}
            }, time.time() - start_time
    
    def analyze_response_quality(self, response: Dict[str, Any], api_type: str) -> Dict[str, Any]:
        """Analyze the quality and completeness of an API response.
        
        Args:
            response: The API response to analyze.
            api_type: Type of API call ('keyword-ideas' or 'historical-metrics').
            
        Returns:
            Dictionary containing quality metrics.
        """
        analysis = {
            "success": response.get("status") == "success",
            "total_results": 0,
            "results_with_monthly_data": 0,
            "avg_monthly_searches_available": 0,
            "competition_data_available": 0,
            "bid_data_available": 0,
            "close_variants_available": 0,
            "unique_suggestions": 0
        }
        
        if not analysis["success"]:
            analysis["error_message"] = response.get("error", {}).get("message", "Unknown error")
            return analysis
        
        # Analyze keyword ideas response
        if api_type == "keyword-ideas":
            keyword_ideas = response.get("keyword_ideas", [])
            analysis["total_results"] = len(keyword_ideas)
            
            if keyword_ideas:
                monthly_data_count = 0
                avg_searches_count = 0
                competition_count = 0
                bid_data_count = 0
                close_variants_count = 0
                unique_texts = set()
                
                for idea in keyword_ideas:
                    # Track unique suggestions
                    unique_texts.add(idea.get("text", ""))
                    
                    # Monthly search volumes
                    if idea.get("monthly_search_volumes"):
                        monthly_data_count += 1
                    
                    # Average monthly searches
                    if idea.get("avg_monthly_searches") is not None:
                        avg_searches_count += 1
                    
                    # Competition data
                    if idea.get("competition"):
                        competition_count += 1
                    
                    # Bid data
                    if (idea.get("low_top_of_page_bid_micros") is not None or 
                        idea.get("high_top_of_page_bid_micros") is not None):
                        bid_data_count += 1
                    
                    # Close variants
                    if idea.get("close_variants"):
                        close_variants_count += 1
                
                analysis["results_with_monthly_data"] = monthly_data_count
                analysis["avg_monthly_searches_available"] = avg_searches_count
                analysis["competition_data_available"] = competition_count
                analysis["bid_data_available"] = bid_data_count
                analysis["close_variants_available"] = close_variants_count
                analysis["unique_suggestions"] = len(unique_texts)
        
        # Analyze historical metrics response
        elif api_type == "historical-metrics":
            historical_metrics = response.get("historical_metrics", [])
            analysis["total_results"] = len(historical_metrics)
            
            if historical_metrics:
                monthly_data_count = 0
                avg_searches_count = 0
                competition_count = 0
                bid_data_count = 0
                close_variants_count = 0
                
                for metric in historical_metrics:
                    # Monthly search volumes
                    if metric.get("monthly_search_volumes"):
                        monthly_data_count += 1
                    
                    # Average monthly searches
                    if metric.get("avg_monthly_searches") is not None:
                        avg_searches_count += 1
                    
                    # Competition data
                    if metric.get("competition"):
                        competition_count += 1
                    
                    # Bid data
                    if (metric.get("low_top_of_page_bid_micros") is not None or 
                        metric.get("high_top_of_page_bid_micros") is not None):
                        bid_data_count += 1
                    
                    # Close variants
                    if metric.get("close_variants"):
                        close_variants_count += 1
                
                analysis["results_with_monthly_data"] = monthly_data_count
                analysis["avg_monthly_searches_available"] = avg_searches_count
                analysis["competition_data_available"] = competition_count
                analysis["bid_data_available"] = bid_data_count
                analysis["close_variants_available"] = close_variants_count
        
        return analysis
    
    def run_batch_test(self, batch_size: int, iteration: int) -> Dict[str, Any]:
        """Run a single test with a specific batch size.
        
        Args:
            batch_size: Number of keywords to include in this test.
            iteration: Which iteration this is (for averaging).
            
        Returns:
            Dictionary containing test results.
        """
        print(f"  Running batch size {batch_size}, iteration {iteration + 1}")
        
        # Select random sample of keywords
        if batch_size > len(self.keyword_pool):
            selected_keywords = self.keyword_pool[:]  # Use all available
        else:
            selected_keywords = random.sample(self.keyword_pool, batch_size)
        
        test_result = {
            "batch_size": batch_size,
            "iteration": iteration,
            "timestamp": datetime.now().isoformat(),
            "selected_keywords": selected_keywords[:10],  # Store first 10 for reference
            "actual_keywords_sent": len(selected_keywords)
        }
        
        # Test Keyword Ideas API
        print(f"    Testing Keyword Ideas API...")
        ideas_response, ideas_time = self.call_keyword_ideas_api(selected_keywords)
        ideas_analysis = self.analyze_response_quality(ideas_response, "keyword-ideas")
        
        test_result["keyword_ideas"] = {
            "response_time": ideas_time,
            "analysis": ideas_analysis,
            "raw_response": ideas_response  # Store for detailed analysis
        }
        
        # Wait between API calls
        time.sleep(DELAY_BETWEEN_REQUESTS)
        
        # Test Historical Metrics API
        print(f"    Testing Historical Metrics API...")
        metrics_response, metrics_time = self.call_historical_metrics_api(selected_keywords)
        metrics_analysis = self.analyze_response_quality(metrics_response, "historical-metrics")
        
        test_result["historical_metrics"] = {
            "response_time": metrics_time,
            "analysis": metrics_analysis,
            "raw_response": metrics_response  # Store for detailed analysis
        }
        
        return test_result
    
    def run_scaling_test(self):
        """Run the complete scaling test across all batch sizes."""
        print("Starting Google Ads Keyword Scaling Test")
        print(f"Testing batch sizes: {BATCH_SIZES}")
        print(f"Iterations per batch: {ITERATIONS_PER_BATCH}")
        print(f"Delay between requests: {DELAY_BETWEEN_REQUESTS}s")
        print("=" * 60)
        
        # Load keywords
        self.keyword_pool = self.load_keywords()
        if not self.keyword_pool:
            print("Error: No keywords loaded. Exiting.")
            return
        
        # Filter batch sizes to available keywords
        max_available = len(self.keyword_pool)
        valid_batch_sizes = [bs for bs in BATCH_SIZES if bs <= max_available]
        
        if len(valid_batch_sizes) < len(BATCH_SIZES):
            print(f"Note: Limited to batch sizes {valid_batch_sizes} due to {max_available} available keywords")
        
        # Run tests
        total_tests = len(valid_batch_sizes) * ITERATIONS_PER_BATCH
        current_test = 0
        
        for batch_size in valid_batch_sizes:
            print(f"\nTesting batch size: {batch_size}")
            
            for iteration in range(ITERATIONS_PER_BATCH):
                current_test += 1
                print(f"Progress: {current_test}/{total_tests}")
                
                try:
                    result = self.run_batch_test(batch_size, iteration)
                    self.results.append(result)
                    
                    # Save intermediate results
                    self.save_results()
                    
                except Exception as e:
                    print(f"    Error in batch test: {e}")
                    # Continue with next test
                    continue
                
                # Wait between batches (except for last test)
                if current_test < total_tests:
                    print(f"    Waiting {DELAY_BETWEEN_REQUESTS}s before next test...")
                    time.sleep(DELAY_BETWEEN_REQUESTS)
        
        print("\n" + "=" * 60)
        print("Scaling test completed!")
        self.save_results()
        self.generate_summary_report()
    
    def save_results(self):
        """Save detailed results to JSON file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{RESULTS_DIR}/scaling_test_detailed_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump({
                "test_config": {
                    "customer_id": CUSTOMER_ID,
                    "batch_sizes": BATCH_SIZES,
                    "iterations_per_batch": ITERATIONS_PER_BATCH,
                    "total_keywords_available": len(self.keyword_pool),
                    "delay_between_requests": DELAY_BETWEEN_REQUESTS
                },
                "results": self.results
            }, f, indent=2, ensure_ascii=False)
        
        print(f"Detailed results saved to: {filename}")
    
    def generate_summary_report(self):
        """Generate a summary CSV report with key metrics."""
        if not self.results:
            print("No results to summarize.")
            return
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Generate summary statistics
        summary_data = []
        
        # Group results by batch size
        batch_groups = {}
        for result in self.results:
            batch_size = result["batch_size"]
            if batch_size not in batch_groups:
                batch_groups[batch_size] = []
            batch_groups[batch_size].append(result)
        
        # Calculate aggregated metrics for each batch size
        for batch_size, batch_results in sorted(batch_groups.items()):
            # Keyword Ideas metrics
            ideas_times = []
            ideas_success_rate = 0
            ideas_total_results = []
            ideas_monthly_data = []
            ideas_unique_suggestions = []
            
            # Historical Metrics metrics
            metrics_times = []
            metrics_success_rate = 0
            metrics_total_results = []
            metrics_monthly_data = []
            
            for result in batch_results:
                # Keyword Ideas
                if result["keyword_ideas"]["analysis"]["success"]:
                    ideas_success_rate += 1
                    ideas_times.append(result["keyword_ideas"]["response_time"])
                    ideas_total_results.append(result["keyword_ideas"]["analysis"]["total_results"])
                    ideas_monthly_data.append(result["keyword_ideas"]["analysis"]["results_with_monthly_data"])
                    ideas_unique_suggestions.append(result["keyword_ideas"]["analysis"]["unique_suggestions"])
                
                # Historical Metrics
                if result["historical_metrics"]["analysis"]["success"]:
                    metrics_success_rate += 1
                    metrics_times.append(result["historical_metrics"]["response_time"])
                    metrics_total_results.append(result["historical_metrics"]["analysis"]["total_results"])
                    metrics_monthly_data.append(result["historical_metrics"]["analysis"]["results_with_monthly_data"])
            
            summary_row = {
                "batch_size": batch_size,
                "total_iterations": len(batch_results),
                
                # Keyword Ideas Summary
                "ideas_success_rate": ideas_success_rate / len(batch_results),
                "ideas_avg_response_time": statistics.mean(ideas_times) if ideas_times else 0,
                "ideas_median_response_time": statistics.median(ideas_times) if ideas_times else 0,
                "ideas_avg_total_results": statistics.mean(ideas_total_results) if ideas_total_results else 0,
                "ideas_avg_monthly_data": statistics.mean(ideas_monthly_data) if ideas_monthly_data else 0,
                "ideas_avg_unique_suggestions": statistics.mean(ideas_unique_suggestions) if ideas_unique_suggestions else 0,
                
                # Historical Metrics Summary
                "metrics_success_rate": metrics_success_rate / len(batch_results),
                "metrics_avg_response_time": statistics.mean(metrics_times) if metrics_times else 0,
                "metrics_median_response_time": statistics.median(metrics_times) if metrics_times else 0,
                "metrics_avg_total_results": statistics.mean(metrics_total_results) if metrics_total_results else 0,
                "metrics_avg_monthly_data": statistics.mean(metrics_monthly_data) if metrics_monthly_data else 0,
                
                # Quality ratios
                "ideas_monthly_data_ratio": (statistics.mean(ideas_monthly_data) / statistics.mean(ideas_total_results)) if ideas_total_results and statistics.mean(ideas_total_results) > 0 else 0,
                "metrics_monthly_data_ratio": (statistics.mean(metrics_monthly_data) / statistics.mean(metrics_total_results)) if metrics_total_results and statistics.mean(metrics_total_results) > 0 else 0,
            }
            
            summary_data.append(summary_row)
        
        # Save summary CSV
        csv_filename = f"{RESULTS_DIR}/scaling_test_summary_{timestamp}.csv"
        
        if summary_data:
            with open(csv_filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=summary_data[0].keys())
                writer.writeheader()
                writer.writerows(summary_data)
            
            print(f"Summary report saved to: {csv_filename}")
            
            # Print key findings
            print("\n" + "=" * 60)
            print("KEY FINDINGS:")
            print("=" * 60)
            
            # Response time trends
            print("\nResponse Time Analysis:")
            for row in summary_data:
                print(f"Batch Size {row['batch_size']:3d}: "
                      f"Ideas={row['ideas_avg_response_time']:.2f}s, "
                      f"Metrics={row['metrics_avg_response_time']:.2f}s")
            
            # Success rate analysis
            print("\nSuccess Rate Analysis:")
            for row in summary_data:
                print(f"Batch Size {row['batch_size']:3d}: "
                      f"Ideas={row['ideas_success_rate']:.1%}, "
                      f"Metrics={row['metrics_success_rate']:.1%}")
            
            # Data quality analysis
            print("\nData Quality Analysis (Monthly Data Ratio):")
            for row in summary_data:
                print(f"Batch Size {row['batch_size']:3d}: "
                      f"Ideas={row['ideas_monthly_data_ratio']:.1%}, "
                      f"Metrics={row['metrics_monthly_data_ratio']:.1%}")
        
        print("\n" + "=" * 60)
        print("Test completed! Check the results directory for detailed analysis.")


def main():
    """Main function to run the keyword scaling test."""
    print("Google Ads Keyword Scaling Test")
    print("This test will analyze API behavior with increasing keyword batch sizes")
    print("to identify optimal request sizes and potential diminishing returns.")
    print()
    
    # Check if keyword file exists
    if not os.path.exists(KEYWORD_LIST_FILE):
        print(f"Error: Keyword file '{KEYWORD_LIST_FILE}' not found.")
        print("Please ensure the keyword master list file exists.")
        return
    
    # Initialize and run test
    tester = KeywordScalingTester()
    
    try:
        tester.run_scaling_test()
    except KeyboardInterrupt:
        print("\nTest interrupted by user.")
        if tester.results:
            print("Saving partial results...")
            tester.save_results()
            tester.generate_summary_report()
    except Exception as e:
        print(f"Test failed with error: {e}")
        if tester.results:
            print("Saving partial results...")
            tester.save_results()


if __name__ == "__main__":
    main()
