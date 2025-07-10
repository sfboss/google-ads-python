#!/usr/bin/env python
"""
Optimized Google Ads Keyword Research Client

Based on the scaling analysis, this script demonstrates optimal usage patterns
for the Google Ads Keyword Planner API with different batch sizes.
"""

import time
import json
import sys
from typing import List, Dict, Any
from itertools import islice
import subprocess

# Configuration based on scaling test results
OPTIMAL_BATCH_SIZES = {
    "real_time": 10,      # For real-time applications (1.5-2s response)
    "standard": 200,      # For standard research (1.3s response)
    "bulk": 500,          # For bulk operations (1.45s response, best efficiency)
    "maximum": 800        # Maximum tested (1.5s response, minor quality degradation)
}

CUSTOMER_ID = "3399365278"
API_DELAY = 2.5  # Seconds between requests (based on rate limiting observations)

class OptimizedKeywordResearcher:
    """Optimized keyword research client using Google Ads API."""
    
    def __init__(self, customer_id: str = CUSTOMER_ID):
        """Initialize the researcher with customer ID."""
        self.customer_id = customer_id
        self.total_requests = 0
        self.total_response_time = 0
        
    def chunk_keywords(self, keywords: List[str], chunk_size: int) -> List[List[str]]:
        """Split keywords into optimal batch sizes."""
        it = iter(keywords)
        while True:
            chunk = list(islice(it, chunk_size))
            if not chunk:
                break
            yield chunk
    
    def call_keyword_ideas_api(self, keywords: List[str]) -> Dict[str, Any]:
        """Call the Keyword Ideas API with error handling and timing."""
        start_time = time.time()
        
        cmd = [
            "python", "adwords_service.py", "keyword-ideas",
            "-c", self.customer_id, "--page_size", "1000"
        ]
        
        for keyword in keywords:
            cmd.extend(["-k", keyword])
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            response_time = time.time() - start_time
            
            if result.stdout:
                try:
                    data = json.loads(result.stdout)
                    self.total_requests += 1
                    self.total_response_time += response_time
                    return data, response_time
                except json.JSONDecodeError:
                    return {"status": "error", "error": "JSON decode error"}, response_time
            else:
                return {"status": "error", "error": result.stderr}, response_time
                
        except subprocess.TimeoutExpired:
            return {"status": "error", "error": "Timeout"}, 30.0
        except Exception as e:
            return {"status": "error", "error": str(e)}, time.time() - start_time
    
    def research_keywords_optimized(
        self, 
        keywords: List[str], 
        mode: str = "standard",
        progress_callback=None
    ) -> List[Dict[str, Any]]:
        """
        Research keywords using optimal batch sizes.
        
        Args:
            keywords: List of seed keywords to research
            mode: One of 'real_time', 'standard', 'bulk', 'maximum'
            progress_callback: Optional function to call with progress updates
            
        Returns:
            List of API responses with metadata
        """
        if mode not in OPTIMAL_BATCH_SIZES:
            raise ValueError(f"Mode must be one of: {list(OPTIMAL_BATCH_SIZES.keys())}")
        
        batch_size = OPTIMAL_BATCH_SIZES[mode]
        chunks = list(self.chunk_keywords(keywords, batch_size))
        results = []
        
        print(f"Processing {len(keywords)} keywords in {len(chunks)} batches of {batch_size}")
        print(f"Expected total time: ~{len(chunks) * (1.5 + API_DELAY):.1f} seconds")
        print("-" * 60)
        
        for i, chunk in enumerate(chunks):
            print(f"Batch {i+1}/{len(chunks)}: Processing {len(chunk)} keywords...")
            
            # Make API call
            response, response_time = self.call_keyword_ideas_api(chunk)
            
            # Store result with metadata
            result = {
                "batch_number": i + 1,
                "keywords_in_batch": chunk,
                "batch_size": len(chunk),
                "response_time": response_time,
                "api_response": response
            }
            results.append(result)
            
            # Progress callback
            if progress_callback:
                progress_callback(i + 1, len(chunks), response_time, response.get("status"))
            
            # Success/error reporting
            if response.get("status") == "success":
                total_ideas = len(response.get("keyword_ideas", []))
                print(f"  ✅ Success: {total_ideas} keyword ideas received in {response_time:.2f}s")
            else:
                error_msg = response.get("error", {})
                if isinstance(error_msg, dict):
                    error_msg = error_msg.get("message", "Unknown error")
                print(f"  ❌ Error: {error_msg}")
                
                # Handle rate limiting
                if "429" in str(error_msg) or "exhausted" in str(error_msg).lower():
                    print("  ⏳ Rate limit detected. Waiting longer before next request...")
                    time.sleep(API_DELAY * 2)  # Double the delay for rate limits
            
            # Wait between requests (except for last batch)
            if i < len(chunks) - 1:
                print(f"  Waiting {API_DELAY}s before next batch...")
                time.sleep(API_DELAY)
        
        # Summary statistics
        successful_batches = sum(1 for r in results if r["api_response"].get("status") == "success")
        avg_response_time = sum(r["response_time"] for r in results) / len(results) if results else 0
        total_ideas = sum(
            len(r["api_response"].get("keyword_ideas", [])) 
            for r in results 
            if r["api_response"].get("status") == "success"
        )
        
        print("\n" + "=" * 60)
        print("RESEARCH SUMMARY:")
        print(f"Mode: {mode.upper()}")
        print(f"Batch size: {batch_size}")
        print(f"Total batches: {len(chunks)}")
        print(f"Successful batches: {successful_batches}/{len(chunks)} ({successful_batches/len(chunks)*100:.1f}%)")
        print(f"Average response time: {avg_response_time:.2f}s")
        print(f"Total keyword ideas: {total_ideas:,}")
        print(f"Ideas per second: {total_ideas/self.total_response_time:.1f}" if self.total_response_time > 0 else "N/A")
        
        return results
    
    def quick_research(self, keywords: List[str], max_keywords: int = 50) -> Dict[str, Any]:
        """Quick research mode for small keyword sets."""
        if len(keywords) > max_keywords:
            keywords = keywords[:max_keywords]
            print(f"Limiting to first {max_keywords} keywords for quick research")
        
        results = self.research_keywords_optimized(keywords, mode="real_time")
        
        # Aggregate results
        all_ideas = []
        for result in results:
            if result["api_response"].get("status") == "success":
                all_ideas.extend(result["api_response"].get("keyword_ideas", []))
        
        return {
            "mode": "quick_research",
            "input_keywords": keywords,
            "total_ideas": len(all_ideas),
            "keyword_ideas": all_ideas,
            "batches_processed": len(results)
        }

def demonstrate_optimal_usage():
    """Demonstrate different usage patterns based on scaling analysis."""
    print("Google Ads Keyword Research - Optimized Usage Demonstration")
    print("Based on comprehensive scaling analysis")
    print("=" * 70)
    
    # Load sample keywords
    try:
        with open("keyword_master_list.txt", "r") as f:
            all_keywords = [line.strip() for line in f if line.strip()][:100]  # Limit for demo
    except FileNotFoundError:
        print("Error: keyword_master_list.txt not found")
        return
    
    researcher = OptimizedKeywordResearcher()
    
    print(f"\nDemo will use first 100 keywords from master list")
    print(f"Available keywords: {len(all_keywords)}")
    
    # Demo different modes
    modes_to_demo = [
        ("real_time", all_keywords[:20], "Real-time application simulation"),
        ("standard", all_keywords[:50], "Standard research workflow"),
        ("bulk", all_keywords, "Bulk research operation")
    ]
    
    for mode, keywords, description in modes_to_demo:
        print(f"\n{'='*70}")
        print(f"DEMONSTRATION: {description.upper()}")
        print(f"Mode: {mode}")
        print(f"Keywords to process: {len(keywords)}")
        print('='*70)
        
        try:
            results = researcher.research_keywords_optimized(keywords, mode=mode)
            
            # Brief analysis
            successful_results = [r for r in results if r["api_response"].get("status") == "success"]
            if successful_results:
                avg_time = sum(r["response_time"] for r in successful_results) / len(successful_results)
                total_ideas = sum(
                    len(r["api_response"].get("keyword_ideas", [])) 
                    for r in successful_results
                )
                print(f"\n✅ Demo completed successfully!")
                print(f"Average response time: {avg_time:.2f}s")
                print(f"Total ideas generated: {total_ideas:,}")
        
        except KeyboardInterrupt:
            print("\nDemo interrupted by user")
            break
        except Exception as e:
            print(f"\nDemo error: {e}")
            continue
        
        # Wait between demos
        if mode != modes_to_demo[-1][0]:  # Not the last demo
            print("\nWaiting 5 seconds before next demonstration...")
            time.sleep(5)

def main():
    """Main function with CLI interface."""
    if len(sys.argv) > 1:
        if sys.argv[1] == "demo":
            demonstrate_optimal_usage()
        elif sys.argv[1] == "quick":
            # Quick research with command line keywords
            keywords = sys.argv[2:] if len(sys.argv) > 2 else ["salesforce automation", "pardot marketing"]
            researcher = OptimizedKeywordResearcher()
            result = researcher.quick_research(keywords)
            print(json.dumps(result, indent=2))
        else:
            print("Usage:")
            print("  python optimized_keyword_research.py demo     # Run demonstrations")
            print("  python optimized_keyword_research.py quick [keywords...]  # Quick research")
    else:
        print("Optimized Google Ads Keyword Research Client")
        print("Based on comprehensive API scaling analysis")
        print()
        print("This client uses optimal batch sizes determined through testing:")
        for mode, size in OPTIMAL_BATCH_SIZES.items():
            print(f"  {mode}: {size} keywords per batch")
        print()
        print("Usage:")
        print("  python optimized_keyword_research.py demo     # Run demonstrations")
        print("  python optimized_keyword_research.py quick [keywords...]  # Quick research")

if __name__ == "__main__":
    main()
