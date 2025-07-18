#!/usr/bin/env python3
"""
Google Ads Keyword Research CLI

A clean, professional CLI tool for Google Ads keyword research with organized output.
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException


class KeywordCLI:
    """Professional Google Ads Keyword Research CLI"""
    
    def __init__(self, output_dir: str = "output"):
        """Initialize the CLI with output directory structure"""
        self.output_dir = Path(output_dir)
        self.json_dir = self.output_dir / "json"
        self.csv_dir = self.output_dir / "csv"
        self.logs_dir = self.output_dir / "logs"
        
        # Create output directories
        for dir_path in [self.json_dir, self.csv_dir, self.logs_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        self._setup_logging()
        
        # Initialize Google Ads client
        try:
            self.client = GoogleAdsClient.load_from_storage("google-ads.yaml")
            self.customer_id = "3399365278"
            self.logger.info("Google Ads client initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize Google Ads client: {e}")
            raise
    
    def _setup_logging(self):
        """Setup logging configuration"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = self.logs_dir / f"keyword_research_{timestamp}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def get_keyword_ideas(self, keywords: List[str], location_id: str = "2840") -> List[Dict[str, Any]]:
        """Get keyword ideas from Google Ads API"""
        self.logger.info(f"Requesting keyword ideas for: {keywords}")
        
        keyword_plan_idea_service = self.client.get_service("KeywordPlanIdeaService")
        
        # Build location resource names
        location_rns = [f"geoTargetConstants/{location_id}"]
        
        # Build language resource name 
        language_rn = self.client.get_service("GoogleAdsService").language_constant_path("1000")
        
        # Create request
        request = self.client.get_type("GenerateKeywordIdeasRequest")
        request.customer_id = self.customer_id
        request.language = language_rn
        request.geo_target_constants = location_rns
        request.include_adult_keywords = False
        request.keyword_plan_network = (
            self.client.enums.KeywordPlanNetworkEnum.GOOGLE_SEARCH_AND_PARTNERS
        )
        
        # Use keyword_seed for keywords only
        request.keyword_seed.keywords.extend(keywords)
        
        try:
            response = keyword_plan_idea_service.generate_keyword_ideas(request=request)
            results = self._process_keyword_ideas(response)
            self.logger.info(f"Successfully retrieved {len(results)} keyword ideas")
            return results
        except GoogleAdsException as ex:
            self.logger.error(f"Google Ads API request failed: {ex}")
            raise
    
    def _process_keyword_ideas(self, response) -> List[Dict[str, Any]]:
        """Process and format keyword ideas"""
        results = []
        
        for idea in response:
            # Extract monthly search volumes
            monthly_volumes = []
            if hasattr(idea.keyword_idea_metrics, 'monthly_search_volumes'):
                for volume in idea.keyword_idea_metrics.monthly_search_volumes:
                    monthly_volumes.append({
                        "year": volume.year,
                        "month": volume.month.name,
                        "monthly_searches": volume.monthly_searches
                    })
            
            keyword_data = {
                "text": idea.text,
                "avg_monthly_searches": idea.keyword_idea_metrics.avg_monthly_searches,
                "competition": idea.keyword_idea_metrics.competition.name,
                "competition_index": idea.keyword_idea_metrics.competition_index,
                "low_top_of_page_bid_micros": idea.keyword_idea_metrics.low_top_of_page_bid_micros,
                "high_top_of_page_bid_micros": idea.keyword_idea_metrics.high_top_of_page_bid_micros,
                "close_variants": [],
                "monthly_search_volumes": monthly_volumes
            }
            results.append(keyword_data)
        
        return results
    
    def save_json(self, data: Dict[str, Any], filename: Optional[str] = None) -> Path:
        """Save results as JSON to output/json directory"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"keyword_research_{timestamp}.json"
        
        if not filename.endswith('.json'):
            filename += '.json'
        
        filepath = self.json_dir / filename
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        self.logger.info(f"Results saved to: {filepath}")
        return filepath
    
    def save_csv(self, results: List[Dict[str, Any]], filename: Optional[str] = None) -> Path:
        """Save results as CSV to output/csv directory"""
        import csv
        
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"keyword_research_{timestamp}.csv"
        
        if not filename.endswith('.csv'):
            filename += '.csv'
        
        filepath = self.csv_dir / filename
        
        if not results:
            self.logger.warning("No results to save as CSV")
            return filepath
        
        # CSV headers
        headers = [
            'keyword', 'avg_monthly_searches', 'competition', 'competition_index',
            'low_bid_micros', 'high_bid_micros', 'low_bid_usd', 'high_bid_usd'
        ]
        
        with open(filepath, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            
            for result in results:
                writer.writerow({
                    'keyword': result['text'],
                    'avg_monthly_searches': result['avg_monthly_searches'],
                    'competition': result['competition'],
                    'competition_index': result['competition_index'],
                    'low_bid_micros': result['low_top_of_page_bid_micros'],
                    'high_bid_micros': result['high_top_of_page_bid_micros'],
                    'low_bid_usd': result['low_top_of_page_bid_micros'] / 1_000_000,
                    'high_bid_usd': result['high_top_of_page_bid_micros'] / 1_000_000
                })
        
        self.logger.info(f"CSV saved to: {filepath}")
        return filepath
    
    def run(self, keywords: List[str], location_id: str = "2840", 
            output_format: str = "json", filename: Optional[str] = None) -> Dict[str, Any]:
        """Main run method"""
        self.logger.info(f"Starting keyword research for: {keywords}")
        
        # Get keyword ideas
        keyword_results = self.get_keyword_ideas(keywords, location_id)
        
        # Create complete response structure
        response_data = {
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "request_params": {
                "customer_id": self.customer_id,
                "keywords": keywords,
                "location_id": location_id,
                "language_id": "1000",
                "include_adult_keywords": False
            },
            "total_results": len(keyword_results),
            "keyword_ideas": keyword_results
        }
        
        # Save results
        if output_format == "json" or output_format == "both":
            json_file = self.save_json(response_data, filename)
            print(f"📄 JSON saved: {json_file}")
        
        if output_format == "csv" or output_format == "both":
            csv_file = self.save_csv(keyword_results, filename)
            print(f"📊 CSV saved: {csv_file}")
        
        # Print summary
        print(f"\n🎯 Summary:")
        print(f"   Keywords researched: {len(keywords)}")
        print(f"   Ideas found: {len(keyword_results)}")
        print(f"   Location: {location_id}")
        print(f"   Output directory: {self.output_dir}")
        
        return response_data


def main():
    """Main CLI function"""
    parser = argparse.ArgumentParser(
        description="Professional Google Ads Keyword Research CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s "digital marketing"
  %(prog)s "seo,ppc,social media" --format csv
  %(prog)s "python training" --location 2826 --output my_research
  %(prog)s "salesforce backup" --format both
        """
    )
    
    parser.add_argument(
        "keywords", 
        help="Comma-separated list of keywords to research"
    )
    parser.add_argument(
        "-l", "--location", 
        default="2840",
        help="Location ID (default: 2840 for US)"
    )
    parser.add_argument(
        "-f", "--format", 
        choices=["json", "csv", "both"],
        default="json",
        help="Output format (default: json)"
    )
    parser.add_argument(
        "-o", "--output", 
        help="Output filename (without extension)"
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Output directory (default: output)"
    )
    
    args = parser.parse_args()
    
    # Parse keywords
    keywords = [k.strip() for k in args.keywords.split(",")]
    
    try:
        # Initialize CLI
        cli = KeywordCLI(output_dir=args.output_dir)
        
        # Run research
        results = cli.run(
            keywords=keywords,
            location_id=args.location,
            output_format=args.format,
            filename=args.output
        )
        
        print(f"\n✅ Keyword research completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
