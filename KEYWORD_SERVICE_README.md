# Google Ads Keyword Research Service

A modular, CLI-friendly service for retrieving Google Ads keyword data in structured JSON format.

## Features

- **Keyword Ideas Generation**: Get keyword suggestions from seed keywords or URLs
- **Historical Metrics**: Retrieve historical search volume and competition data
- **JSON Output**: Structured data format for easy integration
- **Error Handling**: Comprehensive error reporting with request IDs
- **CLI Interface**: Convenient command-line interface for automation

## Installation

Make sure you have the Google Ads Python client library installed and configured:

```bash
pip install google-ads
```

Configure your `google-ads.yaml` file with your API credentials.

## Usage

### Using the Modular Service

#### Command Line Interface

```bash
# Generate keyword ideas from seed keywords
python adwords_service.py keyword-ideas -c YOUR_CUSTOMER_ID -k "python training" "data science"

# Generate keyword ideas from URL
python adwords_service.py keyword-ideas -c YOUR_CUSTOMER_ID -p "https://example.com/training"

# Get historical metrics for keywords
python adwords_service.py historical-metrics -c YOUR_CUSTOMER_ID -k "python training" "data science"

# Pretty print JSON output
python adwords_service.py keyword-ideas -c YOUR_CUSTOMER_ID -k "python" --pretty

# Specify custom locations and language
python adwords_service.py keyword-ideas -c YOUR_CUSTOMER_ID -k "python" -l "2840" "2124" -i "1000"
```

#### Parameters

**Keyword Ideas:**
- `-c, --customer_id`: Google Ads customer ID (required)
- `-k, --keywords`: Seed keywords (space-separated)
- `-p, --page_url`: URL to generate ideas from
- `-l, --location_ids`: Location IDs for targeting (default: New York)
- `-i, --language_id`: Language ID (default: English)
- `--include_adult`: Include adult keywords
- `--page_size`: Number of results (default: 1000)

**Historical Metrics:**
- `-c, --customer_id`: Google Ads customer ID (required)
- `-k, --keywords`: Keywords for metrics (required, space-separated)
- `-l, --location_ids`: Location IDs for targeting (default: USA)
- `-i, --language_id`: Language ID (default: English)
- `--include_adult`: Include adult keywords
- `-n, --network`: Network (GOOGLE_SEARCH or GOOGLE_SEARCH_AND_PARTNERS)

### Using Updated Examples

The existing example scripts now support JSON output:

```bash
# Historical metrics with JSON output
python examples/planning/generate_historical_metrics.py -c YOUR_CUSTOMER_ID -k "python training" --json

# Keyword ideas with JSON output
python examples/planning/generate_keyword_ideas.py -c YOUR_CUSTOMER_ID -k "python training" --json
```

### Programmatic Usage

```python
from google.ads.googleads.client import GoogleAdsClient
from adwords_service import AdWordsKeywordService

# Initialize client
client = GoogleAdsClient.load_from_storage('google-ads.yaml', version="v19")
service = AdWordsKeywordService(client)

# Generate keyword ideas
keyword_ideas = service.generate_keyword_ideas(
    customer_id="YOUR_CUSTOMER_ID",
    keyword_texts=["python training", "data science"],
    location_ids=["2840"],  # USA
    language_id="1000"      # English
)

# Generate historical metrics
historical_metrics = service.generate_historical_metrics(
    customer_id="YOUR_CUSTOMER_ID",
    keywords=["python training", "data science"],
    location_ids=["2840"],
    language_id="1000"
)

print(keyword_ideas)
print(historical_metrics)
```

## JSON Response Format

### Keyword Ideas Response

```json
{
  "status": "success",
  "timestamp": "2024-01-01T12:00:00",
  "request_params": {
    "customer_id": "1234567890",
    "keywords": ["python training"],
    "page_url": null,
    "location_ids": ["2840"],
    "language_id": "1000",
    "include_adult_keywords": false
  },
  "total_results": 100,
  "keyword_ideas": [
    {
      "text": "python programming training",
      "avg_monthly_searches": 1000,
      "competition": "LOW",
      "competition_index": 25,
      "low_top_of_page_bid_micros": 500000,
      "high_top_of_page_bid_micros": 2000000,
      "close_variants": ["python training", "python course"],
      "monthly_search_volumes": [
        {
          "year": 2024,
          "month": "JANUARY",
          "monthly_searches": 1200
        }
      ]
    }
  ]
}
```

### Historical Metrics Response

```json
{
  "status": "success",
  "timestamp": "2024-01-01T12:00:00",
  "request_params": {
    "customer_id": "1234567890",
    "keywords": ["python training"],
    "location_ids": ["2840"],
    "language_id": "1000",
    "keyword_plan_network": "GOOGLE_SEARCH",
    "include_adult_keywords": false
  },
  "total_results": 1,
  "historical_metrics": [
    {
      "text": "python training",
      "close_variants": ["python course", "python education"],
      "avg_monthly_searches": 1000,
      "competition": "LOW",
      "competition_index": 25,
      "low_top_of_page_bid_micros": 500000,
      "high_top_of_page_bid_micros": 2000000,
      "monthly_search_volumes": [
        {
          "year": 2024,
          "month": "JANUARY",
          "monthly_searches": 1200
        }
      ]
    }
  ],
  "aggregate_metrics": null
}
```

### Error Response

```json
{
  "status": "error",
  "timestamp": "2024-01-01T12:00:00",
  "error": {
    "request_id": "abc123",
    "status_code": "INVALID_CUSTOMER_ID",
    "errors": [
      {
        "message": "Invalid customer ID",
        "field_path": ["customer_id"]
      }
    ]
  }
}
```

## Location and Language IDs

- **Common Location IDs:**
  - USA: `2840`
  - New York: `1023191`
  - California: `21137`
  - United Kingdom: `2826`

- **Common Language IDs:**
  - English: `1000`
  - Spanish: `1003`
  - French: `1002`

For complete lists, see:
- [Location targeting](https://developers.google.com/google-ads/api/reference/data/geotargets)
- [Language codes](https://developers.google.com/google-ads/api/reference/data/codes-formats#languages)

## Integration

The service is designed to be easily integrated into:
- Data pipelines
- Web applications
- Automated scripts
- Research workflows

The structured JSON output makes it simple to process results with tools like `jq`, parse into databases, or integrate with other APIs.
