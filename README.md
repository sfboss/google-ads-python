# Google Ads Keyword Research CLI

Professional CLI tool for Google Ads keyword research with organized output and logging.

## Features

- 🎯 **Clean CLI interface** with proper argument parsing
- 📁 **Organized output** - JSON, CSV, and logs in separate directories
- 📊 **Multiple formats** - JSON for data processing, CSV for spreadsheets
- 🔍 **Comprehensive logging** - Track all operations with timestamps
- 🚀 **Professional package** - Installable with setup.py

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Or install as package
pip install -e .
```

## Usage

```bash
# Basic usage
python kwcli.py "digital marketing"

# Multiple keywords
python kwcli.py "seo,ppc,social media"

# CSV output
python kwcli.py "salesforce backup" --format csv

# Both formats
python kwcli.py "python training" --format both

# Custom location (UK)
python kwcli.py "data science" --location 2826

# Custom output filename
python kwcli.py "machine learning" --output ml_research
```

## Output Structure

```
output/
├── json/           # JSON results with full metadata
├── csv/            # CSV files for spreadsheet analysis  
└── logs/           # Detailed operation logs
```

## Example Output

### JSON Format
```json
{
  "status": "success",
  "timestamp": "2025-07-18T10:30:00",
  "request_params": {
    "customer_id": "3399365278",
    "keywords": ["digital marketing"],
    "location_id": "2840"
  },
  "total_results": 500,
  "keyword_ideas": [
    {
      "text": "digital marketing",
      "avg_monthly_searches": 368000,
      "competition": "HIGH",
      "competition_index": 85,
      "monthly_search_volumes": [...]
    }
  ]
}
```

### CSV Format
Clean spreadsheet-ready data with bid estimates in USD.

## Configuration

Ensure `google-ads.yaml` is configured with your API credentials:

```yaml
developer_token: "YOUR_DEVELOPER_TOKEN"
client_id: "YOUR_CLIENT_ID"
client_secret: "YOUR_CLIENT_SECRET"
refresh_token: "YOUR_REFRESH_TOKEN"
```

## Files

- `kwcli.py` - Main CLI application
- `google-ads.yaml` - API configuration
- `requirements.txt` - Dependencies
- `setup.py` - Package installation
- `output/` - All results and logs
