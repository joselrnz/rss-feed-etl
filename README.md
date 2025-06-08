# RSS Feed ETL + Data Processing System

A comprehensive data pipeline for extracting content from RSS feeds, storing them with SCD1 data management, and applying intelligent filtering and processing.

## 🚀 Quick Start

```bash
# Run both regional data pipelines
python3 run_job_pipelines.py both

# Run only Texas region data
python3 run_job_pipelines.py texas

# Run only US-wide data
python3 run_job_pipelines.py us
```

## 📁 Repository Structure

```
rss_feed_etl/
├── 📄 Main Scripts
│   ├── run_job_pipelines.py      # Main script - runs complete data pipelines
│   ├── run_etl.py                # RSS feed extraction and loading
│   ├── run_job_filter.py         # Data filtering and processing
│   └── run_ats_enrichment.py     # AI-powered content analysis
│
├── ⚙️ Configuration
│   ├── config/
│   │   ├── config.yaml           # Texas region configuration
│   │   └── config_us.yaml        # US region configuration
│   └── secrets/
│       └── service_account.json  # Google Sheets credentials
│
├── 🔧 Source Code
│   └── src/
│       ├── etl/                  # ETL processing logic
│       ├── rss_feed_etl/         # RSS feed handling
│       ├── models/               # Data models
│       └── utils/                # Utility functions
│
├── 📚 Documentation
│   ├── docs/
│   │   ├── README_ETL_FILTER.md  # Detailed technical guide
│   │   ├── QUICK_START.md        # Quick reference guide
│   │   └── USJobFeedsConfig_Template.md
│   └── examples/                 # Example configurations
│
├── 📊 Data & Logs
│   ├── logs/                     # Processing logs
│   ├── resume.pdf               # Your resume for ATS matching
│   ├── skills.json              # Skills database
│   └── models.json              # AI model configurations
│
└── 📋 Project Files
    ├── requirements.txt          # Python dependencies
    ├── setup.py                 # Package setup
    └── tests/                   # Unit tests
```

## 🎯 Core Features

- **🔄 SCD1 Data Management** - Never lose data, automatic deduplication
- **🌍 Multi-Region Support** - Separate Texas and US data pipelines
- **🎯 Smart Filtering** - Remove unwanted content, empty descriptions
- **🤖 AI Content Analysis** - Intelligent content enrichment and scoring
- **📊 Google Sheets Integration** - Direct data loading and management
- **⏰ Timestamp Tracking** - AS_OF_DT for audit trails

## 📊 Data Flow

### Texas Region Pipeline
```
DataFeedsConfig → ETL → StageData → Filter → TexasData
```

### US Region Pipeline
```
USDataFeedsConfig → ETL → StageData_US → Filter → USData
```

## 🛠️ Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Google Sheets:**
   - Place `service_account.json` in `secrets/`
   - Set `GOOGLE_SPREADSHEET_ID` environment variable

3. **Create RSS feed configs:**
   - `DataFeedsConfig` sheet for Texas region data
   - `USDataFeedsConfig` sheet for US region data

4. **Test the system:**
   ```bash
   python3 run_job_pipelines.py both --dry-run
   ```

## 📚 Documentation

- **[Detailed Guide](docs/README_ETL_FILTER.md)** - Complete technical documentation
- **[Quick Start](docs/QUICK_START.md)** - 5-minute setup guide
- **[Security Guide](docs/SECURITY.md)** - 🔒 Protect your confidential information
- **[Examples](examples/)** - Sample configurations

## 🎯 Common Commands

```bash
# Daily data processing routine
python3 run_job_pipelines.py both

# Get last 3 days of data only
python3 run_job_pipelines.py both --days-back 3

# Overwrite existing processed data
python3 run_job_pipelines.py both --loading-mode overwrite

# Run AI content analysis
python3 run_ats_enrichment.py

# Test without changes
python3 run_job_pipelines.py both --dry-run
```

## 🔧 Configuration

Edit `config/config.yaml` and `config/config_us.yaml` to customize:
- RSS feed sources
- Keyword filters
- Date ranges
- Output worksheets

## 📈 Output

The system creates these Google Sheets worksheets:
- **StageData** / **StageData_US** - Raw RSS feed data
- **TexasData** / **USData** - Filtered and processed content
- **Enriched data** - AI-analyzed content with scores

---

**Happy Data Processing!** 🚀