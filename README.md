# RSS Feed ETL + Job Filtering System

A comprehensive data pipeline for extracting job postings from RSS feeds, storing them with SCD1 data management, and filtering them for targeted job hunting.

## 🚀 Quick Start

```bash
# Run both Texas and US job pipelines
python3 run_job_pipelines.py both

# Run only Texas jobs
python3 run_job_pipelines.py texas

# Run only US jobs
python3 run_job_pipelines.py us
```

## 📁 Repository Structure

```
rss_feed_etl/
├── 📄 Main Scripts
│   ├── run_job_pipelines.py      # Main script - runs complete job pipelines
│   ├── run_etl.py                # RSS feed extraction and loading
│   ├── run_job_filter.py         # Job filtering and cleaning
│   └── run_ats_enrichment.py     # AI-powered job matching
│
├── ⚙️ Configuration
│   ├── config/
│   │   ├── config.yaml           # Texas job configuration
│   │   └── config_us.yaml        # US job configuration
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

- **🔄 SCD1 Data Management** - Never lose job data, automatic deduplication
- **🌍 Multi-Region Support** - Separate Texas and US job pipelines
- **🎯 Smart Filtering** - Remove managers, contractors, empty descriptions
- **🤖 AI Job Matching** - ATS enrichment with match scores
- **📊 Google Sheets Integration** - Direct data loading and management
- **⏰ Timestamp Tracking** - AS_OF_DT for audit trails

## 📊 Data Flow

### Texas Job Pipeline
```
JobFeedsConfig → ETL → StageData → Filter → TexasJobs
```

### US Job Pipeline
```
USJobFeedsConfig → ETL → StageData_US → Filter → USJobs
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
   - `JobFeedsConfig` sheet for Texas jobs
   - `USJobFeedsConfig` sheet for US jobs

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
# Daily job hunting routine
python3 run_job_pipelines.py both

# Get last 3 days of jobs only
python3 run_job_pipelines.py both --days-back 3

# Overwrite existing filtered data
python3 run_job_pipelines.py both --loading-mode overwrite

# Run AI job matching
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
- **StageData** / **StageData_US** - Raw job data
- **TexasJobs** / **USJobs** - Filtered job opportunities
- **Enriched data** - AI-matched jobs with scores

---

**Happy Job Hunting!** 🚀