# RSS Feed ETL + Job Filtering System

A comprehensive data pipeline for extracting job postings from RSS feeds, storing them with SCD1 data management, and filtering them for targeted job hunting.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Installation](#installation)
- [Configuration](#configuration)
- [ETL Process](#etl-process)
- [Job Filtering](#job-filtering)
- [Workflow](#workflow)
- [Command Reference](#command-reference)
- [Troubleshooting](#troubleshooting)

## 🎯 Overview

This system provides a complete job hunting data pipeline:

1. **RSS Feed ETL**: Extracts job postings from multiple RSS feeds and loads them to Google Sheets
2. **Job Filtering**: Filters and cleans job data based on your criteria
3. **SCD1 Data Management**: Preserves all historical data while keeping it up-to-date
4. **Automated Workflow**: Run both processes together or independently

### Key Features

- ✅ **SCD1 Implementation**: Never lose job data, automatic deduplication
- ✅ **Multi-feed Processing**: Handle 14+ RSS feeds simultaneously
- ✅ **Smart Filtering**: Remove unwanted jobs (managers, contractors, etc.)
- ✅ **Date-based Filtering**: Focus on recent job postings
- ✅ **Timestamp Tracking**: AS_OF_DT column for audit trails
- ✅ **Flexible Loading**: Append or overwrite modes
- ✅ **Google Sheets Integration**: Direct data loading to spreadsheets

## 🏗️ Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ RSS Feeds   │───▶│ ETL Process │───▶│ StageData   │───▶│ Job Filter  │
│ (14 feeds)  │    │ (SCD1)      │    │ (All Jobs)  │    │ (Keywords)  │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                                                                   │
                                                                   ▼
                                                          ┌─────────────┐
                                                          │ TexasJobs   │
                                                          │ (Filtered)  │
                                                          └─────────────┘
```

### Data Flow

1. **RSS Feeds** → Multiple job board RSS feeds (LinkedIn, Indeed, etc.)
2. **ETL Process** → Extracts, transforms, and loads data using SCD1
3. **StageData** → Central repository for all job postings
4. **Job Filter** → Applies keyword and date filters
5. **TexasJobs** → Clean, filtered job postings ready for applications

## 🚀 Installation

### Prerequisites

- Python 3.8+
- Google Sheets API credentials
- Required Python packages (see requirements.txt)

### Setup

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd rss_feed_etl
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up Google Sheets credentials**:
   - Place your `service_account.json` in the `secrets/` directory
   - Ensure your service account has access to your Google Spreadsheet

4. **Configure environment**:
   ```bash
   cp config/.env.example config/.env.development
   # Edit the .env file with your settings
   ```

## ⚙️ Configuration

### Main Configuration File: `config/config.yaml`

#### ETL Configuration

```yaml
etl:
  loading_strategy: "scd1"              # Data loading strategy
  target_worksheet: "StageData"         # Target worksheet for all feeds
  config_sheet: "JobFeedsConfig"        # Sheet containing RSS feed configs
  timezone: "US/Central"                # Timezone for date conversion
  
  columns:                              # Columns to include in output
    - job_title
    - link
    - entry_title
    - published
    - feed_title
    - reader
    - time_window
    - summary
    - notes
```

#### Job Filter Configuration

```yaml
job_filter:
  source_worksheet: "StageData"         # Source data from ETL
  output_worksheet: "TexasJobs"         # Filtered output destination
  loading_mode: "append"                # "append" or "overwrite"
  add_as_of_dt: true                    # Add timestamp column
  
  date_filter:
    enabled: true                       # Enable date filtering
    column: "published"                 # Date column to filter
    days_back: 7                        # Days back to include (0 = all)
  
  exclude_by_column:                    # Keywords to exclude
    entry_title:
      - "Director"
      - "Manager"
      - "10+ years"
      - "Contract"
      - "Lead"
    summary:
      - "15+ years"
      - "lensa"
      - "Dice"
```

### RSS Feed Configuration: Google Sheets

Create a `JobFeedsConfig` worksheet with these columns:

| title | reader | time | url | worksheet_name |
|-------|--------|------|-----|----------------|
| Infrastructure | rss.app | 15min | https://rss.app/feeds/... | DataEngineerRaw |
| DataEngineer | rss.app | 15min | https://rss.app/feeds/... | DataEngineerRaw |

## 🔄 ETL Process

### SCD1 (Slowly Changing Dimension Type 1)

The ETL uses SCD1 methodology:

- **Insert**: New job postings (unique links) are added
- **Update**: Existing job postings (same links) are updated with fresh data
- **Preserve**: Historical job postings are never deleted
- **Deduplicate**: Same job from multiple feeds = single record

### Running the ETL

```bash
# Basic ETL run
python3 run_etl.py --loading_strategy scd1 --config_sheet JobFeedsConfig

# With detailed logging
python3 run_etl.py --loading_strategy scd1 --config_sheet JobFeedsConfig --log-level INFO

# Dry run (no changes)
python3 run_etl.py --loading_strategy scd1 --config_sheet JobFeedsConfig --dry-run
```

### ETL Output

The ETL process will:
1. Read RSS feed configurations from `JobFeedsConfig`
2. Extract job data from each RSS feed
3. Apply SCD1 merge logic
4. Load all data to `StageData` worksheet
5. Log detailed processing information

## 🎯 Job Filtering

### Filtering Process

The job filter applies multiple layers of filtering:

1. **Date Filtering**: Include only recent job postings
2. **Keyword Filtering**: Exclude unwanted job types
3. **AS_OF_DT Addition**: Add processing timestamp
4. **Loading Mode**: Append to or overwrite existing data

### Running the Filter

```bash
# Basic filtering
python3 run_job_filter.py

# Override loading mode
python3 run_job_filter.py --loading-mode overwrite

# Filter last 3 days only
python3 run_job_filter.py --days-back 3

# Get all historical data
python3 run_job_filter.py --days-back 0

# Disable timestamp column
python3 run_job_filter.py --no-as-of-dt
```

### Filter Output

The filter process will:
1. Read all job data from `StageData`
2. Apply date filtering (if enabled)
3. Apply keyword exclusion filters
4. Add AS_OF_DT timestamp column
5. Load filtered data to `TexasJobs` worksheet

## 🔄 Workflow

### Complete Workflow

Run both ETL and filtering in sequence:

```bash
# Complete workflow
python3 run_workflow.py --config-sheet JobFeedsConfig

# Skip ETL, only filter
python3 run_workflow.py --skip-etl

# Skip filtering, only ETL
python3 run_workflow.py --skip-filter

# Dry run mode
python3 run_workflow.py --dry-run
```

### Recommended Schedule

For optimal job hunting:

```bash
# Daily morning run (get fresh jobs)
0 8 * * * cd /path/to/rss_feed_etl && python3 run_workflow.py

# Evening run (catch afternoon postings)
0 18 * * * cd /path/to/rss_feed_etl && python3 run_workflow.py
```

## 📚 Command Reference

### ETL Commands

| Command | Description |
|---------|-------------|
| `--loading_strategy scd1` | Use SCD1 data management |
| `--config_sheet JobFeedsConfig` | RSS feed configuration sheet |
| `--log-level INFO` | Set logging level |
| `--dry-run` | Test run without changes |

### Filter Commands

| Command | Description |
|---------|-------------|
| `--loading-mode append` | Add to existing data |
| `--loading-mode overwrite` | Replace existing data |
| `--days-back 7` | Include last 7 days |
| `--days-back 0` | Include all historical data |
| `--no-as-of-dt` | Disable timestamp column |

### Workflow Commands

| Command | Description |
|---------|-------------|
| `--skip-etl` | Run only job filtering |
| `--skip-filter` | Run only ETL |
| `--dry-run` | Test mode for both processes |

## 🔧 Troubleshooting

### Common Issues

#### 1. "Spreadsheet ID is required"
```bash
# Set environment variable
export GOOGLE_SPREADSHEET_ID="your_spreadsheet_id"

# Or use command line
python3 run_etl.py --spreadsheet_id "your_id"
```

#### 2. "Credentials file not found"
```bash
# Check file exists
ls secrets/service_account.json

# Set correct path
export GOOGLE_CREDS_FILE_PATH="secrets/service_account.json"
```

#### 3. "No data found in StageData"
- Run ETL first: `python3 run_etl.py --loading_strategy scd1`
- Check RSS feed configurations in `JobFeedsConfig`
- Verify RSS feed URLs are accessible

#### 4. "SCD1 validation failed"
- Check that `link` column exists in data
- Verify RSS feeds are returning valid data
- Check for empty or malformed URLs

### Logging

Enable detailed logging for debugging:

```bash
# Debug level logging
python3 run_etl.py --log-level DEBUG

# Info level with file output
python3 run_job_filter.py --log-level INFO --log-to-file
```

### Data Validation

Check your data at each stage:

1. **RSS Feeds**: Verify URLs return valid RSS/XML
2. **StageData**: Check for expected job records
3. **TexasJobs**: Verify filtering worked correctly

## 📊 Data Schema

### StageData Schema

| Column | Type | Description |
|--------|------|-------------|
| job_title | String | Job category/feed title |
| link | String | Unique job posting URL (Primary Key) |
| entry_title | String | Job posting title |
| published | DateTime | Publication date/time |
| feed_title | String | RSS feed source name |
| reader | String | Feed reader service |
| time_window | String | Feed refresh interval |
| summary | Text | Job description/summary |
| notes | Text | Personal notes (preserved) |

### TexasJobs Schema

Same as StageData plus:

| Column | Type | Description |
|--------|------|-------------|
| AS_OF_DT | DateTime | Filter processing timestamp |

## 🎯 Best Practices

1. **Regular Runs**: Schedule ETL to run 2-3 times daily
2. **Monitor Logs**: Check for failed RSS feeds or errors
3. **Backup Data**: Export important worksheets regularly
4. **Update Filters**: Adjust keyword filters based on results
5. **Date Management**: Use appropriate `days_back` settings
6. **Notes Preservation**: Add personal notes - they're preserved across updates

## 📈 Performance

- **ETL Runtime**: ~10-15 seconds for 14 feeds
- **Filter Runtime**: ~3-5 seconds for 1000+ jobs
- **Memory Usage**: Minimal (processes data in chunks)
- **API Limits**: Respects Google Sheets API rate limits

---

**Happy Job Hunting!** 🚀

For issues or questions, check the logs first, then review this documentation.
