# Quick Start Guide - RSS Feed ETL + Job Filtering

## 🚀 TL;DR - Get Started in 5 Minutes

### 1. Run Complete Workflow
```bash
# Get fresh jobs and filter them
python3 run_workflow.py --config-sheet JobFeedsConfig
```

### 2. Check Your Data
- **StageData**: All job postings (raw data)
- **TexasJobs**: Filtered job postings (ready for applications)

---

## 📋 Common Commands

### Daily Job Hunting Routine

```bash
# Morning: Get fresh jobs from last 7 days
python3 run_workflow.py --config-sheet JobFeedsConfig

# Check results in Google Sheets TexasJobs tab
```

### Manual Control

```bash
# 1. Run ETL only (RSS feeds → StageData)
python3 run_etl.py --loading_strategy scd1 --config_sheet JobFeedsConfig

# 2. Run filtering only (StageData → TexasJobs)
python3 run_job_filter.py --days-back 7 --loading-mode append
```

### Different Time Ranges

```bash
# Last 3 days only
python3 run_job_filter.py --days-back 3

# All historical data
python3 run_job_filter.py --days-back 0

# Replace all existing filtered data
python3 run_job_filter.py --loading-mode overwrite
```

---

## 🔧 Configuration Quick Edit

### Add/Remove Keywords to Filter

Edit `config/config.yaml`:

```yaml
job_filter:
  exclude_by_column:
    entry_title:
      - "Director"      # Remove director positions
      - "Manager"       # Remove manager positions
      - "10+ years"     # Remove senior positions
      - "Contract"      # Remove contract work
      - "Remote"        # Remove remote work (if you want local only)
```

### Change Date Range

```yaml
job_filter:
  date_filter:
    days_back: 7        # Change to 3, 14, 30, or 0 (all data)
```

### Change Loading Mode

```yaml
job_filter:
  loading_mode: "append"    # or "overwrite"
```

---

## 📊 Data Locations

| Worksheet | Purpose | Content |
|-----------|---------|---------|
| **JobFeedsConfig** | Configuration | RSS feed URLs and settings |
| **StageData** | Raw Data | All job postings (SCD1 managed) |
| **TexasJobs** | Filtered Data | Clean jobs ready for applications |

---

## 🔍 Troubleshooting Quick Fixes

### No Data in TexasJobs?
```bash
# Check if StageData has data
python3 show_data.py

# Run filter with all historical data
python3 run_job_filter.py --days-back 0
```

### Too Many/Few Jobs?
```bash
# Adjust date range
python3 run_job_filter.py --days-back 3  # More restrictive
python3 run_job_filter.py --days-back 14 # Less restrictive

# Check your keyword filters in config.yaml
```

### ETL Not Working?
```bash
# Check with dry run
python3 run_etl.py --loading_strategy scd1 --config_sheet JobFeedsConfig --dry-run

# Check RSS feed configs in JobFeedsConfig sheet
```

---

## ⏰ Automation Setup

### Cron Job (Linux/Mac)
```bash
# Edit crontab
crontab -e

# Add these lines for twice-daily runs
0 8 * * * cd /path/to/rss_feed_etl && python3 run_workflow.py
0 18 * * * cd /path/to/rss_feed_etl && python3 run_workflow.py
```

### Windows Task Scheduler
1. Open Task Scheduler
2. Create Basic Task
3. Set trigger: Daily at 8:00 AM
4. Action: Start Program
5. Program: `python3`
6. Arguments: `run_workflow.py`
7. Start in: `C:\path\to\rss_feed_etl`

---

## 🎯 Pro Tips

### 1. Monitor Your Pipeline
```bash
# Check logs for issues
python3 run_workflow.py --log-level INFO

# Test without making changes
python3 run_workflow.py --dry-run
```

### 2. Customize for Your Needs
- **Add more RSS feeds**: Update JobFeedsConfig sheet
- **Adjust filters**: Edit config.yaml exclude keywords
- **Change worksheets**: Modify target_worksheet in config

### 3. Preserve Your Notes
- Add personal notes in the `notes` column
- SCD1 preserves your notes across updates
- Use notes to track application status

### 4. Data Management
```bash
# Fresh start (replace all filtered data)
python3 run_job_filter.py --loading-mode overwrite

# Accumulate data (add to existing)
python3 run_job_filter.py --loading-mode append
```

---

## 📈 Expected Results

After running the complete workflow:

- **StageData**: 50-200+ job postings (grows over time)
- **TexasJobs**: 10-50 filtered jobs (depends on your criteria)
- **Processing Time**: 15-30 seconds total
- **Update Frequency**: Run 1-3 times daily for best results

---

## 🆘 Need Help?

1. **Check the logs** - Most issues show up in the console output
2. **Read the full README** - `README_ETL_FILTER.md` has detailed info
3. **Test with dry-run** - Use `--dry-run` to test without changes
4. **Verify your config** - Check `config.yaml` and `JobFeedsConfig` sheet

---

**Happy Job Hunting!** 🎯
