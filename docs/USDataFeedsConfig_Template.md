# USDataFeedsConfig Sheet Template

Create a new worksheet in your Google Spreadsheet called **"USDataFeedsConfig"** with these columns and sample data:

## Column Headers:
| title | reader | time | url | worksheet_name |

## Sample US-Wide RSS Feeds:

| title | reader | time | url | worksheet_name |
|-------|--------|------|-----|----------------|
| USDataFeed1 | rss.app | 15min | https://rss.app/feeds/... | DataRaw |
| USDataFeed2 | rss.app | 15min | https://rss.app/feeds/... | DataRaw |
| USDataFeed3 | rss.app | 15min | https://rss.app/feeds/... | DataRaw |
| USDataFeed4 | rss.app | 15min | https://rss.app/feeds/... | DataRaw |
| USDataFeed5 | rss.app | 15min | https://rss.app/feeds/... | DataRaw |
| USDataFeed6 | rss.app | 15min | https://rss.app/feeds/... | DataRaw |
| USDataFeed7 | rss.app | 15min | https://rss.app/feeds/... | DataRaw |
| USDataFeed8 | rss.app | 15min | https://rss.app/feeds/... | DataRaw |
| USDataFeed9 | rss.app | 15min | https://rss.app/feeds/... | DataRaw |
| USDataFeed10 | rss.app | 15min | https://rss.app/feeds/... | DataRaw |

## Notes:
- Replace the URLs with your actual US-wide RSS feed URLs
- These should be different from your Texas-specific feeds
- Use broader geographic terms (no "TX" or "Texas" in the search)
- Consider remote-friendly data feeds for US-wide coverage
- The worksheet_name can be the same as Texas feeds since they go to different StageData worksheets

## RSS Feed URL Examples for US-Wide:
- LinkedIn: Data in "United States" instead of "Texas"
- Indeed: National data searches
- Remote data boards
- Company data pages (national scope)

## Setup Steps:
1. Create "USDataFeedsConfig" worksheet in your Google Spreadsheet
2. Add the column headers
3. Add your US-wide RSS feed URLs
4. Test with: `python3 run_separate_jobs.py us --dry-run`
