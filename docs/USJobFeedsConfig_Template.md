# USJobFeedsConfig Sheet Template

Create a new worksheet in your Google Spreadsheet called **"USJobFeedsConfig"** with these columns and sample data:

## Column Headers:
| title | reader | time | url | worksheet_name |

## Sample US-Wide RSS Feeds:

| title | reader | time | url | worksheet_name |
|-------|--------|------|-----|----------------|
| USDataEngineer | rss.app | 15min | https://rss.app/feeds/... | DataEngineerRaw |
| USCloudEngineer | rss.app | 15min | https://rss.app/feeds/... | DataEngineerRaw |
| USDevOpsEngineer | rss.app | 15min | https://rss.app/feeds/... | DataEngineerRaw |
| USMachineLearning | rss.app | 15min | https://rss.app/feeds/... | DataEngineerRaw |
| USPlatformEngineer | rss.app | 15min | https://rss.app/feeds/... | DataEngineerRaw |
| USInfrastructure | rss.app | 15min | https://rss.app/feeds/... | DataEngineerRaw |
| USKubernetes | rss.app | 15min | https://rss.app/feeds/... | DataEngineerRaw |
| USAzure | rss.app | 15min | https://rss.app/feeds/... | DataEngineerRaw |
| USAWS | rss.app | 15min | https://rss.app/feeds/... | DataEngineerRaw |
| USDataOps | rss.app | 15min | https://rss.app/feeds/... | DataEngineerRaw |

## Notes:
- Replace the URLs with your actual US-wide RSS feed URLs
- These should be different from your Texas-specific feeds
- Use broader geographic terms (no "TX" or "Texas" in the search)
- Consider remote-friendly job feeds for US-wide coverage
- The worksheet_name can be the same as Texas feeds since they go to different StageData worksheets

## RSS Feed URL Examples for US-Wide:
- LinkedIn: Jobs in "United States" instead of "Texas"
- Indeed: National job searches
- Remote job boards
- Company career pages (national scope)

## Setup Steps:
1. Create "USJobFeedsConfig" worksheet in your Google Spreadsheet
2. Add the column headers
3. Add your US-wide RSS feed URLs
4. Test with: `python3 run_separate_jobs.py us --dry-run`
