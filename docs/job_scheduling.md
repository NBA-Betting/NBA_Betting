# Scheduled Data Jobs - Crontab on EC2

**IMPORTANT** - AWS uses UTC time which is -7 hours from Mountain Time.
* Example: Job scheduled at 17:00 will run at 10:00 mountain time.

Job Name, Description, Run Time and Interval, Logging

## MISC
Daily Bank Account Update
* Cron Command - ```0 9 * * * cd ~/bet_management && python3 financials.py```
* Runtime - Daily at 09:00utc/2:00am mountain

## INBOUND DATA STREAMS and DATABASE ETL 
All Daily Database Updating 
* Cron Command - ```0 17 * * * cd ~ && daily_db_update_script.sh```
* Runtime - Daily at 17:00utc/10:00am mountain
* Script:
```
#!/bin/bash

# exit when any command fails
set -e

# Data Inbound
cd ~/data_feeds/covers
~/.local/bin/scrapy crawl Covers_live_game_results_spider
~/.local/bin/scrapy crawl Covers_live_game_spider
echo "----- Covers Scrapy Complete -----"
cd ~/data_feeds/nba
python3 data_requests.py
echo "----- Data Requests Complete -----"

# Feature Creation and ETL
cd ~/feature_creation
python3 feature_creation_pre_etl.py
echo "----- Feature Creation Pre-ETL Complete -----"
cd ~/etl
python3 daily_db_game_record_update.py
echo "----- Record Update Complete -----"
python3 daily_db_game_record_creation.py
echo "----- Record Creation Complete -----"
cd ~/feature_creation
python3 feature_creation_post_etl.py
echo "----- Feature Creation Post-ETL Complete -----"

# Game Records
cd ~/bet_management
python3 game_records.py
echo "----- Game Records Complete -----"
echo "----- Done -----"
```
