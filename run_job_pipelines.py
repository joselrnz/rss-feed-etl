#!/usr/bin/env python3
"""
Run Texas and US data processing as two completely separate pipelines.

This script treats Texas and US data processing as independent processes:
- Texas Pipeline: DataFeedsConfig → StageData → TexasData
- US Pipeline: USDataFeedsConfig → StageData_US → USData
"""

import subprocess
import sys
import logging
import argparse
from datetime import datetime
from pathlib import Path


def setup_logging(log_level=logging.INFO):
    """Set up logging configuration."""
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=log_level, format=log_format)


def run_command(command, description):
    """Run a command and return success status."""
    logging.info(f"🚀 {description}")
    logging.info(f"📝 Command: {' '.join(command)}")
    
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        logging.info(f"✅ {description} completed successfully")
        
        # Log output if there's any
        if result.stdout.strip():
            # Only show last few lines to avoid spam
            output_lines = result.stdout.strip().split('\n')
            if len(output_lines) > 5:
                logging.info(f"📤 Output (last 5 lines):")
                for line in output_lines[-5:]:
                    logging.info(f"    {line}")
            else:
                logging.info(f"📤 Output:\n{result.stdout}")
        
        return True
        
    except subprocess.CalledProcessError as e:
        logging.error(f"❌ {description} failed with exit code {e.returncode}")
        if e.stdout:
            logging.error(f"📤 Output:\n{e.stdout}")
        if e.stderr:
            logging.error(f"📤 Error:\n{e.stderr}")
        return False


def run_texas_job(args):
    """Run the complete Texas job processing pipeline."""
    logging.info(f"\n{'='*60}")
    logging.info(f"🤠 TEXAS JOB PROCESSING")
    logging.info(f"{'='*60}")
    
    success_count = 0
    total_steps = 0
    
    # Step 1: Texas ETL
    if not args.skip_etl:
        total_steps += 1
        etl_command = [
            "python3", "run_etl.py",
            "--loading_strategy", "scd1",
            "--config_sheet", "DataFeedsConfig",
            "--config", "config/config.yaml",
            "--log-level", args.log_level
        ]
        
        if args.dry_run:
            etl_command.append("--dry-run")
        
        if run_command(etl_command, "Texas ETL (DataFeedsConfig → StageData)"):
            success_count += 1
        else:
            logging.error("❌ Texas ETL failed")
            return False
    
    # Step 2: Texas Filtering
    if not args.skip_filter:
        total_steps += 1
        filter_command = [
            "python3", "run_job_filter.py",
            "--config", "config/config.yaml",
            "--log-level", args.log_level
        ]
        
        if args.loading_mode:
            filter_command.extend(["--loading-mode", args.loading_mode])
        
        if args.days_back is not None:
            filter_command.extend(["--days-back", str(args.days_back)])
        
        if args.dry_run:
            filter_command.append("--dry-run")
        
        if run_command(filter_command, "Texas Filtering (StageData → TexasData)"):
            success_count += 1
        else:
            logging.error("❌ Texas filtering failed")
            return False
    
    logging.info(f"🤠 Texas job completed: {success_count}/{total_steps} steps successful")
    return success_count == total_steps


def run_us_job(args):
    """Run the complete US job processing pipeline."""
    logging.info(f"\n{'='*60}")
    logging.info(f"🇺🇸 US-WIDE JOB PROCESSING")
    logging.info(f"{'='*60}")
    
    success_count = 0
    total_steps = 0
    
    # Step 1: US ETL
    if not args.skip_etl:
        total_steps += 1
        etl_command = [
            "python3", "run_etl.py",
            "--loading_strategy", "scd1",
            "--config_sheet", "USDataFeedsConfig",
            "--config", "config/config_us.yaml",
            "--log-level", args.log_level
        ]
        
        if args.dry_run:
            etl_command.append("--dry-run")
        
        if run_command(etl_command, "US ETL (USDataFeedsConfig → StageData_US)"):
            success_count += 1
        else:
            logging.error("❌ US ETL failed")
            return False
    
    # Step 2: US Filtering
    if not args.skip_filter:
        total_steps += 1
        filter_command = [
            "python3", "run_job_filter.py",
            "--config", "config/config_us.yaml",
            "--log-level", args.log_level
        ]
        
        if args.loading_mode:
            filter_command.extend(["--loading-mode", args.loading_mode])
        
        if args.days_back is not None:
            filter_command.extend(["--days-back", str(args.days_back)])
        
        if args.dry_run:
            filter_command.append("--dry-run")
        
        if run_command(filter_command, "US Filtering (StageData_US → USData)"):
            success_count += 1
        else:
            logging.error("❌ US filtering failed")
            return False
    
    logging.info(f"🇺🇸 US job completed: {success_count}/{total_steps} steps successful")
    return success_count == total_steps


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Run Texas and US job processing as separate jobs")
    
    # Job selection
    parser.add_argument(
        "job", 
        choices=["texas", "tx", "us", "usa", "both"],
        help="Which job to run"
    )
    
    # Processing options
    parser.add_argument("--loading-mode", choices=["append", "overwrite"], help="Loading mode")
    parser.add_argument("--days-back", type=int, help="Days back to filter")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
    
    # Step control
    parser.add_argument("--skip-etl", action="store_true", help="Skip ETL, only run filtering")
    parser.add_argument("--skip-filter", action="store_true", help="Skip filtering, only run ETL")
    
    args = parser.parse_args()
    
    # Set up logging
    log_level = getattr(logging, args.log_level)
    setup_logging(log_level=log_level)
    
    start_time = datetime.now()
    logging.info("🌟 Starting separate job processing")
    logging.info(f"⏰ Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    if args.dry_run:
        logging.info("🧪 DRY RUN MODE: No changes will be made")
    
    # Determine which jobs to run
    jobs_to_run = []
    if args.job in ["texas", "tx"]:
        jobs_to_run = ["texas"]
    elif args.job in ["us", "usa"]:
        jobs_to_run = ["us"]
    elif args.job == "both":
        jobs_to_run = ["texas", "us"]
    
    # Run the jobs
    results = {}
    for job in jobs_to_run:
        if job == "texas":
            results["texas"] = run_texas_job(args)
        elif job == "us":
            results["us"] = run_us_job(args)
    
    # Summary
    end_time = datetime.now()
    elapsed = (end_time - start_time).total_seconds()
    
    logging.info(f"\n🎯 Separate Jobs Summary:")
    logging.info(f"  ⏱️  Total runtime: {elapsed:.2f} seconds")
    logging.info(f"  📊 Jobs requested: {', '.join(jobs_to_run)}")
    
    success_count = sum(1 for success in results.values() if success)
    total_jobs = len(results)
    
    for job, success in results.items():
        status = "✅ SUCCESS" if success else "❌ FAILED"
        logging.info(f"  {job.upper()}: {status}")
    
    if success_count == total_jobs:
        logging.info("🎉 All jobs completed successfully!")
        return 0
    else:
        logging.error("❌ Some jobs failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
