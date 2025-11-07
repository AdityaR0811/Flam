#!/usr/bin/env python3
"""
Example: ETL Pipeline with Dependencies
Demonstrates scheduled jobs and job chaining patterns
"""

import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone


def create_etl_jobs():
    """Create ETL pipeline jobs with scheduling."""
    now = datetime.now(timezone.utc)
    
    # Extract phase - runs immediately
    extract_jobs = [
        {
            "id": "extract-users",
            "command": "echo 'Extracting users from database...' && powershell -Command Start-Sleep -Seconds 2",
            "priority": 100,
            "run_at": now.isoformat(),
        },
        {
            "id": "extract-orders",
            "command": "echo 'Extracting orders from database...' && powershell -Command Start-Sleep -Seconds 2",
            "priority": 100,
            "run_at": now.isoformat(),
        },
        {
            "id": "extract-products",
            "command": "echo 'Extracting products from database...' && powershell -Command Start-Sleep -Seconds 2",
            "priority": 100,
            "run_at": now.isoformat(),
        },
    ]
    
    # Transform phase - scheduled 5 seconds later
    transform_time = now + timedelta(seconds=5)
    transform_jobs = [
        {
            "id": "transform-users",
            "command": "echo 'Transforming users data...' && powershell -Command Start-Sleep -Seconds 2",
            "priority": 90,
            "run_at": transform_time.isoformat(),
        },
        {
            "id": "transform-orders",
            "command": "echo 'Transforming orders data...' && powershell -Command Start-Sleep -Seconds 2",
            "priority": 90,
            "run_at": transform_time.isoformat(),
        },
        {
            "id": "transform-products",
            "command": "echo 'Transforming products data...' && powershell -Command Start-Sleep -Seconds 2",
            "priority": 90,
            "run_at": transform_time.isoformat(),
        },
    ]
    
    # Load phase - scheduled 10 seconds later
    load_time = now + timedelta(seconds=10)
    load_jobs = [
        {
            "id": "load-warehouse",
            "command": "echo 'Loading data to warehouse...' && powershell -Command Start-Sleep -Seconds 3",
            "priority": 80,
            "run_at": load_time.isoformat(),
        },
        {
            "id": "validate-data",
            "command": "echo 'Validating data quality...' && powershell -Command Start-Sleep -Seconds 2",
            "priority": 70,
            "run_at": load_time.isoformat(),
        },
    ]
    
    # Notification - scheduled 15 seconds later
    notify_time = now + timedelta(seconds=15)
    notify_job = {
        "id": "send-notification",
        "command": "echo 'ETL pipeline completed successfully!'",
        "priority": 50,
        "run_at": notify_time.isoformat(),
    }
    
    return extract_jobs + transform_jobs + load_jobs + [notify_job]


def main():
    """Run ETL pipeline example."""
    print("=" * 60)
    print("QueueCtl Example: Scheduled ETL Pipeline")
    print("=" * 60)
    print()
    
    # Create jobs
    print("Creating ETL pipeline jobs...")
    jobs = create_etl_jobs()
    
    print(f"  • {len([j for j in jobs if 'extract' in j['id']])} extract jobs (run immediately)")
    print(f"  • {len([j for j in jobs if 'transform' in j['id']])} transform jobs (run in 5s)")
    print(f"  • {len([j for j in jobs if 'load' in j['id']])} load jobs (run in 10s)")
    print(f"  • 1 notification job (run in 15s)")
    print()
    
    # Save to file
    jobs_file = "etl_pipeline.json"
    with open(jobs_file, "w") as f:
        json.dump(jobs, f, indent=2)
    
    # Initialize
    subprocess.run(["queuectl", "init"], check=True)
    
    # Enqueue
    print("Enqueueing jobs...")
    subprocess.run(["queuectl", "enqueue", "--file", jobs_file], check=True)
    print()
    
    # Start workers
    print("Starting 2 workers...")
    subprocess.run(["queuectl", "worker", "start", "--count", "2"], check=True)
    print()
    
    # Monitor progress
    print("Monitoring pipeline execution...")
    print("(Watch as jobs execute in scheduled phases)")
    print()
    
    import time
    for i in range(20):
        print(f"\n--- Time: {i}s ---")
        subprocess.run(["queuectl", "status"])
        time.sleep(1)
    
    # Final status
    print("\n" + "=" * 60)
    print("Pipeline Execution Complete!")
    print("=" * 60)
    print()
    
    # Show job execution order
    print("Job execution timeline:")
    result = subprocess.run(
        ["queuectl", "list", "--json"],
        capture_output=True,
        text=True,
    )
    
    if result.returncode == 0:
        jobs_data = json.loads(result.stdout)
        # Sort by updated_at to show execution order
        completed = [j for j in jobs_data if j["state"] == "completed"]
        completed.sort(key=lambda x: x["updated_at"])
        
        for job in completed:
            print(f"  {job['updated_at'][:19]} - {job['id']}")
    print()
    
    # Stop workers
    print("Stopping workers...")
    subprocess.run(["queuectl", "worker", "stop"], check=True)
    print()
    
    print("Key Features Demonstrated:")
    print("  • Scheduled jobs with run_at timestamps")
    print("  • Phased execution (extract → transform → load)")
    print("  • Priority-based ordering within phases")
    print("  • Real-time monitoring of job progress")
    print()
    
    # Cleanup
    import os
    os.remove(jobs_file)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted! Stopping workers...")
        subprocess.run(["queuectl", "worker", "stop"])
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)
