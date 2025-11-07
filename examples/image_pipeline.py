#!/usr/bin/env python3
"""
Example: Batch Image Processing Pipeline
Demonstrates queuectl for parallel image processing with retries and DLQ
"""

import json
import subprocess
import sys
from pathlib import Path


def generate_image_processing_jobs():
    """Generate sample image processing jobs."""
    images = [
        {"name": "photo1.jpg", "size": "large", "priority": 10},
        {"name": "photo2.jpg", "size": "medium", "priority": 5},
        {"name": "photo3.jpg", "size": "small", "priority": 1},
        {"name": "photo4.jpg", "size": "large", "priority": 10},
        {"name": "photo5.jpg", "size": "medium", "priority": 5},
        {"name": "corrupted.jpg", "size": "large", "priority": 8},  # Will fail
    ]
    
    jobs = []
    for img in images:
        # Simulate different processing commands
        if img["name"] == "corrupted.jpg":
            # This will fail to demonstrate retry/DLQ
            command = "exit 1"
        else:
            # Simulate image processing (uses sleep to represent work)
            duration = {"large": 3, "medium": 2, "small": 1}[img["size"]]
            command = f"powershell -Command \"Start-Sleep -Seconds {duration}; echo 'Processed {img['name']}'\""
        
        jobs.append({
            "id": f"img-{img['name']}",
            "command": command,
            "priority": img["priority"],
            "max_retries": 3,
            "timeout_s": 10,
        })
    
    return jobs


def main():
    """Run the example pipeline."""
    print("=" * 60)
    print("QueueCtl Example: Image Processing Pipeline")
    print("=" * 60)
    print()
    
    # Generate jobs
    print("Step 1: Generating image processing jobs...")
    jobs = generate_image_processing_jobs()
    jobs_file = Path("image_jobs.json")
    with open(jobs_file, "w") as f:
        json.dump(jobs, f, indent=2)
    print(f"  Created {len(jobs)} jobs in {jobs_file}")
    print()
    
    # Initialize database
    print("Step 2: Initializing queuectl...")
    subprocess.run(["queuectl", "init"], check=True)
    print()
    
    # Enqueue jobs
    print("Step 3: Enqueueing jobs...")
    subprocess.run(["queuectl", "enqueue", "--file", str(jobs_file)], check=True)
    print()
    
    # Show initial status
    print("Step 4: Initial status")
    subprocess.run(["queuectl", "status"])
    print()
    
    # Start workers
    print("Step 5: Starting 3 workers...")
    subprocess.run(["queuectl", "worker", "start", "--count", "3"], check=True)
    print()
    
    # Wait for processing
    print("Step 6: Processing jobs (this will take ~10 seconds)...")
    import time
    for i in range(10):
        time.sleep(1)
        print(f"  {i+1}/10 seconds elapsed...")
    print()
    
    # Check status
    print("Step 7: Current status")
    subprocess.run(["queuectl", "status"])
    print()
    
    # List completed jobs
    print("Step 8: Completed jobs")
    subprocess.run(["queuectl", "list", "--state", "completed"])
    print()
    
    # Check DLQ for failed jobs
    print("Step 9: Dead Letter Queue (failed jobs)")
    subprocess.run(["queuectl", "dlq", "list"])
    print()
    
    # Show logs for a successful job
    print("Step 10: Sample job logs (successful)")
    subprocess.run(["queuectl", "logs", "img-photo1.jpg"])
    print()
    
    # Show logs for failed job
    print("Step 11: Sample job logs (failed)")
    subprocess.run(["queuectl", "logs", "img-corrupted.jpg"])
    print()
    
    # Stop workers
    print("Step 12: Stopping workers...")
    subprocess.run(["queuectl", "worker", "stop"], check=True)
    print()
    
    # Final summary
    print("=" * 60)
    print("Pipeline Complete!")
    print("=" * 60)
    print()
    print("Summary:")
    print("  ✓ Enqueued 6 image processing jobs")
    print("  ✓ Processed with 3 parallel workers")
    print("  ✓ High-priority (large) images processed first")
    print("  ✓ Failed job moved to DLQ after retries")
    print("  ✓ All outputs captured for inspection")
    print()
    print("Key Features Demonstrated:")
    print("  • Priority-based processing")
    print("  • Parallel worker execution")
    print("  • Automatic retries with exponential backoff")
    print("  • Dead Letter Queue for permanent failures")
    print("  • Job output capture (stdout/stderr)")
    print("  • Graceful worker shutdown")
    print()
    
    # Cleanup
    jobs_file.unlink()


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
