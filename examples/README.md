# QueueCtl Examples

This directory contains real-world examples demonstrating queuectl features.

## Available Examples

### 1. Image Processing Pipeline (`image_pipeline.py`)

Demonstrates parallel batch processing with priorities and error handling.

**Features:**
- Priority-based job execution
- Multiple parallel workers
- Automatic retries for failures
- Dead Letter Queue for permanent failures
- Output capture

**Run:**
```bash
python examples/image_pipeline.py
```

### 2. ETL Pipeline (`etl_pipeline.py`)

Shows scheduled job execution in phases (Extract → Transform → Load).

**Features:**
- Scheduled jobs with `run_at` timestamps
- Multi-phase pipeline execution
- Real-time monitoring
- Priority ordering within phases

**Run:**
```bash
python examples/etl_pipeline.py
```

## Creating Your Own Examples

### Basic Pattern

```python
import subprocess

# 1. Initialize
subprocess.run(["queuectl", "init"])

# 2. Enqueue jobs
subprocess.run(["queuectl", "enqueue", '{"command":"echo hello"}'])

# 3. Start workers
subprocess.run(["queuectl", "worker", "start", "--count", "2"])

# 4. Monitor
subprocess.run(["queuectl", "status"])

# 5. Stop workers
subprocess.run(["queuectl", "worker", "stop"])
```

### Tips

- Use `subprocess.run(..., check=True)` to catch errors
- Use `--json` flag for machine-readable output
- Set `QUEUECTL_DB_PATH` environment variable for custom database location
- Always clean up workers with `worker stop`

## Common Use Cases

### Video Transcoding

```python
videos = ["video1.mp4", "video2.mp4", "video3.mp4"]
jobs = [
    {
        "id": f"transcode-{video}",
        "command": f"ffmpeg -i {video} -c:v libx264 output/{video}",
        "priority": 10,
        "timeout_s": 3600,
    }
    for video in videos
]
```

### Web Scraping

```python
urls = ["https://example.com/page1", "https://example.com/page2"]
jobs = [
    {
        "id": f"scrape-{i}",
        "command": f"python scraper.py --url {url}",
        "priority": 5,
        "max_retries": 5,
    }
    for i, url in enumerate(urls)
]
```

### Report Generation

```python
from datetime import datetime, timedelta, timezone

# Schedule daily report for midnight
tomorrow = datetime.now(timezone.utc) + timedelta(days=1)
midnight = tomorrow.replace(hour=0, minute=0, second=0)

job = {
    "id": "daily-report",
    "command": "python generate_report.py --date today",
    "run_at": midnight.isoformat(),
    "priority": 100,
}
```

## More Examples

Check the test suite (`tests/`) for more usage patterns and edge cases.
