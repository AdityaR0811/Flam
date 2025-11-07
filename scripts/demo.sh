#!/bin/bash
# Demo script for queuectl
# Shows all major features of the job queue system

set -e

echo "========================================="
echo "QueueCtl Demo"
echo "========================================="
echo ""

# Set test database
export QUEUECTL_DB_PATH="$HOME/.queuectl/demo.db"

# Clean up any existing demo database
rm -f "$QUEUECTL_DB_PATH" || true
rm -f "$HOME/.queuectl/workers.pid" || true

echo "Step 1: Initialize database"
queuectl init
echo ""

echo "Step 2: Enqueue sample jobs"
echo "  - 1 successful job"
echo "  - 1 invalid command (will fail and retry)"
echo "  - 3 short sleep jobs with different priorities"

# Enqueue success job
queuectl enqueue '{"id":"demo-success","command":"echo Success!","priority":10}'

# Enqueue failing job
queuectl enqueue '{"id":"demo-fail","command":"exit 1","priority":5,"max_retries":2}'

# Enqueue sleep jobs with mixed priorities
for i in {1..5}; do
  priority=$((10 - i))
  queuectl enqueue "{\"id\":\"demo-sleep-$i\",\"command\":\"powershell -Command Start-Sleep -Milliseconds 500; echo Job $i done\",\"priority\":$priority}"
done

# Enqueue a scheduled job (10 seconds in future)
future_time=$(date -u -d '+10 seconds' +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || python -c "from datetime import datetime, timedelta; print((datetime.utcnow() + timedelta(seconds=10)).strftime('%Y-%m-%dT%H:%M:%SZ'))")
queuectl enqueue "{\"id\":\"demo-scheduled\",\"command\":\"echo Scheduled job executed\",\"run_at\":\"$future_time\"}"

echo ""
echo "Step 3: Show initial status"
queuectl status
echo ""

echo "Step 4: Start 3 workers"
queuectl worker start --count 3
echo ""

echo "Waiting for jobs to process..."
sleep 3

echo ""
echo "Step 5: Check status during processing"
queuectl status
echo ""

echo "Step 6: List all jobs"
queuectl list --limit 20
echo ""

echo "Step 7: Wait for more processing"
sleep 5

echo ""
echo "Step 8: Check DLQ (failed jobs)"
queuectl dlq list
echo ""

echo "Step 9: Retry a DLQ job if any"
dlq_job=$(queuectl dlq list --json | python -c "import sys, json; jobs = json.load(sys.stdin); print(jobs[0]['id'] if jobs else '')" || echo "")
if [ -n "$dlq_job" ]; then
  echo "Retrying job: $dlq_job"
  queuectl dlq retry "$dlq_job"
  echo "Waiting for retry..."
  sleep 3
else
  echo "No jobs in DLQ to retry"
fi
echo ""

echo "Step 10: View logs for a specific job"
queuectl logs demo-success || echo "Job not found or not processed yet"
echo ""

echo "Step 11: Show configuration"
queuectl config get
echo ""

echo "Step 12: Final status"
queuectl status --json | python -m json.tool || queuectl status
echo ""

echo "Step 13: Stop workers"
queuectl worker stop
echo ""

echo "========================================="
echo "Demo complete!"
echo "========================================="
echo ""
echo "Key features demonstrated:"
echo "  ✓ Job enqueueing with priorities"
echo "  ✓ Multiple parallel workers"
echo "  ✓ Automatic retries with backoff"
echo "  ✓ Dead Letter Queue (DLQ)"
echo "  ✓ Scheduled jobs"
echo "  ✓ Job output capture"
echo "  ✓ Configuration management"
echo "  ✓ Status monitoring"
echo ""
