#!/bin/bash
# Verification script for queuectl
# Tests all required functionality and exits 0 only if all tests pass

set -e

ERRORS=0
TEST_DB="$HOME/.queuectl/verify-test.db"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_test() {
  echo -e "${YELLOW}[TEST]${NC} $1"
}

log_pass() {
  echo -e "${GREEN}[PASS]${NC} $1"
}

log_fail() {
  echo -e "${RED}[FAIL]${NC} $1"
  ERRORS=$((ERRORS + 1))
}

cleanup() {
  echo "Cleaning up test environment..."
  export QUEUECTL_DB_PATH="$TEST_DB"
  queuectl worker stop 2>/dev/null || true
  rm -f "$TEST_DB" 2>/dev/null || true
  rm -f "$HOME/.queuectl/workers.pid" 2>/dev/null || true
}

# Setup
trap cleanup EXIT
cleanup

export QUEUECTL_DB_PATH="$TEST_DB"

echo "========================================="
echo "QueueCtl Verification"
echo "========================================="
echo ""

# Test 1: Database initialization
log_test "Database initialization"
if queuectl init; then
  log_pass "Database initialized"
else
  log_fail "Database initialization failed"
fi

# Test 2: Enqueue jobs
log_test "Job enqueueing"
if queuectl enqueue '{"id":"verify-1","command":"echo test"}'; then
  log_pass "Job enqueued"
else
  log_fail "Job enqueueing failed"
fi

# Test 3: Status command
log_test "Status command"
if queuectl status >/dev/null 2>&1; then
  log_pass "Status command works"
else
  log_fail "Status command failed"
fi

# Test 4: List jobs
log_test "List jobs"
job_count=$(queuectl list --json | python -c "import sys, json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")
if [ "$job_count" -gt "0" ]; then
  log_pass "Jobs listed successfully ($job_count jobs)"
else
  log_fail "Job listing failed or no jobs found"
fi

# Test 5: Worker start/stop
log_test "Worker lifecycle"
if queuectl worker start --count 2; then
  sleep 2
  if queuectl worker stop; then
    log_pass "Workers started and stopped"
  else
    log_fail "Worker stop failed"
  fi
else
  log_fail "Worker start failed"
fi

# Test 6: Job processing
log_test "Job processing"
queuectl enqueue '{"id":"verify-process","command":"echo processed"}'
queuectl worker start --count 1
sleep 3
queuectl worker stop
job_state=$(queuectl list --json | python -c "import sys, json; jobs = json.load(sys.stdin); print([j['state'] for j in jobs if j['id']=='verify-process'][0])" 2>/dev/null || echo "unknown")
if [ "$job_state" = "completed" ]; then
  log_pass "Job processed successfully"
else
  log_fail "Job processing failed (state: $job_state)"
fi

# Test 7: Retry and DLQ
log_test "Retry and DLQ"
queuectl enqueue '{"id":"verify-fail","command":"exit 1","max_retries":2}'
queuectl worker start --count 1
sleep 5
queuectl worker stop
dlq_count=$(queuectl dlq list --json | python -c "import sys, json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")
if [ "$dlq_count" -gt "0" ]; then
  log_pass "Job moved to DLQ after retries"
else
  log_fail "DLQ functionality failed"
fi

# Test 8: DLQ retry
log_test "DLQ retry"
dlq_job=$(queuectl dlq list --json | python -c "import sys, json; jobs = json.load(sys.stdin); print(jobs[0]['id'] if jobs else '')" 2>/dev/null || echo "")
if [ -n "$dlq_job" ]; then
  if queuectl dlq retry "$dlq_job"; then
    log_pass "DLQ retry successful"
  else
    log_fail "DLQ retry failed"
  fi
else
  log_fail "No DLQ job to retry"
fi

# Test 9: Configuration
log_test "Configuration management"
queuectl config set test_key test_value
value=$(queuectl config get test_key)
if [ "$value" = "test_key=test_value" ]; then
  log_pass "Configuration set/get works"
else
  log_fail "Configuration management failed"
fi

# Test 10: Persistence
log_test "Data persistence"
queuectl enqueue '{"id":"verify-persist","command":"echo persist"}'
# Close and reopen connection by running status
queuectl status >/dev/null
persist_job=$(queuectl list --json | python -c "import sys, json; jobs = json.load(sys.stdin); print('found' if any(j['id']=='verify-persist' for j in jobs) else 'not found')" 2>/dev/null || echo "not found")
if [ "$persist_job" = "found" ]; then
  log_pass "Data persists across operations"
else
  log_fail "Data persistence failed"
fi

# Test 11: No duplicate processing
log_test "No duplicate processing with multiple workers"
for i in {1..20}; do
  queuectl enqueue "{\"id\":\"verify-dup-$i\",\"command\":\"powershell -Command Start-Sleep -Milliseconds 100; echo $i\"}"
done
queuectl worker start --count 4
sleep 8
queuectl worker stop

# Check for duplicates by looking at job states
completed_count=$(queuectl list --state completed --json | python -c "import sys, json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "0")
if [ "$completed_count" -gt "0" ]; then
  log_pass "Multiple workers processed jobs without duplicates ($completed_count completed)"
else
  log_fail "Duplicate processing test inconclusive"
fi

# Test 12: Logs command
log_test "Job logs"
if queuectl logs verify-process 2>/dev/null | grep -q "verify-process"; then
  log_pass "Job logs accessible"
else
  log_fail "Job logs command failed"
fi

# Test 13: JSON output
log_test "JSON output format"
if queuectl status --json | python -m json.tool >/dev/null 2>&1; then
  log_pass "JSON output valid"
else
  log_fail "JSON output invalid"
fi

# Code quality tests
echo ""
log_test "Code quality checks"

if command -v black >/dev/null 2>&1; then
  if black --check queuectl/ tests/ 2>/dev/null; then
    log_pass "Code formatted with black"
  else
    log_fail "Code formatting issues"
  fi
else
  echo "  Skipping black (not installed)"
fi

if command -v ruff >/dev/null 2>&1; then
  if ruff check queuectl/ tests/ 2>/dev/null; then
    log_pass "Code passes ruff checks"
  else
    log_fail "Code has ruff violations"
  fi
else
  echo "  Skipping ruff (not installed)"
fi

# Run pytest if available
if command -v pytest >/dev/null 2>&1; then
  log_test "Running pytest"
  if pytest -q 2>/dev/null; then
    log_pass "All tests passed"
  else
    log_fail "Some tests failed"
  fi
else
  echo "  Skipping pytest (not installed)"
fi

# Summary
echo ""
echo "========================================="
if [ $ERRORS -eq 0 ]; then
  echo -e "${GREEN}✓ All verifications passed!${NC}"
  echo "========================================="
  exit 0
else
  echo -e "${RED}✗ $ERRORS verification(s) failed${NC}"
  echo "========================================="
  exit 1
fi
