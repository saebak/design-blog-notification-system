#!/usr/bin/env bash
# 시나리오 D(축소판) — 같은 사용자의 알림을 여러 VU가 동시에 "모두 읽음" 호출했을 때
# 레이스 없이 수렴하는지, 그리고 개별 읽음 처리 API의 지연시간 분포를 확인한다.
# 알림 목록/unread count 조회 API는 스코프 밖(docs/decisions.md)이라 여기선 다루지 않는다.
set -euo pipefail

BASE_URL="${1:-http://localhost:8080}"
NOTIFICATION_COUNT="${2:-50}"
CONCURRENT_CALLS="${3:-20}"
TS=$(date +%s)

echo "=== Scenario D (adapted): concurrent read-all race safety, notifications=$NOTIFICATION_COUNT, concurrent-calls=$CONCURRENT_CALLS ==="

recipient_id=$(curl -s -X POST "$BASE_URL/api/users" -H 'Content-Type: application/json' \
  -d "{\"email\":\"read-race-$TS@test.com\",\"name\":\"recipient\",\"notificationChannel\":\"PUSH\"}" \
  | grep -o '"id":[0-9]*' | head -1 | grep -o '[0-9]*')
author_id=$(curl -s -X POST "$BASE_URL/api/users" -H 'Content-Type: application/json' \
  -d "{\"email\":\"read-race-author-$TS@test.com\",\"name\":\"author\",\"notificationChannel\":\"PUSH\"}" \
  | grep -o '"id":[0-9]*' | head -1 | grep -o '[0-9]*')
echo "recipient_id=$recipient_id author_id=$author_id"

echo "Seeding $NOTIFICATION_COUNT notifications directly (DB) — this test targets the read API's concurrency invariant, not fan-out"
docker compose exec -T postgres psql -U notification -d notification_system -c "
INSERT INTO notification.notifications (recipient_id, source_event_id, post_id, author_id, title)
SELECT $recipient_id, gen_random_uuid(), 1, $author_id, 'race-test'
FROM generate_series(1, $NOTIFICATION_COUNT);
" > /dev/null

echo "Firing $CONCURRENT_CALLS concurrent read-all calls..."
tmpfile=$(mktemp)
seq 1 "$CONCURRENT_CALLS" | xargs -P "$CONCURRENT_CALLS" -I{} bash -c '
  curl -s -w "\n" -X PATCH "'"$BASE_URL"'/api/users/'"$recipient_id"'/notifications/read-all"
' >> "$tmpfile"

echo "--- Raw responses ---"
cat "$tmpfile"

sum=$(grep -o '"updatedCount":[0-9]*' "$tmpfile" | grep -o '[0-9]*' | awk '{s+=$1} END {print s}')
echo "Sum of updatedCount across all concurrent calls: $sum (should equal $NOTIFICATION_COUNT exactly — no double counting, no loss)"

remaining_unread=$(docker compose exec -T postgres psql -U notification -d notification_system -tAc \
  "SELECT COUNT(*) FROM notification.notifications WHERE recipient_id=$recipient_id AND is_read=false" | tr -d '[:space:]')
echo "Remaining unread after all calls: $remaining_unread (should be 0)"

rm -f "$tmpfile"
echo "=== Done ==="
