#!/usr/bin/env bash
# 시나리오 A(축소판) — 단일 작가 발행 -> Fan-out 완료까지 걸리는 시간 측정.
# 사용법: ./fanout-sla.sh [SUBSCRIBER_COUNT] [BASE_URL]
set -euo pipefail

SUBSCRIBER_COUNT="${1:-300}"
BASE_URL="${2:-http://localhost:8080}"
TS=$(date +%s)

echo "=== Scenario A (adapted): fanout SLA, subscribers=$SUBSCRIBER_COUNT ==="

author_id=$(curl -s -X POST "$BASE_URL/api/users" -H 'Content-Type: application/json' \
  -d "{\"email\":\"loadtest-author-$TS@test.com\",\"name\":\"author\",\"notificationChannel\":\"PUSH\"}" \
  | grep -o '"id":[0-9]*' | head -1 | grep -o '[0-9]*')
echo "author_id=$author_id"

echo "Seeding $SUBSCRIBER_COUNT subscribers..."
seed_start=$(date +%s%3N)
seq 1 "$SUBSCRIBER_COUNT" | xargs -P 20 -I{} bash -c '
  BASE_URL="'"$BASE_URL"'" TS="'"$TS"'" AUTHOR_ID="'"$author_id"'" i={}
  sub_id=$(curl -s -X POST "$BASE_URL/api/users" -H "Content-Type: application/json" \
    -d "{\"email\":\"loadtest-sub-$TS-$i@test.com\",\"name\":\"sub-$i\",\"notificationChannel\":\"PUSH\"}" \
    | grep -o "\"id\":[0-9]*" | head -1 | grep -o "[0-9]*")
  curl -s -X POST "$BASE_URL/api/subscriptions" -H "Content-Type: application/json" \
    -d "{\"userId\":$sub_id,\"authorId\":$AUTHOR_ID}" > /dev/null
'
seed_end=$(date +%s%3N)
echo "Seeding took $((seed_end - seed_start)) ms"

post_id=$(curl -s -X POST "$BASE_URL/api/posts" -H 'Content-Type: application/json' \
  -d "{\"authorId\":$author_id,\"title\":\"load-test-$TS\",\"content\":\"content\"}" \
  | grep -o '"id":[0-9]*' | head -1 | grep -o '[0-9]*')
echo "post_id=$post_id"

publish_start=$(date +%s%3N)
curl -s -X POST "$BASE_URL/api/posts/$post_id/publish" > /dev/null

deadline=$((publish_start + 30000))
count=0
while true; do
  now=$(date +%s%3N)
  count=$(docker compose exec -T postgres psql -U notification -d notification_system -tAc \
    "SELECT COUNT(*) FROM notification.notifications WHERE post_id=$post_id" | tr -d '[:space:]')
  if [ "$count" = "$SUBSCRIBER_COUNT" ]; then
    echo "Fan-out complete: $count/$SUBSCRIBER_COUNT notifications at $((now - publish_start)) ms"
    break
  fi
  if [ "$now" -gt "$deadline" ]; then
    echo "TIMEOUT after $((now - publish_start)) ms, only $count/$SUBSCRIBER_COUNT notifications created"
    break
  fi
  sleep 0.2
done

sent_deadline=$((now + 30000))
while true; do
  now2=$(date +%s%3N)
  sent=$(docker compose exec -T postgres psql -U notification -d notification_system -tAc \
    "SELECT COUNT(*) FROM notification.notification_delivery_log dl JOIN notification.notifications n ON n.id = dl.notification_id WHERE n.post_id=$post_id AND dl.status='SENT'" | tr -d '[:space:]')
  if [ "$sent" = "$SUBSCRIBER_COUNT" ]; then
    echo "Push delivery complete: $sent/$SUBSCRIBER_COUNT SENT at $((now2 - publish_start)) ms since publish"
    break
  fi
  if [ "$now2" -gt "$sent_deadline" ]; then
    echo "TIMEOUT waiting for push delivery, only $sent/$SUBSCRIBER_COUNT SENT"
    break
  fi
  sleep 0.2
done

echo "=== Done ==="
