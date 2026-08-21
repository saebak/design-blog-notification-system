#!/usr/bin/env bash
# Chaos-2 (was C4, adapted to 단일 컨슈머 구조) — Fan-out 진행 중 앱 프로세스를 강제 종료하고
# 재기동했을 때 중복/누락 없이 정확히 구독자 수만큼 알림이 생성되는지 확인한다.
# 사용법: ./app-crash-midfanout.sh [SUBSCRIBER_COUNT] [BASE_URL] [JAR_PATH]
set -euo pipefail

SUBSCRIBER_COUNT="${1:-500}"
BASE_URL="${2:-http://localhost:8080}"
JAR_PATH="${3:-build/libs/notification-system-0.0.1-SNAPSHOT.jar}"
TS=$(date +%s)

echo "=== Chaos-2: app crash mid-fanout, subscribers=$SUBSCRIBER_COUNT ==="

author_id=$(curl -s -X POST "$BASE_URL/api/users" -H 'Content-Type: application/json' \
  -d "{\"email\":\"chaos2-author-$TS@test.com\",\"name\":\"author\",\"notificationChannel\":\"PUSH\"}" \
  | grep -o '"id":[0-9]*' | head -1 | grep -o '[0-9]*')
echo "author_id=$author_id"

echo "Seeding $SUBSCRIBER_COUNT subscribers..."
seq 1 "$SUBSCRIBER_COUNT" | xargs -P 20 -I{} bash -c '
  BASE_URL="'"$BASE_URL"'" TS="'"$TS"'" AUTHOR_ID="'"$author_id"'" i={}
  sub_id=$(curl -s -X POST "$BASE_URL/api/users" -H "Content-Type: application/json" \
    -d "{\"email\":\"chaos2-sub-$TS-$i@test.com\",\"name\":\"sub-$i\",\"notificationChannel\":\"PUSH\"}" \
    | grep -o "\"id\":[0-9]*" | head -1 | grep -o "[0-9]*")
  curl -s -X POST "$BASE_URL/api/subscriptions" -H "Content-Type: application/json" \
    -d "{\"userId\":$sub_id,\"authorId\":$AUTHOR_ID}" > /dev/null
'

# read model 동기화가 끝났는지 확인 (eventual consistency race를 피하기 위해) —
# 여기서 확인하려는 건 그 레이스가 아니라 앱 크래시 중 멱등성이므로 미리 안정 상태로 만들어둔다.
echo "Waiting for subscriber_read_model to sync..."
for i in $(seq 1 30); do
  cnt=$(docker compose exec -T postgres psql -U notification -d notification_system -tAc \
    "SELECT COUNT(*) FROM notification.subscriber_read_model WHERE author_id=$author_id" | tr -d '[:space:]')
  [ "$cnt" = "$SUBSCRIBER_COUNT" ] && break
  sleep 1
done
echo "read_model synced count=$cnt"

post_id=$(curl -s -X POST "$BASE_URL/api/posts" -H 'Content-Type: application/json' \
  -d "{\"authorId\":$author_id,\"title\":\"chaos2-$TS\",\"content\":\"content\"}" \
  | grep -o '"id":[0-9]*' | head -1 | grep -o '[0-9]*')
echo "post_id=$post_id"

old_pid=$(tasklist //FI "IMAGENAME eq java.exe" //FO CSV 2>/dev/null | tail -1 | cut -d',' -f2 | tr -d '"')
echo "current app PID=$old_pid"

echo "Publishing, then killing the app shortly after..."
curl -s -X POST "$BASE_URL/api/posts/$post_id/publish" > /dev/null &
publish_pid=$!
sleep 0.15
echo "Killing PID $old_pid now"
taskkill //F //PID "$old_pid" > /dev/null 2>&1 || echo "(process may have already exited)"
wait "$publish_pid" 2>/dev/null || true

pre_crash_count=$(docker compose exec -T postgres psql -U notification -d notification_system -tAc \
  "SELECT COUNT(*) FROM notification.notifications WHERE post_id=$post_id" | tr -d '[:space:]')
echo "notifications created before restart: $pre_crash_count / $SUBSCRIBER_COUNT"

echo "Restarting app..."
nohup java -Xmx384m -Xss512k -jar "$JAR_PATH" > app-restart.log 2>&1 &
disown
new_pid=$!
echo "new PID (shell-tracked)=$new_pid"

for i in $(seq 1 40); do
  code=$(curl -s -o /dev/null -w "%{http_code}" "$BASE_URL/actuator/health" 2>/dev/null || echo 000)
  [ "$code" = "200" ] && break
  sleep 2
done
echo "app back up after restart (health=$code)"

echo "Waiting for fan-out to complete (redelivery + reprocessing)..."
deadline=$(( $(date +%s%3N) + 30000 ))
while true; do
  now=$(date +%s%3N)
  cnt=$(docker compose exec -T postgres psql -U notification -d notification_system -tAc \
    "SELECT COUNT(*) FROM notification.notifications WHERE post_id=$post_id" | tr -d '[:space:]')
  distinct_cnt=$(docker compose exec -T postgres psql -U notification -d notification_system -tAc \
    "SELECT COUNT(DISTINCT recipient_id) FROM notification.notifications WHERE post_id=$post_id" | tr -d '[:space:]')
  if [ "$cnt" = "$SUBSCRIBER_COUNT" ]; then
    echo "Fan-out complete after restart: total=$cnt distinct_recipients=$distinct_cnt (expected $SUBSCRIBER_COUNT for both — equal means no duplicates)"
    break
  fi
  if [ "$now" -gt "$deadline" ]; then
    echo "TIMEOUT: total=$cnt distinct_recipients=$distinct_cnt / expected $SUBSCRIBER_COUNT"
    break
  fi
  sleep 0.5
done

echo "=== Done ==="
