#!/usr/bin/env bash
# 시나리오 B(축소판) — 여러 작가가 동시에 발행했을 때 작가별 Fan-out 완료 시간 편차 측정.
# 사용법: ./concurrent-authors.sh [AUTHOR_COUNT] [SUBSCRIBERS_PER_AUTHOR] [BASE_URL]
set -euo pipefail

AUTHOR_COUNT="${1:-5}"
SUBS_PER_AUTHOR="${2:-50}"
BASE_URL="${3:-http://localhost:8080}"
TS=$(date +%s)

echo "=== Scenario B (adapted): concurrent authors=$AUTHOR_COUNT, subscribers each=$SUBS_PER_AUTHOR ==="

declare -a AUTHOR_IDS
declare -a POST_IDS

for a in $(seq 1 "$AUTHOR_COUNT"); do
  author_id=$(curl -s -X POST "$BASE_URL/api/users" -H 'Content-Type: application/json' \
    -d "{\"email\":\"loadtest-b-author-$TS-$a@test.com\",\"name\":\"author-$a\",\"notificationChannel\":\"PUSH\"}" \
    | grep -o '"id":[0-9]*' | head -1 | grep -o '[0-9]*')
  AUTHOR_IDS[$a]=$author_id
  echo "author $a -> id=$author_id"

  seq 1 "$SUBS_PER_AUTHOR" | xargs -P 20 -I{} bash -c '
    BASE_URL="'"$BASE_URL"'" TS="'"$TS"'" A="'"$a"'" AUTHOR_ID="'"$author_id"'" i={}
    sub_id=$(curl -s -X POST "$BASE_URL/api/users" -H "Content-Type: application/json" \
      -d "{\"email\":\"loadtest-b-sub-$TS-$A-$i@test.com\",\"name\":\"sub-$A-$i\",\"notificationChannel\":\"PUSH\"}" \
      | grep -o "\"id\":[0-9]*" | head -1 | grep -o "[0-9]*")
    curl -s -X POST "$BASE_URL/api/subscriptions" -H "Content-Type: application/json" \
      -d "{\"userId\":$sub_id,\"authorId\":$AUTHOR_ID}" > /dev/null
  '

  post_id=$(curl -s -X POST "$BASE_URL/api/posts" -H 'Content-Type: application/json' \
    -d "{\"authorId\":$author_id,\"title\":\"load-test-b-$TS-$a\",\"content\":\"content\"}" \
    | grep -o '"id":[0-9]*' | head -1 | grep -o '[0-9]*')
  POST_IDS[$a]=$post_id
done

echo "Publishing all $AUTHOR_COUNT posts as close together as possible..."
publish_start=$(date +%s%3N)
for a in $(seq 1 "$AUTHOR_COUNT"); do
  curl -s -X POST "$BASE_URL/api/posts/${POST_IDS[$a]}/publish" > /dev/null &
done
wait
echo "All publish calls dispatched at t=0 (baseline $publish_start)"

deadline=$((publish_start + 30000))
declare -A DONE
remaining=$AUTHOR_COUNT
while [ "$remaining" -gt 0 ]; do
  now=$(date +%s%3N)
  for a in $(seq 1 "$AUTHOR_COUNT"); do
    [ -n "${DONE[$a]:-}" ] && continue
    cnt=$(docker compose exec -T postgres psql -U notification -d notification_system -tAc \
      "SELECT COUNT(*) FROM notification.notifications WHERE post_id=${POST_IDS[$a]}" | tr -d '[:space:]')
    if [ "$cnt" = "$SUBS_PER_AUTHOR" ]; then
      DONE[$a]=$((now - publish_start))
      remaining=$((remaining - 1))
      echo "author $a (post ${POST_IDS[$a]}) complete at ${DONE[$a]} ms"
    fi
  done
  if [ "$now" -gt "$deadline" ]; then
    echo "TIMEOUT — remaining incomplete authors: $remaining"
    break
  fi
  sleep 0.2
done

echo "=== Done ==="
