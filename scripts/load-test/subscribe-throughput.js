// 시나리오 C — 구독/구독취소 API 처리량. Ramping VUs로 목표 QPS까지 에러율/지연시간 확인.
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const errors = new Counter('subscribe_errors');

export const options = {
  scenarios: {
    subscribe_cancel: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: '10s', target: 10 },
        { duration: '20s', target: 30 },
        { duration: '10s', target: 0 },
      ],
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<500'],
  },
};

// setup에서 작가 1명을 미리 만들어 모든 VU가 같은 작가를 구독/해지하게 한다
// (재구독 upsert 경로, database-design.md §3.1을 함께 태운다).
export function setup() {
  const res = http.post(
    `${BASE_URL}/api/users`,
    JSON.stringify({ email: `throughput-author-${Date.now()}@test.com`, name: 'author', notificationChannel: 'PUSH' }),
    { headers: { 'Content-Type': 'application/json' } },
  );
  return { authorId: res.json('id') };
}

export default function (data) {
  const email = `throughput-user-${__VU}-${__ITER}-${Date.now()}@test.com`;
  const userRes = http.post(
    `${BASE_URL}/api/users`,
    JSON.stringify({ email, name: 'sub', notificationChannel: 'PUSH' }),
    { headers: { 'Content-Type': 'application/json' } },
  );
  const ok1 = check(userRes, { 'user created': (r) => r.status === 201 });
  if (!ok1) { errors.add(1); return; }
  const userId = userRes.json('id');

  const subRes = http.post(
    `${BASE_URL}/api/subscriptions`,
    JSON.stringify({ userId, authorId: data.authorId }),
    { headers: { 'Content-Type': 'application/json' } },
  );
  const ok2 = check(subRes, { 'subscribed': (r) => r.status === 201 });
  if (!ok2) errors.add(1);

  const cancelRes = http.del(`${BASE_URL}/api/subscriptions?userId=${userId}&authorId=${data.authorId}`);
  const ok3 = check(cancelRes, { 'cancelled': (r) => r.status === 200 });
  if (!ok3) errors.add(1);
}
