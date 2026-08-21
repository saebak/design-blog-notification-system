// 시나리오 D 보충 — 개별 알림 읽음 처리 API의 동시 요청 지연시간 분포.
// setup()에서 알림 N건을 DB에 직접 시딩(seed-notifications.sql 대체, 순수 API 레이턴시 측정이 목적이라
// fan-out 경유는 A/B 시나리오가 이미 검증했으므로 생략)한 뒤, VU마다 서로 다른 알림 id를 1회씩 읽음 처리한다.
import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const NOTIFICATION_IDS = __ENV.NOTIFICATION_IDS.split(',').map(Number);
const RECIPIENT_ID = __ENV.RECIPIENT_ID;

export const options = {
  scenarios: {
    read_one_each: {
      executor: 'shared-iterations',
      vus: Math.min(30, NOTIFICATION_IDS.length),
      iterations: NOTIFICATION_IDS.length,
      maxDuration: '30s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<300'],
  },
};

export default function () {
  const id = NOTIFICATION_IDS[(__VU - 1 + __ITER * 30) % NOTIFICATION_IDS.length];
  const res = http.patch(`${BASE_URL}/api/notifications/${id}/read?recipientId=${RECIPIENT_ID}`);
  check(res, { 'read ok': (r) => r.status === 200 || r.status === 404 });
}
