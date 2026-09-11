# 알림 파이프라인 운영 가이드

## 관측 지표

`/actuator/prometheus`에서 다음 지표를 수집한다. event ID나 사용자 ID는 라벨로 사용하지 않는다.

| 지표 | 의미 |
|---|---|
| `notification_fanout_dispatches{status}` | `in_progress`, `waiting_retry`, `failed` 상태별 현재 작업 수 |
| `notification_fanout_chunks_dispatched_total` | Kafka에 발행된 Fan-out 청크 누계 |
| `notification_fanout_chunks_completed_total` | Chunk Worker가 완료한 청크 누계 |
| `notification_fanout_dispatch_deferred_total` | Read Model 미동기화 등으로 연기된 횟수 |
| `notification_fanout_dispatch_failed_total` | 자동 재시도를 모두 소진한 횟수 |
| `notification_delivery_attempts{channel="push",status}` | Push 발송 상태별 현재 건수 |
| `notification_delivery_claimed_total{channel="push"}` | 워커가 claim한 발송 건수 |
| `notification_delivery_sent_total{channel="push"}` | 성공한 Push 발송 누계 |
| `notification_delivery_failed_total{channel="push",terminal}` | Push 발송 실패 누계 |
| `notification_manual_recovery_total{type}` | 운영자가 수동 재처리한 누계 |

경보 규칙 예시는 [`../ops/prometheus/notification-alerts.yml`](../ops/prometheus/notification-alerts.yml)에 있다. backlog 임계값은 실제 운영 트래픽을 측정한 뒤 조정한다.

## 수동 복구

장애 원인을 먼저 해소한 뒤 호출한다.

- `POST /api/internal/notification-recovery/fanout/{eventId}`: `FAILED` Fan-out을 `WAITING_RETRY`로 전환한다.
- `POST /api/internal/notification-recovery/delivery/{deliveryId}`: `DEAD_LETTER` Push 발송을 `PENDING`으로 전환하고 시도 횟수를 초기화한다.

잘못된 상태는 `409 Conflict`, 존재하지 않는 Fan-out은 `404 Not Found`를 반환한다. 인증 기능은 확장 범위이므로 이 API는 외부에 공개하지 말고 내부 네트워크나 API Gateway 접근 제어로 보호해야 한다.

## 장애 검증

- Kafka pause 중에도 글 발행과 Outbox 트랜잭션은 커밋되고, 복구 후 Relay와 Fan-out이 자동으로 따라잡는다.
- 만료된 Push claim은 다른 워커가 회수하며 이전 claim token으로는 완료할 수 없다.
- 저장된 Fan-out cursor와 chunk index 이후부터 재개해 최종 알림의 중복과 누락을 막는다.
- FAILED/DEAD_LETTER 수동 재처리와 상태별 gauge 노출을 검증한다.

## 확장 구현 백로그

다음 항목은 현재 구현 범위에 포함하지 않는다.

1. Fan-out 스캔 중 변경까지 발행 시점 기준으로 고정하는 watermark 또는 versioned snapshot
2. 인증·인가와 내부 복구 API의 역할 기반 접근 제어
3. Email 발송 채널과 채널별 독립 장애 격리
4. 사용자용 알림 목록·실시간 전송 등 제품 기능 확장
