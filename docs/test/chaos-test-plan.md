# 장애 주입(Chaos) 테스트 시나리오 설계

> 실행 결과는 [`chaos-test-report.md`](./chaos-test-report.md)에 기록한다. 과거 수동 기준선과 함께 2026-09-10 현행 영속 재시도/claim lease 구조의 자동 복구 테스트 결과가 기록돼 있다.

## 0. 목적 및 범위

- **검증 대상**: NFR-2(High Availability) — 알림 시스템(팬아웃 워커, 발송기, 큐)의 장애가 블로그 글 등록(Write Path)에 영향을 주지 않아야 한다. 원 요구사항이 예시로 든 "알림 워커 강제 종료 후 글 등록 API 정상 동작 확인"(NFR-2.4)을 최소 1개 시나리오로 포함한다.
- **범위 밖 (의도적 제외)**: DB 인스턴스 자체를 강제 종료하는 시나리오는 포함하지 않는다. `architecture.md` §8은 PostgreSQL이 Post/Subscription/Notification이 공유하는 단일 인스턴스라 DB 자체 장애가 공동 장애점(SPOF)이며, 이 격리는 NFR-2.1이 명시한 애플리케이션 프로세스 레벨(팬아웃 워커, 발송기, 큐)까지라 DB 장애는 그 범위 밖이라고 결론 내렸다(이 토폴로지 결정 자체는 `database-design.md` §0 참고). 이 문서도 그 스코프를 따라 애플리케이션 프로세스/메시지 브로커/캐시 레벨의 장애만 다룬다.

## 1. 장애 주입 방법

- 로컬 `docker-compose.yml` 기반 환경에서 Kafka는 `docker pause`/`unpause`로 장애를 주입한다. 현재 애플리케이션은 Dispatcher/Chunk Worker가 한 Spring Boot 프로세스에 있으므로 C1/C5의 프로세스 크래시는 `scripts/chaos/app-crash-midfanout.sh`에 정확한 APP_PID를 넘겨 검증한다.
- 네트워크 파티션(지연/패킷 유실 주입, 예: toxiproxy)은 현재 범위를 넘어서는 정교함이라 이번 범위에서는 다루지 않는다 — 필요 시 향후 확장 항목으로만 언급한다.

## 2. 시나리오

각 시나리오는 절차 → 관찰 지표 → 기대 결과 → 실패 판정 기준 순으로 기술한다.

### C1. Fan-out Dispatcher / Chunk Worker 강제 종료 중 글 등록 API 정상 동작 (NFR-2.1, 원 요구사항 예시)
- **절차**: Dispatcher와 Chunk Worker 컨테이너를 모두 중단시킨 상태에서 글 등록 API(`POST /posts`, `POST /posts/:id/publish`)를 반복 호출.
- **관찰 지표**: 글 등록 API의 응답 코드/지연시간, `outbox_events` 테이블에 이벤트가 정상 적재되는지.
- **기대 결과**: Notification 관련 프로세스가 전부 죽어 있어도 글 등록 API는 정상 응답(2xx)하고 Outbox에는 이벤트가 쌓인다. 워커를 복구하면 밀린 이벤트가 순차적으로 팬아웃 처리된다.
- **실패 판정**: 글 등록 API가 타임아웃/5xx를 반환하거나, Outbox 적재가 실패하면 실패로 간주.

### C2. Kafka 브로커 다운 중 글 발행 (NFR-2.2)
- **자동 검증**: `KafkaOutageRecoveryIntegrationTest`가 Kafka pause → 글 발행 → 복구 → 최종 알림 생성을 검증한다.
- **절차**: Kafka 컨테이너를 중단시킨 상태에서 글 등록/발행 API 호출 → 이후 Kafka를 복구.
- **관찰 지표**: 발행 API 응답, `outbox_events.status`(PENDING 유지 여부), 브로커 복구 후 Outbox Relay가 재시도로 catch-up 발행하는지, 최종적으로 Fan-out이 정상 완료되는지.
- **기대 결과**: 브로커 장애와 무관하게 글 등록 트랜잭션은 커밋된다(Outbox 패턴, NFR-2.2). 브로커 복구 후 Relay가 자동으로 밀린 이벤트를 발행하고 Fan-out이 지연은 있었지만 정상 완료된다.
- **실패 판정**: 글 등록 트랜잭션 자체가 브로커 장애로 실패하거나, 브로커 복구 후에도 밀린 이벤트가 발행되지 않으면 실패.

### C3. Push Worker 중단·claim 만료·수동 복구
- **절차**: Push Worker가 행을 `PROCESSING`으로 claim한 뒤 처리를 중단하거나 실패를 반복시킨다. lease 만료 후 다른 워커의 회수와 `DEAD_LETTER` 수동 복구를 확인한다.
- **관찰 지표**: 상태별 `notification_delivery_log` 건수, claim token/lease, `notification_delivery_attempts{channel="push",status}`, 복구 API 응답.
- **기대 결과**: 유효한 claim은 한 워커만 소유하고, 만료 claim은 다른 워커가 회수한다. 재시도 초과 건은 `DEAD_LETTER`에 남으며 운영자가 다시 `PENDING`으로 전환할 수 있다.
- **자동 검증**: `DeliveryClaimIntegrationTest`, `NotificationRecoveryIntegrationTest`.
- **확장 범위**: Email 채널 장애 격리는 Email 구현 이후 검증한다.

### C4. 팬아웃 도중 Chunk Worker 일부 크래시 후 재시작 (`architecture.md` §4.3 멱등성)
- **절차**: 대량 구독자(예: 10만 명) 대상 Fan-out이 진행 중일 때 Chunk Worker 인스턴스 일부를 강제 종료 → 컨슈머 그룹 리밸런싱 후 재시작.
- **관찰 지표**: 재시작 전후 `notifications` 테이블의 `(recipient_id, source_event_id)` 중복 여부, 최종적으로 전체 구독자 수만큼 알림이 정확히 1건씩 생성됐는지.
- **기대 결과**: `uq_recipient_event` 유니크 제약(`database-design.md` §4.2)이 최종 방어선 역할을 해 일부 청크가 중복 재처리돼도 중복 INSERT는 conflict로 걸러진다. 최종 알림 건수는 구독자 수와 정확히 일치한다.
- **실패 판정**: 중복 알림이 발생하거나, 반대로 일부 구독자가 알림을 아예 받지 못하면 실패.
- **자동 검증**: `FanoutChunkBoundaryIntegrationTest`가 청크 경계와 수신자 멱등성을 검증한다.

### C5. Dispatcher 크래시 후 재기동 (`architecture.md` §4.3 멱등성)
- **절차**: Dispatcher가 키셋 스캔 도중(예: 10만 명 중 절반쯤 스캔했을 때) 강제 종료되도록 유도 → 재시작.
- **관찰 지표**: 처리 이력 테이블의 `cursor`/`status` 값, 재시작 후 스캔이 처음부터 다시 시작되는지 아니면 저장된 cursor부터 재개되는지, 최종 발행된 청크의 커버리지(중복/누락 여부).
- **기대 결과**: `architecture.md` §4.3에 정의된 대로 `status='IN_PROGRESS'`인 상태로 재시작 시 저장된 cursor부터 스캔을 재개하고, 전체 재스캔에 따른 대량 중복 발행 없이 스캔이 완료된다(§4.3에 따라 완전한 무중복까지는 보장하지 않으나, 최종 방어선인 `notifications`의 unique 제약으로 중복 알림은 없어야 한다).
- **실패 판정**: 재시작 후 스캔이 처음부터 다시 시작되어 대량 중복 청크가 발행되거나, cursor 이후 구간이 누락되면 실패.
- **자동 검증**: `FanoutDispatcherRetryIntegrationTest`가 영속 상태 전이/재시도를, 수동 스크립트가 실제 프로세스 크래시 후 재개를 검증한다.

### C6. Redis(Pub/Sub, unread 캐시) 다운 — 확장 시나리오
- **절차**: Redis 컨테이너를 중단시킨 상태에서 (1) 신규 알림 발생 시 실시간 WebSocket push가 어떻게 되는지, (2) 알림 목록 조회 API가 정상 응답하는지 확인.
- **관찰 지표**: WebSocket 클라이언트의 실시간 수신 여부(끊김 예상), 알림 목록/읽음 처리 API의 응답 코드와 지연시간(RDB 직접 조회로 폴백되는지).
- **기대 결과**: 실시간 push는 Best-effort이므로 Redis 장애 시 끊기는 것이 정상이다(`architecture.md` §6 — 실시간 채널 장애가 나도 알림 자체는 DB에 저장돼 있어 유실되지 않음). 반면 알림 목록 조회(Source of truth)는 Redis 없이도 RDB만으로 정상 응답해야 한다.
- **실패 판정**: Redis 장애로 알림 목록 조회 API까지 실패하거나 5xx를 반환하면 실패(Read Path가 캐시에 과도하게 의존하고 있다는 신호).

> 현재 Redis/WebSocket/알림 목록 조회는 런타임 경로에 없으므로 이 시나리오는 현행 합격 판정에서 제외한다.

## 3. 결과 판정 공통 기준

- **"정상 동작"의 판단 기준**: (1) API 응답 코드가 2xx인가, (2) 응답 지연시간이 평상시 대비 비정상적으로(예: 수 배 이상) 증가하지 않았는가, (3) 데이터 정합성(중복/누락 없음)이 유지되는가 — 세 가지를 모두 만족해야 해당 시나리오를 "통과"로 판정한다.
- 각 시나리오의 실패는 원인(어느 컴포넌트가 어떤 방식으로 격리에 실패했는지)까지 함께 기록해 설계 문서(`architecture.md`)의 어느 가정이 깨졌는지 역추적할 수 있게 한다.

## 4. 다음 단계

- C2~C5 자동 통합 테스트를 회귀 테스트로 유지한다.
- 배포 토폴로지가 프로세스를 분리하면 C1을 컴포넌트별 중단 시나리오로 확장한다. 현재는 `scripts/chaos/app-crash-midfanout.sh`에 명시적 APP_PID를 전달한다.
- 실행 결과는 [`chaos-test-report.md`](./chaos-test-report.md)에 날짜와 구현 기준을 구분해 기록한다.
