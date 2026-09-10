# 부하 테스트 시나리오 설계

> 실행 결과는 [`load-test-report.md`](./load-test-report.md)에 기록한다. 현재 보고서는 이전 단일 Fan-out Consumer 구현의 기준선이다. 현행 구조는 1,000명 키셋 청크, 32개 Kafka 파티션, Chunk Worker 동시성 6이며 이 구성으로 재측정해야 한다.

## 0. 목적 및 범위

- **검증 대상**: NFR-1(Low Latency — 5초 이내 10만 명 팬아웃, 20,000 msg/sec), NFR-3(Scalability — 순간 QPS 흡수, 핫 파티션 대응, 백프레셔).
- **테스트 범위**: API 계층(글 등록/구독/읽음 처리), Fan-out 파이프라인, DB 폴링 Push 작업 적재까지다. Email, 알림 목록, WebSocket/SSE는 확장 범위라 현행 성능 합격 판정에서 제외한다.

## 1. 도구 및 측정 방법

- **부하 생성 도구**: k6 (`docs/architecture.md` §2에서 선정 — HTTP + WebSocket 시나리오를 하나의 스크립트 체계로 작성 가능).
- **Fan-out 완료 시점 측정**: Fan-out은 비동기이므로 발행 API의 HTTP 응답 시간만으로는 SLA를 검증할 수 없다. 아래 두 방법 중 하나(또는 병행)로 간접 측정한다.
  1. `fanout_dispatches`에서 대상 event ID의 상태가 `DONE`인지 확인하고, `notifications`의 `WHERE source_event_id = :eventId` 행 수가 기대 수신자 수와 같은 시점을 완료로 삼는다.
  2. Prometheus의 전역 counter/gauge(`notification_fanout_chunks_*`, `notification_fanout_dispatches{status}`)는 전체 처리 추세와 적체 감시에 사용한다. 고 cardinality를 피하려고 event ID 라벨은 노출하지 않으므로 단일 이벤트 완료 판정에는 DB를 사용한다.
- **공통 측정 지표**: API p50/p95/p99 지연시간, 에러율, Kafka consumer lag, Fan-out wall-clock 시간, msg/sec, Push pending/dead-letter gauge. 정확한 지표명은 [`../operations.md`](../operations.md)를 따른다.

## 2. 사전 조건

- 부하 테스트 실행 전, 기존 `scripts/load-test`의 시드/검증 스크립트로 특정 작가 1명에 대해 10만 명의 구독자와 Subscriber Read Model을 준비한다(시나리오 A/B의 전제).
- 테스트는 로컬 `docker-compose.yml` 기반 인프라(PostgreSQL/Kafka/Redis) 위에서 수행하며, 실제 프로덕션 등급 인프라 사양이 아니므로 절대적 처리량보다는 **설계가 목표한 배율(예: 워커 수 대비 처리량 선형 증가 여부)**을 확인하는 데 의의를 둔다.

## 3. 시나리오

### A. 대량 팬아웃 SLA 검증 (핵심 시나리오, NFR-1.1 / NFR-1.3)
- **목적**: 인기 작가 1명이 글을 발행했을 때 10만 구독자 전원에게 5초 이내(20,000 msg/sec 이상)로 팬아웃이 완료되는지 확인한다.
- **절차**: 사전 조건의 10만 구독자 보유 작가로 글 발행 API(`POST /posts/:id/publish`)를 1회 호출 → §1의 측정 방법으로 Fan-out 완료 시점까지의 경과 시간을 기록.
- **측정 지표**: 발행 시각부터 `fanout_dispatches.status = DONE`이고 `notifications` 행 수가 10만이 될 때까지의 wall-clock 시간과 초당 처리량(msg/sec).
- **성공 기준**: 완료까지 5초 이내, 평균 처리량 20,000 msg/sec 이상. 우선 현재 동시성 6으로 측정하고, 목표 미달 시 동시성과 인스턴스 수를 단계적으로 올려 처리량 상관관계를 기록한다.

### B. 동시 다발 발행 — 순간 QPS 흡수 (NFR-3.1 / NFR-3.3)
- **목적**: 여러 인기 작가가 짧은 시간 내에 동시에 글을 발행해도 특정 Kafka 파티션에 부하가 쏠리지 않고 전체적으로 처리량이 유지되는지 확인한다.
- **절차**: 각각 1만~10만 명 구독자를 가진 작가 5~10명이 수 초 간격으로 동시에 발행 → 각 작가별 Fan-out 완료 시간을 개별 측정.
- **측정 지표**: 작가별 Fan-out 완료 시간 편차(특정 작가만 유독 느려지는지), `fanout.chunk.requested` 토픽의 파티션별 컨슈머 lag 분포(`architecture.md` §7 — 청크 토픽은 랜덤/round-robin 파티셔닝이므로 고르게 분산돼야 함).
- **성공 기준**: 동시 발행 작가 수와 무관하게 개별 작가의 Fan-out 완료 시간이 시나리오 A 대비 크게 저하되지 않음. 파티션 간 lag 편차가 특정 파티션에 집중되지 않음.

### C. 구독/구독취소 API 처리량
- **목적**: 구독/구독취소 API(Stateless HTTP 서버, Consumer Group 기반은 아님)가 인스턴스 수에 비례해 처리량이 늘어나는지 확인한다. NFR-3.2가 명시하는 대상(팬아웃 워커/발송기)과는 다른 컴포넌트이므로 이 시나리오는 특정 NFR 번호에 대응시키지 않는다 — NFR-3.2 자체는 시나리오 G에서 다룬다.
- **절차**: `POST /api/subscriptions`(구독), `DELETE /api/subscriptions?userId=&authorId=`(취소, 재구독 upsert 케이스 포함)에 대해 점증 부하(ramping VUs) 인가.
- **측정 지표**: 처리량(req/sec), p95 지연시간, `subscription_outbox_events` 적재 지연(NFR-2.2 Outbox 패턴이 지연을 유발하지 않는지).
- **성공 기준**: 목표 QPS(예: 1,000 req/sec)까지 에러율 0%, p95 지연시간 SLA 내(추후 구체값은 구현 단계에서 확정).

### D. 읽음 처리 API 동시성 (NFR-4.3)
- **목적**: 개별/전체 읽음 처리의 원자적 UPDATE가 동시 요청에서도 일관되게 수렴하는지 확인한다.
- **절차**: 같은 사용자가 여러 VU로 `PATCH /api/users/{recipientId}/notifications/read-all`을 동시에 호출하고, 개별 읽음 요청도 섞어 실행한다.
- **측정 지표**: 응답 코드/지연시간과 실행 후 DB의 `is_read`/`read_at` 최종 상태.
- **성공 기준**: 데이터 불일치 0건. 알림 목록/unread count/Redis Read Path 성능은 해당 제품 기능 구현 이후 확장 시나리오로 측정한다.

### E. WebSocket 동시 접속 및 실시간 push 지연 — 확장 시나리오
- **목적**: 다수 사용자가 WebSocket에 동시 접속한 상태에서 Fan-out으로 생성된 알림이 Redis Pub/Sub을 통해 지연 없이 브로드캐스트되는지 확인한다.
- **절차**: 시나리오 A의 구독자 중 일부(예: 1만 명)를 WebSocket으로 사전 접속시킨 뒤 글 발행 → 각 클라이언트가 실시간 알림을 수신하기까지의 지연시간 측정.
- **측정 지표**: 알림 생성(DB insert) 시각 대비 WebSocket 클라이언트 수신 시각의 지연 분포. 이 값은 5초 SLA(NFR-1.1)의 크리티컬 패스에는 포함되지 않는 Best-effort 지표임을 명시(`architecture.md` §6).
- **성공 기준**: 별도 하드 SLA는 없음(Best-effort) — 다만 접속자 수 증가에 따라 지연이 비정상적으로 증가하지 않는지(Redis Pub/Sub 브로드캐스트가 병목이 되지 않는지) 확인.

> 현재 WebSocket/SSE와 Redis Pub/Sub은 구현돼 있지 않으므로 이 시나리오는 실행/합격 판정 대상이 아니다.

### F. Push 작업 적체와 복구 확인 (NFR-3.4)
- **목적**: Push 위임이 느려지는 상황에서 발송 워커가 죽거나 작업을 유실하지 않고 DB 작업 적체로 흡수한 뒤 복구하는지 확인한다.
- **절차**: 목업 게이트웨이에 인위적 지연(예: 응답 500ms~1s)을 주입한 상태에서 시나리오 A 수준의 Fan-out을 재현.
- **측정 지표**: `notification_delivery_attempts{channel="push",status="pending"}`, `notification_delivery_attempts{channel="push",status="dead_letter"}`, 만료 `PROCESSING` claim 회수 여부, 워커 생존 여부.
- **성공 기준**: 워커 크래시와 작업 유실이 없고 지연 해소 후 backlog가 drain된다. 최대 재시도 초과 건만 `DEAD_LETTER`가 되며 수동 복구 API로 다시 처리할 수 있다.

### G. Chunk Worker / Push Worker 수평 확장 처리량 (NFR-3.2)
- **목적**: Kafka Consumer Group의 Chunk Worker와 DB claim 기반 Push Worker가 인스턴스 수 증가에 따라 처리량이 증가하는지 확인한다.
- **절차**: 동일 부하를 Chunk Worker 1 → 3 → 6개로 반복하고, Push Worker도 동일하게 인스턴스를 늘려 DB claim 처리량을 측정한다. Email 발송기는 확장 구현 후 별도 측정한다.
- **측정 지표**: 워커/발송기 인스턴스 수 대비 처리량(msg/sec) 증가율, Consumer Group 리밸런싱이 처리량 저하 없이 이뤄지는지.
- **성공 기준**: 인스턴스 수 증가에 따라 처리량이 대체로 비례해서 늘어난다(완전한 선형은 아니더라도 뚜렷한 우상향). 특정 인스턴스 수 이후 처리량이 정체되면 그 지점과 원인(브로커 파티션 수 한계 등)을 기록.

## 4. 공통 성공/실패 판정 기준 요약

| 시나리오 | 관련 NFR | 핵심 판정 기준 |
|---|---|---|
| A. 대량 팬아웃 SLA | NFR-1.1, 1.3 | 5초 이내 완료, 20,000 msg/sec 이상 |
| B. 동시 다발 발행 | NFR-3.1, 3.3 | 작가별 완료 시간 저하 없음, 파티션 lag 고른 분산 |
| C. 구독 API 처리량 | (특정 NFR 미대응) | 목표 QPS까지 에러율 0% |
| D. 읽음 처리 동시성 | NFR-4.3 | 동시 요청 후 데이터 일관성 |
| E. 실시간 push 지연(확장) | - | 현행 판정 제외 |
| F. Push 작업 적체/복구 | NFR-3.4 | 워커 생존, backlog drain, 작업 유실 없음 |
| G. Chunk/Push Worker 수평 확장 | NFR-3.2 | 인스턴스 수 증가에 따른 처리량 우상향 |

## 5. 다음 단계

- 기존 `scripts/load-test`를 현행 API/측정 방식에 맞춰 점검한 뒤 A → B → F → G 순서로 실행한다.
- 실행 결과는 [`load-test-report.md`](./load-test-report.md)에 과거 기준선과 구분해 날짜·커밋·환경과 함께 기록한다.
