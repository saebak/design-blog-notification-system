# 부하 테스트 시나리오 설계

> 실제 실행 결과는 구현 완료 후 [`load-test-report.md`](./load-test-report.md)(추후 작성)에 기록한다. 이 문서는 **시나리오 설계**만 다룬다 — 아직 대상 API/파이프라인이 구현되지 않았으므로 k6 스크립트 등 실행 가능한 산출물은 구현 단계에서 작성한다.

## 0. 목적 및 범위

- **검증 대상**: NFR-1(Low Latency — 5초 이내 10만 명 팬아웃, 20,000 msg/sec), NFR-3(Scalability — 순간 QPS 흡수, 핫 파티션 대응, 백프레셔).
- **테스트 범위**: API 계층(글 등록/구독/알림 조회)과 Fan-out 파이프라인 전체. Push/Email 게이트웨이는 목업이므로 그 자체의 성능은 측정 대상이 아니다 — 목업까지 도달하는 것 자체(발송 요청 적재)가 SLA 기준이다(`requirements.md` NFR-1.2).

## 1. 도구 및 측정 방법

- **부하 생성 도구**: k6 (`docs/architecture.md` §2에서 선정 — HTTP + WebSocket 시나리오를 하나의 스크립트 체계로 작성 가능).
- **Fan-out 완료 시점 측정**: Fan-out은 비동기이므로 발행 API의 HTTP 응답 시간만으로는 SLA를 검증할 수 없다. 아래 두 방법 중 하나(또는 병행)로 간접 측정한다.
  1. `architecture.md` §4.4의 Prometheus 지표(`fanout_chunks_dispatched_total{eventId}`, `fanout_chunks_completed_total{eventId}`)를 폴링해 두 값이 같아지는 시점을 완료 시점으로 삼는다.
  2. 테스트 환경에서는 `notifications` 테이블의 `WHERE source_event_id = :eventId` 행 수가 구독자 수와 같아지는 시점을 완료 시점으로 삼는다 (`database-design.md` §4.2).
- **공통 측정 지표**: API p50/p95/p99 지연시간, 에러율, Kafka consumer lag, Fan-out 완료까지 걸린 wall-clock 시간, msg/sec 처리량.

## 2. 사전 조건

- 부하 테스트 실행 전, 특정 작가 1명에 대해 최소 10만 명의 구독자를 시드 데이터로 미리 적재해야 한다(시나리오 A/B의 전제). 시드 데이터 생성 스크립트 자체는 이 문서의 범위가 아니며 구현 단계에서 별도로 작성한다.
- 테스트는 로컬 `docker-compose.yml` 기반 인프라(PostgreSQL/Kafka/Redis) 위에서 수행하며, 실제 프로덕션 등급 인프라 사양이 아니므로 절대적 처리량보다는 **설계가 목표한 배율(예: 워커 수 대비 처리량 선형 증가 여부)**을 확인하는 데 의의를 둔다.

## 3. 시나리오

### A. 대량 팬아웃 SLA 검증 (핵심 시나리오, NFR-1.1 / NFR-1.3)
- **목적**: 인기 작가 1명이 글을 발행했을 때 10만 구독자 전원에게 5초 이내(20,000 msg/sec 이상)로 팬아웃이 완료되는지 확인한다.
- **절차**: 사전 조건의 10만 구독자 보유 작가로 글 발행 API(`POST /posts/:id/publish`)를 1회 호출 → §1의 측정 방법으로 Fan-out 완료 시점까지의 경과 시간을 기록.
- **측정 지표**: 발행 시각부터 `dispatched_total == completed_total`(또는 `notifications` 행 수 == 10만)까지의 wall-clock 시간, 초당 처리량(msg/sec), 청크별 처리 시간 분포(`fanout_chunk_duration_seconds`).
- **성공 기준**: 완료까지 5초 이내, 평균 처리량 20,000 msg/sec 이상. `architecture.md` §4.2에서 워커 15개(50% 여유) 기준으로 설계했으므로, 목표 미달 시 워커 수와 처리량의 상관관계도 함께 기록.

### B. 동시 다발 발행 — 순간 QPS 흡수 (NFR-3.1 / NFR-3.3)
- **목적**: 여러 인기 작가가 짧은 시간 내에 동시에 글을 발행해도 특정 Kafka 파티션에 부하가 쏠리지 않고 전체적으로 처리량이 유지되는지 확인한다.
- **절차**: 각각 1만~10만 명 구독자를 가진 작가 5~10명이 수 초 간격으로 동시에 발행 → 각 작가별 Fan-out 완료 시간을 개별 측정.
- **측정 지표**: 작가별 Fan-out 완료 시간 편차(특정 작가만 유독 느려지는지), `fanout.chunk.requested` 토픽의 파티션별 컨슈머 lag 분포(`architecture.md` §7 — 청크 토픽은 랜덤/round-robin 파티셔닝이므로 고르게 분산돼야 함).
- **성공 기준**: 동시 발행 작가 수와 무관하게 개별 작가의 Fan-out 완료 시간이 시나리오 A 대비 크게 저하되지 않음. 파티션 간 lag 편차가 특정 파티션에 집중되지 않음.

### C. 구독/구독취소 API 처리량
- **목적**: 구독/구독취소 API(Stateless HTTP 서버, Consumer Group 기반은 아님)가 인스턴스 수에 비례해 처리량이 늘어나는지 확인한다. NFR-3.2가 명시하는 대상(팬아웃 워커/발송기)과는 다른 컴포넌트이므로 이 시나리오는 특정 NFR 번호에 대응시키지 않는다 — NFR-3.2 자체는 시나리오 G에서 다룬다.
- **절차**: `POST /subscriptions`(구독), `DELETE /subscriptions/:id`(취소, 재구독 upsert 케이스 포함 — `database-design.md` §3.1)에 대해 점증 부하(ramping VUs) 인가.
- **측정 지표**: 처리량(req/sec), p95 지연시간, `subscription_outbox_events` 적재 지연(NFR-2.2 Outbox 패턴이 지연을 유발하지 않는지).
- **성공 기준**: 목표 QPS(예: 1,000 req/sec)까지 에러율 0%, p95 지연시간 SLA 내(추후 구체값은 구현 단계에서 확정).

### D. 알림 조회/읽음 처리 API 처리량 및 동시성 (NFR-4.3)
- **목적**: 알림 목록 조회(`idx_notifications_recipient_feed`), unread count(`idx_notifications_unread`), 읽음 처리(`architecture.md` §8.1 원자적 UPDATE) API가 Read Path 격리(NFR-2.3, Redis 캐시) 설계대로 발송 파이프라인과 무관하게 안정적으로 응답하는지 확인한다.
- **절차**: (1) Fan-out이 진행 중인 상태에서 동시에 알림 목록/unread count 조회 부하 인가 — Write 경합이 Read 지연에 영향을 주는지 확인. (2) 같은 사용자가 여러 VU로 동시에 "모두 읽음"을 호출해 §8.1의 원자적 UPDATE가 레이스 없이 수렴하는지 확인(최종 `is_read` 상태 일관성 검증).
- **측정 지표**: 목록/unread count API의 p95 지연시간(Fan-out 진행 중 vs. 유휴 시 비교), 동시 읽음 처리 후 DB 최종 상태의 일관성 여부.
- **성공 기준**: Fan-out 진행 여부와 무관하게 조회 API p95 지연시간 변동폭이 크지 않음(Read Path 격리 효과 확인). 동시 읽음 처리 후 데이터 불일치 0건.

### E. WebSocket 동시 접속 및 실시간 push 지연 (FR-5.4, `architecture.md` §6)
- **목적**: 다수 사용자가 WebSocket에 동시 접속한 상태에서 Fan-out으로 생성된 알림이 Redis Pub/Sub을 통해 지연 없이 브로드캐스트되는지 확인한다.
- **절차**: 시나리오 A의 구독자 중 일부(예: 1만 명)를 WebSocket으로 사전 접속시킨 뒤 글 발행 → 각 클라이언트가 실시간 알림을 수신하기까지의 지연시간 측정.
- **측정 지표**: 알림 생성(DB insert) 시각 대비 WebSocket 클라이언트 수신 시각의 지연 분포. 이 값은 5초 SLA(NFR-1.1)의 크리티컬 패스에는 포함되지 않는 Best-effort 지표임을 명시(`architecture.md` §6).
- **성공 기준**: 별도 하드 SLA는 없음(Best-effort) — 다만 접속자 수 증가에 따라 지연이 비정상적으로 증가하지 않는지(Redis Pub/Sub 브로드캐스트가 병목이 되지 않는지) 확인.

### F. 백프레셔 확인 (NFR-3.4)
- **목적**: 다운스트림(Push/Email 목업 게이트웨이)이 느려지는 상황에서 발송 워커가 과부하로 죽거나 메시지를 유실하지 않고, 큐 적체(consumer lag)만 늘어나는 형태로 정상 대응하는지 확인한다.
- **절차**: 목업 게이트웨이에 인위적 지연(예: 응답 500ms~1s)을 주입한 상태에서 시나리오 A 수준의 Fan-out을 재현.
- **측정 지표**: `delivery.push.requested`/`delivery.email.requested` 토픽의 consumer lag 추이, 발송 워커 프로세스의 생존 여부(재시작/크래시 없음), 토큰 버킷 기반 Rate Limit이 실제로 다운스트림 호출 속도를 제한하는지.
- **성공 기준**: 워커 프로세스 크래시 없음. lag는 증가하되 지연 해소 후 정상적으로 소진(drain)됨. 메시지 유실 없음(DLQ 적재 건수가 실제 재시도 초과 건수와 일치).

### G. Chunk Worker / 발송기 수평 확장 처리량 (NFR-3.2)
- **목적**: NFR-3.2가 명시하는 대상 — 팬아웃 Chunk Worker와 Push/Email 발송기(둘 다 Stateless, Consumer Group 기반) — 가 인스턴스 수를 늘릴수록 처리량이 선형에 가깝게 증가하는지 확인한다. 시나리오 A는 고정된 워커 수(15개)를 전제로 SLA 충족 여부만 보므로, 이 시나리오는 별도로 워커 수 자체를 변수로 둔다.
- **절차**: 동일한 Fan-out 부하(시나리오 A와 동일 규모)를 Chunk Worker 인스턴스 수를 예를 들어 5 → 10 → 15개로 늘려가며 반복 실행. 발송기도 동일한 방식으로 인스턴스 수를 늘려가며 `delivery.*.requested` 토픽 처리량을 측정.
- **측정 지표**: 워커/발송기 인스턴스 수 대비 처리량(msg/sec) 증가율, Consumer Group 리밸런싱이 처리량 저하 없이 이뤄지는지.
- **성공 기준**: 인스턴스 수 증가에 따라 처리량이 대체로 비례해서 늘어난다(완전한 선형은 아니더라도 뚜렷한 우상향). 특정 인스턴스 수 이후 처리량이 정체되면 그 지점과 원인(브로커 파티션 수 한계 등)을 기록.

## 4. 공통 성공/실패 판정 기준 요약

| 시나리오 | 관련 NFR | 핵심 판정 기준 |
|---|---|---|
| A. 대량 팬아웃 SLA | NFR-1.1, 1.3 | 5초 이내 완료, 20,000 msg/sec 이상 |
| B. 동시 다발 발행 | NFR-3.1, 3.3 | 작가별 완료 시간 저하 없음, 파티션 lag 고른 분산 |
| C. 구독 API 처리량 | (특정 NFR 미대응) | 목표 QPS까지 에러율 0% |
| D. 알림 조회/읽음 동시성 | NFR-2.3, 4.3 | Fan-out 중 조회 지연 영향 미미, 동시 읽음 처리 데이터 일관성 |
| E. 실시간 push 지연 | FR-5.4 | Best-effort, 접속자 증가에 따른 비정상 지연 증가 없음 |
| F. 백프레셔 | NFR-3.4 | 워커 생존, lag 정상 drain, 메시지 유실 없음 |
| G. Chunk Worker/발송기 수평 확장 | NFR-3.2 | 인스턴스 수 증가에 따른 처리량 우상향 |

## 5. 다음 단계

- 실제 k6 스크립트 및 시드 데이터 생성 스크립트는 구현 단계에서 작성한다.
- 실행 결과는 [`load-test-report.md`](./load-test-report.md)(추후 작성)에 시나리오별로 기록한다.
