# 부하 테스트 실행 결과

> 시나리오 설계는 [`load-test-plan.md`](./load-test-plan.md) 참고. 이 문서는 **실제 실행 결과**를 기록한다.

## 2026-09-11 현행 구조 10만 명 재측정

### 실행 조건

| 항목 | 값 |
|---|---|
| 코드 기준 | `91ff2f9` (`develop`) |
| 애플리케이션 | 단일 Spring Boot 4.1.0 프로세스, Java 17.0.20 |
| 인프라 | 로컬 Docker Compose, PostgreSQL 16.15, Kafka 3.8.0 단일 브로커 |
| Fan-out 구성 | 1,000명 키셋 청크, `fanout.chunk.requested` 32파티션, Chunk Worker 동시성 6 |
| 데이터 준비 | 실행별 신규 작가 1명, PUSH 구독자 100,000명. 원본 구독과 Read Model을 동일하게 벌크 시딩 |
| SLA 시작/종료 | `PostPublished` Outbox 생성 시각 → 마지막 Notification 및 Push delivery row 생성 시각 |
| 실행 스크립트 | `scripts/load-test/fanout-sla-100k.ps1` |

시딩은 SLA 측정에서 제외했다. HTTP 폴링 오차를 피하기 위해 `outbox_events.created_at`, `fanout_dispatches.updated_at`, `notifications.created_at`, `notification_delivery_log.created_at`의 DB 타임스탬프로 시간을 계산했다. Dispatcher의 `DONE`은 청크 발행 완료일 뿐 Chunk Worker 완료가 아니므로, 최종 SLA는 알림과 Push 작업 100,000건이 모두 생성된 시점으로 판정했다.

### 결과

| 지표 | 실행 1 | 실행 2 |
|---|---:|---:|
| Dispatcher 완료 | 291,016.4 ms | 669,627.9 ms |
| 알림 100,000건 생성 완료 | **390,725.7 ms** | **761,649.5 ms** |
| Push delivery row 100,000건 생성 완료 | **390,725.7 ms** | **761,649.5 ms** |
| 처리량(최종 생성 기준) | **255.9 msg/sec** | **131.3 msg/sec** |
| 발행/완료 청크 | 100 / 100 | 100 / 100 |
| Dispatcher 재시도 | 0 | 0 |
| 최종 Push 상태 | SENT 100,000건 | SENT 100,000건 |
| 5초 / 20,000 msg/sec SLA | **실패** | **실패** |

두 실행 모두 알림과 Push 작업의 중복·누락 없이 정확히 100,000건을 만들었고 최종 Push 상태도 전부 `SENT`였다. Prometheus 누계도 `dispatched=200`, `completed=200`, `claimed=200,000`, `sent=200,000`으로 DB 결과와 일치했으며 failed/dead-letter/in-progress/waiting-retry gauge는 모두 0이었다.

성능은 목표에 미달했다. 실행 1은 5초보다 약 78.1배, 실행 2는 약 152.3배 오래 걸렸다. 데이터가 누적된 실행 2가 더 느려져 현재 쿼리/쓰기 경로가 데이터 증가에 민감하다는 신호도 확인됐다.

### 관측된 병목

1. **membership gate**: 실행 2에서 첫 청크 전 원본 ACTIVE 구독과 Read Model의 양방향 일치 검사 SQL이 PostgreSQL에서 10분 이상 `active` 상태로 실행됐다. 잠금 대기는 아니었다. `EXPLAIN`에서는 `subscriptions`의 `idx_subscriptions_user`를 스캔한 뒤 `author_id`를 필터링했다. 현재 `(author_id, id) WHERE status='ACTIVE'` 인덱스는 정확한 집합 비교에 필요한 `(author_id, user_id)` 순서를 제공하지 않는다.
2. **수신자별 개별 INSERT**: `FanoutChunkWorker`는 청크 안의 사용자마다 Notification INSERT와 Push delivery INSERT를 각각 호출한다. 10만 명 기준 최소 20만 번의 개별 쓰기가 발생하며, Dispatcher 완료 뒤에도 최종 생성까지 실행 1은 약 99.7초, 실행 2는 약 92.0초가 더 필요했다.
3. **로컬 단일 노드 한계**: Kafka/PostgreSQL/애플리케이션이 한 로컬 머신에서 동작한 결과이므로 프로덕션 용량 수치로 일반화할 수 없다. 다만 동일 환경에서도 목표 대비 차이가 매우 커, 인프라 증설 전에 위 두 코드/쿼리 병목을 먼저 해소해야 한다.

### 판정과 다음 우선순위

- **정확성: 통과** — 100청크, 알림/Push 작업 각 100,000건, 최종 SENT 100,000건, 재시도·DLQ 0건.
- **성능 SLA: 실패** — 390.7~761.6초, 131.3~255.9 msg/sec.
- **우선순위 1**: ACTIVE 구독 집합 비교에 맞는 `(author_id, user_id)` 부분 인덱스와 쿼리 계획을 검증한다.
- **우선순위 2**: Chunk Worker의 수신자별 두 번 INSERT를 JDBC batch 또는 set-based bulk INSERT로 바꾼다.
- **우선순위 3**: 동일 10만 명 테스트를 다시 실행한 뒤 Chunk Worker 인스턴스/동시성별 확장 효율을 측정한다.

---

## 과거 기준선 — 단일 Fan-out Consumer

> 아래 수치는 2026-08 단일 `PostPublishedFanoutConsumer` 시절의 과거 결과이며 현행 구조의 성능 근거로 사용하지 않는다.

## 0. 실행 환경과 스코프 축소 — 왜 원 계획과 다르게 실행했는가

2026-08 실행 당시 `load-test-plan.md`는 청크 Dispatcher/Chunk Worker, Kafka 기반 Push/Email 발송 토픽, WebSocket, Redis Pub/Sub, Prometheus 커스텀 메트릭을 전제로 했지만 실제 코드는 청크 미분산 단일 Consumer와 DB 폴링 Push 구조였다. 그래서 당시 구현에 맞게 시나리오를 재설계해 실행했다. 이 설명은 현재 구조를 뜻하지 않는다.

또한 이 실행 환경(로컬 개발 머신, 여유 메모리 약 1.2GB 수준)이 매우 제한적이라 원 계획의 절대 규모(구독자 10만 명)로는 안정적으로 실행할 수 없었다. `load-test-plan.md` §2가 이미 "로컬 인프라이므로 절대적 처리량보다는 설계가 목표한 배율 확인에 의의를 둔다"고 명시했듯, 이번 실행도 **정확성/패턴 검증 목적**으로 규모를 대폭 축소했다(수백 명 단위). NFR-1의 절대 수치(5초/10만 명/20,000 msg/sec)를 이 규모로 검증했다고 주장하지 않는다.

| 항목 | 값 |
|---|---|
| 실행 일시 | 2026-08-21 |
| 인프라 | 로컬 `docker-compose.yml` (PostgreSQL 16, Kafka 3.8.0, Redis 7) + `java -jar`로 실행한 앱(힙 384MB로 제한) |
| 부하 도구 | k6 v2.2.0 (HTTP 시나리오), 일부 시나리오는 DB 폴링 완료 시점 측정이 핵심이라 bash+curl+psql 조합 사용 |
| 스크립트 위치 | `scripts/load-test/` |

## 1. 시나리오 실행 결과

### A. 대량 팬아웃 SLA 검증 (축소판) — `scripts/load-test/fanout-sla.sh`

- **절차**: 작가 1명 + 구독자 300명(전원 PUSH 채널) 시딩 → 글 발행 1회 → `notifications` 행 수가 300이 될 때까지 폴링, `notification_delivery_log` 상태가 전부 `SENT`가 될 때까지 폴링.
- **결과**:

| 지표 | 값 |
|---|---|
| 구독자 수 | 300 (원 계획 100,000의 0.3%) |
| 시딩(사용자 생성+구독) 소요 시간 | 74,643 ms (SLA 측정 대상 아님) |
| 발행 → 알림 생성(Fan-out) 완료 | **2,666 ms** |
| 발행 → Push 발송(delivery_log 전부 SENT) 완료 | **4,664 ms** |
| 환산 처리량(알림 생성 기준) | 약 112 msg/sec |

- **판정**: 이 축소 규모에서는 5초 이내 완료라는 시간 기준 자체는 충족했다. 하지만 목표 처리량(20,000 msg/sec)에는 크게 못 미친다(약 112 msg/sec) — 애초에 이 처리량은 청크 Dispatcher/Chunk Worker 병렬 처리를 전제로 설계된 목표치(`architecture.md` §4.2)이고, 지금 구현은 단일 컨슈머가 순차 처리한다(`docs/decisions.md` §1, 이미 예견되고 받아들인 트레이드오프). **이 결과는 "지금 구현이 100,000명 규모에서 SLA를 만족한다"는 근거가 아니다** — 오히려 §1의 "부하 테스트로 처리량 한계를 실측한 뒤 청크 분리로 전환" 결정 트리거에 실측 데이터를 제공하는 것이 이 실행의 실질적 의의다. 300명 기준 선형 추정 시 100,000명은 단일 컨슈머로 약 890초(약 15분) 소요될 것으로 추정된다(순수 선형 가정, 실제로는 배치 오버헤드 등으로 달라질 수 있음).

### B. 동시 다발 발행 — 순간 QPS 흡수 (축소판) — `scripts/load-test/concurrent-authors.sh`

- **절차**: 작가 5명, 각 50명 구독자 시딩 → 5개 글을 최대한 동시에 발행 → 작가별 Fan-out 완료 시간 개별 측정.
- **결과**: 5명 전원 동일한 폴링 구간(발행 후 476ms, 폴링 주기 200ms라 더 정밀한 편차는 측정 불가)에 완료. 특정 작가만 유독 느려지는 현상은 관측되지 않았다.
- **참고**: `post.published` 토픽은 `docker-compose.yml`에 파티션 수를 명시하지 않아 Kafka 기본값(1개)으로 자동 생성된다 — `architecture.md` §7이 가정한 다중 파티션(예: 6개) 전제가 아니다. 이 규모(5명)에서는 단일 파티션이어도 병목이 드러나지 않았지만, 이는 시나리오 자체가 작아서일 가능성이 높다(모든 이벤트가 어차피 하나의 Fan-out 컨슈머로 순차 처리되므로, 파티션 수를 늘려도 지금 구현에서는 처리량이 늘지 않는다 — §1과 동일한 한계).

### C. 구독/구독취소 API 처리량 — `scripts/load-test/subscribe-throughput.js`

- **절차**: k6 ramping VUs(0→10→30, 40초) — 각 반복마다 사용자 생성 → 구독 → 구독취소.
- **결과**:

| 지표 | 값 |
|---|---|
| 총 반복 | 5,606 iterations (16,819 HTTP 요청) |
| 에러율 | **0.00%** |
| 평균 지연시간 | 34.78 ms |
| p95 지연시간 | **70.23 ms** (임계값 500ms 이내로 통과) |
| 최대 지연시간 | 305.86 ms |
| 처리량 | 약 419 req/sec (VU 30개 기준, 더 높은 VU로는 시도하지 않음 — 리소스 제약) |

- **판정**: 통과. 에러 없이 안정적으로 처리됐고, `subscription_outbox_events` 적재도 트랜잭션 안에서 함께 이뤄져 지연을 유발하지 않았다(다만 §2의 부작용 참고 — 아래).
- **부작용(중요)**: 이 5,606건의 구독/취소가 만든 `SubscriptionChanged` 이벤트가 `SubscriptionOutboxRelay`의 처리 속도를 넘어서면서 **약 11,700건의 outbox 백로그**를 만들었다. 이 백로그는 이후 장애 주입 테스트(`chaos-test-report.md`)에서 실제 알림 유실을 유발하는 원인이 됐다 — 상세 내용은 그 문서 참고.

### D. 알림 읽음 처리 동시성 및 지연시간 (축소판, 목록/unread count 제외)

목록 조회/unread count API는 스코프 밖(`docs/decisions.md`)이라 이 시나리오는 **읽음 처리 API의 동시성 안전성과 지연시간**으로 좁혀 실행했다.

**D-1. 동시 "모두 읽음" 레이스 안전성** — `scripts/load-test/read-concurrency.sh`
- **절차**: 사용자 1명에게 알림 50건을 시딩(팬아웃 경유 없이 직접 INSERT — 이 테스트의 목적은 Fan-out이 아니라 읽음 API의 원자성이므로) → 동시에 `PATCH .../notifications/read-all` 20회 호출.
- **결과**: 정확히 1건의 호출만 `updatedCount=50`을 반환했고, 나머지 19건은 전부 `updatedCount=0`. 전체 `updatedCount` 합계 = 50 (정확히 일치, 중복/누락 없음). 호출 종료 후 미읽음 알림 0건.
- **판정**: 통과. `architecture.md` §8.1이 설계한 "락 없는 단일 원자적 UPDATE" 방식이 실제 동시 요청 20개 아래에서도 레이스 없이 정확히 수렴함을 확인했다.

**D-2. 개별 읽음 처리 API 지연시간** — `scripts/load-test/read-latency.js`
- **절차**: 알림 300건 시딩 → k6 `shared-iterations`(VU 30개)로 서로 다른 알림 300건을 각 1회씩 읽음 처리.
- **결과**:

| 지표 | 값 |
|---|---|
| 요청 수 | 300 |
| 에러율 | 0.00% |
| 평균 지연시간 | 128.52 ms |
| p95 지연시간 | 306.43 ms (자체 설정 임계값 300ms 근소 초과) |
| 최대 지연시간 | 363.33 ms |

- **판정**: 에러는 없었으나 p95가 임의로 잡은 임계값(300ms)을 근소하게 넘었다. 이 환경이 힙 384MB로 제한하고 Postgres/Kafka/Redis를 동시에 돌리는 리소스 제약 상태였다는 점을 감안하면, 정상적인 배포 환경에서는 이보다 낮을 것으로 예상되나 별도 검증은 하지 않았다 — 수치를 있는 그대로 기록한다.

### E/F/G — 실행하지 않음

| 시나리오 | 사유 |
|---|---|
| E. WebSocket 실시간 push 지연 | WebSocket/SSE 자체가 스코프 밖(`docs/decisions.md` §9) — 측정 대상 없음 |
| F. 백프레셔(다운스트림 지연 주입) | Push/Email 발송 토픽 구조 자체가 없음(DB 폴링 워커로 대체, `docs/decisions.md` §7). 유사한 재시도/DLQ 동작은 `chaos-test-report.md`에서 별도 검증 |
| G. Chunk Worker/발송기 수평 확장 | 2026-08 당시에는 청크 Dispatcher/Chunk Worker가 존재하지 않아 측정할 수 없었음 |

## 2. 종합 판정

| 시나리오 | 판정 | 비고 |
|---|---|---|
| A. 대량 팬아웃 SLA(축소) | 시간 기준 충족(300명/2.7초), **처리량 목표(20,000 msg/sec) 대비 대폭 미달(~112 msg/sec)** | 청크 미분산이 원인, `docs/decisions.md` §1 |
| B. 동시 다발 발행(축소) | 통과 — 작가 간 편차 없음 | 이 규모에서는 유의미한 신호 없음 |
| C. 구독 API 처리량 | 통과 — 에러 0%, p95 70ms | 부작용으로 대량 outbox 백로그 생성(§1) |
| D. 읽음 처리 동시성/지연 | 동시성 통과, 지연시간 임계값 근소 초과 | 리소스 제약 환경 |
| E/F/G | 미실행 | 대상 컴포넌트 없음(스코프 밖) |

## 3. 다음 단계로 이어지는 시사점

- **청크 분산 전환 판단 근거 확보**: `docs/decisions.md` §1의 "부하 테스트로 처리량 한계를 실측한 뒤 결정" 트리거가 이번에 충족됐다 — 약 112 msg/sec는 목표(20,000 msg/sec) 대비 179배 부족하다. 실제 10만 명 규모 서비스를 지향한다면 청크 Dispatcher/Chunk Worker 분리가 필요하다는 근거가 마련됐다.
- **outbox 백로그가 실제 위험으로 이어짐**: Scenario C가 만든 백로그가 장애 주입 테스트에서 실제 알림 유실을 유발했다 — `docs/decisions.md` §4(동기 블로킹 발행)/§5(공유 스케줄러 스레드)의 트레이드오프가 이론이 아니라 실측된 위험임이 확인됐다. `chaos-test-report.md` 참고.
