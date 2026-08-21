# 장애 주입(Chaos) 테스트 실행 결과

> 시나리오 설계는 [`chaos-test-plan.md`](./chaos-test-plan.md) 참고. 이 문서는 **실제 실행 결과**를 기록한다.

## 0. 왜 원 계획과 다르게 실행했는가

`chaos-test-plan.md`의 C1~C6은 청크 Dispatcher/Chunk Worker, Email 채널, Redis 기반 실시간 채널 등 실제로는 구현하지 않은 컴포넌트를 전제로 한다(`docs/decisions.md` 참고). 또한 이 시스템은 별도로 배포된 여러 워커 프로세스가 아니라 **단일 Spring Boot 프로세스**(Outbox Relay, Fan-out 컨슈머, Push 발송 워커가 전부 같은 JVM 안에서 스케줄러/컨슈머로 동작) 구조라, "알림 워커만 따로 죽인다"는 원 시나리오의 전제 자체가 성립하지 않는다. 그래서 이번 실행은 실제 구조에 맞게 시나리오를 재설계했다.

| 항목 | 값 |
|---|---|
| 실행 일시 | 2026-08-21 |
| 장애 주입 방법 | `docker compose stop/start`(Kafka), `taskkill /F`(앱 프로세스 강제 종료) |
| 스크립트 위치 | `scripts/chaos/` |

## 1. Chaos-1 (원 C1+C2 병합) — Kafka 다운 중 글 등록/구독 API + 복구 후 자동 재발행

**절차**: Kafka 컨테이너를 중단시킨 상태에서 사용자 생성 → 구독 → 글 작성 → 발행 API를 순서대로 호출. 이후 Kafka를 재시작하고 outbox가 자동으로 catch-up 발행되는지 관찰.

**결과 — Kafka 다운 중**:

| API | 응답 코드 |
|---|---|
| `POST /api/users` (작가) | 201 |
| `POST /api/users` (구독자) | 201 |
| `POST /api/subscriptions` | 201 |
| `POST /api/posts` | 201 |
| `POST /api/posts/:id/publish` | 200 |

모든 API가 Kafka 장애와 무관하게 정상 응답했다. `post.outbox_events`/`subscription.subscription_outbox_events`에는 각각 `PENDING` 상태로 이벤트가 정상 적재됐다. 앱 프로세스 자체도 죽지 않고 살아있었다(`/actuator/health` 200 유지) — Kafka 컨슈머들은 연결 재시도만 반복했다.

**기대 결과 대비 판정**: **통과** — NFR-2.1/2.2(Outbox 패턴)이 의도한 대로, 브로커 장애가 글 등록 트랜잭션에 전혀 영향을 주지 않았다.

**결과 — Kafka 복구 후**: 최종적으로 `post.outbox_events`/`subscription.subscription_outbox_events` 모두 `PENDING`이 남지 않고 전량 `PUBLISHED`로 전이됐다(`subscription` 쪽은 이 시점에 약 11,763건이 쌓여있었다 — 아래 §1.1 참고). `docs/decisions.md` §3("무한 재시도, Dead Letter 없음")가 의도한 대로 메시지가 유실되지 않고 결국 전부 발행됐다.

### 1.1 예상 밖의 발견 — Read Model 동기화 레이스가 실제 알림 유실로 이어짐

이번 Chaos-1 실행 직전에 진행한 부하 테스트 시나리오 C(`load-test-report.md`)가 `SubscriptionChanged` 이벤트 약 11,700여 건의 outbox 백로그를 만들어 놓은 상태였다. Kafka를 내렸다가 복구했을 때, 이 백로그가 다음과 같이 실제 알림 유실로 이어지는 것을 관찰했다.

**관찰된 인과관계**:
1. `PostOutboxRelay`와 `SubscriptionOutboxRelay`는 같은 단일 스레드 스케줄러(`docs/decisions.md` §5)를 공유한다.
2. Kafka 복구 직후 리더 재선출 등으로 일부 Kafka 발행이 최대 120초(`request.timeout.ms`/`delivery.timeout.ms` 기본값)까지 블로킹됐다 — 로그에 `TimeoutException: Expiring 1 record(s)...120001ms has passed` 확인.
3. `SubscriptionOutboxRelay`가 약 11,700건의 백로그를 처리하는 동안, 우리 테스트용 구독 이벤트(작가 A → 구독자 B) 1건은 그 뒤에 밀려 있었다.
4. 반면 `PostOutboxRelay`는 이벤트가 1건뿐이라 Kafka 복구 후 180ms 만에 `post.published`를 발행했고, `PostPublishedFanoutConsumer`가 거의 즉시 이를 소비했다 — 이 시점에 `subscriber_read_model`에는 아직 구독자 B가 없었다(구독 이벤트가 아직 처리되지 않았으므로).
5. Fan-out 컨슈머는 "구독자 없음"으로 판단해 알림을 만들지 않고 정상 종료했고, Kafka는 해당 오프셋을 커밋했다.
6. 그로부터 약 4분 뒤 `subscriber_read_model`이 뒤늦게 동기화됐지만, 이미 커밋된 `post.published` 메시지는 재전달되지 않으므로 **이 구독자는 해당 글의 알림을 영구히 받지 못했다**.

**의미**: `docs/decisions.md` §2("subscriber_read_model 동기화와 Fan-out 사이의 eventual consistency, 구독 직후 즉시 발행하면 알림이 누락될 수 있음")는 원래 자동화 테스트에서 밀리초 단위의 레이스로 관측됐던 트레이드오프였다. 이번 실행은 그 레이스가 **§4(동기 블로킹 발행)와 §5(공유 단일 스레드 스케줄러)라는 별개의 트레이드오프와 결합하면, 레이스 윈도우가 밀리초가 아니라 수 분 단위로 벌어질 수 있고, 실제로 영구적인 알림 유실을 일으킨다**는 것을 실측으로 확인했다. 세 가지 트레이드오프가 개별적으로는 각각 감수할 만하다고 판단됐지만, **동시에 발생하면 서로를 증폭시킨다**는 것이 이번 장애 주입 테스트의 핵심 발견이다.

**실패 판정**: 원 계획서(§3 결과 판정 기준)의 "데이터 정합성(중복/누락 없음) 유지" 기준으로 보면 이 하위 관찰은 **실패**다 — 알림 1건이 영구 누락됐다. 다만 이는 Kafka 다운/복구 자체의 실패가 아니라(Outbox 패턴은 의도대로 작동했다) 위에서 설명한 3중 트레이드오프의 복합 효과다.

## 2. Chaos-2 (원 C4 재해석) — 팬아웃 도중 앱 프로세스 강제 종료 후 재기동

원 시나리오는 "Chunk Worker 일부만 강제 종료"를 전제로 하지만, 이 구현에는 별도 Chunk Worker가 없다(단일 컨슈머가 전체 프로세스와 같은 JVM에서 동작). 그래서 **앱 프로세스 전체를 강제 종료**하는 것으로 재해석해 실행했다 — 이는 원 시나리오가 검증하려던 것(Kafka 재전달 + `uq_recipient_event` unique 제약 기반 멱등성)을 그대로 검증한다.

**절차**: 작가 1명 + 구독자 400명 시딩(Read Model 동기화 완료까지 대기) → 글 발행 API 호출 → 150ms 후 앱 프로세스 강제 종료(`taskkill /F`) → 재기동 → Fan-out 완료까지 관찰.

**결과**:

| 시점 | 관찰 |
|---|---|
| 강제 종료 직전 | 발행 API는 호출됨(202 무관하게 fire-and-forget으로 처리) |
| 강제 종료 시점 알림 수 | 0 / 400 — `post.published` 오프셋이 아직 커밋되지 않은 상태에서 프로세스가 죽음 |
| 재기동 후 health 정상화 | 정상 (수 초 내) |
| 최종 알림 수 | **400 / 400**, `DISTINCT recipient_id` 역시 **400** — 중복 없음, 누락 없음 |

**판정**: **통과**. Kafka는 커밋되지 않은 오프셋의 메시지를 재기동한 컨슈머에게 다시 전달했고, `NotificationJdbcDao.insertAndGetId`의 `ON CONFLICT ... DO UPDATE ... RETURNING id` 멱등 upsert와 `uq_recipient_event` unique 제약이 재처리를 안전하게 흡수했다.

**한계**: 강제 종료 시점(발행 후 150ms)에 알림이 0건이었다는 것은, 이번 실행에서는 "일부만 처리되고 나머지는 미처리인 상태"(진짜 부분 처리 상태)를 포착하지 못했고 대신 "전혀 처리 안 된 상태 → 완전 재처리"만 검증했다는 뜻이다. 다만 이 결과가 검증하려는 안전장치(unique 제약 기반 멱등성)는 부분 재처리든 완전 재처리든 동일하게 적용되므로, 이 실행 결과가 그 안전장치의 유효성을 부정하지는 않는다 — 더 큰 구독자 규모나 더 늦은 타이밍으로 재시도하면 진짜 부분 처리 상태를 포착할 가능성이 높다(후속 검증 여지로 남겨둔다).

## 3. 실행하지 않은 시나리오

| 시나리오 | 사유 |
|---|---|
| C3. Push 발송기만 강제 종료 (Email 격리 확인) | Email 채널 자체가 스코프 밖(`docs/decisions.md` §7) — 격리할 대상 채널이 없음. 대신 Push 발송 실패/DLQ 전이 자체는 이미 `PushDeliveryDeadLetterIntegrationTest`(자동화 테스트)로 검증돼 있음 |
| C5. Dispatcher 크래시 후 cursor 재개 | Dispatcher/청크 분산이 없음(`docs/decisions.md` §1) — cursor 개념 자체가 존재하지 않음 |
| C6. Redis 다운 | 현재 구현에서 Redis 미사용(`README.md` 기술스택 표 참고) |

## 4. 종합 판정 및 후속 조치

| 시나리오 | 판정 |
|---|---|
| Chaos-1: Kafka 다운 중 API 정상 동작 | 통과 |
| Chaos-1: Kafka 복구 후 catch-up (메시지 유실 없음) | 통과(outbox 레벨) — 단, §1.1의 복합 효과로 알림 1건 영구 유실 관찰 |
| Chaos-2: 앱 크래시 중 Fan-out → 재기동 후 정합성 | 통과 (중복 0건, 누락 0건) |
| C3/C5/C6 | 미실행 (대상 컴포넌트 없음) |

**후속 조치 제안** (`docs/decisions.md` 갱신 필요 — 별도 커밋에서 반영):
1. §2(Read Model 레이스)에 이번 실측 결과를 append — "이론상 밀리초 단위"라는 기존 서술을 "다른 트레이드오프와 결합 시 분 단위로 확대될 수 있음, 실측 확인됨"으로 보강.
2. Relay가 하나의 이벤트 종류(예: Subscription)에서 대량 백로그를 처리하는 동안 다른 Relay/Worker가 굶주리지 않도록, 최소한 Relay별로 별도 스케줄러 스레드를 쓰는 방안을 §5의 "결정 필요 시점"에 추가 — 지금까지는 "인스턴스 스케일아웃 전"이 트리거였는데, 이번 관찰로 "단일 인스턴스에서도 한 Relay의 백로그가 다른 Relay를 지연시킬 수 있다"는 것이 추가 트리거가 됐다.
