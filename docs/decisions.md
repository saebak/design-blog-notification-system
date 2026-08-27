# 구현 트레이드오프와 남은 결정사항

> Post 발행 → 구독자 알림 생성(Outbox + Relay + Fan-out) 1차 구현 과정에서 스코프를 좁히며 내린 선택들과, 아직 결정되지 않은 채 남겨둔 사항들을 정리한다. `docs/architecture.md`/`domain-design.md`가 "최종 목표 아키텍처"를 그린 문서라면, 이 문서는 "지금 무엇을 미뤘고 왜 미뤘는지"를 추적하는 문서다. 각 항목은 다시 논의되어야 할 시점(트리거)을 함께 적는다.

## 1. Fan-out을 청크로 분산하지 않고 단일 컨슈머가 전체 처리

- **현재 구현**: `PostPublishedFanoutConsumer` 하나가 `post.published` 메시지 1건당 구독자 전체를 조회해 한 번에 처리한다.
- **원래 설계(`architecture.md` §4)**: Dispatcher가 구독자를 1,000명 단위 청크로 쪼개 `fanout.chunk.requested`에 발행하고, 여러 Chunk Worker가 병렬로 처리 — NFR-1(10만 명/5초) 충족을 위한 핵심 장치.
- **트레이드오프**: 단일 컨슈머 방식은 구현이 단순하고 정확성 검증이 쉽지만, 인기 작가 팬아웃이 파티션 1개·컨슈머 1개에 몰려 순차 처리된다 — 대량 트래픽에서는 SLA를 못 지킨다(§0 참고).
- **결정 필요 시점**: 부하 테스트(`docs/test/load-test-plan.md`)로 현재 구현의 처리량 한계를 실측한 뒤, 그 수치가 NFR-1 목표에 못 미치면 청크 Dispatcher/Worker 분리로 전환한다.
- **[2026-08-21 실측 갱신]**: `docs/test/load-test-report.md` 시나리오 A에서 실측 — 구독자 300명 기준 알림 생성까지 약 112 msg/sec, 목표(20,000 msg/sec) 대비 약 179배 부족. 300명 기준 선형 추정 시 10만 명 처리에는 약 15분이 걸릴 것으로 추정된다. 청크 Dispatcher/Worker 분리가 필요하다는 근거가 실측으로 확보됐다.
- **[2026-08-27 부분 조치 완료 — SubscriberSyncConsumer 처리 속도]**: §1.3(`chaos-test-report.md`)에서 실제 병목으로 드러난 건 `PostPublishedFanoutConsumer`가 아니라 `SubscriberSyncConsumer`(`subscription.changed` 컨슈머)의 순차 처리 속도(259초)였다. `post.published`/`subscription.changed` 두 토픽을 파티션 6개로 명시 생성(`KafkaTopicConfig`, 이전엔 브로커 기본값인 1개로 자동 생성되고 있었다)하고, `SubscriberSyncConsumer`를 배치 리스너(`concurrency=6`) + `@Transactional`로 전환해 메시지마다 개별 커밋하던 것을 배치당 1커밋으로 묶었다.
  - **구현 중 겪은 문제 1 — Testcontainers 동적 포트 우회**: 배치 리스너 전용 `ConsumerFactory`를 `KafkaProperties.buildConsumerProperties()`로 직접 만들었더니, Spring Boot의 `KafkaConnectionDetails` 기반 동적 포트 주입(Testcontainers)을 우회해 `application.yml`의 고정 주소로 연결을 시도하며 파티션을 영영 할당받지 못했다. 자동설정된 `ConsumerFactory` 빈을 그대로 재사용하도록 고쳐 해결(`BatchKafkaListenerConfig`).
  - **구현 중 겪은 문제 2 — 겹쳐서 드러난 §5-b**: concurrency를 6으로 늘리자 `SubscriberSyncConsumer`와 `PostPublishedFanoutConsumer`가 같은 consumer group(`notification-system`)을 공유하던 §5-b 이슈가 실제 장애로 나타났다 — 서로 다른 구독을 가진 멤버들이 리밸런싱을 반복하며 `subscription.changed` 쪽에 파티션이 아예 할당되지 않았다. 두 리스너에 명시적으로 별도 group-id(`subscriber-sync`, `post-fanout`)를 부여해 §5-b까지 함께 해결(§5 참고).
  - **검증**: 기존 통합 테스트 전체(`./gradlew test`) 회귀 없이 통과. 다만 §1.3에서 실측한 "7,610건 백로그 / 259초" 시나리오를 같은 조건으로 재현하는 chaos 재검증은 아직 하지 않았다 — 처리 속도가 실제로 얼마나 개선됐는지는 다음 재현 테스트에서 수치로 확인해야 한다.
  - **남은 부분**: `PostPublishedFanoutConsumer` 자체의 청크 분산(Dispatcher/Chunk Worker, `architecture.md` §4)은 여전히 미착수 — 이번 조치는 §1 중 "컨슈머 처리 속도" 부분만 다뤘다.

## 2. `subscriber_read_model` 동기화와 Fan-out 사이의 최종적 일관성(eventual consistency)

- **현재 구현**: `subscription.changed`(Read Model 동기화)와 `post.published`(Fan-out)는 서로 독립된 비동기 파이프라인이라 처리 순서를 보장하지 않는다. 구독 직후 곧바로 글을 발행하면, Read Model이 아직 갱신되기 전에 Fan-out이 먼저 실행돼 그 구독자가 알림 대상에서 누락될 수 있다.
- **검증된 사실**: `PostPublishedFanoutIntegrationTest`에서 실제로 이 레이스로 인한 실패를 재현했다(구독 직후 발행 시 간헐적으로 알림 누락). 운영 시나리오에서는 구독과 발행 사이 간격이 보통 이 갭보다 훨씬 크므로 실질적 영향은 낮다고 보고 테스트만 수정(발행 전 Read Model 동기화 완료를 기다리도록)하고 프로덕션 코드는 그대로 두었다.
- **트레이드오프**: 이 갭을 없애려면 Fan-out이 Read Model 대신 Subscription 원본을 동기 조회하거나, Post 발행을 Read Model 동기화 완료 후로 지연시켜야 하는데, 둘 다 `domain-design.md` §2가 의도적으로 피한 "Context 간 동기 결합"을 다시 끌어들인다.
- **결정 필요 시점**: "구독 직후 즉시 발행" 같은 실사용 패턴이 실제로 발생하는지 확인되면(예: 작가가 막 구독자를 얻고 바로 공지를 올리는 경우), 허용 가능한 지연 SLA를 정하고 그에 맞는 보완책(예: Fan-out 재시도/지연 처리)을 설계한다. 지금은 "받아들이는 트레이드오프"로 문서화만 해둔다.
- **[2026-08-21 실측 갱신]**: 장애 주입 테스트(`docs/test/chaos-test-report.md` §1.1)에서 이 레이스가 실제 알림 영구 유실로 이어지는 것을 관찰했다. 원래 "밀리초 단위"라고 가정했던 레이스 윈도우가, §4(동기 블로킹 발행)와 §5(공유 단일 스레드 스케줄러) 트레이드오프와 결합하면 **분 단위로 벌어질 수 있다** — 한 Relay(Subscription)가 대량 백로그를 처리하느라 다른 Relay(Post)보다 훨씬 늦게 이벤트를 발행하면서, Fan-out이 Read Model이 비어있는 상태로 먼저 실행돼버렸다. 세 트레이드오프가 개별적으로는 감수할 만해도 **동시에 겹치면 서로를 증폭시킨다**는 게 핵심 교훈이다. 이제 이 항목의 "결정 필요 시점"은 이론적 우려가 아니라 실측된 리스크로 격상됐다.
- **[2026-08-24 재현 테스트]**: §5(스케줄러 분리)를 고친 뒤 동일 조건으로 재현했다(`docs/test/chaos-test-report.md` §1.2) — 레이스 노출 시간은 약 240초에서 약 70초로 줄었지만, **알림은 여전히 영구 유실됐다**. 즉 §5는 노출 시간만 줄일 뿐 이 레이스 자체를 없애지 못한다는 게 확인됐다. 근본 해결은 이 항목(§2) 자체를 고치는 것뿐이다.
- **[2026-08-24 조치 완료 — Fan-out 재시도]**: `PostPublishedFanoutConsumer.findSubscribersWithRetry`를 추가했다 — `subscriber_read_model`에서 구독자가 0명 조회되면 즉시 포기하지 않고 1초 간격으로 최대 5회(약 4초) 재조회한 뒤에도 없으면 그때 포기하고 경고 로그를 남긴다. `SubscribeThenImmediatePublishIntegrationTest`(신규)로 "구독 직후 read model 동기화를 기다리지 않고 바로 발행"하는 흔한 케이스를 검증했다 — 재시도 덕분에 안정적으로 알림이 생성된다.
  - **한계**: 이 재시도 예산(~4초)은 §1.2에서 실측한 대량 백로그 시나리오(동기화까지 70초)는 흡수하지 못한다 — 그 정도 규모의 지연에는 여전히 취약하다. 다만 "구독하자마자 바로 발행" 같은 일반적인 케이스(수백 밀리초~수 초 지연)는 확실히 흡수한다. 완전한 해결은 §4(동기 블로킹 발행)를 고쳐 백로그 자체가 쌓이지 않게 하는 것과 함께 가야 한다.
- **[2026-08-25 §4 조치 후 재재현]**: §4(비동기 발행)를 고친 뒤 다시 재현했다 — Outbox→Kafka 구간은 7초로 크게 빨라졌지만, 그 다음 컨슈머가 백로그를 처리해 Read Model에 반영하는 데 약 259초가 걸려 재시도 예산을 훨씬 초과했고, 알림은 또 유실됐다(`docs/test/chaos-test-report.md` §1.3). §4는 필요조건이었지만 충분조건은 아니었다 — §1(단일 컨슈머)까지 고쳐야 대량 백로그 상황에서도 이 레이스가 실질적으로 닫힌다.

## 3. Outbox Relay 실패 처리 — 무한 재시도, Dead Letter 없음

- **현재 구현**: Kafka 발행이 실패해도 `outbox_events.status`를 바꾸지 않는다 — 다음 폴링(1초 간격)에서 같은 이벤트를 자동으로 다시 시도한다. 최대 재시도 횟수나 `FAILED`/Dead Letter 상태는 없다.
- **트레이드오프**: 구현이 단순하고 "이벤트를 영구히 잃어버리는" 실수를 원천 차단하지만, 브로커가 장기간 죽어 있으면 같은 이벤트를 계속 재시도하며 로그만 쌓인다 — 잘못된 payload(포이즌 메시지)로 인한 영구 실패도 동일하게 무한 재시도된다.
- **결정 필요 시점**: 운영 관측(메트릭/알림)이 붙기 전까지는 무해하지만, 실제 장애 주입 테스트(`docs/test/chaos-test-plan.md`)를 실행할 때 이 부분이 "재시도 폭주"로 보이는지 확인하고, 필요하면 재시도 횟수 상한 + Dead Letter 컬럼을 추가한다.
- **[2026-08-21 실측 갱신]**: 장애 주입 테스트(`docs/test/chaos-test-report.md` §1)에서 Kafka를 내렸다 올렸을 때 약 11,700건의 백로그가 재시도 폭주 없이(로그만 남기고) 결국 전량 발행되는 것을 확인했다 — 이 설계가 의도한 대로 동작했다. 다만 그 과정에서 개별 발행이 최대 120초까지 블로킹되는 부작용이 §4/§5와 겹쳐 §2의 알림 유실로 이어졌다(별개 이슈, §4 참고).

## 4. Kafka 발행을 Relay 루프 안에서 동기 블로킹(`send().get()`)으로 처리

- **현재 구현**: `PostOutboxRelay`/`SubscriptionOutboxRelay`가 이벤트를 하나씩 순회하며 `kafkaTemplate.send(...).get()`으로 ack까지 기다린 뒤 다음 이벤트로 넘어간다.
- **트레이드오프**: 순서 보장과 실패 처리(상태 롤백 없이 다음 폴링 재시도)가 단순해지지만, 이벤트 건수가 많아지면 배치 하나(현재 500건)를 처리하는 데 걸리는 시간이 늘어나 relay 폴링 주기(1초)를 못 맞출 수 있다.
- **결정 필요 시점**: 부하 테스트에서 outbox 적체(backlog)가 관찰되면, 비동기 콜백 기반 발행 + 배치 단위 상태 업데이트로 전환한다.
- **[2026-08-21 실측 갱신]**: 부하 테스트(시나리오 C)가 만든 약 11,700건의 outbox 백로그를 장애 주입 테스트 중 처리하면서, Kafka 복구 직후 리더 재선출 등으로 개별 `.send().get()` 호출이 최대 120초(`request.timeout.ms`/`delivery.timeout.ms` 기본값)까지 블로킹되는 것을 실제로 관측했다(`docs/test/chaos-test-report.md` §1.1). 이 블로킹이 §5(공유 스케줄러)와 겹쳐 다른 이벤트의 발행을 지연시켰고, 결국 §2의 알림 영구 유실로 이어졌다 — "outbox 적체가 관찰되면"이라는 트리거가 실제로 발생했다.
- **[2026-08-25 조치 완료 — 비동기 콜백 전환]**: `kafkaTemplate.send(...).get()` 블로킹을 제거하고 `CompletableFuture.whenComplete { }` 콜백으로 상태 갱신하도록 바꿨다. **구현 중 직접 만든 버그와 그 수정도 함께 기록한다** — 처음엔 in-flight 추적 없이 콜백만 붙였는데, Kafka 장애 중 매 폴링(1초)마다 같은 `PENDING` 행을 다시 제출해버려서, 장애가 걷힌 순간 쌓여있던 중복 제출분이 한꺼번에 타임아웃 폭주를 일으켰다(재현 테스트에서 로그에 `TimeoutException: Expiring ... record(s)` **90,438건** 관측 — §4가 원래 막으려던 문제보다 오히려 더 심각한 형태로 재발). `ConcurrentHashMap` 기반 `inFlight` Set으로 "ack 안 돌아온 이벤트는 재제출 안 함"을 추가해 해결했다.
  - **개선 확인**: 같은 조건(약 7,610건 백로그, Kafka 다운→복구)으로 재현했을 때, subscription outbox가 전부 `PENDING`→`PUBLISHED`로 드레인되는 데 걸린 시간이 **약 70~240초 → 약 7초**로 단축됐다(`docs/test/chaos-test-report.md` §1.3). Relay 자체의 처리량 문제는 확실히 해결됐다.
  - **예상 밖의 발견 — 병목이 컨슈머 쪽으로 이동**: relay가 Kafka에 메시지를 다 밀어넣는 데는 7초밖에 안 걸렸지만, 그 뒤 `SubscriberSyncConsumer`가 쌓여있는 메시지를 순서대로(단일 컨슈머, §1) 소비해 `subscriber_read_model`에 반영하는 데는 여전히 **약 259초**가 걸렸다 — 그 사이 Fan-out 재시도 예산(§2, ~4초)은 이미 소진돼 알림은 또 영구 유실됐다. 즉 **§4는 "Outbox → Kafka" 구간의 병목만 없앴을 뿐, "Kafka → Read Model"(컨슈머 처리) 구간의 병목(§1)은 그대로 남아있다** — 전체 파이프라인의 체감 지연은 병목이 옮겨갔을 뿐 크게 줄지 않을 수 있다. §2를 완전히 닫으려면 §1(청크 분산 또는 컨슈머 자체의 처리량 개선)까지 손대야 한다.

## 5. `SubscriberSyncConsumer`와 `PostPublishedFanoutConsumer`가 같은 Kafka consumer group을 공유

- **현재 구현**: `application.yml`의 `spring.kafka.consumer.group-id: notification-system` 하나를 두 리스너가 그대로 공유한다 — 각기 다른 토픽(`subscription.changed`, `post.published`)을 구독하지만 그룹은 동일하다.
- **트레이드오프**: 지금은 문제없이 동작하지만(각자 자기 토픽의 파티션만 할당받음), 같은 그룹 안에 서로 다른 구독 목록을 가진 컨슈머가 섞이는 구성은 Kafka에서 권장되지 않는 패턴이다 — 인스턴스를 여러 개로 늘리거나 리밸런싱이 잦아지면 예상치 못한 파티션 재할당이 생길 수 있다.
- **결정 필요 시점**: 인스턴스를 2개 이상으로 스케일아웃하기 전에 컨슈머별로 별도 group-id(`notification-system-subscriber-sync`, `notification-system-fanout` 등)로 분리한다.
- **[2026-08-21 실측 추가]**: 이건 Kafka consumer group 얘기와는 별개로, `@Scheduled` 작업(`PostOutboxRelay`, `SubscriptionOutboxRelay`, `PushDeliveryWorker`)도 Spring Boot 기본 설정상 **단일 스레드 스케줄러 하나를 전부 공유**한다. 장애 주입 테스트(`docs/test/chaos-test-report.md` §1.1)에서 `SubscriptionOutboxRelay`가 약 11,700건의 백로그를 처리하는 동안 다른 Relay/Worker가 지연되는 것을 실측했다 — 단일 인스턴스에서도 발생하는 문제라 "인스턴스 스케일아웃 전"이라는 기존 트리거로는 충분하지 않다. **새 결정 필요 시점**: Relay/Worker별로 별도 스케줄러 스레드(또는 스레드 풀)를 쓰도록 `TaskScheduler` 빈을 분리한다 — 지금처럼 여러 스케줄 작업이 하나의 스레드를 공유하면, 한 작업의 백로그가 다른 작업의 지연 보장을 깨뜨릴 수 있다.
- **[2026-08-24 조치 완료 — 스케줄러 풀 크기 확장]**: `application.yml`에 `spring.task.scheduling.pool.size: 3`을 추가해 해결했다. Spring Boot가 기본 제공하는 `ThreadPoolTaskScheduler`의 풀 크기를 현재 스케줄 컴포넌트 수(3개)만큼 늘린 것으로, 새 스케줄러 구현이나 코드 변경 없이 설정 한 줄로 해결됐다(가장 작은 변경으로 가장 큰 위험을 없애는 방향을 택함 — Bean을 직접 나누는 대신 기본 자동설정이 이미 제공하는 풀 크기 옵션만 조정). `/actuator/threaddump`로 `scheduling-1`/`scheduling-2`/`scheduling-3` 세 개의 독립된 스레드가 실제로 존재함을 확인했고, 전체 테스트 스위트도 회귀 없이 통과했다. Kafka consumer group 공유(같은 섹션 상단) 자체는 여전히 미해결 — 이건 별도 그룹 ID 분리가 필요하며 스케줄러 풀 크기와는 무관하다.
- **[2026-08-27 조치 완료 — Consumer group 분리]**: §1의 `SubscriberSyncConsumer` 배치 전환 작업 중 이 이슈가 이론적 우려가 아니라 실제 장애(리밸런싱 반복으로 파티션 미할당)로 나타나는 것을 직접 확인했다. `@KafkaListener(groupId = ...)`로 `SubscriberSyncConsumer`는 `subscriber-sync`, `PostPublishedFanoutConsumer`는 `post-fanout`으로 그룹을 분리해 해결했다. 인스턴스 스케일아웃 여부와 무관하게, 단일 인스턴스에서도 문제였다는 게 이번에 드러난 점 — 기존에 "스케일아웃 전"으로 적어둔 트리거는 실제보다 늦은 기준이었다.

## 6. 초기 백필 배치는 범위 밖

- **현재 구현**: `subscriber_read_model` 초기 백필 배치는 미구현 — 신규 배포 시 기존 구독 데이터가 있어도 이벤트 재생 없이는 Read Model이 비어있는 상태로 시작한다.
- **트레이드오프**: 이번 요청("구독+알림설정된 사용자에게 알림 생성")의 최소 범위를 벗어나므로 의도적으로 미룸. 백필 전까지는 팬아웃 대상에서 기존 구독자가 전부 누락된다.
- **결정 필요 시점**: 이 시스템을 실제로 기존 구독 데이터가 있는 환경에 배포하기 전에 반드시 백필 배치부터 구현해야 한다 — 그 전까지는 "처음부터 새로 시작하는 시스템"에서만 정합성이 보장된다.

## 7. Push 발송 재시도/DLQ — Kafka 토픽 대신 DB 폴링 워커

- **결정**: `architecture.md` §5가 설계한 `delivery.push.requested`/`delivery.push.dlq` Kafka 토픽 기반 구조 대신, `PushDeliveryWorker`가 `notification.notification_delivery_log`를 `@Scheduled(fixedDelay=1000)`로 폴링하는 방식으로 구현했다. `PostOutboxRelay`/`SubscriptionOutboxRelay`와 동일한 패턴이며, 이 테이블에 이미 있던 `idx_notification_delivery_retry_queue` 인덱스가 정확히 이 폴링을 겨냥한 것이었다.
- **근거**: `architecture.md` §5가 "채널별 토픽 분리"를 원한 이유는 FR-3.4(한 채널 장애가 다른 채널에 영향 없음)였는데, 이 시스템은 Push 채널만 지원하므로 격리할 다른 채널이 없다 — 토픽 분리의 원래 근거가 소멸했다. 새 Kafka 토픽/컨슈머 그룹을 추가하는 비용 대비 얻는 게 없다고 판단했다.
- **재시도 정책**: `architecture.md` §5의 예시(1s, 2s, 4s, 최대 3회)를 그대로 따르지 않고 근사치로 단순화했다 — attempt 1회차는 즉시, 이후 `2^(attempt_count-1)`초 백오프(1s, 2s)로 최대 3회 시도 후 `DEAD_LETTER` 전이. "최대 3회"면 3번째 시도 전 대기가 2s로 끝나 4s 대기까지는 가지 않는다.
- **트레이드오프**: `delivery.push.requested`의 파티션 분산(원 설계 — "랜덤/round-robin, 높음(32)")이 주려던 "발송 요청을 여러 워커에 넓게 분산"하는 수평 확장성은 지금 없다 — `PushDeliveryWorker`는 인스턴스 하나당 순차 폴링이다. 인기 작가 팬아웃으로 delivery log가 대량 쌓이면 이 워커 하나가 병목이 될 수 있다.
- **결정 필요 시점**: Push 외 다른 채널을 다시 지원하게 되거나, 부하 테스트에서 이 워커가 병목으로 드러나면 Kafka 토픽 기반 구조로 전환한다.

## 요약 — 지금 결정이 필요한 것 vs 나중으로 미뤄도 되는 것

| 항목 | 지금 결정 필요? | 트리거 |
|---|---|---|
| 1. 청크 미분산 / 컨슈머 처리 속도 | ⚠️ **부분 조치 완료(2026-08-27)** | `SubscriberSyncConsumer` 배치 전환 + 파티션 6개로 처리 속도 개선, 통합 테스트 회귀 없음 확인. `PostPublishedFanoutConsumer`의 청크 Dispatcher/Worker 분리(`architecture.md` §4)는 여전히 미착수. §1.3 시나리오(7,610건/259초) 재현 재검증도 아직 안 함 |
| 2. Read Model 동기화 레이스 | ⚠️ **부분 조치 완료, §1 처리 속도 개선과 함께 재평가 필요** | Fan-out 재시도(~4초)로 일반적인 케이스는 흡수. §1의 컨슈머 처리 속도 개선이 대량 백로그 시나리오(2026-08-25 재재현, 259초)를 얼마나 줄였는지는 재현 테스트로 다시 확인해야 함 |
| 3. Relay 무한 재시도 | 아니오 (정상 동작 확인됨) | 장애 주입 테스트에서 재시도 폭주 없이 결국 전량 발행됨을 확인(`chaos-test-report.md` §1) |
| 4. Relay 동기 블로킹 발행 | ✅ **조치 완료(2026-08-25)** | 비동기 콜백 + in-flight 중복 방지로 전환. Outbox→Kafka 드레인 시간 70~240초 → 약 7초로 단축 실측(`chaos-test-report.md` §1.3) |
| 5-a. 스케줄러 스레드 공유 | ✅ **조치 완료(2026-08-24)** | `spring.task.scheduling.pool.size: 3`으로 해결, `/actuator/threaddump`로 검증 |
| 5-b. Consumer group 공유 | ✅ **조치 완료(2026-08-27)** | `subscriber-sync`/`post-fanout`로 group-id 분리. 단일 인스턴스에서도 리밸런싱 반복으로 실제 장애가 나는 것을 확인하고 고침 |
| 6. 백필 배치 미구현 | **예 — 기존 데이터 있는 환경 배포 전** | 프로덕션/기존 구독 데이터 마이그레이션 시 |
| 7. Push 재시도/DLQ — DB 폴링, Kafka 토픽 없음 | 확정됨 (재논의 불필요) | Push 이외 채널을 다시 지원하며 채널별 장애 격리가 다시 필요해질 때 |
