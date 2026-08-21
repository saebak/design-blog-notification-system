# 대용량 블로그 알림 시스템 (Design Blog Notification System)

작가가 글을 등록하면 수십만 명의 구독자에게 저지연으로 알림을 발송하는 대용량 알림 시스템을 설계/구현한다.

## 프로젝트 개요

- **문제 정의**: 인기 작가 1명의 글 등록 이벤트를 10만 명 이상의 구독자에게 5초 이내 팬아웃(fan-out)하면서도, 블로그 글쓰기라는 핵심 기능은 알림 시스템 장애와 무관하게 항상 정상 동작해야 한다.
- **핵심 설계 챌린지**: Low Latency 팬아웃, Write/Read Path 분리를 통한 고가용성, 순간 대량 트래픽(10만 QPS+)을 견디는 확장성.
- 상세 요구사항은 [`docs/requirements.md`](./docs/requirements.md) 참고.

## 기술 스택

> 근거는 [`docs/architecture.md`](./docs/architecture.md) §2에서 항목별로 상세히 정리.

| 영역 | 기술 | 비고 |
|---|---|---|
| Language / Runtime | Kotlin (JVM) | 코루틴 기반 비동기 처리가 Kafka 컨슈머/청크 워커 동시성 코드에 유리, JVM 생태계 활용. 근거: `docs/architecture.md` §2 |
| Backend Framework | Spring Boot | Kafka/트랜잭셔널 아웃박스를 표준 지원. 근거: `docs/architecture.md` §2 |
| Message Broker | Kafka | 컨슈머 그룹 기반 수평 확장. 근거: [`docs/architecture.md`](./docs/architecture.md) §2, §7 |
| DB (Write) | PostgreSQL (가정), 단일 인스턴스 + Context별 스키마 분리 | [`docs/database-design.md`](./docs/database-design.md)에서 DDL 작성을 위해 가정, 최종 확정 전. 모듈러 모놀리식(모듈 경계=Context 경계)으로 시작 |
| DB (Read / Cache) | Redis | 발송 파이프라인과 분리된 Read Path 캐시 용도(NFR-2.3). 현재 구현에서는 미사용. 근거: `docs/architecture.md` §2 |
| Infra / Deploy | Docker Compose | 로컬에서 Kafka/PostgreSQL/Redis/앱 다중 인스턴스를 함께 구동. 근거: `docs/architecture.md` §2 |
| Load Test | k6 | HTTP 시나리오 작성 및 Grafana 연동. 근거: `docs/architecture.md` §2 |

## 빌드 및 실행 방법

```bash
# 1. 로컬 인프라(PostgreSQL, Kafka, Redis) 기동
docker compose up -d

# 2. 빌드
./gradlew build

# 3. 실행 (기동 시 Flyway가 스키마/테이블을 자동 생성)
./gradlew bootRun

# 테스트 (Testcontainers로 Postgres/Kafka/Redis를 매번 새로 띄움 — Docker 필요)
./gradlew test
```

## 구현 현황

`docs/requirements.md` §4 개발 태스크 목록 기준. 상세 트레이드오프/스코프 축소 내역은 [`docs/decisions.md`](./docs/decisions.md) 참고.

| 상태 | 항목 |
|---|---|
| ✅ | 사용자/작가/구독 도메인 모델 및 API |
| ✅ | 알림 수신 설정(Push/Email/Mute) API |
| ✅ | 글 등록 API + Outbox 테이블 + Relay 프로세스 (Post/Subscription 둘 다) |
| ✅ | Kafka 연동 (`post.published`, `subscription.changed` 토픽) |
| ✅ | Fan-out 컨슈머 (Subscriber Read Model 동기화 → 구독자 조회 → 알림 생성) |
| ✅ | Push 발송 위임 (외부 Push 서버로 위임, 목업 어댑터) + 재시도/DLQ |
| ✅ | 알림 읽음 처리 API (개별/전체) |
| ⬜ | 부하 테스트 실행 및 결과 문서화 (`docs/test/load-test-plan.md` → `load-test-report.md`) |
| ⬜ | 장애 주입 테스트 실행 및 결과 문서화 (`docs/test/chaos-test-plan.md` → `chaos-test-report.md`) |

**스코프에서 제외된 것** (구현 안 함, 원래 요구사항에서 의도적으로 축소): Email 실제 발송, 인앱 알림 목록/unread count 조회, 실시간 채널(WebSocket/SSE), 커스텀 메트릭/모니터링 대시보드(기본 로깅만 유지), 인증/인가. 배경은 `docs/requirements.md` §3, `docs/decisions.md` 참고.

## API 엔드포인트

인증 없음 — 모든 API가 사용자/수신자 id를 요청 파라미터로 직접 받는다(`docs/decisions.md` 참고).

| 메서드 | 경로 | 설명 |
|---|---|---|
| `POST` | `/api/users` | 사용자 생성 |
| `GET` | `/api/users/{id}` | 사용자 조회 |
| `PATCH` | `/api/users/{id}/notification-channel` | 알림 수신 채널(Push/Email/Mute) 변경 |
| `POST` | `/api/subscriptions` | 구독 |
| `DELETE` | `/api/subscriptions?userId=&authorId=` | 구독 취소 |
| `GET` | `/api/users/{userId}/subscriptions` | 내 구독 목록 |
| `GET` | `/api/authors/{authorId}/subscribers` | 작가 기준 구독자 목록 (내부/백필용) |
| `POST` | `/api/posts` | 글 작성 |
| `GET` | `/api/posts/{id}` | 글 조회 |
| `POST` | `/api/posts/{id}/publish` | 글 발행 (→ Outbox → Fan-out → 알림 생성 트리거) |
| `PATCH` | `/api/notifications/{id}/read?recipientId=` | 알림 개별 읽음 처리 |
| `PATCH` | `/api/users/{recipientId}/notifications/read-all` | 알림 전체 읽음 처리 |

## 설계 문서

- [요구사항 정의서](./docs/requirements.md) — 기능/비기능 요구사항을 구현 단위로 분해한 문서
- [도메인 설계](./docs/domain-design.md) — Post / Subscription / Notification Bounded Context 및 Context Map
- [데이터베이스 설계](./docs/database-design.md) — Context별 DDL 및 인덱스 전략
- [아키텍처 설계](./docs/architecture.md) — 메시지 브로커/2단계 Fan-out/Delivery 파이프라인/실시간 채널
- [부하 테스트 시나리오 설계](./docs/test/load-test-plan.md) — NFR-1/NFR-3 검증 시나리오 (실행 결과는 구현 후 `docs/test/load-test-report.md`에 기록 예정)
- [장애 주입(Chaos) 테스트 시나리오 설계](./docs/test/chaos-test-plan.md) — NFR-2 검증 시나리오 (실행 결과는 구현 후 `docs/test/chaos-test-report.md`에 기록 예정)
- [구현 트레이드오프와 남은 결정사항](./docs/decisions.md) — 구현하며 스코프를 좁힌 지점과 아직 결정하지 않고 미뤄둔 사항들, 각각 다시 논의할 시점(트리거)

## 프로젝트 구조

```
.
├── README.md
├── docker-compose.yml          # 로컬 PostgreSQL / Kafka / Redis
├── build.gradle.kts
├── docs/
│   ├── requirements.md
│   ├── domain-design.md
│   ├── database-design.md
│   ├── architecture.md
│   └── test/
│       ├── load-test-plan.md
│       └── chaos-test-plan.md
└── src/main/
    ├── kotlin/com/blog/notification/
    │   ├── NotificationSystemApplication.kt
    │   ├── common/
    │   │   ├── kafka/        # 토픽 이름 상수
    │   │   ├── outbox/       # Outbox Relay 공용 레코드 타입
    │   │   └── (예외 처리 등 도메인에 속하지 않는 공통 인프라)
    │   ├── post/             # Post Bounded Context
    │   │   ├── domain/       # Post, PostPublishedEvent
    │   │   ├── enum/         # PostStatus
    │   │   ├── relay/        # PostOutboxRelay — outbox_events 폴링 후 Kafka 발행
    │   │   ├── controller/ / service/ / repository/     # 레이어
    │   │   └── dto/
    │   ├── subscription/     # Subscription Bounded Context
    │   │   ├── domain/       # Subscription, SubscriptionChangedEvent
    │   │   ├── enum/         # SubscriptionStatus
    │   │   ├── relay/        # SubscriptionOutboxRelay
    │   │   ├── controller/ / service/ / repository/     # 레이어
    │   │   └── dto/
    │   ├── notification/     # Notification Bounded Context (Fan-out/Delivery)
    │   │   ├── domain/       # Notification
    │   │   ├── consumer/     # SubscriberSyncConsumer, PostPublishedFanoutConsumer (+ 각 이벤트의 로컬 DTO)
    │   │   ├── gateway/      # PushGatewayPort, SimulatedPushGatewayAdapter (목업)
    │   │   ├── delivery/     # PushDeliveryWorker — delivery_log 폴링 재시도/DLQ
    │   │   ├── controller/ / service/ / repository/     # 레이어
    │   │   └── dto/
    │   └── user/             # 공유 참조 테이블(users) 모듈 — 어느 Context도 소유하지 않음
    │       ├── domain/       # User
    │       ├── enum/         # NotificationChannel
    │       ├── controller/ / service/ / repository/     # 레이어
    │       └── dto/
    └── resources/
        ├── application.yml
        └── db/migration/    # Flyway — 스키마 및 Context별 테이블 DDL
```

모듈(패키지) 경계는 `docs/domain-design.md` §2의 Bounded Context 경계와 1:1로 맞춘다 — Context 간에는 서로의 패키지를 직접 참조하지 않고 이벤트로만 통신한다(모듈러 모놀리식, `docs/architecture.md` §2 DB 배포 토폴로지 참고).
