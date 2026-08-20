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
| Backend Framework | Spring Boot | Kafka/트랜잭셔널 아웃박스/WebSocket을 표준 지원. 근거: `docs/architecture.md` §2 |
| Message Broker | Kafka | 컨슈머 그룹 기반 수평 확장, DLQ 토픽 구성. 근거: [`docs/architecture.md`](./docs/architecture.md) §2, §7 |
| DB (Write) | PostgreSQL (가정), 단일 인스턴스 + Context별 스키마 분리 | [`docs/database-design.md`](./docs/database-design.md)에서 DDL 작성을 위해 가정, 최종 확정 전. 모듈러 모놀리식(모듈 경계=Context 경계)으로 시작 |
| DB (Read / Cache) | Redis | unread count 등 고빈도 조회를 발송 파이프라인과 격리(NFR-2.3). 근거: `docs/architecture.md` §2 |
| Realtime | WebSocket + Redis Pub/Sub | 다중 인스턴스 브로드캐스트. 근거: `docs/architecture.md` §2, §6 |
| Infra / Deploy | Docker Compose | 로컬에서 Kafka/PostgreSQL/Redis/앱 다중 인스턴스를 함께 구동. 근거: `docs/architecture.md` §2 |
| Load Test | k6 | HTTP+WebSocket 시나리오 작성 및 Grafana 연동. 근거: `docs/architecture.md` §2 |

## 빌드 및 실행 방법

```bash
# 1. 로컬 인프라(PostgreSQL, Kafka, Redis) 기동
docker compose up -d

# 2. 빌드
./gradlew build

# 3. 실행 (기동 시 Flyway가 스키마/테이블을 자동 생성)
./gradlew bootRun
```

> API/도메인 로직은 아직 스캐폴딩 단계 — 빌드 가능한 뼈대(Gradle/Kotlin/Spring Boot 프로젝트 구조, DB 마이그레이션, 로컬 인프라)만 갖춘 상태이며, 구현 진행에 따라 갱신 예정.

## 설계 문서

- [요구사항 정의서](./docs/requirements.md) — 기능/비기능 요구사항을 구현 단위로 분해한 문서
- [도메인 설계](./docs/domain-design.md) — Post / Subscription / Notification Bounded Context 및 Context Map
- [데이터베이스 설계](./docs/database-design.md) — Context별 DDL 및 인덱스 전략
- [아키텍처 설계](./docs/architecture.md) — 메시지 브로커/2단계 Fan-out/Delivery 파이프라인/실시간 채널
- 부하 테스트 리포트 (예정) — `docs/load-test-report.md`
- 장애 주입(Chaos) 테스트 리포트 (예정) — `docs/chaos-test-report.md`

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
│   └── architecture.md
└── src/main/
    ├── kotlin/com/blog/notification/
    │   ├── NotificationSystemApplication.kt
    │   ├── post/            # Post Bounded Context
    │   ├── subscription/    # Subscription Bounded Context
    │   ├── notification/    # Notification Bounded Context (Fan-out/Delivery/실시간)
    │   └── user/            # 공유 참조 테이블(users) 모듈 — 어느 Context도 소유하지 않음
    └── resources/
        ├── application.yml
        └── db/migration/    # Flyway — 스키마 및 Context별 테이블 DDL
```

모듈(패키지) 경계는 `docs/domain-design.md` §2의 Bounded Context 경계와 1:1로 맞춘다 — Context 간에는 서로의 패키지를 직접 참조하지 않고 이벤트로만 통신한다(모듈러 모놀리식, `docs/architecture.md` §2 DB 배포 토폴로지 참고).
