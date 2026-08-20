# 데이터베이스 설계 — DDL & 인덱스 전략

> [`domain-design.md`](./domain-design.md)의 Aggregate/Entity를 실제 테이블로 변환한 문서. **단일 PostgreSQL 인스턴스, Context별로 별도 스키마(`post`, `subscription`, `notification`)로 논리 분리**한다 (모듈러 모놀리식 — 실제 구현은 단일 레포로 시작하되 모듈/스키마 경계는 Context 경계와 일치시킨다). Context 간 FK를 걸지 않고 ID만 값으로 참조하는 것이 원칙이다.

## 0. 가정 (Assumptions)

- **DB 엔진**: PostgreSQL 15+ (README 기술스택은 아직 TBD이나, 구체적 DDL 작성을 위해 가정. 다른 RDBMS 채택 시 타입/파티셔닝 문법만 교체하면 구조는 동일)
- **배포 토폴로지**: 단일 DB 인스턴스, Context별 스키마로 논리 분리한다. Bounded Context 분리(코드/모듈 경계)와 물리 배포 토폴로지는 별개 문제로 취급한다 — 인스턴스를 여러 개 두는 인프라 복잡도는 불필요하다고 판단해 단일 인스턴스로 시작하고, 특정 Context의 부하가 커지면 그 스키마만 별도 인스턴스로 분리하는 경로를 열어둔다.
- **ID 전략**:
  - Context 내부 PK: `BIGINT GENERATED ALWAYS AS IDENTITY` (UUID 대비 인덱스 크기/locality가 유리 — 10만 건 단위 벌크 insert/scan이 많은 시스템 특성상 중요)
  - **이벤트 ID**(`eventId`, `sourceEventId`): `UUID` — 여러 프로세스(Outbox Relay, Fan-out Consumer)가 중앙 조정 없이 생성/전파해야 하므로 전역 유일성이 필요
  - 다른 테이블을 참조하는 컬럼(`author_id`, `user_id`, `post_id`, `notification_id` 등)은 대상 테이블이 발급한 BIGINT를 값으로만 저장한다. **FK를 이 프로젝트 전체에서 쓰지 않는다** — Context 간 참조는 특정 Context(스키마)를 나중에 별도 서비스/DB로 분리할 수 있는 여지를 남겨두기 위해서고, 같은 스키마 안의 참조(예: §4.3 `notification_delivery_log.notification_id` → `notifications.id`)도 동일한 규율을 일관되게 적용한다. FK가 주는 참조 무결성은 애플리케이션 레벨(멱등 upsert, 트랜잭션 내 순서 보장 등)에서 대신 책임진다.

---

## 1. User (공유 참조 테이블)

Post/Subscription/Notification 세 Context 모두 `user_id`(구독자/작가/알림 수신자 공통 식별자)를 값으로만 참조한다 (`domain-design.md` §6). 회원가입/프로필 관리가 요구사항 범위 밖이므로 최소 속성만 가진 테이블로 둔다 — FK로 참조되는 대상이 아니라, 각 Context가 발급받은 `user_id` 값의 **출처(source of truth)** 역할만 한다. 어느 Context의 스키마에도 속하지 않으므로 물리적으로는 PostgreSQL 기본 스키마인 `public`에 둔다.

알림 채널 설정(Push/Email/Mute)도 이 테이블에 둔다. 작가별 mute는 별도로 두지 않는다 — 특정 작가의 알림을 받고 싶지 않으면 **구독을 취소**하면 되므로, "이 작가의 알림을 보낼지"는 ①구독 여부(Subscription Context) ②전역 채널 설정(이 테이블) 두 가지만 조합하면 충분하다 (`notification_preferences`처럼 작가 단위 예외를 별도 관리할 필요가 없어져 테이블 자체를 제거했다).

```sql
CREATE TABLE users (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email               VARCHAR(255) NOT NULL,
    name                VARCHAR(100) NOT NULL,
    notification_channel VARCHAR(10) NOT NULL DEFAULT 'PUSH'
                         CHECK (notification_channel IN ('PUSH', 'EMAIL', 'MUTE')),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_users_email UNIQUE (email)
);
```

**인덱스 근거**
- `uq_users_email`: 로그인/식별에 이메일을 키로 쓰므로 유니크 제약 겸 조회 인덱스로 사용.
- `notification_channel`은 사용자 1명당 값 1개뿐이라(카디널리티 3) 별도 인덱스가 필요 없다 — Fan-out 시 이미 청크 단위로 뽑아온 `user_id` 목록으로 `WHERE id = ANY(:chunkIds)` 조회하므로 PK만으로 충분.

> **Notification Context의 `users` 조회에 대해**: Fan-out 프로세스는 청크 처리 시 이 테이블을 직접 조회(`WHERE id = ANY(:chunkIds)`)해서 `notification_channel = 'MUTE'`인 대상을 걸러낸다. 같은 DB 인스턴스 안의 조회라 별도 서비스로의 네트워크 호출이 아니므로, 지금 시점에는 이게 NFR-1(SLA)이나 NFR-2(장애 격리)에 새로운 가용성 리스크를 추가하지 않는다 — `users`가 속한 스키마의 소유자를 딱히 못박지 않은 채로 두는 것도 이 때문에 별문제가 안 된다. 다만 FK를 걸지 않는 규율(§0)은 그대로 지킨다: 나중에 Notification Context를 별도 서비스/DB로 분리하는 시점이 오면 이 조회는 다시 진짜 원격 의존이 되므로, 그때는 `subscriber_read_model`과 같은 방식으로 로컬 복제하는 재설계가 필요하다는 점을 미리 남겨둔다.

---

## 2. Post Context DB

### 2.1 `posts`

```sql
CREATE TABLE posts (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    author_id    BIGINT NOT NULL,
    title        VARCHAR(200) NOT NULL,
    content      TEXT NOT NULL,
    status       VARCHAR(10) NOT NULL DEFAULT 'DRAFT'
                 CHECK (status IN ('DRAFT', 'PUBLISHED')),
    published_at TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT chk_published_at_consistency
        CHECK (
            (status = 'PUBLISHED' AND published_at IS NOT NULL)
            OR (status = 'DRAFT' AND published_at IS NULL)
        )
);

CREATE INDEX idx_posts_author_id ON posts (author_id, created_at DESC);
```

**인덱스 근거**
- `idx_posts_author_id (author_id, created_at DESC)`: 작가의 글 목록 조회(에디터 "내 글" 화면) 대비. 알림 시스템 자체의 핫패스는 아니지만 최소 CRUD 지원용.
- `status`는 저카디널리티(2개 값)라 별도 인덱스 불필요 — 발행 여부 조회는 `id`로 단건 조회가 대부분이라 풀스캔 대상 아님.

### 2.2 `outbox_events`

```sql
CREATE TABLE outbox_events (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),  -- 곧 PostPublished.eventId
    aggregate_type VARCHAR(50) NOT NULL,       -- 'Post'
    aggregate_id   BIGINT NOT NULL,            -- post_id
    event_type     VARCHAR(50) NOT NULL,       -- 'PostPublished'
    payload        JSONB NOT NULL,
    status         VARCHAR(10) NOT NULL DEFAULT 'PENDING'
                   CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED')),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at   TIMESTAMPTZ
);

-- Relay 프로세스의 폴링 쿼리: WHERE status = 'PENDING' ORDER BY created_at LIMIT N
CREATE INDEX idx_outbox_pending ON outbox_events (created_at)
    WHERE status = 'PENDING';
```

**인덱스 근거**
- **부분 인덱스(Partial Index)**: `status = 'PENDING'`인 행만 인덱싱. 시간이 지나 대부분의 행이 `PUBLISHED`로 바뀌어도 인덱스 크기는 "아직 발행 안 된 소수"에 비례해 작게 유지됨 — Relay의 폴링 쿼리가 항상 빠름 (NFR-1과 직결: Relay 지연이 곧 알림 지연).
- 오래된 `PUBLISHED` 행은 별도 배치로 주기적 삭제/아카이빙 (Outbox 테이블이 무한히 커지지 않도록).

---

## 3. Subscription Context DB

### 3.1 `subscriptions`

```sql
CREATE TABLE subscriptions (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id  BIGINT NOT NULL,
    author_id      BIGINT NOT NULL,
    status         VARCHAR(10) NOT NULL DEFAULT 'ACTIVE'
                   CHECK (status IN ('ACTIVE', 'CANCELLED')),
    subscribed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    cancelled_at   TIMESTAMPTZ,

    CONSTRAINT uq_subscription_user_author UNIQUE (user_id, author_id)
);

-- 작가 기준 구독자 벌크 조회 (부트스트랩/백필용, FR-1.2)
CREATE INDEX idx_subscriptions_author_active
    ON subscriptions (author_id, id)
    WHERE status = 'ACTIVE';

-- "내 구독 목록" 조회
CREATE INDEX idx_subscriptions_user
    ON subscriptions (user_id)
    WHERE status = 'ACTIVE';
```

**인덱스 근거**
- `uq_subscription_user_author`: 중복 구독 방지 불변식을 DB 레벨에서 강제. 이 유니크 인덱스가 곧 `(user_id, author_id)` 조회의 커버링 인덱스 역할도 겸함. **이 제약은 `status`와 무관하게 행 자체에 걸려 있다** — Cancelled 행도 유일성 검사 대상이므로, 재구독을 단순 `INSERT`로 구현하면 과거 Cancelled 행과 충돌해 실패한다.

**재구독(subscribe) 쿼리 — upsert로 구현**
```sql
INSERT INTO subscriptions (user_id, author_id, status, subscribed_at, cancelled_at)
VALUES (:userId, :authorId, 'ACTIVE', now(), NULL)
ON CONFLICT (user_id, author_id)
DO UPDATE SET status = 'ACTIVE', subscribed_at = now(), cancelled_at = NULL
WHERE subscriptions.status = 'CANCELLED';
```
`DO UPDATE ... WHERE`는 기존 행이 `CANCELLED`일 때만 재활성화하고, 이미 `ACTIVE`인 행에 대한 재구독 요청은 조용히 무시한다(멱등). `domain-design.md` §4의 `subscribe()` 행위와 매핑된다.

- `idx_subscriptions_author_active`: **키셋 페이지네이션**(`WHERE author_id = ? AND id > :lastId ORDER BY id LIMIT N`)을 위해 `id`를 두 번째 컬럼으로 포함. `OFFSET` 방식은 10만 건 규모에서 뒤로 갈수록 느려지므로 배제.
- 두 인덱스 모두 `status = 'ACTIVE'` 부분 인덱스로 만들어 취소된 구독이 인덱스 크기에 영향을 주지 않게 함.

### 3.2 `subscription_outbox_events`
Post Context의 `outbox_events`와 동일한 구조 (event_type = `'SubscriptionChanged'`). DDL 생략 (§2.2와 동일 패턴).

---

## 4. Notification Context DB

### 4.1 `subscriber_read_model`
Subscription Context의 `SubscriptionChanged` 이벤트를 구독해 유지하는 로컬 복제본 (§`domain-design.md` 2절 설계 결정 참고).

```sql
CREATE TABLE subscriber_read_model (
    author_id     BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (author_id, user_id)
);
```

**인덱스 근거**
- PK를 `(author_id, user_id)` 복합키로 잡아 **PK 자체가 곧 Fan-out 스캔 인덱스**가 되도록 함: `WHERE author_id = ? AND user_id > :cursor ORDER BY user_id LIMIT 1000` — 청크 단위 스캔(FR-2.4, `architecture.md` §4.1의 Dispatcher 키셋 스캔과 동일한 크기)에 최적.
- 별도 인덱스 불필요 (읽기 패턴이 이 하나뿐).
- `SubscriptionChanged(status=Cancelled)` 이벤트 수신 시 해당 행을 `DELETE`. 이벤트 순서 역전(구독 이벤트가 취소 이벤트보다 늦게 도착) 가능성에 대비해 `upsert` 시 `updated_at`을 비교해 최신 이벤트만 반영 (Last-Write-Wins).

**Fan-out 흐름**: 이 테이블은 Dispatcher가 `WHERE author_id=? AND user_id > :cursor ORDER BY user_id LIMIT 1000`(키셋, OFFSET 아님)으로 순차 스캔하며, 1,000명이 모일 때마다 그 `user_id` 목록을 청크 메시지에 담아 즉시 발행한다(`architecture.md` §4.1). Chunk Worker는 이 테이블을 다시 조회하지 않고, 메시지에 포함된 `user_id` 목록으로 바로 `SELECT id FROM users WHERE id = ANY(:chunkIds) AND notification_channel != 'MUTE'`를 실행해 §1의 `users` 테이블에서 Mute 대상만 제외한다 (예전 `notification_preferences` 조회를 대체).

### 4.2 `notifications`

```sql
CREATE TABLE notifications (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    recipient_id    BIGINT NOT NULL,
    source_event_id UUID NOT NULL,      -- PostPublished.eventId, 멱등성 키
    post_id         BIGINT NOT NULL,
    author_id       BIGINT NOT NULL,
    title           VARCHAR(200) NOT NULL,
    is_read         BOOLEAN NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    read_at         TIMESTAMPTZ,

    CONSTRAINT uq_recipient_event UNIQUE (recipient_id, source_event_id)
);

-- FR-5.1: 알림 목록 조회 (최신순, 키셋 페이지네이션)
CREATE INDEX idx_notifications_recipient_feed
    ON notifications (recipient_id, id DESC);

-- FR-5.3: 미읽음 개수 조회
CREATE INDEX idx_notifications_unread
    ON notifications (recipient_id)
    WHERE is_read = false;
```

**인덱스 근거**
- `uq_recipient_event`: 도메인 불변식(같은 이벤트로 동일 수신자에게 중복 알림 불가, FR-2.5)을 DB가 강제. Fan-out Consumer는 `INSERT ... ON CONFLICT (recipient_id, source_event_id) DO NOTHING`으로 재처리 시에도 안전하게 멱등 삽입.
- `idx_notifications_recipient_feed`: `created_at` 대신 **단조 증가하는 `id`**를 정렬 기준으로 사용 — 동시 삽입 시 `created_at` 동률(tie) 문제를 피하고, 커서를 `id`(정수) 하나로 단순화.
- `idx_notifications_unread`: 부분 인덱스로 "읽지 않은 행"만 인덱싱. 대부분의 알림이 결국 읽음 처리되어 `is_read=true`로 바뀌는 시스템 특성상, 이 인덱스는 시간이 지나도 작게 유지됨 → `COUNT(*)` 쿼리가 테이블 크기와 무관하게 빠름.

**규모 관련 노트 (파티셔닝)**
- 인기 작가 1건 발행마다 10만 행이 쌓이는 구조라 이 테이블은 가장 빠르게 커진다. Postgres 선언적 파티셔닝(`PARTITION BY HASH (recipient_id)`, 예: 16개 파티션)을 적용하면 (a) Fan-out 벌크 insert가 여러 파티션에 분산되어 쓰기 경합이 줄고, (b) `recipient_id` 기준 조회(FR-5.1/5.3)가 단일 파티션 프루닝의 이점을 받는다.
- Postgres에서 파티션된 테이블은 **PK를 포함한 모든 유니크 제약이 파티션 키를 포함**해야 한다 — `uq_recipient_event(recipient_id, source_event_id)`는 이미 `recipient_id`를 포함해 조건을 충족하지만, 현재 PK(`id` 단독)는 포함하지 않으므로 파티셔닝을 적용하려면 PK를 `(id, recipient_id)` 복합키로 바꿔야 한다. 반대로 `created_at` 기준 Range 파티셔닝은 `uq_recipient_event`도, PK도 `created_at`을 포함하지 않아 채택하지 않음.
- 파티셔닝은 초기 구현 범위에서는 생략하고, 데이터 증가 후 도입하는 것으로 설계만 남겨둔다 (조기 최적화 방지) — 위 PK 변경도 그 시점에 함께 적용한다.

### 4.3 `notification_delivery_log`
`domain-design.md` §5.3의 `DeliveryAttempt` Entity가 매핑되는 물리 테이블이다 (`Notification` 1건당 채널별 최대 1행).

```sql
CREATE TABLE notification_delivery_log (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    notification_id  BIGINT NOT NULL,
    channel          VARCHAR(10) NOT NULL CHECK (channel IN ('PUSH', 'EMAIL')),
    status           VARCHAR(15) NOT NULL DEFAULT 'PENDING'
                     CHECK (status IN ('PENDING', 'SENT', 'FAILED', 'DEAD_LETTER')),
    attempt_count    INT NOT NULL DEFAULT 0,
    last_attempt_at  TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_notification_channel UNIQUE (notification_id, channel)
);

-- 발송 워커의 재시도 대상 폴링
CREATE INDEX idx_notification_delivery_retry_queue
    ON notification_delivery_log (channel, last_attempt_at)
    WHERE status IN ('PENDING', 'FAILED');
```

**인덱스 근거**
- `uq_notification_channel`: 채널당 발송 이력은 알림 1건에 1행만 존재 (재시도는 같은 행의 `attempt_count`/`status`를 갱신하는 것이지 새 행 추가가 아님).
- `idx_notification_delivery_retry_queue`: 발송기 워커가 `WHERE channel = ? AND status IN ('PENDING','FAILED') ORDER BY last_attempt_at`로 재시도 대상을 뽑는 쿼리를 지원. 부분 인덱스로 `SENT`/`DEAD_LETTER` 완료 건은 제외해 인덱스를 작게 유지.
- `notification_id`는 `notifications.id`를 값으로만 참조한다(이 프로젝트 전반의 FK 미사용 원칙 — `database-design.md` §0 — 을 같은 스키마 내 테이블 간에도 동일하게 적용). 별도 인덱스는 만들지 않음 — 이 컬럼으로 조회하는 패턴(알림 상세에서 발송 상태 보기)이 저빈도이고, `uq_notification_channel` 유니크 인덱스가 `(notification_id, channel)` 선두 컬럼으로서 이미 `notification_id` 단독 조회도 어느 정도 커버.

### 4.4 Fan-out 진행률 추적 — DB 테이블 대신 메트릭 (NFR-4.1)

팬아웃 진행률(총 대상자 수, 처리된 수, SLA 위반 여부)은 별도 DB 테이블(`fanout_jobs`)로 두지 않는다.

- 이유: 팬아웃 1건당 최대 100개 Chunk Worker가 "처리된 수" 하나를 공유해서 갱신해야 하므로, 테이블로 만들면 **쓰기 경합(hot row)**이 생긴다. NFR-1(5초 내 처리)을 지키려는 관측 장치가 오히려 그 SLA를 깎아먹는 역설이 생길 수 있음.
- 대신 Dispatcher/Chunk Worker가 각자 독립적으로 Prometheus 카운터·히스토그램을 emit하고(`fanout_chunks_dispatched_total`, `fanout_chunks_completed_total`, `fanout_chunk_duration_seconds`), `eventId`를 공통 라벨로 붙여 상관관계를 추적한다. "5초 넘게 안 끝난 팬아웃"은 Grafana 알림 룰로 잡는다.
- 이벤트 단위 멱등성(같은 `PostPublished` 재처리 방지)은 이 문서의 테이블이 아니라 Dispatcher의 `eventId` 처리 이력(Redis SETNX 등, `architecture.md` §4.3)이 담당하고, 수신자 단위 멱등성은 §4.2 `notifications.uq_recipient_event`가 담당한다.
- 자세한 내용은 `architecture.md` §4.4 참고.

---

## 5. 쿼리 패턴 ↔ 인덱스 매핑 요약

| 쿼리 패턴 | 관련 요구사항 | 테이블 | 인덱스 |
|---|---|---|---|
| 이메일 기준 사용자 조회(로그인) | - | users | uq_users_email |
| Outbox 미발행 이벤트 폴링 | NFR-2.2 | outbox_events | idx_outbox_pending (partial) |
| 작가별 구독자 벌크 조회(백필) | FR-1.2 | subscriptions | idx_subscriptions_author_active |
| Fan-out 청크 스캔 | FR-2.4 | subscriber_read_model | PK (author_id, user_id) |
| 수신 채널(Mute) 벌크 조회 | FR-3.3 | users | PK (`WHERE id = ANY(:chunkIds)`) |
| 멱등 알림 삽입 | FR-2.5 | notifications | uq_recipient_event |
| 알림 목록(최신순) | FR-5.1 | notifications | idx_notifications_recipient_feed |
| 미읽음 개수 | FR-5.3 | notifications | idx_notifications_unread (partial) |
| 발송 재시도 대상 폴링 | FR-4.3 | notification_delivery_log | idx_notification_delivery_retry_queue (partial) |
| Fan-out SLA 위반 모니터링 | NFR-1 | (메트릭 — DB 테이블 아님) | Prometheus 카운터/알림 룰, §4.4 참고 |

**공통 원칙**
1. **부분 인덱스(Partial Index) 적극 활용**: "처리 대기 중"인 소수 행만 인덱싱해, 완료된 대다수 행이 인덱스 크기/성능에 영향을 주지 않게 함 (Outbox, 미읽음, 재시도 큐 모두 동일 패턴).
2. **OFFSET 페이지네이션 배제**: 모든 목록 조회는 정수 PK 기반 키셋 페이지네이션(`WHERE id > :cursor`)으로 설계 — 10만 건 이상 스캔에서 `OFFSET`은 선형 이상의 비용이 든다.
3. **유니크 인덱스 = 멱등성 강제 수단**: 애플리케이션 레벨의 "중복 방지" 요구사항(FR-2.5, FR-1.1)을 DB 제약으로 이중 보장.
