-- Bounded Context별 논리 분리 (docs/database-design.md §0)
CREATE SCHEMA IF NOT EXISTS post;
CREATE SCHEMA IF NOT EXISTS subscription;
CREATE SCHEMA IF NOT EXISTS notification;
-- users는 어느 Context에도 속하지 않는 공유 참조 테이블이므로 public 스키마에 둔다 (§1)
