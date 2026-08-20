package com.blog.notification.post

/**
 * Post Bounded Context — 글 작성/발행, PostPublished 이벤트 발행 (docs/domain-design.md §3).
 * 다른 Context의 패키지를 참조하지 않는다 — Context 간 통신은 이벤트로만 한다.
 */
internal object PostContext
