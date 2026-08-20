package com.blog.notification.user

/**
 * 세 Context가 공통으로 참조하는 User(공유 참조 테이블, `public.users`) 모듈 (docs/domain-design.md §6,
 * docs/database-design.md §1). 특정 Bounded Context가 소유하지 않으며, 다른 Context는 이 모듈이 노출하는
 * 값(id, notificationChannel)만 참조한다.
 */
internal object UserModule
