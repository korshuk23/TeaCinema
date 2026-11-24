Я причесал твою техническую выжимку под стандартный красивый GitHub README — с авторендерящейся схемой Mermaid, чёткой структурой и таблицей статусов.
Важно: перед коммитом обязательно сверь блок Quick Start (названия команд сборки, наличие ./gradlew, пути к конфигам и порты Grafana/Prometheus/Keycloak) со своим реальным docker-compose.yml.
Markdown
# 🎟️ TeaCinema — Distributed Cinema Ticket Booking Platform

Распределённая система бронирования и покупки билетов в кино в реальном времени, построенная на микросервисной архитектуре с упором на защиту от овербукинга, консистентность данных, безопасность периметра и сквозную наблюдаемость (Observability).

---

## 🏗️ Архитектура

Система построена по принципу **Database-per-Service** с гибридным межсервисным взаимодействием (синхронный gRPC + асинхронный Event-Driven через Apache Kafka).

```mermaid
flowchart LR
    Client(["Frontend / Client"]) -->|HTTP / SSE| GW["Gateway Service :8088"]
    GW -.->|OIDC / JWKS| KC["Keycloak :8080"]

    GW --> Auth["Auth Service :8081"]
    GW --> Booking["Booking Service :8084"]
    GW --> Movie["Movie Service :8083"]

    Booking -->|gRPC + Circuit Breaker| Movie

    Booking <-->|Events / Saga| Kafka(("Apache Kafka :9092"))
    Movie <-->|Events / Saga| Kafka
    Payment["Payment Service :8085"] <-->|Events / Saga| Kafka

    Booking -.->|SSE Pub/Sub| Redis[("Redis 7")]
    Auth -.->|Rate Limit / OTP| Redis
Сервисы и модули
gateway-service (:8088) — единая точка входа (Spring Cloud Gateway WebFlux). Валидирует JWT (iss, azp), выполняет санитарию входящих заголовков и маршрутизирует запросы во внутреннюю сеть.
discovery-server — Service Discovery на базе Netflix Eureka.
auth-service (:8081) — беспарольная аутентификация по OTP, защита от брутфорса/флуда через Redis, синхронизация пользователей с Keycloak (HMAC-SHA256).
booking-service (:8084) — управление заказами, инициация бронирования, real-time стриминг статусов клиенту через SSE и Redis Pub/Sub.
movie-service (:8083) — каталог фильмов, расписание сеансов, управление инвентарём мест и предотвращение овербукинга.
payment-service (:8085) — обработка оплат, автоматические возвраты (refunds), фоновая сверка зависших транзакций.
core — общие Protobuf-контракты (gRPC) и DTO событий для Kafka.
🛠️ Стек технологий
Core: Java 21 (Eclipse Temurin), Gradle 9.x (Multi-module)
Frameworks: Spring Boot 3.4.1, Spring Cloud 2024.0.0 (Gateway, Eureka), Spring Security 6.4
Data & Cache: PostgreSQL 15 (изолированные БД на каждый сервис), Liquibase, Redis 7 (Lettuce, Pub/Sub), Caffeine Cache
Messaging & RPC: Apache Kafka 3.7.0 (KRaft, DLQ/DLT с ExponentialBackOff), gRPC 1.80.0 / Protobuf 3.25.1, Resilience4j
Security: Keycloak 24.0.2 (OIDC / OAuth2 Resource Server), Nimbus Reactive JWT Decoder
Observability: Prometheus, Grafana, Grafana Tempo (OpenTelemetry / OTLP), Grafana Loki + Promtail, Micrometer Tracing
Testing: Testcontainers (PostgreSQL, Redis, Kafka), JUnit 5, AssertJ, Mockito
⚙️ Ключевые инженерные решения
Конкурентность без внешних распределённых локов: защита от овербукинга реализована через атомарные row-level операции в PostgreSQL (UPDATE seats SET status='RESERVED', expires_at=:exp, version=version+1 WHERE id=:id AND status='FREE'), без накладных расходов на внешний Redisson.
Transactional Outbox (FOR UPDATE SKIP LOCKED): бизнес-состояние и событие для Kafka сохраняются в одной ACID-транзакции, что исключает проблему Dual-Write и позволяет безопасно масштабировать воркеры-издатели на несколько инстансов.
Choreography Saga & Orphaned Payments: если подтверждение оплаты от банка приходит после истечения таймаута брони (15 мин), movie-service фиксирует потерю места и публикует ORPHANED_PAYMENT, после чего payment-service автоматически инициирует возврат (REFUND).
Transaction Sandwich в биллинге: сетевой вызов внешнего платёжного шлюза вынесен за пределы транзакции БД (Tx1: PENDING -> HTTP-вызов PSP -> Tx2: SUCCESS/FAILED), чтобы не удерживать коннекты из пула HikariCP при задержках банка.
Идемпотентность: детерминированные ключи (Idempotency-Key) при обращении к платёжному шлюзу и защита консьюмеров от дубликатов событий.
Кластерный SSE: решение проблемы Lost Wake-up (регистрация слушателя в CopyOnWriteArraySet до чтения из БД), маршрутизация событий между подами через Redis Pub/Sub и отправка строго после коммита (TransactionSynchronizationManager.afterCommit).
Безопасность периметра: внутренние микросервисы в docker-compose.yml используют expose вместо публикации портов наружу; каждый сервис самостоятельно валидирует подпись JWT. В auth-service реализован атомарный Cooldown (SETNX) и защита от подбора OTP.
📊 Статус реализации
Компонент / Фича	Статус	Детали
Ядро бронирования (State Machine)	🟢 Готово	Исключение овербукинга, атомарный захват и освобождение мест
Transactional Outbox & Kafka	🟢 Готово	Гарантия At-Least-Once, дедупликация, DLQ, партиционирование по seatId
Безопасность (Auth & Gateway)	🟢 Готово	Keycloak OIDC, валидация iss/azp, Rate Limiting в Redis
Real-time уведомления (SSE)	🟢 Готово	Мультидевайс (CopyOnWriteArraySet), Redis Pub/Sub
Фоновая сверка (Reconciliation)	🟢 Готово	Опрос зависших PENDING-платежей без блокировки пула БД
Observability (LGTM Stack)	🟢 Готово	Метрики (Prometheus), логи (Loki), распределённые трейсы (Tempo)
Платёжный шлюз (PSP)	🟡 Заглушка	Mock-адаптер, эмулирующий charge, refund и checkStatus
Транспорт OTP (SMS/Email)	🟡 Заглушка	Код подтверждения выводится в лог (// TODO: SMS Provider API)
Уведомления о REFUND_FAILED	🟡 Заглушка	Перевод в статус ошибки + логирование уровня CRITICAL
🚀 Быстрый запуск (Local Quick Start)
(Проверь точные команды и порты в своём docker-compose.yml перед сохранением)
1. Сборка проекта
Bash
git clone https://github.com/korshuk23/booking-app.git
cd booking-app
./gradlew clean build -x test
2. Запуск инфраструктуры и сервисов
Bash
docker compose up -d --build
3. Доступные эндпоинты
API Gateway: http://localhost:8088
Keycloak Admin Console: http://localhost:8080
Grafana (Metrics / Logs / Traces): http://localhost:3000
Prometheus: http://localhost:9090
Eureka Dashboard: http://localhost:8761
4. Запуск тестов (требуется запущенный Docker для Testcontainers)
Bash
./gradlew test
Code
---

Если хочешь, чтобы я **точно** подогнал раздел **Quick Start** и проверил, нет ли лишнего мусора/паролей в конфигах: скинь сюда свой `docker-compose.yml` и вывод структуры корня (или скрин списка файлов с GitHub) — проверю за минуту.
