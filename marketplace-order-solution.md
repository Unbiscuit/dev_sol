# Техническое решение: Оформление заказа на маркетплейсе

## 1. Введение

«Заказ товара на маркетплейсе» — это техническое решение для системы оформления заказов в B2C-маркетплейсе с многими селлерами. Проект сосредоточен на критическом пути от инициации чекаута до подтверждения оплаты заказа. Архитектура построена с учётом высоких нагрузок (целевая аудитория 30 млн MAU, до 150 RPS на запись в пиковых сценариях), требований к финансовой консистентности и устойчивости к сбоям.

Ключевые архитектурные принципы: микросервисная декомпозиция с изоляцией хранилищ, распределённая транзакция через паттерн Saga с явными компенсациями, идемпотентность всех изменяющих операций на нескольких уровнях, и Transactional Outbox для надёжной интеграции с Kafka.

---

## 2. Глоссарий

| Термин | Определение |
|---|---|
| Маркетплейс | Платформа, агрегирующая множество селлеров и предоставляющая покупателям единый интерфейс для покупки их товаров. |
| Покупатель (Customer) | Зарегистрированный пользователь, оформляющий заказы. |
| Селлер (Seller) | Внешний продавец, имеющий товары на маркетплейсе. В рамках scope — внешний субъект. |
| Заказ (Order) | Агрегатная сущность верхнего уровня, объединяющая покупки покупателя у разных селлеров в рамках одной транзакции оформления. |
| Подзаказ (Suborder) | Часть заказа, относящаяся к одному селлеру/складу. |
| Чекаут (Checkout) | Процесс оформления заказа от нажатия «Оформить» до перенаправления на платёжную страницу. |
| Inventory Reservation | Временная блокировка остатка товара на время от создания заказа до его оплаты. |
| Idempotency-Key | Уникальный идентификатор запроса от клиента, обеспечивающий безопасный повтор операций без дублирования. |
| Saga | Паттерн распределённой транзакции через последовательность локальных транзакций в разных сервисах с компенсирующими действиями при сбоях. |
| Transactional Outbox | Паттерн надёжной публикации событий: запись события в outbox-таблицу в рамках транзакции БД, асинхронная публикация в брокер отдельным процессом. |
| CDC (Change Data Capture) | Технология (Debezium) для отслеживания изменений в БД через WAL и публикации их в брокер. |
| At-least-once delivery | Гарантия, что сообщение будет доставлено как минимум один раз; возможны дубликаты, обрабатываемые на стороне consumer'а через идемпотентность. |
| Eventual consistency | Модель согласованности, при которой данные в разных узлах системы согласуются спустя некоторое время. |
| Shard | Логически независимый сегмент БД с собственной частью данных. Шардирование позволяет горизонтально масштабировать запись. |
| Hot Product | Товар с непропорционально высокой конкурентной нагрузкой (популярный товар в распродажу). Источник race conditions на остатки. |
| Lifecycle Worker | Фоновый сервис, обрабатывающий отложенные операции жизненного цикла заказа (таймауты оплаты, повторные попытки). |
| Webhook | HTTP-вызов от внешней системы (платёжного провайдера) для уведомления об асинхронном событии. |
| API Gateway | Входная точка системы, отвечающая за аутентификацию, маршрутизацию, rate limiting и приём webhook'ов. |
| P95 Latency | 95-й перцентиль времени отклика: 95% запросов обрабатываются быстрее этой границы. |
| RPS (Requests Per Second) | Количество запросов в секунду — метрика нагрузки. |
| MAU / DAU | Monthly / Daily Active Users — количество уникальных активных пользователей за месяц/день. |

---

## 3. Функциональные требования

**ФТ-1. Открытие чекаута.** Пользователь, имея корзину, может перейти к оформлению. Система проверяет актуальность товаров, рассчитывает стоимость доставки по селлерам, формирует список подзаказов, возвращает финальную сумму.

**ФТ-2. Создание заказа.** Пользователь подтверждает оформление. Система резервирует товары на складах атомарно, создаёт заказ и подзаказы, инициирует оплату у платёжного провайдера, возвращает пользователю URL для оплаты.

**ФТ-3. Идемпотентность чекаута.** Повторный запрос на создание заказа с тем же `Idempotency-Key` не создаёт дубликат, а возвращает результат первого запроса.

**ФТ-4. Обработка callback от платежа.** При получении webhook от платёжного провайдера система переводит заказ в статус `PAID`, инициирует фулфилмент.

**ФТ-5. Обработка таймаута оплаты.** Если за 15 минут оплата не пришла, заказ автоматически отменяется, резерв товаров снимается.

---

## 4. Нефункциональные требования

### 4.1. Масштаб системы

| Метрика | Значение |
|---|---|
| MAU | 30 млн |
| DAU | 5 млн |
| Заказов в день | 250 000 |
| Заказов в секунду в пике (распродажа) | 150 RPS |

### 4.2. Нагрузка по операциям

| Операция | Пиковый RPS | Тип |
|---|---|---|
| Создание заказа | 150 | Write |
| Webhook'и от платежей | 150 | Write |
| Попытки резервирования | 500 | Write |

### 4.3. Latency-цели

**Синхронные операции:**
- Открытие чекаута: P95 ≤ 500 мс
- Создание заказа: P95 ≤ 1000 мс

**Асинхронные операции:**
- Обработка callback платежа → обновление статуса: P95 ≤ 5 сек

### 4.4. Доступность

- Чекаут и создание заказа: **99.95%**.
- Интеграции со складами: **99%**.

### 4.5. Корректность и надёжность

- **Нулевая терпимость к потере оплаченного заказа.**
- **Невозможность oversell.**
- **Идемпотентность всех изменяющих операций** на нескольких уровнях.
- **Eventual consistency между подсистемами** допустима в пределах SLA.

---

## 5. Пользовательские сценарии

### Сценарий: Оформление заказа

1. Покупатель находится на странице корзины и нажимает «Оформить заказ».
2. Система открывает чекаут: показывает товары, сгруппированные по селлерам, актуальные цены, варианты доставки, итоговую сумму.
3. Покупатель выбирает адрес доставки и способ оплаты.
4. Покупатель нажимает «Перейти к оплате».
5. Система резервирует выбранные товары, создаёт заказ, инициирует оплату, перенаправляет на страницу оплаты.
6. Покупатель оплачивает заказ на стороне провайдера.
7. Система получает webhook от провайдера, переводит заказ в статус `PAID` и передаёт его на склады для фулфилмента.

**Альтернативные ветки:**
- 2a. Товар стал недоступен → предупреждение, предложение убрать.
- 2b. Цена товара изменилась → новая цена + подтверждение.
- 5a. Не удалось зарезервировать товар → ошибка, заказ не создаётся.
- 6a. Покупатель не оплатил в течение 15 минут → автоматическая отмена, снятие резервов.

---

## 6. Модель данных

В системе используется три независимых хранилища: основная БД OMS, БД Inventory Service, и небольшая БД Payment Adapter для idempotency-лога.

### 6.1. OMS (основная БД заказов)

```mermaid
erDiagram
    direction LR

    ORDER {
        uuid id PK "UUIDv7-like с зашитым shard_id"
        uuid customer_id
        varchar idempotency_key "часть UNIQUE с customer_id"
        varchar status "CREATED, PENDING_PAYMENT, PAID, CANCELLED, FAILED, PAID_LATE"
        decimal total_amount
        jsonb delivery_address "embedded snapshot"
        timestamp created_at "partition key"
        timestamp updated_at
        timestamp payment_deadline
    }

    SUBORDER {
        uuid id PK
        uuid order_id FK
        uuid seller_id
        uuid warehouse_id
        varchar status "CREATED, PAID, CANCELLED"
        decimal items_amount
        decimal delivery_amount
        timestamp created_at
        timestamp updated_at
    }

    ORDER_ITEM {
        uuid id PK
        uuid suborder_id FK
        varchar sku
        varchar product_name "snapshot"
        decimal price_at_order "immutable"
        int quantity
        uuid reservation_id "ссылка на резерв в Inventory"
    }

    PAYMENT {
        uuid id PK
        uuid order_id FK
        varchar provider_payment_id
        varchar status "INITIATED, SUCCEEDED, FAILED"
        decimal amount
        varchar provider_callback_idempotency_key "UNIQUE"
        timestamp created_at
        timestamp updated_at
    }

    ORDER_STATUS_HISTORY {
        uuid id PK
        uuid order_id FK
        varchar from_status
        varchar to_status
        varchar reason
        timestamp changed_at
    }

    OUTBOX_EVENT {
        uuid id PK
        varchar event_type
        jsonb payload
        timestamp created_at
        timestamp published_at "nullable"
    }

    ORDER ||--o{ SUBORDER : "contains"
    SUBORDER ||--|{ ORDER_ITEM : "consists_of"
    ORDER ||--|| PAYMENT : "has_one"
    ORDER ||--o{ ORDER_STATUS_HISTORY : "has_audit"
```

**Ключевые решения:**
- **`ORDER.id` как UUIDv7-like** с зашитым `shard_id` — приложение извлекает шард прямо из идентификатора, без отдельного lookup'а.
- **Идемпотентность через unique constraint** `(customer_id, idempotency_key)`. Привязка к пользователю исключает кросс-юзерные коллизии UUID.
- **`delivery_address` как embedded JSONB** — иммутабельный snapshot на момент заказа.
- **`price_at_order` snapshot** — цена в каталоге может меняться, в заказе остаётся согласованная.
- **`reservation_id` без FK** — cross-service ссылка на резерв в БД Inventory.
- **`provider_callback_idempotency_key`** — защита от повторной обработки webhook'ов.
- **`OUTBOX_EVENT`** — Transactional Outbox: записи добавляются в той же транзакции, что и бизнес-данные. Debezium читает Postgres WAL и публикует в Kafka.

**Шардирование:** по `customer_id` (зашитый в UUID). Все заказы одного покупателя — на одном шарде.
**Партиционирование:** по `created_at` (по месяцам).

### 6.2. Inventory Service

```mermaid
erDiagram
    INVENTORY_STOCK {
        varchar sku "PK part 1"
        uuid warehouse_id "PK part 2"
        int total_quantity
        int reserved_quantity
        timestamp updated_at
    }

    INVENTORY_RESERVATION {
        uuid id PK
        varchar sku
        uuid warehouse_id
        uuid suborder_id
        int quantity
        varchar status "ACTIVE, CONFIRMED, RELEASED"
        timestamp created_at
        timestamp expires_at
    }
```

**Ключевые решения:**
- Композитный PK `(sku, warehouse_id)` в `INVENTORY_STOCK`. Шардирование по этому же ключу.
- БД хранит источник правды; Redis — горячий путь для атомарного резервирования.

**Структура в Redis:**
```
stock:{sku}:{warehouse_id} → int (доступно к резервированию)
reservation:{reservation_id} → hash, TTL 900 сек
```

**Жизненный цикл резерва:** `ACTIVE → CONFIRMED` (после оплаты) или `ACTIVE → RELEASED` (при отмене/таймауте).

**Сверка Redis ↔ БД:** фоновый процесс Inventory Reconciler раз в 5 минут считает в БД сумму активных резервов и сверяет с Redis-счётчиком. При расхождении переписывает Redis из БД.

### 6.3. Payment Adapter (служебная БД)

Маленькая БД для idempotency webhook'ов и Outbox. Не хранит сами `PAYMENT` (они в OMS).

```mermaid
erDiagram
    WEBHOOK_EVENT {
        varchar event_id PK "уникальный ID от провайдера"
        varchar provider_payment_id
        jsonb raw_payload
        timestamp received_at
        varchar status
    }

    PA_OUTBOX_EVENT {
        uuid id PK
        varchar event_type "PaymentReceived"
        jsonb payload
        timestamp created_at
        timestamp published_at
    }
```

При получении webhook: `INSERT INTO webhook_events ON CONFLICT DO NOTHING` — атомарная дедупликация. Если новый — INSERT в outbox, Debezium публикует в Kafka.

---

## 7. Архитектура

### 7.1. Компоненты системы

**1. API Gateway** — входная точка. Аутентификация JWT, маршрутизация, rate limiting, TLS termination, приём webhook'ов.

**2. Checkout Service** — синхронный сервис чекаута. Открывает чекаут (read-heavy с параллельными вызовами) и оркестрирует Saga при создании заказа.

**3. Order Management Service (OMS)** — хозяин жизненного цикла заказов в части оформления. Хранит заказы, обрабатывает событие оплаты, публикует бизнес-события через Outbox.

**4. Inventory Service** — управление резервированием товаров. gRPC API: `Reserve`, `Release`, `Confirm`. Redis для конкурентных атомарных операций + PostgreSQL для аудита.

**5. Payment Adapter** — обёртка над платёжным провайдером. Инициация платежей, приём и нормализация webhook'ов, публикация в Kafka через Outbox.

**6. Lifecycle Worker** — фоновый сервис для отложенных операций (отмена неоплаченных заказов). Развёртывается как пул реплик с использованием `FOR UPDATE SKIP LOCKED`.

**Инфраструктура:**
- PostgreSQL OMS Cluster (sharded by customer_id) — 8 шардов × (master + 1 replica для отказоустойчивости).
- PostgreSQL Inventory Cluster (sharded by sku/warehouse).
- PostgreSQL Payment Adapter DB.
- Redis Cluster для Inventory.
- Apache Kafka.
- Debezium для CDC.

### 7.2. Архитектурная диаграмма

```mermaid
graph TB
    subgraph Clients
        Mobile[Mobile App]
        Web[Web App]
    end

    subgraph External["Внешние системы"]
        Catalog[Catalog Service]
        Cart[Cart Service]
        Auth[Auth Service]
        Provider[Payment Provider]
        Warehouse[Warehouse / Fulfillment]
        Delivery[Delivery Service]
        Notify[Notification Service]
    end

    subgraph Edge
        Gateway[API Gateway]
    end

    subgraph CoreServices["Core Services"]
        Checkout[Checkout Service]
        OMS[Order Management]
        Inventory[Inventory Service]
        PayAdapter[Payment Adapter]
        Worker[Lifecycle Worker]
    end

    subgraph Storage
        OMS_DB[(PostgreSQL OMS)]
        Inv_DB[(PostgreSQL Inventory)]
        Inv_Redis[(Redis Inventory hot path)]
        PA_DB[(PostgreSQL PayAdapter)]
    end

    subgraph Bus
        Kafka[Apache Kafka]
        Debezium[Debezium CDC]
    end

    Mobile --> Gateway
    Web --> Gateway
    Gateway -.-> Auth
    Gateway --> Checkout
    Gateway --> PayAdapter

    Checkout --> Catalog
    Checkout --> Cart
    Checkout -- gRPC --> Inventory
    Checkout -- gRPC --> OMS
    Checkout -- gRPC --> PayAdapter

    OMS --> OMS_DB
    Inventory --> Inv_DB
    Inventory --> Inv_Redis
    PayAdapter --> PA_DB

    PayAdapter --> Provider
    Provider -- webhook --> Gateway

    OMS_DB --> Debezium
    PA_DB --> Debezium
    Debezium --> Kafka

    Kafka --> OMS
    Kafka --> Notify
    Kafka --> Warehouse

    Worker --> OMS_DB
```

### 7.3. Обоснование разделения на сервисы

| Сервис | Обоснование |
|---|---|
| Checkout vs OMS | Разный профиль: Checkout — синхронный оркестратор Saga, OMS — write-heavy event-driven хозяин жизненного цикла. |
| Inventory vs OMS | Своё хранилище (Redis), специфичная нагрузка, шардирование по другому ключу. |
| Payment Adapter vs OMS | Изоляция внешней зависимости. При смене провайдера — меняется только адаптер. |
| Lifecycle Worker | Фоновые задачи изолированы от user-facing OMS. |

### 7.4. Sync vs Async и транспорт

**Синхронные пути (юзер ждёт):** Mobile/Web → Gateway → Checkout → Inventory + OMS + Payment Adapter.

**Асинхронные пути (через Kafka):** Payment Adapter → Outbox → Kafka → OMS (обработка PaymentReceived); OMS → Outbox → Kafka → внешние подписчики (Warehouse, Notification); Worker → OMS_DB → Outbox → Kafka.

**Принцип:** критический путь «оформить → оплатить» — синхронно. После оплаты — async.

**Транспорт:** REST/JSON наружу, gRPC между сервисами, Kafka для асинхронных событий.

### 7.5. Гарантии доставки и идемпотентность

**Transactional Outbox** обеспечивает at-least-once delivery событий из OMS и Payment Adapter в Kafka. Запись события в outbox происходит в той же БД-транзакции, что и бизнес-операция. Debezium асинхронно читает WAL и публикует в Kafka.

**Идемпотентность многослойная:**
1. **Клиент → Checkout**: `Idempotency-Key` + UNIQUE в `orders.idempotency_key`.
2. **Payment Adapter → Provider**: свой UUID, отправляемый провайдеру.
3. **Provider → нас**: `event_id` + UNIQUE в `webhook_events`.
4. **Kafka consumers**: idempotent processing через статусные проверки.

---

## 8. Технические сценарии

### 8.1. Открытие чекаута

Read-only сценарий построения preview без создания заказа. 5 параллельных вызовов внешних сервисов (Cart, User, Catalog, Inventory, Delivery) обязательны для P95 ≤ 500 мс.

**Алгоритм:**
1. Параллельно: получить корзину из Cart Service и адреса из User Service.
2. Параллельно: получить актуальные данные товаров (цена, селлер, склад) из Catalog и проверить доступность из Inventory (`GET stock:{sku}:{warehouse}` в Redis, без блокировок).
3. Сгруппировать items по селлерам/складам.
4. Параллельно для каждой группы: рассчитать стоимость доставки в Delivery Service.
5. Собрать preview, сравнить цены с теми, что были в корзине, добавить warnings о расхождениях.
6. Вернуть клиенту.

**Highload-аспекты:**
- Параллелизация снижает latency с суммы (~250 мс) до максимума (~50 мс).
- Read-only характер позволяет неограниченно масштабировать Checkout Service горизонтально.
- Graceful degradation: при недоступности Delivery — placeholder; при недоступности Catalog/Cart — fail-fast.

### 8.2. Создание заказа

Saga из 3 шагов с компенсациями: Reserve → CreateOrder → InitiatePayment.

**Sequence-диаграмма:**

```mermaid
sequenceDiagram
    participant Client
    participant Gateway as API Gateway
    participant Checkout
    participant OMS
    participant OMSDB as OMS DB
    participant Inventory
    participant InvRedis as Redis
    participant PayAdapter as Payment Adapter
    participant Provider as Payment Provider

    Client->>Gateway: POST /checkout {items, address, Idempotency-Key}
    Gateway->>Checkout: gRPC CreateOrder

    Checkout->>OMS: CheckIdempotency(customer_id, key)
    OMS->>OMSDB: SELECT orders WHERE customer_id=? AND idempotency_key=?
    OMSDB-->>OMS: not found

    Checkout->>Inventory: Reserve([items])
    Inventory->>InvRedis: EVAL Lua: if stock>=qty then DECRBY (атомарно)
    InvRedis-->>Inventory: ok
    Inventory-->>Checkout: [reservation_ids]

    Checkout->>OMS: CreateOrder(items, reservations, key)
    OMS->>OMSDB: BEGIN
    OMS->>OMSDB: INSERT orders (CREATED, deadline=now()+15m)
    OMS->>OMSDB: INSERT suborders, order_items, outbox_events (OrderCreated)
    OMS->>OMSDB: COMMIT
    OMS-->>Checkout: order_id

    Checkout->>PayAdapter: InitiatePayment(order_id, amount)
    PayAdapter->>Provider: HTTP POST /payments
    Provider-->>PayAdapter: payment_id, payment_url
    PayAdapter-->>Checkout: payment_url, provider_payment_id

    Checkout->>OMS: RecordPaymentInitiated(...)
    OMS->>OMSDB: INSERT payments (INITIATED), UPDATE orders SET status='PENDING_PAYMENT'

    Checkout-->>Client: 200 OK {order_id, payment_url}
```

**Обработка ошибок:**

- **Дубль idempotency-ключа:** `CheckIdempotency` возвращает существующий заказ → клиенту возвращается прежний `payment_url` без побочных действий.
- **OUT_OF_STOCK:** Inventory при провале Lua-скрипта откатывает (`INCRBY`) уже сделанные DECRBY и возвращает ошибку. Checkout отвечает клиенту 409 Conflict, заказа нет.
- **OMS падает после Reserve:** Checkout вызывает `Inventory.Release(reservation_ids)` как компенсацию. Возвращает 500.
- **Provider не ответил (timeout):** Checkout переводит заказ в `PAYMENT_INITIATION_FAILED`, **не отпускает резерв** (риск oversell, если запрос всё-таки прошёл к провайдеру). Lifecycle Worker через 15+2 минут гарантированно разберётся.
- **Provider ответил отказом:** Checkout вызывает `Inventory.Release`, переводит заказ в `FAILED`, возвращает клиенту 400.

**Highload-аспекты:**
- **Конкуренция за hot products** решается атомарным Lua-скриптом в Redis: операции по одному ключу сериализуются на уровне Redis-shard за микросекунды без блокировок в БД.
- **Многослойная идемпотентность**: `Idempotency-Key` от клиента + собственный idempotent key Payment Adapter → Provider.
- **Saga с явными компенсациями.**
- **Outbox pattern**: `outbox_events` пишется в той же транзакции, что и `orders`.
- **Bottleneck-анализ**: внешний провайдер (~500 мс) — главный bottleneck латентности; БД OMS на одном шарде имеет запас 10x; Redis Inventory имеет запас 100x.

### 8.3. Обработка callback оплаты

Webhook от провайдера → дедупликация → Outbox → Kafka → OMS consumer → fan-out событий.

**Sequence-диаграмма:**

```mermaid
sequenceDiagram
    participant Provider as Payment Provider
    participant Gateway as API Gateway
    participant PayAdapter as Payment Adapter
    participant PADb as PayAdapter DB
    participant Kafka
    participant OMS
    participant OMSDB as OMS DB
    participant Inventory
    participant Warehouse as External Warehouse

    Provider->>Gateway: POST /webhooks/payment {event_id, status}
    Gateway->>Gateway: Verify HMAC
    Gateway->>PayAdapter: HandleWebhook

    PayAdapter->>PADb: INSERT webhook_events ON CONFLICT DO NOTHING
    PayAdapter->>PADb: INSERT outbox_events (PaymentReceived)
    PayAdapter-->>Gateway: 200 OK
    Gateway-->>Provider: 200 OK

    Note over PADb, Kafka: Debezium → Kafka
    PADb-->>Kafka: PaymentReceived

    Kafka->>OMS: consume
    OMS->>OMSDB: BEGIN
    OMS->>OMSDB: UPDATE orders SET status='PAID' WHERE status IN ('CREATED','PENDING_PAYMENT')
    OMS->>OMSDB: UPDATE payments SET status='SUCCEEDED'
    OMS->>OMSDB: INSERT outbox (OrderPaid, ConfirmReservations, DispatchToWarehouses)
    OMS->>OMSDB: COMMIT
    OMS-->>Kafka: ACK

    Note over OMSDB, Kafka: Debezium публикует outbox → Kafka

    par Параллельная обработка
        Kafka->>Inventory: ConfirmReservations
        Inventory->>Inventory: status ACTIVE → CONFIRMED в БД, обновление stock
    and
        Kafka->>Warehouse: DispatchToWarehouses
        Note over Warehouse: внешний consumer, начинает фулфилмент
    end
```

**Обработка ошибок и дублей:**

- **Дубль webhook от провайдера**: `INSERT webhook_events ... ON CONFLICT DO NOTHING` возвращает 0 rows → отвечаем 200 OK без повторной публикации в Kafka.
- **Двойная обработка Kafka consumer'ом**: `SELECT payment FOR UPDATE`, проверяем статус. Если уже SUCCEEDED — ACK без действий.
- **Race с Lifecycle Worker** (worker уже отменил заказ): подробно в разделе 8.5 «Особые случаи».

**Highload-аспекты:**
- **Idempotency на двух уровнях**: `event_id` в `webhook_events` (от провайдера) и `provider_payment_id` в `payments` (для Kafka retry).
- **At-least-once delivery + idempotent processing = effective exactly-once.**
- **Async fan-out через Kafka**: одно событие OrderPaid → подписчики (Inventory confirm, внешний Warehouse, Notification), развязанные во времени.
- **Transactional Outbox в Payment Adapter**: webhook не теряется даже при сбое адаптера между ответом провайдеру и публикацией.

### 8.4. Таймаут оплаты (Lifecycle Worker)

Polling по шардам с конкурентным доступом через `FOR UPDATE SKIP LOCKED`. Buffer 2 минуты для исключения race с поздним webhook. Release резервов через Kafka.

**Sequence-диаграмма:**

```mermaid
sequenceDiagram
    participant Worker as Lifecycle Worker
    participant OMSDB as OMS DB (shard)
    participant Kafka
    participant Inventory
    participant InvDB as Inventory DB
    participant InvRedis as Redis

    Worker->>OMSDB: SELECT WHERE deadline < now()-2m AND status='PENDING_PAYMENT' FOR UPDATE SKIP LOCKED LIMIT 100
    OMSDB-->>Worker: [order_ids batch]

    loop для каждого order
        Worker->>OMSDB: BEGIN
        Worker->>OMSDB: UPDATE orders SET status='CANCELLED' WHERE id=? AND status='PENDING_PAYMENT'
        Worker->>OMSDB: UPDATE suborders SET status='CANCELLED'
        Worker->>OMSDB: INSERT outbox_events (OrderCancelled)
        Worker->>OMSDB: COMMIT
    end

    Note over OMSDB, Kafka: Debezium → Kafka
    Kafka->>Inventory: OrderCancelled
    loop для каждого reservation_id
        Inventory->>InvDB: SELECT FOR UPDATE и UPDATE status в RELEASED если ACTIVE
        Inventory->>InvRedis: INCRBY stock на quantity
    end
```

**Обработка race condition:** при `UPDATE orders` дополнительно `WHERE status='PENDING_PAYMENT'`. Если вернулось 0 строк — webhook опередил Worker'а, пропускаем (заказ уже в `PAID`).

**Highload-аспекты:**
- **`FOR UPDATE SKIP LOCKED`** — нативный паттерн PostgreSQL для конкурентной обработки заданий несколькими worker-инстансами без distributed lock.
- **Batch size 100** — компромисс между overhead итераций и длительностью блокировок.
- **Развёртывание как пул реплик** — любой worker берёт любой шард, естественная балансировка.
- **Release через Kafka, а не gRPC**: Worker отвечает только за надёжную запись в OMS_DB + outbox; Inventory подхватывает асинхронно. Гарантия at-least-once.
- **Идемпотентность Inventory consumer**: проверка `status='ACTIVE'` перед UPDATE.
- **Bottleneck-анализ**: 8 шардов × 30 batch'ей/мин × 100 заказов = 24000/мин = 400/сек. Запас 10x от пиковых отмен.

### 8.5. Особые случаи

#### Случай 1: Race между Lifecycle Worker и webhook оплаты

**Ситуация:** пользователь оплатил на 14:59 минуте. Worker мог успеть отменить заказ до того, как webhook от провайдера дошёл.

**Решение:**
1. Buffer 2 минуты в Worker'е: `payment_deadline < now() - interval '2 minutes'`.
2. Условный UPDATE при обработке webhook: `WHERE status IN ('CREATED', 'PENDING_PAYMENT')`.
3. Ветка `PAID_LATE`: при 0 rows обновлено — заказ переводится в `PAID_LATE`, инициируется автоматический возврат денег у провайдера.

#### Случай 2: Двойная обработка webhook'а Kafka consumer'ом

**Ситуация:** OMS-консьюмер обработал событие, успел COMMIT, но не успел ACK в Kafka (упал, рестарт). Kafka переотправит то же событие.

**Решение:**
- В `payments` unique constraint на `provider_callback_idempotency_key`.
- Перед обновлением OMS делает `SELECT ... FOR UPDATE`. Если уже SUCCEEDED — ACK без действий.
- Аналогично для Inventory (`status='ACTIVE'`).

**Гарантия:** at-least-once delivery + idempotent processing = effective exactly-once.

---

## 9. Архитектурные компромиссы

1. **Saga вместо 2PC.** Распределённая транзакция «зарезервировать товар → создать заказ → инициировать оплату» реализована через сагу с явными компенсациями. 2PC неприменим, так как внешний платёжный провайдер не поддерживает участие в распределённых транзакциях.
2. **Hybrid storage в Inventory (Redis + PostgreSQL).** Redis обеспечивает атомарные операции для конкурентного доступа (десятки тысяч RPS на ключ для популярных товаров), PostgreSQL — source of truth для аудита и восстановления. Согласованность поддерживается фоновым Reconciler'ом.
3. **Шардирование с момента запуска.** При текущей нагрузке 150 RPS на запись одна нода справилась бы, однако шардирование обеспечивает запас под рост в 5–10 раз и упрощает горизонтальное масштабирование.
4. **Outbox + Debezium вместо прямой публикации в Kafka.** Гарантирует at-least-once delivery финансовых событий: при недоступности Kafka событие сохраняется в БД и публикуется при восстановлении.
5. **Изоляция платёжного провайдера через Payment Adapter.** Замена провайдера (например, ЮKassa → Stripe) затрагивает только адаптер; OMS и остальные сервисы не модифицируются.
6. **Идемпотентность на нескольких уровнях.** Реализована на каждой сетевой границе (клиент↔Checkout, Adapter↔Provider, Provider↔Webhook, Kafka consumer'ы), что даёт effective exactly-once семантику поверх at-least-once delivery.
