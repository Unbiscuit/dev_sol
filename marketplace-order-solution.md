# Техническое решение: Оформление заказа на маркетплейсе

## 1. Введение

Техническое решение описывает highload-систему оформления заказа в B2C-маркетплейсе. Система поддерживает работу с корзиной (добавление, удаление, изменение количества товаров), оформление заказа на основе корзины с атомарной фиксацией остатков, и просмотр истории заказов пользователем.

Целевая нагрузка: до 2 000 RPS на создание заказов и 10 000 RPS на чтение корзин и заказов. Ключевые архитектурные принципы: микросервисная декомпозиция с изоляцией хранилищ, CQRS-разделение записи и чтения через event-driven обновление read-model, распределённая транзакция оформления через паттерн Saga с явными компенсациями, идемпотентность операции оформления, и Transactional Outbox для надёжной интеграции с Kafka.

---

## 2. Глоссарий

| Термин | Определение |
|---|---|
| Маркетплейс | Платформа, агрегирующая множество селлеров и предоставляющая покупателям единый интерфейс для покупки товаров. |
| Покупатель (Customer) | Зарегистрированный пользователь, оформляющий заказы. |
| Селлер (Seller) | Внешний продавец, имеющий товары на маркетплейсе. |
| Корзина (Cart) | Набор товаров, выбранных пользователем для оформления. |
| Позиция корзины (Cart Item) | Отдельный товар и его количество в составе корзины. |
| Заказ (Order) | Подтверждённая заявка пользователя на покупку товаров. |
| Подзаказ (Suborder) | Часть заказа, относящаяся к одному селлеру/складу. |
| Позиция заказа (Order Item) | Отдельный товар и его количество в составе заказа. |
| Остаток (Stock) | Доступное для продажи количество товара на складе. |
| Резервирование (Reservation) | Временная фиксация части остатка под конкретный заказ. |
| Чекаут (Checkout) | Процесс подтверждения и оформления заказа. |
| Idempotency-Key | Уникальный идентификатор запроса от клиента, обеспечивающий безопасный повтор операций без дублирования. |
| Saga | Паттерн распределённой транзакции через последовательность локальных транзакций в разных сервисах с компенсирующими действиями при сбоях. |
| Transactional Outbox | Паттерн надёжной публикации событий: запись события в outbox-таблицу в рамках транзакции БД, асинхронная публикация в брокер отдельным процессом. |
| CQRS | Command Query Responsibility Segregation — разделение записи и чтения с независимыми моделями данных. |
| Read-Model | Денормализованная проекция данных, оптимизированная для чтения. Обновляется асинхронно через события. |
| CDC | Change Data Capture — технология (Debezium) для отслеживания изменений в БД через WAL и публикации их в брокер. |
| At-least-once delivery | Гарантия, что сообщение будет доставлено как минимум один раз; возможны дубликаты, обрабатываемые на стороне consumer'а через идемпотентность. |
| Eventual consistency | Модель согласованности, при которой данные в разных узлах системы согласуются спустя некоторое время. |
| Shard | Логически независимый сегмент БД с собственной частью данных. |
| Hot Product | Товар с непропорционально высокой конкурентной нагрузкой (популярный товар в распродажу). |
| Lifecycle Worker | Фоновый сервис, обрабатывающий отложенные операции жизненного цикла заказа. |
| API Gateway | Входная точка системы: аутентификация, маршрутизация, rate limiting. |
| P95 Latency | 95-й перцентиль времени отклика. |
| RPS | Requests Per Second — метрика нагрузки. |
| Статус заказа | Текущее состояние заказа: `created`, `confirmed` или `cancelled`. |

---

## 3. Функциональные требования

**ФТ-1. Работа с корзиной.** Пользователь может добавлять товары в корзину, удалять их, изменять количество, просматривать актуальный состав корзины. Каждая позиция содержит `product_id`, название, цену, количество.

**ФТ-2. Просмотр итоговой информации.** Система отображает список товаров в корзине, стоимость каждой позиции, итоговую сумму заказа.

**ФТ-3. Оформление заказа.** Пользователь подтверждает оформление, система проверяет доступность товаров, резервирует остатки атомарно, создаёт заказ в статусе `created`, подтверждает резерв и переводит заказ в `confirmed`. Возвращает пользователю результат операции.

**ФТ-4. Идемпотентность оформления.** Повторный запрос на оформление с тем же `Idempotency-Key` не создаёт дубликат, а возвращает результат первого запроса.

**ФТ-5. Работа с остатками.** Система учитывает доступный остаток товара, не допускает оформления заказа на количество, превышающее остаток, и предотвращает одновременное подтверждение одного остатка в нескольких заказах сверх доступного количества.

**ФТ-6. Таймаут резерва.** Если заказ в статусе `created` не переходит в `confirmed` в течение заданного времени (по причине сбоя), Lifecycle Worker отменяет заказ (статус `cancelled`), снимает резервы товаров.

**ФТ-7. Просмотр истории заказов.** Пользователь может просматривать список своих заказов: состав, итоговую сумму, текущий статус.

**ФТ-8. Просмотр конкретного заказа.** Пользователь может получить детали конкретного заказа: статус, состав, итоговую сумму.

---

## 4. Нефункциональные требования

### 4.1. Нагрузка

| Операция | Пиковый RPS | Тип |
|---|---|---|
| Чтение корзины | 7 000 | Read |
| Чтение заказов (история, детали) | 3 000 | Read |
| Создание заказа | 2 000 | Write |
| Изменения корзины (add/remove/update) | 1 500 | Write |
| Попытки резервирования | 6 000 | Write |

Совокупное чтение корзин и заказов — **10 000 RPS**, создание заказов — **2 000 RPS**. Соотношение чтение/запись ~5:1 обосновывает CQRS.

### 4.2. Latency-цели

| Операция | P95 |
|---|---|
| Получение корзины | ≤ 150 мс |
| Изменения корзины | ≤ 200 мс |
| Оформление заказа | ≤ 300 мс |
| Получение информации о заказе | ≤ 200 мс |

### 4.3. Доступность

- Оформление заказа: **99.95%**.
- Чтение корзин и заказов: **99.9%**.

### 4.4. Корректность и надёжность

- **Нулевая терпимость к потере подтверждённого заказа.**
- **Невозможность oversell**: товар не подтверждается в количестве, превышающем остаток.
- **Идемпотентность операции оформления** на нескольких уровнях.
- **Корректная обработка повторной отправки** запроса на оформление.
- **Зафиксированный состав** подтверждённого заказа (immutable snapshot).
- **Eventual consistency** между write и read side истории заказов допустима в пределах SLA (<1 сек типично).

### 4.5. Масштабируемость

Горизонтальное масштабирование по контурам: работа с корзиной (Cart Service), оформление заказов (Checkout Service, OMS), хранение и чтение заказов (read-model), обработка остатков (Inventory Service).

---

## 5. Пользовательские сценарии

### Сценарий 1: Сбор корзины

1. Покупатель находит товар и нажимает «Добавить в корзину».
2. Система добавляет позицию в корзину (или увеличивает количество, если товар уже там).
3. Покупатель открывает корзину и видит список товаров, цену каждой позиции, итоговую сумму.
4. Покупатель может изменить количество товара или удалить его из корзины.

### Сценарий 2: Оформление заказа

1. Покупатель находится в корзине и нажимает «Оформить заказ».
2. Система проверяет актуальность товаров и наличие остатков.
3. Покупатель выбирает адрес доставки и подтверждает оформление.
4. Система резервирует выбранные товары, создаёт заказ, подтверждает резерв.
5. Покупатель получает подтверждение оформления заказа с его номером.

**Альтернативные ветки:**

- 2a. Какой-то товар стал недоступен → предупреждение, предложение убрать товар или вернуться в корзину.
- 4a. Не удалось зарезервировать товар (раскупили в момент оформления) → ошибка, заказ не создаётся.

### Сценарий 3: Просмотр истории заказов

1. Покупатель открывает раздел «Мои заказы».
2. Система отображает список заказов с пагинацией: номер заказа, дата, итоговая сумма, статус.
3. Покупатель может открыть детали конкретного заказа: статус, состав, сумма.

---

## 6. Модель данных

В системе используется четыре независимых хранилища: БД Cart Service (PostgreSQL + Redis cache), основная БД OMS (write side), read-model БД OMS (read side), БД Inventory Service.

### 6.1. Cart Service

```mermaid
erDiagram
    direction LR

    CART {
        uuid id PK
        uuid customer_id "UNIQUE"
        timestamp created_at
        timestamp updated_at
    }

    CART_ITEM {
        uuid id PK
        uuid cart_id FK
        varchar sku
        int quantity
        timestamp added_at
        timestamp updated_at
    }

    CART ||--o{ CART_ITEM : "contains"
```

**Ключевые решения:**

- **Одна корзина на пользователя** — `UNIQUE (customer_id)`. При первом добавлении товара корзина создаётся, дальше дополняется.
- **Без snapshot цены и названия** — корзина хранит только `sku` и `quantity`. Актуальные цена и название берутся из Catalog Service при отображении. Так гарантируется, что пользователь видит свежую цену.
- **Redis-кэш** хранит сериализованное состояние корзины: `cart:{customer_id}` → JSON с массивом items. TTL не задан (cache invalidation при write).
- **Шардирование** по `customer_id` в обеих частях (PostgreSQL и Redis).

### 6.2. OMS Write Side

```mermaid
erDiagram
    direction LR

    ORDER {
        uuid id PK "UUIDv7-like с зашитым shard hint от customer_id"
        uuid customer_id
        varchar status "created, confirmed, cancelled"
        decimal total_amount
        jsonb delivery_address "embedded snapshot"
        timestamp created_at "partition key"
        timestamp updated_at
        timestamp confirm_deadline "created_at + 5 минут"
    }

    SUBORDER {
        uuid id PK
        uuid order_id FK
        uuid seller_id
        uuid warehouse_id
        decimal items_amount
        decimal delivery_amount
        timestamp created_at
    }

    ORDER_ITEM {
        uuid id PK
        uuid suborder_id FK
        varchar sku
        varchar product_name "snapshot"
        decimal price_at_order "immutable"
        int quantity
        uuid reservation_id
    }

    IDEMPOTENCY_KEY {
        varchar key PK "UUID от клиента"
        uuid customer_id
        uuid order_id
        timestamp created_at
        timestamp expires_at "TTL 24 часа"
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
    ORDER ||--o{ ORDER_STATUS_HISTORY : "has_audit"
```

**Ключевые решения:**

- **`ORDER.id` как UUIDv7-like** с зашитым shard hint, выводимым из `customer_id` при создании. Это позволяет роутить запрос `GET /orders/{order_id}` в правильный шард без отдельного lookup'а; проверка прав (что заказ принадлежит этому customer_id) выполняется на уровне приложения с JWT.
- **Идемпотентность через отдельную таблицу `IDEMPOTENCY_KEY`** — не привязана к партиционированной таблице `orders`, что позволяет иметь UNIQUE constraint по `key`. Таблица сама партиционируется по `created_at` для управления ростом.
- **`delivery_address` как embedded JSONB** — иммутабельный snapshot на момент заказа.
- **`price_at_order` и `product_name` как snapshot** в `ORDER_ITEM` — состав подтверждённого заказа неизменен, не зависит от изменений в Catalog.
- **`reservation_id` без FK** — cross-service ссылка на резерв в БД Inventory.
- **`OUTBOX_EVENT`** — Transactional Outbox для надёжной публикации событий в Kafka через Debezium.
- **`confirm_deadline`** — крайний срок для перевода в `confirmed`. Если не успели — Lifecycle Worker отменит.

**Шардирование:** по `customer_id`. Все заказы одного покупателя на одном шарде.
**Партиционирование:** `orders` по `created_at` (по месяцам). Таблица `idempotency_keys` партиционируется отдельно по `created_at`.

### 6.3. OMS Read Side (read-model)

```mermaid
erDiagram
    ORDERS_VIEW {
        uuid order_id PK
        uuid customer_id
        varchar status
        decimal total_amount
        jsonb suborders_snapshot "массив подзаказов с items"
        jsonb delivery_address
        int items_count
        timestamp created_at
        timestamp last_status_changed_at
        bigint version
    }
```

**Ключевые решения:**

- Один документ на заказ — никаких JOIN при чтении.
- `suborders_snapshot` как JSONB — все подзаказы и items в одной записи.
- `version` — монотонный счётчик для защиты от out-of-order событий.
- Шардирование по `customer_id`, синхронно с write side.
- Каждый шард: master + 3 read replicas для обслуживания 3 000 RPS чтения заказов.

### 6.4. Inventory Service

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
        uuid order_id
        int quantity
        varchar status "ACTIVE, CONFIRMED, RELEASED"
        timestamp created_at
        timestamp expires_at
    }
```

**Ключевые решения:**

- Композитный PK `(sku, warehouse_id)` в `INVENTORY_STOCK`. Шардирование по этому же ключу.
- БД хранит источник правды; Redis — горячий путь для атомарных операций.

**Структура в Redis:**

```
stock:{sku}:{warehouse_id} → int (доступно к резервированию)
reservation:{reservation_id} → hash, TTL 300 сек
```

**Жизненный цикл резерва:** `ACTIVE → CONFIRMED` (после подтверждения заказа) или `ACTIVE → RELEASED` (при отмене/таймауте).

**Запись в БД синхронна с Redis:** в рамках одного gRPC-вызова `Inventory.Reserve` выполняется Lua-скрипт в Redis (`DECRBY` атомарно), затем INSERT записи `inventory_reservation` со статусом `ACTIVE` и UPDATE `inventory_stock.reserved_quantity`. При сбое в БД-транзакции — компенсирующий `INCRBY` в Redis. Reconciler выступает страховкой на случай сбоев Redis (failover, частичная потеря данных), сверяя счётчики раз в 5 минут.

---

## 7. Архитектура

### 7.1. Компоненты системы

**1. API Gateway** — входная точка. Аутентификация JWT, маршрутизация, rate limiting, TLS termination.

**2. Cart Service** — управление корзинами. Синхронный сервис; PostgreSQL для durability + Redis-кэш для чтения. Read 7 000 RPS, write 1 500 RPS.

**3. Checkout Service** — синхронный оркестратор оформления. При нажатии «Оформить» запускает Saga: проверка идемпотентности → Reserve → CreateOrder → ConfirmReservation → UpdateOrder.

**4. Order Management Service (OMS)** — хозяин жизненного цикла заказов. Хранит заказы, обрабатывает таймауты резерва, публикует бизнес-события через Outbox.

**5. Inventory Service** — управление резервированием товаров. gRPC API: `Reserve`, `Release`, `Confirm`. Redis для конкурентных атомарных операций + PostgreSQL для аудита.

**6. Order Query Service** — синхронный read-сервис: история заказов, детали заказа. Читает только из read-model. Read 3 000 RPS.

**7. Order Projector** — Kafka consumer, обновляющий read-model на основе событий OMS.

**8. Lifecycle Worker** — фоновый сервис: отмена заказов в `created`, не перешедших в `confirmed` в течение `confirm_deadline`. Развёртывается как пул реплик с `FOR UPDATE SKIP LOCKED`.

**Инфраструктура:**

- PostgreSQL Cart Cluster (sharded by customer_id) + Redis Cluster для кэша корзин.
- PostgreSQL OMS Cluster (sharded by customer_id).
- PostgreSQL Read-Model Cluster (sharded by customer_id) — master + 3 replicas на шард.
- PostgreSQL Inventory Cluster (sharded by sku/warehouse) + Redis Cluster для горячего пути.
- Apache Kafka.
- Debezium для CDC.

### 7.2. Архитектурная диаграмма

```mermaid
graph TB
    subgraph Clients
        Mobile[Mobile App]
        Web[Web App]
    end

    subgraph External["internal service"]
        Catalog[Catalog Service]
        Auth[Auth Service]
        Warehouse[Warehouse / Fulfillment]
        Notify[Notification Service]
    end

    subgraph Edge
        Gateway[API Gateway]
    end

    subgraph CoreServices["Core Services"]
        Cart[Cart Service]
        Checkout[Checkout Service]
        OMS[Order Management]
        Inventory[Inventory Service]
        QueryAPI[Order Query Service]
        Projector[Order Projector]
        Worker[Lifecycle Worker]
    end

    subgraph Storage
        Cart_DB[(PostgreSQL Cart)]
        Cart_Redis[(Redis Cart cache)]
        OMS_DB[(PostgreSQL OMS)]
        Read_DB[(PostgreSQL Read-model)]
        Inv_DB[(PostgreSQL Inventory)]
        Inv_Redis[(Redis Inventory hot path)]
    end

    subgraph Bus
        Kafka[Apache Kafka]
        Debezium[Debezium CDC]
    end

    Mobile --> Gateway
    Web --> Gateway
    Gateway -.-> Auth
    Gateway --> Cart
    Gateway --> Checkout
    Gateway --> QueryAPI

    Cart --> Cart_DB
    Cart --> Cart_Redis
    Cart --> Catalog

    Checkout -- gRPC --> Cart
    Checkout -- gRPC --> Inventory
    Checkout -- gRPC --> OMS

    OMS --> OMS_DB
    Inventory --> Inv_DB
    Inventory --> Inv_Redis

    OMS_DB --> Debezium
    Debezium --> Kafka

    Kafka --> OMS
    Kafka --> Projector
    Kafka --> Notify
    Kafka --> Warehouse

    Projector --> Read_DB
    QueryAPI --> Read_DB

    Worker --> OMS_DB
```

### 7.3. Обоснование разделения на сервисы

| Сервис | Обоснование |
|---|---|
| Cart vs OMS | Эфемерная корзина с высокой read-нагрузкой и Redis-кэшем — отдельный профиль от транзакционного OMS. |
| Checkout vs OMS | Checkout — синхронный оркестратор Saga; OMS — хозяин жизненного цикла. Разные паттерны. |
| Inventory vs OMS | Своё хранилище (Redis), специфичная конкурентная нагрузка, шардирование по другому ключу. |
| Query vs OMS | Классический CQRS. Read/write 5:1, разные паттерны масштабирования. |
| Projector vs Query | Projector — write-side для read-model, Query — read-side. Изоляция нагрузок. |
| Lifecycle Worker | Фоновые задачи изолированы от user-facing OMS. |

### 7.4. Sync vs Async и транспорт

**Синхронные пути:** Mobile/Web → Gateway → Cart/Checkout/QueryAPI → внутренние сервисы и БД.

**Асинхронные пути:** OMS → Outbox → Kafka → Projector / Inventory / Notification / Warehouse; Worker → OMS_DB → Outbox → Kafka → Inventory.

**Принцип:** критический путь оформления — синхронно. Всё, что после подтверждения заказа (передача на склад, обновление read-model, уведомления) — async.

**Транспорт:** REST/JSON наружу, gRPC между сервисами, Kafka для асинхронных событий.

### 7.5. Гарантии доставки и идемпотентность

**Transactional Outbox** обеспечивает at-least-once delivery событий из OMS в Kafka. Запись события в `outbox_events` происходит в той же БД-транзакции, что и бизнес-операция. Debezium асинхронно читает WAL и публикует в Kafka.

**Идемпотентность многослойная:**

1. **Клиент → Checkout**: `Idempotency-Key` от клиента + UNIQUE в таблице `idempotency_keys`.
2. **Внутри Saga**: атомарные операции в Inventory (Lua-скрипты) и в OMS (локальные транзакции).
3. **Kafka consumers**: idempotent processing через статусные проверки и версионирование.

---

## 8. Технические сценарии

### 8.1. Получение корзины (Read, 7 000 RPS пиково)

Самая частая операция системы. Чтение идёт через Redis-кэш, fallback в PostgreSQL при cache miss.

**Алгоритм:**

1. Клиент отправляет в API Gateway запрос `GET /cart`.
2. Gateway перенаправляет запрос в Cart Service.
3. Cart Service пытается получить корзину из Redis по ключу `cart:{customer_id}`.
4. При cache hit — десериализует и возвращает.
5. При cache miss — читает из PostgreSQL, сохраняет в Redis, возвращает клиенту.
6. Cart Service параллельно запрашивает у Catalog Service актуальные цены и названия товаров по SKU.
7. Cart Service формирует итоговый ответ: позиции с актуальными ценами, итоговая сумма.

**Sequence-диаграмма:**

```mermaid
sequenceDiagram
    participant Client
    participant Gateway as API Gateway
    participant Cart as Cart Service
    participant CartRedis as Redis (Cart)
    participant CartDB as Cart DB
    participant Catalog as Catalog Service

    Client->>Gateway: GET /cart
    Gateway->>Cart: gRPC GetCart(customer_id)

    Cart->>CartRedis: GET cart:{customer_id}
    alt Cache hit
        CartRedis-->>Cart: serialized cart items
    else Cache miss
        CartRedis-->>Cart: nil
        Cart->>CartDB: SELECT cart, cart_items WHERE customer_id=?
        CartDB-->>Cart: cart items
        Cart->>CartRedis: SET cart:{customer_id} (no TTL)
    end

    Cart->>Catalog: GetProducts(skus)
    Catalog-->>Cart: products with current price, name
    Cart->>Cart: Compose response with totals
    Cart-->>Gateway: CartResponse
    Gateway-->>Client: 200 OK
```

**Highload-аспекты:**

- **Redis-кэш** обслуживает большинство запросов; одна нода Redis Cluster даёт десятки тысяч ops/sec.
- **Шардирование Cart_Redis и Cart_DB** по `customer_id` — запрос идёт сразу в нужный шард.
- **Cache invalidation на write**: при изменении корзины ключ удаляется или перезаписывается, eventual consistency исключена для собственной корзины пользователя.
- **Stateless Cart Service** масштабируется горизонтально без ограничений.

### 8.2. Изменение корзины (Write, 1 500 RPS пиково)

Операции добавления товара, изменения количества, удаления.

**Алгоритм (на примере добавления товара):**

1. Клиент отправляет в API Gateway запрос `POST /cart/items` с `{sku, quantity}`.
2. Gateway перенаправляет в Cart Service.
3. Cart Service выполняет в одной транзакции PostgreSQL: создаёт корзину если её нет, добавляет позицию (или увеличивает количество при наличии).
4. Cart Service инвалидирует Redis-кэш: `DEL cart:{customer_id}`.
5. Клиенту возвращается обновлённое состояние корзины.

**Sequence-диаграмма:**

```mermaid
sequenceDiagram
    participant Client
    participant Gateway as API Gateway
    participant Cart as Cart Service
    participant CartDB as Cart DB
    participant CartRedis as Redis (Cart)

    Client->>Gateway: POST /cart/items {sku, quantity}
    Gateway->>Cart: gRPC AddItem(customer_id, sku, quantity)

    Cart->>CartDB: BEGIN
    Cart->>CartDB: INSERT cart if not exists ON CONFLICT DO NOTHING
    Cart->>CartDB: INSERT cart_item ON CONFLICT (cart_id, sku) DO UPDATE quantity
    Cart->>CartDB: COMMIT

    Cart->>CartRedis: DEL cart:{customer_id}

    Cart-->>Gateway: updated cart
    Gateway-->>Client: 200 OK
```

**Highload-аспекты:**

- **ON CONFLICT DO UPDATE** — атомарное upsert поведение, без дополнительных запросов.
- **Write-through invalidation**: после COMMIT удаляем ключ в Redis. При следующем чтении произойдёт fetch из БД.
- **Транзакция в одной БД одного шарда** — низкая latency, никаких распределённых блокировок.

### 8.3. Оформление заказа (Write, 2 000 RPS пиково)

Saga из четырёх шагов с явными компенсациями: проверка идемпотентности → Reserve → CreateOrder (status=created) → ConfirmReservation → UpdateOrder (status=confirmed).

**Алгоритм:**

1. Клиент отправляет в API Gateway запрос `POST /checkout` с телом `{address, Idempotency-Key}`.
2. Gateway перенаправляет в Checkout Service.
3. Checkout проверяет идемпотентность: `INSERT INTO idempotency_keys (key, customer_id) ON CONFLICT DO NOTHING`. Если ключ уже был — возвращает существующий `order_id`.
4. Checkout читает текущую корзину пользователя из Cart Service.
5. Checkout вызывает `Inventory.Reserve(items)` — атомарно резервирует все товары в Redis (Lua-скрипт) с синхронной записью в Inventory DB.
6. Checkout вызывает `OMS.CreateOrder` — в одной транзакции создаются `orders` (status=`created`), `suborders`, `order_items`, обновляется `idempotency_keys.order_id`, пишется outbox-событие `OrderCreated`.
7. Checkout вызывает `Inventory.Confirm(reservation_ids)` — резервы переходят в `CONFIRMED`, `inventory_stock.total_quantity` уменьшается.
8. Checkout вызывает `OMS.ConfirmOrder` — `orders.status` обновляется на `confirmed`, пишется outbox-событие `OrderConfirmed`.
9. Checkout очищает корзину пользователя через `Cart.Clear`.
10. Клиенту возвращается `order_id` и статус `confirmed`.

**Sequence-диаграмма:**

```mermaid
sequenceDiagram
    participant Client
    participant Gateway as API Gateway
    participant Checkout
    participant Cart as Cart Service
    participant Inventory
    participant InvRedis as Redis (Inventory)
    participant InvDB as Inventory DB
    participant OMS
    participant OMSDB as OMS DB

    Client->>Gateway: POST /checkout {address, Idempotency-Key}
    Gateway->>Checkout: gRPC CreateOrder

    Checkout->>OMSDB: INSERT idempotency_keys ON CONFLICT DO NOTHING
    alt Конфликт ключа
        OMSDB-->>Checkout: existing key, order_id
        Checkout-->>Client: 200 OK (existing order)
    else Новый ключ
        OMSDB-->>Checkout: inserted

        Checkout->>Cart: GetCart(customer_id)
        Cart-->>Checkout: cart items

        Checkout->>Inventory: Reserve(items)
        Inventory->>InvRedis: EVAL Lua атомарно DECRBY
        InvRedis-->>Inventory: ok
        Inventory->>InvDB: BEGIN
        Inventory->>InvDB: INSERT inventory_reservation (ACTIVE)
        Inventory->>InvDB: UPDATE inventory_stock reserved_quantity
        Inventory->>InvDB: COMMIT
        Inventory-->>Checkout: reservation_ids

        Checkout->>OMS: CreateOrder(items, reservations, key)
        OMS->>OMSDB: BEGIN
        OMS->>OMSDB: INSERT orders (created, deadline=now()+5m)
        OMS->>OMSDB: INSERT suborders, order_items
        OMS->>OMSDB: UPDATE idempotency_keys SET order_id=?
        OMS->>OMSDB: INSERT outbox_events (OrderCreated)
        OMS->>OMSDB: COMMIT
        OMS-->>Checkout: order_id

        Checkout->>Inventory: Confirm(reservation_ids)
        Inventory->>InvDB: UPDATE inventory_reservation status=CONFIRMED
        Inventory->>InvDB: UPDATE inventory_stock total_quantity
        Inventory-->>Checkout: ok

        Checkout->>OMS: ConfirmOrder(order_id)
        OMS->>OMSDB: UPDATE orders SET status=confirmed
        OMS->>OMSDB: INSERT outbox_events (OrderConfirmed)
        OMS-->>Checkout: ok

        Checkout->>Cart: Clear(customer_id)

        Checkout-->>Client: 201 Created {order_id, status=confirmed}
    end
```

**Обработка ошибок:**

- **Дубль idempotency-ключа:** `INSERT ON CONFLICT` возвращает существующую запись, клиенту отдаётся прежний `order_id`.
- **OUT_OF_STOCK на Reserve:** Inventory откатывает все уже сделанные DECRBY (`INCRBY`-компенсация), возвращает ошибку. Checkout отвечает клиенту 409 Conflict, заказа нет.
- **OMS падает после Reserve:** Checkout вызывает `Inventory.Release(reservation_ids)` как компенсацию, возвращает 500. Если компенсация тоже не прошла — Lifecycle Worker через таймаут разберётся.
- **ConfirmReservation падает:** заказ остаётся в `created`, Lifecycle Worker через `confirm_deadline` (5 минут) переведёт его в `cancelled` и сделает `Inventory.Release`.
- **UpdateOrder (confirmed) падает после ConfirmReservation:** аналогично — Lifecycle Worker увидит заказ в `created` и обработает.

**Highload-аспекты:**

- **Конкуренция за hot products** решается атомарным Lua-скриптом в Redis. Сериализация по ключу на уровне Redis-shard за микросекунды.
- **Многослойная идемпотентность:** UNIQUE constraint в отдельной таблице `idempotency_keys` (без партиционирования).
- **Saga с явными компенсациями** покрывает сбои на любом шаге.
- **Outbox pattern**: события `OrderCreated`, `OrderConfirmed` пишутся в той же транзакции, что и бизнес-данные.
- **Шардирование** по `customer_id` — каждое оформление работает в рамках одного шарда OMS DB (запись локальна). 2 000 RPS на 8 шардов = 250 RPS на шард — комфортная нагрузка.
- **Bottleneck-анализ:** самые медленные шаги — Reserve (Redis Lua + INSERT в InvDB) и CreateOrder (транзакция OMS). Запас по латентности при ~50 мс каждый и общем бюджете 300 мс — комфортный.

### 8.4. Получение заказа из истории (Read, 3 000 RPS пиково)

Чтение из read-model, отдельной от write-side.

**Алгоритм:**

1. Клиент отправляет в API Gateway запрос `GET /orders/{order_id}` или `GET /orders` (список).
2. Gateway аутентифицирует пользователя, извлекает `customer_id` из JWT, перенаправляет в Order Query Service.
3. Для одного заказа: Order Query извлекает shard hint из `order_id` (UUIDv7-like), идёт в нужный шард read-model, выполняет `SELECT FROM orders_view WHERE order_id=? AND customer_id=?` (проверка прав через `customer_id`).
4. Для списка заказов: шардовый ключ — `customer_id`, выполняется `SELECT FROM orders_view WHERE customer_id=? ORDER BY created_at DESC LIMIT/OFFSET`.
5. Запрос идёт на одну из read replicas шарда (round-robin балансировка).
6. Результат возвращается клиенту.

**Sequence-диаграмма:**

```mermaid
sequenceDiagram
    participant Client
    participant Gateway as API Gateway
    participant QueryAPI as Order Query Service
    participant ReadDB as Read-Model DB

    Client->>Gateway: GET /orders/{order_id}
    Gateway->>Gateway: Auth, extract customer_id from JWT
    Gateway->>QueryAPI: gRPC GetOrder(order_id, customer_id)

    QueryAPI->>QueryAPI: Extract shard hint from order_id, route to shard
    QueryAPI->>ReadDB: SELECT FROM orders_view WHERE order_id=? AND customer_id=?
    ReadDB-->>QueryAPI: order data with suborders snapshot
    QueryAPI-->>Gateway: OrderResponse
    Gateway-->>Client: 200 OK
```

**Highload-аспекты:**

- **Read path полностью изолирован от write**: Order Query Service не имеет доступа к OMS DB.
- **Шардирование = отсутствие scatter**: запрос с `customer_id` в JWT и shard hint в `order_id` роутится в один конкретный шард без cross-shard query.
- **Read replicas для масштабирования**: 3 000 RPS / (8 шардов × 3 реплики) ≈ 125 RPS на реплику. Огромный запас.
- **Версионирование read-model** защищает от out-of-order событий при обновлении через Kafka.
- **Eventual consistency**: лаг от COMMIT в OMS DB до видимости в read-model — типично <1 сек.

### 8.5. Таймаут резерва (Background)

Lifecycle Worker отменяет заказы, оставшиеся в `created` дольше `confirm_deadline` (5 минут). Это страховочный механизм на случай сбоя где-то между CreateOrder и ConfirmOrder.

**Алгоритм:**

1. Lifecycle Worker по таймеру (каждые 30 секунд) обращается к шарду OMS DB.
2. Запрашивает заказы со статусом `created` и истёкшим `confirm_deadline`: `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 100`.
3. Для каждого заказа открывает транзакцию: переводит в `cancelled` с фильтром `WHERE status='created'`, пишет событие `OrderCancelled` в outbox, коммитит.
4. Debezium публикует `OrderCancelled` в Kafka.
5. Inventory как consumer обрабатывает событие: для каждого `reservation_id` со статусом `ACTIVE` или `CONFIRMED` переводит в `RELEASED`, обновляет stock в БД, делает `INCRBY` в Redis.

**Sequence-диаграмма:**

```mermaid
sequenceDiagram
    participant Worker as Lifecycle Worker
    participant OMSDB as OMS DB (shard)
    participant Kafka
    participant Inventory
    participant InvDB as Inventory DB
    participant InvRedis as Redis

    Worker->>OMSDB: SELECT WHERE status=created AND deadline < now() FOR UPDATE SKIP LOCKED LIMIT 100
    OMSDB-->>Worker: [order_ids batch]

    loop для каждого order
        Worker->>OMSDB: BEGIN
        Worker->>OMSDB: UPDATE orders SET status=cancelled WHERE id=? AND status=created
        Worker->>OMSDB: INSERT outbox_events (OrderCancelled)
        Worker->>OMSDB: COMMIT
    end

    Note over OMSDB, Kafka: Debezium → Kafka
    Kafka->>Inventory: OrderCancelled
    loop для каждого reservation_id
        Inventory->>InvDB: UPDATE inventory_reservation status=RELEASED if ACTIVE or CONFIRMED
        Inventory->>InvDB: UPDATE inventory_stock (reserved_quantity или total_quantity)
        Inventory->>InvRedis: INCRBY stock
    end
```

**Highload-аспекты:**

- **`FOR UPDATE SKIP LOCKED`** — нативный паттерн PostgreSQL для конкурентной обработки заданий без distributed lock.
- **Условный UPDATE** с проверкой `status='created'` защищает от race: если Checkout всё-таки успел перевести в `confirmed` параллельно, Worker увидит 0 rows.
- **Release через Kafka, а не gRPC**: Worker отвечает только за надёжную запись в OMS DB + outbox; Inventory подхватывает асинхронно.
- **Идемпотентность Inventory consumer**: проверка статуса резерва перед обновлением.

### 8.6. Особые случаи

#### Случай 1: Двойная обработка события Kafka consumer'ом

**Ситуация:** OMS или Inventory consumer обработал событие, успел COMMIT, но не успел ACK в Kafka (упал, рестарт). Kafka переотправит то же событие.

**Решение:**

- Перед обновлением: `SELECT ... FOR UPDATE`, проверка текущего статуса. Если уже в целевом состоянии — ACK без действий.
- Для read-model — проверка `version` в условном UPDATE.

**Гарантия:** at-least-once delivery + idempotent processing = effective exactly-once.

#### Случай 2: Расхождение Redis ↔ Inventory DB

**Ситуация:** Redis потерял данные при failover, или INSERT в БД не прошёл после успешного DECRBY.

**Решение:**

- Inventory Reconciler раз в 5 минут проходит по `(sku, warehouse_id)`, считает в БД сумму активных резервов и сверяет с Redis-счётчиком. При расхождении переписывает Redis из БД (БД — source of truth).
- Окно неконсистентности до 5 минут.

---

## 9. Архитектурные компромиссы

1. **Saga вместо 2PC.** Распределённая транзакция оформления реализована через сагу с явными компенсациями. 2PC неприменим, так как Inventory работает с in-memory Redis на горячем пути.
2. **Hybrid storage в Inventory (Redis + PostgreSQL).** Redis обеспечивает атомарные операции для конкурентного доступа (десятки тысяч RPS на ключ для популярных товаров), PostgreSQL — source of truth для аудита и восстановления. Согласованность поддерживается Reconciler'ом.
3. **Cart Service: PostgreSQL + Redis cache.** Корзина должна сохраняться (пользователь возвращается через час и ждёт увидеть свою корзину), поэтому БД — source of truth. Redis обслуживает большую часть чтений.
4. **CQRS с отдельной read-model.** Read/write ratio 5:1 (10к на чтение vs 2к на запись) делает CQRS оправданным: write-side оптимизирована под транзакционность оформления, read-side — под быстрое чтение списков и деталей заказов с денормализацией в JSONB.
5. **Шардирование с момента запуска.** При 2 000 RPS на запись одна нода работала бы на пределе; шардирование обеспечивает запас под рост в 5–10 раз.
6. **`order_id` с зашитым shard hint.** Позволяет роутить запрос `GET /orders/{order_id}` в правильный шард без дополнительного lookup'а, при этом customer_id из JWT используется для проверки прав.
7. **Отдельная таблица `idempotency_keys`** без партиционирования. Альтернатива (UNIQUE в `orders`) невозможна из-за ограничения PostgreSQL: unique constraint партиционированной таблицы должен содержать ключ партиционирования.
8. **Outbox + Debezium вместо прямой публикации в Kafka.** Гарантирует at-least-once delivery бизнес-событий: при недоступности Kafka событие сохраняется в БД и публикуется при восстановлении.
9. **Двухфазный жизненный цикл заказа `created → confirmed`.** Промежуточный статус `created` существует короткое время между созданием записи заказа и подтверждением резерва. Это позволяет Lifecycle Worker гарантированно очистить «застрявшие» заказы при сбое любого шага саги.
