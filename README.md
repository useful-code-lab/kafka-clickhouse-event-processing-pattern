# Kafka → ClickHouse: паттерн обработки и отслеживания событий

Практический пример того, как построить поток обработки событий между **Apache Kafka** и **ClickHouse** на Go.

Проект показывает, как получать сообщения из Kafka, сохранять данные в ClickHouse, формировать журнал событий и отслеживать новые события без прямого взаимодействия наблюдателя с Kafka.

## Что можно получить из этого проекта

Репозиторий можно использовать как **готовый учебный и практический шаблон** для систем, в которых данные поступают через Kafka, а ClickHouse используется для их хранения, аналитики и обработки событий.

Из проекта можно забрать следующие решения:

* пример интеграции **Go + Kafka + ClickHouse**;
* схему передачи данных `Producer → Kafka → Consumer → ClickHouse`;
* использование **Kafka Engine** внутри ClickHouse;
* использование **Materialized View** для автоматического переноса данных;
* отдельную таблицу событий (`events`) как журнал изменений;
* подход к отслеживанию новых событий из ClickHouse;
* раздельные `producer`, `consumer` и `events_watcher`, которые можно запускать независимо;
* Docker Compose-конфигурацию для быстрого локального запуска всей инфраструктуры.

Это удобно, если нужно быстро разобраться, **как связать Kafka и ClickHouse в событийной архитектуре**, не собирая демонстрационный стенд с нуля.

---

## Как работает решение

Общий поток данных:

```text
┌─────────────┐
│  Producer   │
│    Go       │
└──────┬──────┘
       │ JSON
       ▼
┌─────────────┐
│    Kafka    │
│ data_topic  │
└──────┬──────┘
       │
       ├──────────────────────┐
       │                      │
       ▼                      ▼
┌─────────────┐       ┌─────────────────┐
│   Consumer  │       │  Kafka Engine   │
│     Go      │       │ kafka_input     │
└──────┬──────┘       └────────┬────────┘
       │                       │
       ▼                       ▼
┌─────────────┐       ┌─────────────────┐
│  my_table   │       │ Materialized    │
│  ClickHouse │       │ View            │
└──────┬──────┘       └────────┬────────┘
       │                       │
       │                       ▼
       │              ┌─────────────────┐
       └─────────────►│     events      │
                      │    MergeTree     │
                      └────────┬────────┘
                               │
                               ▼
                      ┌─────────────────┐
                      │ Events Watcher  │
                      │      Go        │
                      └─────────────────┘
```

### Основная идея

Kafka используется как транспорт событий.

ClickHouse принимает данные двумя способами:

1. Go `consumer` получает сообщения из Kafka и записывает их в `my_table`.
2. ClickHouse самостоятельно читает Kafka через `Kafka Engine`, после чего `Materialized View` переносит данные в `events`.

`events_watcher` периодически проверяет таблицу `events` и обнаруживает новые события.

При этом watcher **не подключается к Kafka напрямую** — он работает только с ClickHouse.

---

## Что демонстрирует проект

### 1. Producer

Go-приложение генерирует JSON-сообщения и отправляет их в Kafka topic:

```text
data_topic
```

Пример сообщения:

```json
{
  "id": 1,
  "value": "hello"
}
```

Producer можно использовать как источник тестовых событий для локальной разработки.

---

### 2. Consumer

Go-приложение получает сообщения из Kafka и сохраняет данные в ClickHouse.

Основная таблица:

```sql
CREATE TABLE IF NOT EXISTS default.my_table
(
    id UInt64,
    value String
)
ENGINE = MergeTree()
ORDER BY id;
```

Таким образом можно увидеть классический сценарий:

```text
Kafka → Go Consumer → ClickHouse
```

---

### 3. Kafka Engine

ClickHouse может самостоятельно читать сообщения из Kafka.

Для этого используется таблица:

```sql
CREATE TABLE IF NOT EXISTS default.kafka_input
(
    id UInt64,
    value String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9093',
    kafka_topic_list = 'data_topic',
    kafka_group_name = 'ch_group',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;
```

Это позволяет рассмотреть другой вариант интеграции:

```text
Kafka → ClickHouse Kafka Engine
```

Без отдельного Go-consumer на этом участке.

---

### 4. Materialized View

Materialized View используется как автоматический маршрут между Kafka Engine и таблицей событий.

```text
kafka_input
     ↓
Materialized View
     ↓
events
```

Такой подход полезен для построения потоковой обработки данных непосредственно средствами ClickHouse.

---

### 5. Event Log

Таблица `events` используется как журнал событий:

```sql
CREATE TABLE IF NOT EXISTS default.events
(
    event_id UInt64,
    my_table_id UInt64,
    value String,
    event_type String,
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY event_id;
```

В ней сохраняются:

* идентификатор события;
* идентификатор исходного объекта;
* данные объекта;
* тип события;
* время создания события.

Получается отдельный поток событий, который можно использовать для последующего анализа или обработки.

---

### 6. Events Watcher

`events_watcher` — отдельное Go-приложение, которое периодически проверяет таблицу `events`.

Watcher может обнаруживать новые записи:

```text
ClickHouse
    ↓
events
    ↓
Events Watcher
    ↓
лог / дальнейшая обработка
```

Главная особенность — watcher не зависит от Kafka.

Это позволяет рассматривать ClickHouse как источник уже сохранённых событий.

---

# Когда этот проект может быть полезен

Проект особенно полезен как отправная точка, если вы:

* изучаете интеграцию Kafka и ClickHouse;
* проектируете event-driven архитектуру;
* хотите сохранять Kafka-события в аналитической БД;
* ищете пример использования ClickHouse Kafka Engine;
* хотите понять, как работают Materialized Views для потоковых данных;
* хотите отделить получение событий от их последующего наблюдения;
* создаёте прототип сервиса обработки событий;
* хотите быстро поднять локальный стенд Kafka + ClickHouse + Go.

Вместо того чтобы отдельно собирать Kafka, ClickHouse, producer, consumer и watcher, можно использовать этот репозиторий как **готовую основу для экспериментов и адаптации под собственную задачу**.

---

# Структура проекта

```text
.
├── consumer/
│   └── Go consumer
│
├── producer/
│   └── Go producer
│
├── events_watcher/
│   └── Go watcher событий
│
├── images/
│   └── скриншоты работы
│
├── docker-compose.yml
└── README.md
```

---

# Быстрый запуск

Для запуска инфраструктуры:

```bash
docker compose up --build
```

или в фоновом режиме:

```bash
docker compose up -d
```

После запуска будут доступны ClickHouse и Kafka.

---

# Events Watcher

Войти в контейнер:

```bash
docker exec -it clickhouse-kafka-events_watcher-1 sh
```

Запустить watcher:

```bash
./events_watcher
```

Watcher начнёт отслеживать новые записи в таблице `events`.

![Events Watcher](images/events_watcher.png)

---

# Consumer

Войти в контейнер:

```bash
docker exec -it clickhouse-kafka-consumer-1 sh
```

Запустить consumer:

```bash
./consumer
```

![Consumer](images/consumer.png)

---

# Producer

Войти в контейнер:

```bash
docker exec -it clickhouse-kafka-producer-1 sh
```

Запустить producer:

```bash
./producer
```

![Producer](images/producer.png)

---

# ClickHouse

Для подключения к ClickHouse через Tabix:

```text
http://localhost:8124
```

![ClickHouse](images/login.png)

После отправки сообщений можно посмотреть данные в ClickHouse:

![Data](images/data.png)

Также можно посмотреть таблицу, работающую через Kafka Engine:

![Kafka Input](images/kafka_input.png)

---

# Kafka UI

Web-интерфейс Kafka:

```text
http://localhost:8080/
```

![Kafka UI](images/kafka.png)

---

# Таблицы ClickHouse

## `my_table`

Основная таблица с объектами, полученными из Kafka.

```sql
CREATE TABLE IF NOT EXISTS default.my_table
(
    id UInt64,
    value String
)
ENGINE = MergeTree()
ORDER BY id;
```

## `kafka_input`

Таблица Kafka Engine, которая читает сообщения из Kafka topic `data_topic`.

```sql
CREATE TABLE IF NOT EXISTS default.kafka_input
(
    id UInt64,
    value String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9093',
    kafka_topic_list = 'data_topic',
    kafka_group_name = 'ch_group',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;
```

## `events`

Журнал событий.

```sql
CREATE TABLE IF NOT EXISTS default.events
(
    event_id UInt64,
    my_table_id UInt64,
    value String,
    event_type String,
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY event_id;
```

---

# Что можно попробовать изменить

Репозиторий можно использовать как экспериментальную площадку.

Например:

* добавить новые типы событий;
* изменить структуру JSON-сообщений;
* добавить новые поля в ClickHouse;
* изменить период опроса watcher;
* добавить собственную обработку событий;
* заменить тестовый producer реальным источником Kafka;
* добавить дополнительные Materialized Views;
* использовать `events` как источник для последующей бизнес-логики.

Таким образом, проект можно рассматривать не только как демонстрацию, но и как **минимальный каркас для собственного прототипа событийной системы**.

---

# Технологии

* Go
* Apache Kafka
* ClickHouse
* Kafka Engine
* Materialized View
* MergeTree
* Docker Compose
* Tabix
* Kafka UI

---

## Итог

Этот проект показывает несколько способов построения потока:

```text
Kafka → Go → ClickHouse
```

и

```text
Kafka → Kafka Engine → Materialized View → ClickHouse
```

а также отдельный механизм наблюдения:

```text
ClickHouse → Events Watcher
```

Основная ценность репозитория — **готовый воспроизводимый пример**, который можно запустить локально, изучить по частям и использовать как основу для собственных решений с Kafka, ClickHouse и событийной обработкой.
