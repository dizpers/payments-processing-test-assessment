# Сервис процессинга платежей

Принимает платёж по API, обрабатывает его в фоне через эмулятор платёжного шлюза и уведомляет клиента webhook'ом. FastAPI, SQLAlchemy 2.0 async, PostgreSQL, RabbitMQ через FastStream, Alembic, Docker Compose.

## Запуск

```bash
docker compose up --build
```

Поднимаются postgres, rabbitmq, одноразовый `migrate` (накатывает миграции), затем `api` и `consumer`. API на http://localhost:8000, панель RabbitMQ на http://localhost:15672 (guest / guest). Ключ API по умолчанию `dev-api-key`, задаётся переменной `API_KEY`. Если порты заняты, переопределите их: `RABBITMQ_PORT=5673 RABBITMQ_UI_PORT=15673 docker compose up --build`.

## Примеры

Создать платёж. Заголовки `X-API-Key` и `Idempotency-Key` обязательны:

```bash
curl -X POST localhost:8000/api/v1/payments \
  -H "X-API-Key: dev-api-key" -H "Idempotency-Key: order-1" -H "Content-Type: application/json" \
  -d '{"amount": "10.50", "currency": "USD", "description": "Order 1", "metadata": {"order_id": 1}, "webhook_url": "http://webhook-mock:8080/ok"}'
```

```
HTTP 202
{"payment_id":"23d74500-ff1b-4a84-b279-6e7ad03a05b0","status":"pending","created_at":"2026-10-15T10:04:49.692912Z"}
```

Тот же запрос с тем же `Idempotency-Key` вернёт тот же платёж и 202. Тот же ключ с другим телом вернёт 409. Без ключа API или с неверным: 401. Без `Idempotency-Key` или с невалидным телом (сумма не больше нуля, больше двух знаков после запятой, валюта не из RUB/USD/EUR, адрес не http/https): 422.

Получить платёж:

```bash
curl localhost:8000/api/v1/payments/23d74500-ff1b-4a84-b279-6e7ad03a05b0 -H "X-API-Key: dev-api-key"
```

```
HTTP 200
{"id":"23d74500-ff1b-4a84-b279-6e7ad03a05b0","amount":"10.50","currency":"USD","description":"Order 1","metadata":{"order_id":1},"status":"succeeded","idempotency_key":"order-1","webhook_url":"http://webhook-mock:8080/ok","created_at":"2026-10-15T10:04:49.692912Z","processed_at":"2026-10-15T10:04:53.229316Z","webhook_sent_at":"2026-10-15T10:04:53.243563Z"}
```

Через 2–5 секунд после создания статус становится `succeeded` (90 %) или `failed` (10 %), и на `webhook_url` уходит POST с JSON `{payment_id, status, amount, currency, processed_at, metadata}`. Неизвестный id: 404.

## Демо: webhook, повторы и DLQ

Профиль `demo` добавляет приёмник webhook'ов `webhook-mock` с двумя адресами: `/ok` отвечает 200, `/fail` всегда 500.

```bash
docker compose --profile demo up --build
```

Создайте платёж с `"webhook_url": "http://webhook-mock:8080/fail"` и смотрите `docker compose logs -f consumer webhook-mock`. Consumer сделает четыре попытки доставить webhook: сразу, затем через 2, 4 и 8 секунд. После этого сообщение окажется в очереди `payments.dlq`, видно в панели RabbitMQ: в заголовках `x-attempt: 3` и `x-death` с историей. Статус платежа при этом уже `succeeded` или `failed`, пустым остаётся только `webhook_sent_at`.

## Демо: outbox

```bash
docker compose stop rabbitmq
# создать платёж: API отвечает 202, событие лежит в таблице outbox с пустым published_at
docker compose start rabbitmq
# через несколько секунд событие опубликовано, платёж обработан, webhook доставлен
```

## Как это устроено

```
POST /api/v1/payments ──► одна транзакция: строка в payments + строка в outbox
                                       │
                          relay (в процессе consumer, опрос раз в 0,5 с)
                                       ▼
      exchange payments ──► payments.new ──► handler: шлюз → статус в БД → webhook
                                 ▲                 │ ошибка, попытка n < 3
         TTL 2 / 4 / 8 с ◄── payments.retry.1/2/3 ◄┘
                                                   │ ошибка после 3 повторов
                                        payments.dlq (DLX у payments.new)
```

- `app/api.py`, `app/service.py`: эндпоинты, проверка ключа, идемпотентность. Платёж и событие `payment.created` пишутся в одной транзакции.
- `app/outbox.py`: relay берёт неопубликованные строки с `FOR UPDATE SKIP LOCKED`, публикует с publisher confirms и помечает `published_at`.
- `app/broker.py`: обменник `payments` (direct), очередь `payments.new` с dead-letter в `payments.dlq`, три очереди повторов с TTL, которые возвращают сообщение в `payments.new`.
- `app/consumer.py`: один обработчик. Берёт строку платежа под `FOR UPDATE`, если статус `pending`, зовёт шлюз (2–5 с, 90 % успех) и коммитит статус. Затем в новой транзакции снова блокирует строку и, если `webhook_sent_at` пуст, шлёт webhook. Любая ошибка: публикация в `payments.retry.<n>` с заголовком `x-attempt`, после третьего повтора сообщение отвергается и уходит в DLQ.

## Тесты

```bash
uv sync
uv run pytest
```

Нужен запущенный Docker: testcontainers поднимает одноразовые PostgreSQL и RabbitMQ, тесты не читают `DATABASE_URL` и `RABBITMQ_URL` из окружения. Миграции накатываются Alembic'ом, так что схема в тестах та же, что в compose. Обработчик проверяется в памяти через `TestRabbitBroker`; цепочка повторов и DLQ проверяется на живом RabbitMQ в `tests/test_topology.py`. Линтеры: `uv run ruff check . && uv run mypy app`.

## Решения

- **Доставка at-least-once.** Relay может опубликовать событие дважды, брокер может доставить сообщение повторно. Обработчик рассчитан на это: статус меняется только из `pending`, строка платежа заблокирована на время вызова шлюза и снова на время отправки webhook, так что две одновременные доставки не отправят его дважды. Остаётся окно между ответом 2xx и записью `webhook_sent_at`: если consumer упадёт именно там, webhook придёт повторно, поэтому получатель дедуплицирует по `payment_id`.
- **Статус коммитится до webhook.** Неудачный webhook не откатывает результат шлюза. Сообщение в DLQ значит «уведомление не доставлено», а не «платёж не обработан». Если ошибка случилась до шлюза (например, недоступна БД), платёж остаётся `pending`; вернуть сообщение из DLQ в `payments.new` можно через панель RabbitMQ или shovel.
- **10 % ошибок шлюза это отказ, а не сбой.** Платёж получает финальный `failed`, webhook отправляется, повторов нет. Повторы и DLQ срабатывают на инфраструктурные ошибки: недоступный webhook, ответ не 2xx, падение БД.
- **Попытки: 1 + 3.** Первая доставка и три повтора с паузами 2, 4, 8 секунд. Паузы делает брокер: у каждой очереди повторов свой TTL, по истечении сообщение возвращается в `payments.new`. Consumer при этом свободен, а счётчик попыток переживает его рестарт.
- **Idempotency-Key.** Тот же ключ и то же тело (после нормализации, `10.5` и `10.50` равны) возвращают исходный платёж. Другое тело: 409. Гонка двух одновременных запросов решается уникальным индексом в БД. Ключ глобальный, потому что API-ключ один; с несколькими клиентами он был бы уникален в паре с клиентом.
- **Схема.** `currency` и `status` хранятся как строки с CHECK, а не как PostgreSQL enum: добавить значение проще. Сумма `numeric(18, 2)`, в JSON отдаётся строкой, чтобы не терять точность. Единственный индекс сверх первичных ключей и UNIQUE: частичный на `outbox` по `published_at IS NULL`, его читает relay. Колонка `metadata` в модели называется `metadata_`, имя занято SQLAlchemy.
- **Relay живёт в процессе consumer** и стартует после объявления топологии, чтобы не публиковать в обменник без привязанных очередей. Пока consumer остановлен, события копятся в outbox.
- **Настройки без значений по умолчанию** для адресов БД, брокера и ключа API: сервис не стартует без явного окружения. Паузы повторов, задержка и доля успеха шлюза настраиваются через переменные, см. `app/config.py`.
- **SQLAlchemy закреплён на 2.0.x**, как в задании, хотя уже вышла 2.1.

## Что осталось за рамками

- Подпись webhook'ов и защита от SSRF по `webhook_url`.
- Очистка опубликованных строк `outbox`.
- Для прода: `compose.yml` описывает и инфраструктуру, и сервисы; с внешней БД и брокером сервисы запускаются из того же образа с другими `DATABASE_URL` и `RABBITMQ_URL`.

Код написан с Claude Code, правила для агента в [AGENTS.md](AGENTS.md).
