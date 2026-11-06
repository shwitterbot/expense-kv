# Подключение ExpenseKV к приложениям

ExpenseKV запускается отдельным процессом. Приложение подключается по TCP через
Redis-клиент в режиме RESP2. Устанавливать Rust-библиотеку в приложение не нужно.
React/Vue и другие браузерные приложения обращаются к вашему HTTP-backend;
подключение к ExpenseKV и пароль остаются на сервере приложения.

| Приложение | Подключение | Готовый пример |
|---|---|---|
| Django | Django Cache API + небольшой backend поверх `RedisCache` | [settings](../examples/django_settings.py), [backend](../examples/django_backend.py) |
| FastAPI | Асинхронный `redis.asyncio`, клиент на время lifespan | [fastapi_app.py](../examples/fastapi_app.py) |
| Flask | Синхронный `redis-py`, один пул на процесс | [flask_app.py](../examples/flask_app.py) |
| Express / Node.js | `ioredis`, один клиент на процесс | [express.mjs](../examples/express.mjs) |

## 1. Сервер и переменные окружения

Из корня этого репозитория:

```sh
cargo build --release --locked
KV_BIND=127.0.0.1:6380 KV_TIMEOUT_SECONDS=60 ./target/release/expense-kv
```

В терминале приложения:

```sh
export EXPENSE_KV_URL='redis://127.0.0.1:6380/0'
```

Если сервер запущен с `KV_PASSWORD`, задайте приложению тот же пароль через
`EXPENSE_KV_PASSWORD`. В примерах пароль передаётся отдельным параметром клиента;
URL оставьте без имени пользователя и пароля. Поддерживается `AUTH password`,
ACL-вариант `AUTH username password` отсутствует. Примеры работают и без пароля,
если сервер доступен только на loopback.

Общие требования:

- RESP2: у Python-клиента и `ioredis` явно указано `protocol=2`.
- Только DB 0: в URL используется `/0`.
- Обычный TCP `redis://`. TLS на сервере не реализован.
- Суммарное число подключений всех worker-процессов и приложений должно укладываться
  в `KV_MAX_CLIENTS` (по умолчанию 128). В Python-примерах пул ограничен 8 соединениями
  на процесс; у Express один клиент на процесс.
- Таймауты подключения/команды — 1 с. Это пределы отдельных операций, а не всего
  HTTP-запроса. В примерах отключены повторные попытки команд; повтор записи после
  потерянного ответа может дать неоднозначный результат.

## 2. Django: настройка Cache API

В окружении существующего Django-проекта установите Python-клиент:

```sh
python -m pip install redis==8.1.0
```

Скопируйте [django_backend.py](../examples/django_backend.py) в пакет вашего проекта,
например `myproject/expensekv.py`:

```python
from django.core.cache.backends.base import BaseCache
from django.core.cache.backends.redis import RedisCache


class ExpenseKVCache(RedisCache):
    set_many = BaseCache.set_many
```

Штатный `RedisCache.set_many()` использует транзакционный pipeline. Наш backend
берёт реализацию `BaseCache.set_many()`: отдельный `SET` для каждого значения с TTL.
При ошибке часть ключей может уже сохраниться; атомарности всей группы нет.
Остальные операции и сериализация наследуются от Django `RedisCache`.

В `settings.py` добавьте отдельный alias кэша, сохранив существующий `default`:

```python
import os
from redis.backoff import NoBackoff
from redis.retry import Retry

# Если CACHES уже задан, добавьте только элемент CACHES["expensekv"].
CACHES = {
    "default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"},
    "expensekv": {
        "BACKEND": "myproject.expensekv.ExpenseKVCache",
        "LOCATION": os.getenv("EXPENSE_KV_URL", "redis://127.0.0.1:6380/0"),
        "KEY_PREFIX": "myapp:django:v1",
        "TIMEOUT": 60,
        "OPTIONS": {
            "protocol": 2,
            "password": os.getenv("EXPENSE_KV_PASSWORD") or None,
            "socket_connect_timeout": 1,
            "socket_timeout": 1,
            "max_connections": 8,
            "health_check_interval": 5,
            "retry": Retry(NoBackoff(), 0),
        },
    },
}
```

Замените `myproject` на имя Python-пакета, где находится ваш `settings.py`.
Полный конфиг для запуска примеров прямо из этого репозитория находится в
[examples/django_settings.py](../examples/django_settings.py).

Проверка в `python manage.py shell`:

```python
from django.core.cache import caches

cache = caches["expensekv"]
cache.set("report:demo", {"total_minor": 12500, "currency": "RUB"}, timeout=60)
assert cache.get("report:demo")["total_minor"] == 12500
assert not cache.add("report:demo", "overwrite", timeout=60)
cache.set_many({"counter": 1, "label": "Расходы"}, timeout=60)
assert cache.incr("counter") == 2
assert cache.get_many(["counter", "label"]) == {"counter": 2, "label": "Расходы"}
cache.delete_many(["report:demo", "counter", "label"])
```

Проверены `get`, `set`, `add`, `set_many`, `get_many`, `incr`, `decr`, `touch`,
`delete`, `delete_many`, а также `timeout=0` и `timeout=None`.
`cache.clear()` вызывает `FLUSHDB` и очищает всю DB 0, включая ключи других префиксов.
Для выборочного удаления используйте `delete`/`delete_many`.

Django сериализует Python-объекты через pickle. Его ключи не следует читать как JSON
из Node.js или других приложений. Для обмена между фреймворками используйте JSON
и отдельный согласованный префикс. Не допускайте недоверенных писателей в пространство
ключей, из которого Django загружает pickle.

По умолчанию ошибки кэша пробрасываются вызывающему коду. Если кэш необязателен,
при ошибке чтения вычисляйте результат из основной БД, а ошибку заполнения обрабатывайте:

```python
from redis.exceptions import RedisError

try:
    result = cache.get(cache_key)
except RedisError:
    result = None

if result is None:
    result = calculate_from_database()  # ваша функция расчёта
    try:
        cache.set(cache_key, result, timeout=60)
    except RedisError:
        pass
```

## 3. FastAPI

Готовый пример использует async-клиент; создание и закрытие пула привязаны к lifespan.
В `async def` не вызывается блокирующий синхронный Redis-клиент.

```sh
python -m pip install redis==8.1.0 fastapi==0.162.0 uvicorn==0.54.0
python -m uvicorn examples.fastapi_app:app --host 127.0.0.1 --port 8000
```

Запускать из корня репозитория. Приложение —
[examples/fastapi_app.py](../examples/fastapi_app.py); его можно скопировать в свой проект.
Проверка из другого терминала:

```sh
curl http://127.0.0.1:8000/summary
curl http://127.0.0.1:8000/summary
```

Первый ответ содержит `"source":"computed"`, второй — `"source":"cache"`.
Ключ `demo:fastapi:v1:summary` живёт 60 с. После истечения TTL или при ошибке ExpenseKV
ответ вычисляется заново. Повреждённый JSON также считается промахом кэша.

## 4. Flask

Пример использует синхронный клиент с ограниченным пулом, созданным на процесс.
Пул не закрывается после каждого HTTP-запроса.

```sh
python -m pip install redis==8.1.0 Flask==3.1.3
python -m flask --app examples.flask_app run --host 127.0.0.1 --port 8001
```

Приложение — [examples/flask_app.py](../examples/flask_app.py).

```sh
curl http://127.0.0.1:8001/summary
curl http://127.0.0.1:8001/summary
```

Первый ответ вычисляется, второй берётся из `demo:flask:v1:summary` с TTL 60 с.
При ошибке кэша или некорректном JSON пример продолжает вычислять ответ.

## 5. Express / Node.js

В примере `ioredis` использует RESP2, а `enableReadyCheck=false` отключает проверку
готовности, рассчитанную на метаданные полноценного Redis. Повторная отправка
неподтверждённых команд и очередь команд во время отсутствия соединения отключены.
Клиент может переподключаться для последующих запросов.

```sh
npm ci --prefix examples
PORT=8002 node examples/express.mjs
```

Приложение — [examples/express.mjs](../examples/express.mjs).
В существующем Node-проекте установите `ioredis@6.0.0` и перенесите настройки клиента;
если нужен этот HTTP-пример, также установите `express@5.3.0`.

```sh
curl http://127.0.0.1:8002/summary
curl http://127.0.0.1:8002/summary
```

Пример использует `demo:express:v1:summary`, JSON и TTL 60 с. Первый ответ вычисляется,
второй берётся из кэша. Если ExpenseKV недоступен, маршрут вычисляет ответ без кэша.
Те же настройки клиента можно использовать в сервисе NestJS; готовые NestJS cache/queue
адаптеры отдельно не проверялись.

## 6. Обычный Python и другие фреймворки

Синхронный клиент подходит для Python-приложений с обычными обработчиками:

```python
import json
import os
from redis import Redis
from redis.backoff import NoBackoff
from redis.retry import Retry

with Redis.from_url(
    os.getenv("EXPENSE_KV_URL", "redis://127.0.0.1:6380/0"),
    password=os.getenv("EXPENSE_KV_PASSWORD") or None,
    protocol=2,
    decode_responses=True,
    socket_connect_timeout=1,
    socket_timeout=1,
    max_connections=8,
    retry=Retry(NoBackoff(), 0),
) as kv:
    kv.set("myapp:json:v1:demo", json.dumps({"total_minor": 12500}), ex=60)
    assert json.loads(kv.get("myapp:json:v1:demo"))["total_minor"] == 12500
    with kv.pipeline(transaction=False) as pipe:
        pipe.set("myapp:json:v1:a", "1", ex=60)
        pipe.set("myapp:json:v1:b", "2", ex=60)
        pipe.execute()
```

В долгоживущем приложении переиспользуйте клиент/пул между запросами; `with` выше
демонстрирует закрытие при завершении отдельной программы.

Для других языков нужен Redis-клиент, который умеет RESP2, одиночный пароль AUTH
и только поддерживаемые команды. Наличие Redis-адаптера у фреймворка не гарантирует
совместимость: адаптер может вызывать Lua, SCAN, коллекции или транзакции.
Celery/RQ, Pub/Sub и Redis Cluster не поддерживаются этим сервером.

## 7. Ключи, обновления и ограничения

- Разделяйте приложения/форматы префиксами: `myapp:django:v1:*` и `myapp:json:v1:*`.
  Для пользовательских данных включайте идентификатор пользователя в ключ и сохраняйте
  обычную авторизацию приложения. Демонстрационные `/summary` возвращают статический
  учебный ответ; они не читают финансовую БД и не реализуют пользовательскую авторизацию.
- При изменении источника обновляйте или инвалидируйте кэш после commit SQL.
  Одного удаления недостаточно для строгой согласованности при конкурентном заполнении;
  вариант с поколениями кэша описан в [ARCHITECTURE.md](../ARCHITECTURE.md#корректность-кэша-django).
- Не используйте `pipeline()` с настройками по умолчанию в redis-py или `multi()`
  в ioredis. Нужны `pipeline(transaction=False)` и обычный `pipeline()` соответственно.
  `MULTI/EXEC` не реализованы; отправленные между ними команды могут выполниться
  по отдельности, несмотря на ошибки транзакционных команд.
- Максимальная длина ключа — 1024 байта вместе с префиксом/версией Django; значение —
  1 MiB после сериализации. В запросе максимум 1024 аргумента и 2 MiB их содержимого.
  Поэтому один MGET/get_many ограничен 1023 ключами и суммарно 2 MiB значений ответа.
- `SET EX/PX`, NX/XX, счётчики, удаление и TTL поддерживаются. Дополнительные SET-опции,
  Lua, коллекции, сканирование и RESP3 отсутствуют. Полный список — в [README](../README.md#команды).
- SQL остаётся источником данных. Эти примеры демонстрируют подключение кэша;
  очереди, распределённые блокировки и полная замена Redis не заявляются.

## 8. Проверка примеров

Проверено на Python 3.13, Django 6.1.3, redis-py 8.1.0, FastAPI 0.162.0,
Flask 3.1.3, Node.js 25.2.1, ioredis 6.0.0 и Express 5.3.0 на macOS.
Для полного набора Python-примеров нужен Python 3.12+; Rust-сервер Python не требует.

```sh
cargo build --release --locked
python3.13 -m venv .venv
.venv/bin/python -m pip install -r examples/requirements.txt
npm ci --prefix examples
.venv/bin/python -m examples.check_integrations
```

Проверка сама запускает отдельный ExpenseKV на свободном порту, в новом временном
каталоге и со случайным паролем. Рабочая база `./data` не используется.
Проверяются AUTH, обычный pipeline, Django Cache API, ответы трёх HTTP-примеров,
некорректный JSON и вычисление ответа при ошибке аутентификации кэша.
Это smoke-проверка совместимости перечисленных версий, а не нагрузочное испытание
реального приложения. Установленные пакеты находятся только в окружении примеров;
Rust-проект по-прежнему не имеет сторонних Rust-зависимостей.

Основания API: [Django cache](https://docs.djangoproject.com/en/6.1/topics/cache/),
[redis-py](https://redis.readthedocs.io/en/stable/connections.html),
[FastAPI lifespan](https://fastapi.tiangolo.com/advanced/events/),
[Flask](https://flask.palletsprojects.com/en/stable/),
[ioredis](https://github.com/redis/ioredis).
