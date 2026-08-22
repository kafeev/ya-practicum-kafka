# Задание 1. Развёртывание и настройка Kafka-кластера в Yandex Cloud


## 0. Общая информация о кластере

| Параметр | Значение |
| --- | --- |
| Сервис | Yandex Managed Service for Apache Kafka® |
| Хост брокера | `rc1a-6ibie76edoio2ab7.mdb.yandexcloud.net` |
| Порт | `9091` (внешний SSL-эндпоинт) |
| Протокол безопасности | `SASL_SSL` |
| Механизм аутентификации | `SCRAM-SHA-512` |
| Пользователь | `practicumuser` |
| Количество брокеров | 3 |
| Корневой CA-сертификат | `YandexInternalRootCA.crt` (в корне репозитория) |

Используемый топик: **`topic-1`**.

Клиентское подключение (Python) реализовано в `producer.py` и `consumer.py`
(библиотека `kafka-python`, см. `requirements.txt`).

---

## Шаг 1. Развёртывание Kafka

Кластер развёрнут в Yandex Cloud через Managed Service for Apache Kafka® с
**3 брокерами** в разных зонах доступности для отказоустойчивости.

![kafka-nit](/picts/kafka-cloud-init.png)

### Параметры кластера

- `default.replication.factor = 3` — репликация по умолчанию равна числу брокеров.
- `min.insync.replicas = 2` — запись подтверждается после синхронизации с 2 репликами (допускает отказ 1 брокера без потери данных).
- `num.partitions = 3` — число партиций по умолчанию.
- `auto.create.topics.enable = false` — топики создаются явно.

## Шаг 2. Настройка репликации и хранения данных

### Создание топика

![kafka-topic](/picts/topic-created.png)

### Вывод `kafka-topics.sh --describe`

Запускал через docker образ


![topic-describe](picts/kafka-topic-describe.png)

> Команда выполняется из Docker-образа `confluentinc/cp-kafka:7.6.0`
> (утилита `kafka-topics` там лежит по пути `/usr/bin/kafka-topics`):
>
> ```bash
> docker run --rm --workdir /config \
>   -v ${PWD}/config:/config \
>   -v ${PWD}/YandexInternalRootCA.crt:/config/YandexInternalRootCA.crt:ro \
>   confluentinc/cp-kafka:7.6.0 \
>   /usr/bin/kafka-topics \
>     --bootstrap-server rc1a-6ibie76edoio2ab7.mdb.yandexcloud.net:9091 \
>     --command-config /config/client.properties \
>     --describe --topic topic-1
> ```
>
> Фактический скриншот вывода: `picts/topic-all-messages.png`
> (либо добавьте отдельный `picts/topic-describe.png`).

---

## Шаг 3. Настройка Schema Registry

Confluent Schema Registry развёрнут локально в Docker и доступен по
`http://localhost:8081`.

### Подготовка в Yandex Cloud (один раз)

Confluent Schema Registry хранит схемы в служебном топике `_schemas`. Его
нужно создать заранее и выдать права пользователю `registry`. Команды —
в `scripts/setup-registry.sh` (нужны утилита `yc` и переменные
`CLUSTER_ID`, `REGISTRY_PASSWORD`):

```bash
# 1) создать пользователя registry
yc managed-kafka user create <CLUSTER_ID> --name registry --password <REGISTRY_PASSWORD>

# 2) создать топик _schemas (1 партиция, политика compact)
yc managed-kafka topic create <CLUSTER_ID> --name _schemas \
  --partitions 1 --replication-factor 3 --cleanup-policy CLEANUP_POLICY_COMPACT

# 3) выдать registry права ACCESS_ROLE_PRODUCER и ACCESS_ROLE_CONSUMER на _schemas
yc managed-kafka topic grant-permission <CLUSTER_ID> --topic-name _schemas --user-name registry --role ACCESS_ROLE_PRODUCER
yc managed-kafka topic grant-permission <CLUSTER_ID> --topic-name _schemas --user-name registry --role ACCESS_ROLE_CONSUMER
```

`<CLUSTER_ID>` получить командой `yc managed-kafka cluster list`.
Пароль `<REGISTRY_PASSWORD>` должен совпадать с тем, что указан в
`docker-compose.schema-registry.yml`.


### Развёртывание локально через Docker

Используется `docker-compose.schema-registry.yml` (образ
`confluentinc/cp-schema-registry:7.6.0`). Schema Registry подключается к
кластеру Yandex по `SASL_SSL`/`SCRAM-SHA-512` от имени пользователя `registry`
и хранит схемы в `_schemas`.



```bash
docker compose -f docker-compose.schema-registry.yml up -d

# проверка доступности
curl http://localhost:8081/
```

вывод
```bash
PS C:\Practicum\kafka\ya-practicum-kafka> curl http://localhost:8081/

Предупреждение безопасности: риск выполнения сценария                                                                                                                                                                                                      
Invoke-WebRequest анализирует содержимое веб-страницы. При анализе страницы может выполняться код сценария на веб-странице.                                                                                                                                
      РЕКОМЕНДУЕМОЕ ДЕЙСТВИЕ:                                                                                                                                                                                                                              
      Используйте параметр -UseBasicParsing, чтобы предотвратить выполнение кода сценария.                                                                                                                                                                 

      Продолжить?
    
[Y] Да - Y  [A] Да для всех - A  [N] Нет - N  [L] Нет для всех - L  [S] Приостановить - S  [?] Справка (значением по умолчанию является "N"): y


StatusCode        : 200
StatusDescription : OK
Content           : {123, 125}
RawContent        : HTTP/1.1 200 OK
                    X-Request-ID: 87416d16-63c4-4210-a984-17913479d5dc
                    Vary: Accept-Encoding, User-Agent
                    Content-Length: 2
                    Content-Type: application/vnd.schemaregistry.v1+json
                    Date: Sat, 22 Aug 2026 ...
Headers           : {[X-Request-ID, 87416d16-63c4-4210-a984-17913479d5dc], [Vary, Accept-Encoding, User-Agent], [Content-Length, 2], [Content-Type, application/vnd.schemaregistry.v1+json]...}
RawContentLength  : 2
```

Настройки подключения (bootstrap, SASL, CA, пароль `registry`) лежат в
`schema-registry.custom.properties`. CA-сертификат Yandex монтируется в
контейнер как `/etc/schema-registry/YandexInternalRootCA.crt` (PEM truststore).

Если группа `schema-registry` в кластере «зависла» из-за другого SR, её можно
сбросить скриптом `scripts/reset_sr_group.py` (Kafka Admin API).

### Файл схемы

Схема данных (Avro) — файл **`schema.avsc`** (соответствует сообщению продюсера
`{"message": "..."}`):

```json
{
  "type": "record",
  "name": "Message",
  "namespace": "practicum.kafka",
  "fields": [ { "name": "message", "type": "string" } ]
}
```

### Регистрация схемы

Схема регистрируется **автоматически** при первой отправке Avro-сообщения
продюсером `avro_producer.py` (субъект значения — `topic-1-value`, ключа —
`topic-1-key`). Файл схемы — `schema.avsc`. Зарегистрировать схему вручную
также можно скриптом `scripts/register-schema.sh`.

> **Совместимость клиента:** в свежих версиях `confluent-kafka` (2.x)
> Schema Registry-клиент использует `httpx`, который некорректно работает с
> Jetty в Schema Registry 7.6.0 (все запросы возвращают HTTP 503). Для
> обхода модуль `sr_http.py` подменяет HTTP-транспорт клиента на `requests`
> (который работает корректно). `avro_producer.py` и `avro_consumer.py`
> импортируют `sr_http` первой строкой.

### Проверка регистрации схем

![producer-consumer](picts/consumer-read-messages.png)

Проверить ответы Schema Registry:
```bash
curl http://localhost:8081/subjects
# ["topic-1-value"]

curl -X GET http://localhost:8081/subjects/topic-1-value/versions
# [1]
```
![schema](/picts/schema-registry-versions.png)


## Шаг 4. Проверка работы Kafka


Подключается по `SASL_SSL`/`SCRAM-SHA-512`, отправляет одно сообщение
`{"message": "hello from producer"}` с ключом `key-1` в топик `topic-1`.

![producer](/picts/producer-sent-message.png)

---



## Структура репозитория

| Файл | Назначение |
| --- | --- |
| `readme.md` | описание шагов задания (этот файл) |
| `producer.py` | продюсер тестовых сообщений |
| `consumer.py` | консьюмер тестовых сообщений |
| `requirements.txt` | зависимости Python (`kafka-python`) |
| `schema.avsc` | Avro-схема данных |
| `config/client.properties` | настройки Kafka CLI (SASL_SSL) |
| `scripts/create-topic.sh` | создание топика |
| `scripts/describe-topic.sh` | описание топика |
| `scripts/register-schema.sh` | регистрация схемы в Schema Registry (curl) |
| `scripts/setup-registry.sh` | создание пользователя `registry` и топика `_schemas` в Yandex |
| `docker-compose.schema-registry.yml` | локальный запуск Confluent Schema Registry (Docker) |
| `schema-registry.custom.properties` | конфиг SR: своя группа `schema-registry-standalone` (обход конфликта с другим SR) |
| `scripts/reset_sr_group.py` | сброс «зависшей» группы `schema-registry` в кластере (Kafka Admin API) |
| `avro_producer.py` | Avro-продюсер (регистрирует схему в SR, пишет в `topic-1`) |
| `avro_consumer.py` | Avro-консьюмер (читает из `topic-1` через SR) |
| `sr_http.py` | подмена HTTP-транспорта SR-клиента на `requests` (обход бага `httpx` ↔ SR 7.6.0) |
| `YandexInternalRootCA.crt` | корневой CA для SSL |
| `picts/*.png` | скриншоты проверки (продюсер/консьюмер/топик) |


