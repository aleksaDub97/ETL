# GitHub Repositories ETL Pipeline

## Описание проекта

Данный проект реализует ETL-пайплайн для сбора данных о репозиториях с платформы GitHub через публичное API.

Пайплайн загружает только новые или обновлённые репозитории (инкрементальная загрузка), сохраняет их в базу данных PostgreSQL и дополнительно выгружает в файловую систему в формате Parquet с сжатием.

---

## Основной функционал

✅ Инкрементальная загрузка (по `updated_at`)

✅ Логирование всех этапов ETL

✅ Обработка ошибок и retry (через tenacity)

✅ Конфигурация вынесена в отдельный YAML файл

✅ Генерация `etl_id` для трассировки запуска

✅ Сохранение данных в PostgreSQL

✅ Выгрузка в Parquet (snappy compression)

✅ Уведомления в Telegram

---

## Архитектура

```
GitHub API
     ↓
  Extract
     ↓
 Transform
     ↓
 ├── PostgreSQL
 └── Parquet (Snappy)
```

---

## Структура проекта

```
.
├── github_etl.py        # основной ETL-скрипт
├── config.yaml          # конфигурация
├── state.txt            # хранение последнего запуска
├── data/                # parquet файлы
├── github_etl.log       # лог файл
└── README.md
```

---

## Инкрементальная логика

Пайплайн загружает только те репозитории, которые были обновлены после последнего запуска.

Используется:

* поле `updated_at` из API
* файл `state.txt`, где хранится timestamp последней загрузки

---

## Схема таблицы

```sql
CREATE TABLE github_repos (
    repo_id BIGINT PRIMARY KEY,
    name TEXT,
    owner TEXT,
    stars INT,
    forks INT,
    language TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    url TEXT,
    etl_id TEXT
);
```

---

## Конфигурация

Файл `config.yaml`:

```yaml
api:
  query: "data-engineer language:python"
  per_page: 100
  token: ""  # (GitHub token)

postgres:
  user: postgres
  password: postgres
  host: localhost
  port: 5432
  db: etl_db
  table: github_repos

storage:
  parquet_path: data

telegram:
  token: "YOUR_TOKEN"
  chat_id: "YOUR_CHAT_ID"
```

---

## Запуск проекта

### 1. Установить зависимости

```bash
pip install -r requirements.txt
```

### 2. Запустить ETL

```bash
python github_etl.py
```

---

## Подключение PostgreSQL

Пример запуска через Docker:

```bash
docker run -d \
  --name postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  postgres
```

---

## Telegram уведомления

После выполнения ETL отправляется сообщение в Telegram:

* количество загруженных записей
* длительность выполнения
* сообщение об ошибке (если возникла)

---

## Обработка ошибок

* Retry: до 5 попыток
* Логирование всех ошибок
* Финальное уведомление в Telegram при падении

---

## Пример выходных данных

### PostgreSQL

Таблица `github_repos`

### Parquet

Файлы сохраняются в папку:

```
data/
```

---
