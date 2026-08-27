#!/bin/bash
# Инициализация БД маркетплейса (выполняется внутри контейнера Postgres).
# Запускается от имени приложения (POSTGRESQL_USERNAME), поэтому таблицы
# принадлежат нужному пользователю и не требуют дополнительных GRANT.
set -euo pipefail

export PGPASSWORD="${POSTGRES_PASSWORD:-${POSTGRESQL_PASSWORD}}"
PSQL="psql -v ON_ERROR_STOP=1 -U ${POSTGRES_USER:-${POSTGRESQL_USERNAME}} -d ${POSTGRES_DB:-${POSTGRESQL_DATABASE}}"

echo ">>> Создание схемы БД маркетплейса"
$PSQL <<'SQL'
CREATE TABLE IF NOT EXISTS products (
  product_id      TEXT PRIMARY KEY,
  name           TEXT NOT NULL,
  description    TEXT,
  price_amount   NUMERIC(12,2),
  price_currency TEXT,
  category       TEXT,
  brand          TEXT,
  stock_available INT,
  stock_reserved  INT,
  sku            TEXT,
  tags           JSONB,
  images         JSONB,
  specifications JSONB,
  created_at     TEXT,
  updated_at     TEXT,
  index          TEXT,
  store_id       TEXT
);

CREATE INDEX IF NOT EXISTS idx_products_category ON products (category);
CREATE INDEX IF NOT EXISTS idx_products_brand    ON products (brand);
CREATE INDEX IF NOT EXISTS idx_products_tags     ON products USING gin (tags);

CREATE TABLE IF NOT EXISTS client_events (
  id          SERIAL PRIMARY KEY,
  user_id     TEXT,
  event_type  TEXT,
  query       TEXT,
  product_ids TEXT[],
  created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_client_events_user ON client_events (user_id);

-- Шаг 5 (расширенный вариант): полнотекстовый поиск по товарам.
-- Хранилище (отфильтрованные товары) уже наполняется из топика products-allowed
-- сервисом product-sink; здесь добавляем поисковый индекс на существующий PG.
ALTER TABLE products ADD COLUMN IF NOT EXISTS search_vector tsvector;
CREATE INDEX IF NOT EXISTS idx_products_search ON products USING gin (search_vector);

CREATE OR REPLACE FUNCTION products_search_vector_update() RETURNS trigger AS $$
BEGIN
  NEW.search_vector :=
      setweight(to_tsvector('russian', coalesce(NEW.name, '')), 'A') ||
      setweight(to_tsvector('russian', coalesce(NEW.brand, '')), 'A') ||
      setweight(to_tsvector('russian', coalesce(NEW.category, '')), 'B') ||
      setweight(to_tsvector('russian', coalesce(NEW.description, '')), 'C') ||
      setweight(to_tsvector('russian', coalesce(NEW.tags::text, '')), 'C');
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_products_search_vector ON products;
CREATE TRIGGER trg_products_search_vector
  BEFORE INSERT OR UPDATE ON products
  FOR EACH ROW EXECUTE FUNCTION products_search_vector_update();

-- Заполнить search_vector для уже существующих строк (идемпотентно).
UPDATE products SET search_vector =
    setweight(to_tsvector('russian', coalesce(name, '')), 'A') ||
    setweight(to_tsvector('russian', coalesce(brand, '')), 'A') ||
    setweight(to_tsvector('russian', coalesce(category, '')), 'B') ||
    setweight(to_tsvector('russian', coalesce(description, '')), 'C') ||
    setweight(to_tsvector('russian', coalesce(tags::text, '')), 'C')
WHERE search_vector IS NULL;
SQL

echo ">>> Готово"
