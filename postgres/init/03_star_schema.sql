-- Создаём только схему; таблицы создаст Spark с помощью mode("overwrite").
-- Без FK-констрейнтов — Spark их не поддерживает при авто-создании.

CREATE SCHEMA IF NOT EXISTS star;
