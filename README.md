✈️ Airline Data Warehouse ETL
End-to-End Data Engineering проект, реализующий полный цикл обработки данных: от загрузки сырых CSV файлов до построения аналитической витрины и дашборда.

Проект демонстрирует построение ELT-пайплайна (Extract, Load, Transform) с использованием современной архитектуры "Медальон" (Bronze → Silver → Gold).

🏗 Архитектура
Данные проходят следующий путь:

Source: Локальные CSV файлы с данными о полетах.

Orchestration: Apache Airflow (в Docker) управляет загрузкой и вызовом процедур.

Data Warehouse (Snowflake):

Bronze Layer: Сырая загрузка "как есть" (Raw Data).

Silver Layer: Очистка, дедупликация, SCD Type 1, схема "Звезда" (Facts & Dimensions).

Gold Layer: Агрегированные витрины данных для аналитики.

Visualization: Power BI подключается к Gold слою.

Code snippet

graph LR
    A[Local CSV] -->|Airflow Upload| B[Snowflake Stage];
    B -->|COPY INTO| C[(Bronze: RAW)];
    C -->|Stored Proc| D[(Silver: Star Schema)];
    D -->|Stored Proc| E[(Gold: Analytics)];
    E -->|Native Connector| F[Power BI Dashboard];
🛠 Технологический стек
Infrastructure: Docker & Docker Compose.

Orchestration: Apache Airflow 2.x.

DWH: Snowflake (Standard Edition).

Languages: SQL (Snowflake Dialect), Python (Airflow DAGs).

BI: Microsoft Power BI Desktop.

🚀 Как запустить проект (Step-by-Step)
Шаг 1. Регистрация и настройка Snowflake
Создайте аккаунт на signup.snowflake.com (Trial версия подойдет).

Войдите в консоль Snowflake под ролью ACCOUNTADMIN.

Скопируйте ваш Account URL (он понадобится для Airflow и Power BI).

Формат: abc12345.us-east-1 (Organization-Account) или классический URL.

Шаг 2. Запуск инфраструктуры (Docker)
Убедитесь, что у вас установлен Docker Desktop.

Клонируйте репозиторий:

Bash

git clone https://github.com/YOUR_USERNAME/snowflake-airflow-etl.git
cd snowflake-airflow-etl
Запустите Airflow:

Bash

docker-compose up -d
Проверьте, что контейнеры запущены:

Bash

docker ps
Откройте UI Airflow: http://localhost:8080 (логин/пароль: airflow/airflow).

Шаг 3. Подключение Airflow к Snowflake
В UI Airflow перейдите в Admin -> Connections.

Найдите соединение snowflake_default (или создайте новое).

Заполните поля:

Connection Id: snowflake_default

Conn Type: Snowflake

Host: <ваш_account_id>.snowflakecomputing.com

Schema: PUBLIC

Login: Ваш логин Snowflake.

Password: Ваш пароль.

Account: <ваш_account_id> (первая часть URL).

Warehouse: COMPUTE_WH

Database: AIRLINE_DWH

Role: ACCOUNTADMIN (или SYSADMIN, если права настроены).

Шаг 4. Деплой объектов базы данных (DAG 1)
Запустите DAG 01_init_snowflake_objects. Этот пайплайн выполняет DDL скрипты и создает:

Базу данных AIRLINE_DWH.

Схемы BRONZE, SILVER, GOLD, UTILS.

File Formats (CSV парсеры).

Stored Procedures (Логика трансформации с правами EXECUTE AS OWNER).

Шаг 5. Запуск ETL Пайплайна (DAG 2)
Запустите DAG 02_airline_etl_pipeline. Он выполнит следующие шаги:

Extract: Загрузит локальный CSV в Snowflake Stage.

Load Bronze: Выполнит COPY INTO в таблицу RAW_AIRLINE_DATA.

Transform Silver: Разнесет данные по таблицам DIM_PASSENGER, DIM_AIRPORT, FACT_FLIGHT (очистка, дедупликация).

Transform Gold: Рассчитает витрину FLIGHT_ANALYTICS.

Quality Check: Проверит целостность данных.

📊 Подключение Power BI
Откройте Power BI Desktop.

Нажмите Get Data -> Snowflake.

Server: Ваш URL (без https://).

Warehouse: COMPUTE_WH.

Mode: Import.

Введите учетные данные (User/Password).

В навигаторе выберите: AIRLINE_DWH -> GOLD -> FLIGHT_ANALYTICS.

Нажмите Load.

📂 Структура проекта
Plaintext

├── dags/
│   ├── 01_init_snowflake_objects.py  # DAG настройки окружения
│   └── 02_airline_etl_pipeline.py    # Основной ETL пайплайн
├── sql/
│   ├── 00_ddl/                       # Скрипты создания таблиц
│   ├── 01_procedures/                # Хранимые процедуры (ELT логика)
│   └── 02_dml/                       # Вспомогательные скрипты
├── data/                             # Исходные CSV файлы
├── docker-compose.yaml               # Конфигурация Airflow
└── README.md                         # Документация
💡 Особенности реализации (Lessons Learned)
Snowflake RBAC: Используется модель EXECUTE AS OWNER в процедурах, чтобы Airflow мог выполнять сложные трансформации без выдачи избыточных прав сервисному аккаунту.

Merge Logic: Реализована дедупликация записей в Silver слое через GROUP BY перед MERGE, чтобы избежать ошибок Duplicate row detected.

SCD Strategy: Для измерений используется подход, близкий к SCD Type 1 (обновление атрибутов с UPDATE_TIMESTAMP).

Data Parsing: Настроен кастомный FILE FORMAT в Snowflake для корректной обработки кавычек в CSV.