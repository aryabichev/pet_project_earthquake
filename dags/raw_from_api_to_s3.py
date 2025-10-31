import logging
import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Конфигурация DAG
OWNER = "a.ryabichev"
DAG_ID = "raw_from_api_to_s3"

# Используемые таблицы в DAG
LAYER = "raw"  # Слой данных: raw - сырые, silver - очищенные, gold - бизнес-метрики
SOURCE = "earthquake"  # Источник данных: сервис землетрясений USGS

# S3 (MinIO) - получаем ключи доступа из переменных Airflow
ACCESS_KEY = Variable.get("access_key")  # Логин для MinIO (хранится в секретах Airflow)
SECRET_KEY = Variable.get("secret_key")  # Пароль для MinIO (хранится в секретах Airflow)

LONG_DESCRIPTION = """
# LONG DESCRIPTION
"""

SHORT_DESCRIPTION = "SHORT DESCRIPTION"

# Аргументы DAG по умолчанию
args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 10, 20, tz="Europe/Moscow"),  
    "catchup": True,  # Airflow догонит все пропущенные запуски с start_date
    "retries": 3,  # Количество попыток перезапуска при ошибке
    "retry_delay": pendulum.duration(hours=1),  # Ждать 1 час между перезапусками
}


def get_dates(**context) -> tuple[str, str]:
    #Получаем даты интервала выполнения из контекста Airflow
    start_date = context["data_interval_start"].format("YYYY-MM-DD")  
    end_date = context["data_interval_end"].format("YYYY-MM-DD")      

    return start_date, end_date


def get_and_transfer_api_data_to_s3(**context):
    """
    Основная функция: забирает данные из API землетрясений и сохраняет в S3
    Процесс:
    1. Получает даты интервала выполнения
    2. Настраивает DuckDB для работы с S3
    3. Читает CSV данные из USGS API
    4. Сохраняет в Parquet формате в MinIO
    """
    
    # Получаем даты для текущего запуска DAG
    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start load for dates: {start_date}/{end_date}")
    
    # Создаём соединение с DuckDB (in-memory база данных)
    con = duckdb.connect()

    # Выполняем SQL запрос в DuckDB
    con.sql(
        f"""
        -- Настройка часового пояса
        SET TIMEZONE='UTC';
        
        -- Устанавливаем расширение для работы с HTTP и S3
        INSTALL httpfs;
        LOAD httpfs;
        
        -- Настройка подключения к MinIO (S3-совместимое хранилище)
        SET s3_url_style = 'path';           
        SET s3_endpoint = 'minio:9000';      
        SET s3_access_key_id = '{ACCESS_KEY}';     
        SET s3_secret_access_key = '{SECRET_KEY}'; 
        SET s3_use_ssl = FALSE;             

        -- Копируем данные из API в S3
        COPY
        (
            SELECT
                *
            FROM
                -- Читаем CSV данные из USGS API за указанный период
                read_csv_auto('https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime={start_date}&endtime={end_date}') AS res
        ) 
        -- Сохраняем в MinIO в формате Parquet
        TO 's3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet';
        """,
    )

    con.close()  # Закрываем соединение с DuckDB
    logging.info(f"✅ Download for date success: {start_date}")


# Создаём DAG с настройками выполнения
with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",  # CRON выражение: каждый день в 05:00 UTC
    default_args=args,
    tags=["s3", "raw"],  # Теги для поиска в Airflow UI
    description=SHORT_DESCRIPTION,
    concurrency=1,        # Одновременно может выполняться только 1 экземпляр DAG
    max_active_tasks=1,   # Одновременно может выполняться только 1 задача
    max_active_runs=1,    # Одновременно может выполняться только 1 запуск DAG
) as dag:
    dag.doc_md = LONG_DESCRIPTION  # Документация для Airflow UI

    # Создаём задачи DAG
    start = EmptyOperator(task_id="start")  # Пустая задача-заглушка для начала

    # python_callable - это переменная-ссылка на функцию
    # airflow сам вызовет эту функцию когда придёт время
    # мы только говорим Airflow какую функцию выполнить, поэтому без () в конце функции
    get_and_transfer_api_data_to_s3 = PythonOperator(
        task_id="get_and_transfer_api_data_to_s3",
        python_callable=get_and_transfer_api_data_to_s3,  # Функция которая будет выполняться
    )

    end = EmptyOperator(task_id="end")  # Пустая задача-заглушка для конца

    # Определяем порядок выполнения задач: start → основная задача → end
    start >> get_and_transfer_api_data_to_s3 >> end




