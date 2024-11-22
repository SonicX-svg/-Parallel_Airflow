import os
import pandas as pd
import subprocess
import logging
from io import StringIO
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import TaskInstance

# Функция для загрузки данных из файла profit_table.csv
def load_data_from_csv(file_path: str) -> pd.DataFrame:
    df = pd.read_csv(file_path)
    return df

# Функция для передачи данных во внешний скрипт через stdin и получения результата
def run_transform_script(product: str, df: pd.DataFrame, ti: TaskInstance) -> None:
    # Фильтрация данных по продукту
    product_data = df[['id', 'date', f'sum_{product}', f'count_{product}']]
    csv_data = product_data.to_csv(index=False)
    
    try:
        # Запуск внешнего скрипта через subprocess
        result = subprocess.run(
            ['python3', '/opt/airflow/your_project/transform_script.py', product],
            input=csv_data,
            text=True,
            capture_output=True,
            check=True
        )
        
        # Отправка результата в XCom
        ti.xcom_push(key=f'product_{product}_result', value=result.stdout)
        logging.info(f"Product {product} processed and result pushed to XCom.")

    
    except subprocess.CalledProcessError as e:
        logging.error(f"Error while processing product {product}: {e.stderr}")
        ti.xcom_push(key=f'product_{product}_result', value=None)
        
    # Здесь возвращается результат (будет сохранен с ключом return_value)
    return result.stdout  # Это возвращаемое значение будет сохранено в XCom с ключом return_value.

# Функция для объединения результатов всех задач
def combine_results_and_save(ti: TaskInstance) -> None:
    # Инициализация переменной для хранения основной таблицы
    main_df = None

    # Перебор всех продуктов
    for product in ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']:
        task_id = f'product_processing.transform_{product}'
        result = ti.xcom_pull(task_ids=task_id, key=f'product_{product}_result')

        if result:
            # Чтение данных из XCom
            product_df = pd.read_csv(StringIO(result))

            # Находим столбец, который содержит 'flag' в названии
            flag_columns = [col for col in product_df.columns if 'flag' in col]

            # Для продукта 'a' создаем главную таблицу
            if product == 'a':
                main_df = product_df  # Сохраняем столбец 'id' и все флаги
            else:
                # Для остальных продуктов добавляем только столбец с флагом
                if flag_columns:
                    main_df[f'product_{product}_flag'] = product_df[flag_columns[0]]  # Добавляем только один столбец флага

        else:
            logging.warning(f"No result found for product: {product}")
            
    try:
        main_df.to_csv('/opt/airflow/your_project/flags_activity.csv', mode='a', header=False, index=False)
        logging.info("Combined data appended to flags_activity.csv.")
    except Exception as e:
        logging.error(f"Error saving data: {e}")
        
# Основная DAG функция
def create_dag(dag_name: str, default_args: dict, schedule_interval: str) -> DAG:
    with DAG(dag_name, default_args=default_args, schedule_interval=schedule_interval, catchup=False) as dag:
        input_file = '/opt/airflow/your_project/profit_table.csv'
        df = load_data_from_csv(input_file)

        with TaskGroup("product_processing", dag=dag) as group:
            transform_tasks = []
            for product in ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']:
                transform_task = PythonOperator(
                    task_id=f"transform_{product}",
                    python_callable=run_transform_script,
                    op_args=[product, df],
                    provide_context=True,
                    dag=dag,
                )
                transform_tasks.append(transform_task)

        # Задача для объединения результатов
        combine_task = PythonOperator(
            task_id='combine_results',
            python_callable=combine_results_and_save,
            provide_context=True,
            dag=dag,
        )

        # Зависимости
        for transform_task in transform_tasks:
            transform_task >> combine_task

        return dag

# Параметры по умолчанию для DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 11, 1),
}

dag = create_dag(
    'product_processing_dag',
    default_args,
    '0 0 5 * *'  # CRON выражение для ежедневного запуска
)

