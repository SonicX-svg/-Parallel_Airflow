import pandas as pd
import sys
import io
import logging
import os
import fcntl 

# Настройка логирования
# Пример добавления логирования в скрипт
logging.basicConfig(filename='/opt/airflow/your_project/script.log', level=logging.INFO)

logging.info("Script started")

def transform(profit_table, date, product):
    """ Собирает таблицу флагов активности по продукту
        на основании прибыли и кол-ва транзакций
        
        :param profit_table: таблица с суммой и кол-вом транзакций для одного продукта
        :param date: дата расчёта флага активности
        :param product: конкретный продукт (например, 'a', 'b', 'c' и т.д.)
        
        :return df_tmp: pandas DataFrame флагов за указанную дату
    """
    logging.info(f"Start transforming data for product: {product}")

    # Логирование столбцов DataFrame
    logging.info(f"Columns in the input data: {profit_table.columns.tolist()}")
    
    # Вычисление начала и конца периода для расчёта флагов
    start_date = pd.to_datetime(date) - pd.DateOffset(months=2)
    end_date = pd.to_datetime(date) + pd.DateOffset(months=1)
    
    # Генерация списка дат для анализа
    date_list = pd.date_range(
        start=start_date, end=end_date, freq='M'
    ).strftime('%Y-%m-01')
    
    # Фильтруем таблицу по датам
    df_tmp = (
        profit_table[profit_table['date'].isin(date_list)]
        .drop('date', axis=1)
        .groupby('id')
        .sum()
    )
    
    # Применяем флаг активности только для одного продукта
    if f'sum_{product}' not in df_tmp.columns or f'count_{product}' not in df_tmp.columns:
        logging.error(f"Expected columns 'sum_{product}' or 'count_{product}' not found in the data.")
        return pd.DataFrame()  # Возвращаем пустой DataFrame, чтобы избежать дальнейших ошибок
    
    df_tmp[f'flag_{product}'] = (
        df_tmp.apply(
            lambda x: x[f'sum_{product}'] != 0 and x[f'count_{product}'] != 0,
            axis=1
        ).astype(int)
    )
    
    # Отбираем только флаговый столбец и сбрасываем индекс
    df_tmp = df_tmp[[f'flag_{product}']].reset_index()
    
    logging.info(f"Transformation completed for product {product}, data shape: {df_tmp.shape}")
    
    return df_tmp
    
def write_to_file_with_lock(filename, data, product):
    """Записывает данные в файл с блокировкой"""
    try:
        logging.info(f"Attempting to write to file: {filename}")
        logging.info(f"Data to be written: {data.head()}")

        # Открываем файл с блокировкой
        with open(filename, 'a') as f:
            # Получаем блокировку для файла
            fcntl.flock(f, fcntl.LOCK_EX)

            # Чтение данных из файла, если он уже существует
            if os.path.exists(filename):
                # Читаем существующий файл
                existing_data = pd.read_csv(filename)
                # Проверяем, есть ли уже столбцы с флагами для этого продукта
                if f'flag_{product}' not in existing_data.columns:
                    # Если столбца нет, добавляем новый столбец с данными флага
                    existing_data = existing_data.merge(data[['id', f'flag_{product}']], on='id', how='left')
                    # Перезаписываем файл с добавленным столбцом
                    existing_data.to_csv(f, index=False)
                else:
                    logging.info(f"Column 'flag_{product}' already exists in the file.")
            else:
                # Если файл пустой, то создаем его с нужными данными
                data.to_csv(f, index=False, header=f.tell() == 0)

            # Снимаем блокировку
            fcntl.flock(f, fcntl.LOCK_UN)

        logging.info(f"Data successfully written to {filename}")

    except Exception as e:
        logging.error(f"Error while writing to file {filename}: {e}")
        
def main():
    # Чтение данных из stdin
    input_data = sys.stdin.read()  # Читаем все данные из stdin

    # Чтение аргументов командной строки (параметр продукта)
    if len(sys.argv) != 2:
        logging.error("Expected 2 arguments (product), but received " + str(len(sys.argv)))
        sys.exit(1)
    
    product = sys.argv[1]  # Получаем название продукта из аргумента
    logging.info(f"Processing product: {product}")
    
    # Загружаем данные из CSV, переданные через stdin
    try:
        df = pd.read_csv(io.StringIO(input_data))  # Читаем CSV-данные из строки
        logging.info("Profit data loaded successfully from stdin.")
    except Exception as e:
        logging.error(f"Error reading data from stdin: {e}")
        sys.exit(1)
    
    # Проверка, есть ли нужные столбцы в данных
    if f'sum_{product}' not in df.columns or f'count_{product}' not in df.columns:
        logging.error(f"Expected columns 'sum_{product}' or 'count_{product}' not found in the data.")
        sys.exit(1)  # Завершаем скрипт с ошибкой, если не хватает нужных столбцов

    # Фильтруем данные для одного продукта
    product_data = df[['id', 'date', f'sum_{product}', f'count_{product}']]
    
    # Преобразуем данные для выбранного продукта
    flags_activity = transform(product_data, '2024-03-01', product)
    logging.info("Data transformation completed")
    
    # Проверяем размер данных перед записью
    if flags_activity.empty:
        logging.warning("Generated data is empty, nothing to write to the file.")
    else:
        # Преобразуем DataFrame в строку CSV
        csv_data = flags_activity.to_csv(index=False)  # Убираем индексы для чистоты
        logging.info(f"DATA RETURN IS: {flags_activity.head(10)}")
        # Выводим результат на stdout
        sys.stdout.write(csv_data)  # Передаем строку CSV в stdout

        # Возвращаем результат для XCom
        return csv_data  # Возвращаем данные для передачи в XCom

if __name__ == "__main__":
    main()

