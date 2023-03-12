# Импорт необходимых библиотек
import sqlite3

import pandas as pd
import requests


def extract_currency(date):
  start_date = date
  end_date = date
  base = 'EUR'
  symbols='USD'
  url = f'https://api.exchangerate.host/timeseries?start_date={start_date}&' \
                                                  f'end_date={end_date}&' \
                                                  f'base={base}&' \
                                                  f'symbols={symbols}'
  response = requests.get(url)
  data = response.json()
  df = pd.DataFrame(data={'date': [date],
                          'currency': [data['base']],
                          'rate': [data['rates'][date]['USD']]})
  return df


def extract_data(date):
  url = f'https://github.com/dm-novikov/stepik_airflow_course/blob/main/data_new/{date}.csv?raw=true'
  df = pd.read_csv(url)
  return df




def insert_to_db(data, table_name, conn):
  data.to_sql(table_name, con=conn, index=False, if_exists='append')

def sql_query(sql, conn):
  cursor = conn.cursor()
  cursor.execute(sql)
  for i in cursor:
      print(i)
  conn.commit()
  conn.close()

def send_report():
  print('По кредам из ноубука учебного не работала, а чет свои вставлять не хочу, про отправку понял')
  pass


# Запустите ваш код в функции main

from datetime import date

# Напишите генерацию дат, так чтобы у вас получился список
# 2021-01-02, 2021-01-03 ... etc
# Нужны даты с 2021-01-01 по 2021-01-04
start_date = date(2021, 1, 1)
end_date = date(2021, 1, 4)


def main(date, email, conn, path='airflow_course.db'):
    # Выгружаем данные по валютам и из источника

    currency = extract_currency(date)
    data = extract_data(date)

  #  Создайте необходимые таблицы если это нужно
  #  'currency', 'data' и 'join_data (таблица с объединенными данными) currency	value	date rate
  #   sql_query("""CREATE TABLE join_data
  #             (date datetime,
  #              currency text,
  #              value int,
  #              rate float);
  #
  #             """, sqlite3.connect(path))

    # Вставляем данные в БД
    # У вас долнжо получиться 2 таблицы CURRENCY, DATA
    insert_to_db(currency, 'currency', conn=sqlite3.connect(path))
    insert_to_db(data, 'data', conn=sqlite3.connect(path))

    # Объединение данных в отдельную таблицу JOIN_DATA
    # CURRENCY, DATA объединить через JOIN по дате и валюте
    # Используйте insert into select ... inner join ...
    # И соответственно создать таблицу для этого
    sql_query("""
            INSERT INTO join_data  
            SELECT date,
                   currency,
                   value,	
                   rate 
              FROM currency
        INNER JOIN data 
             USING (date, currency)
            """, sqlite3.connect(path))

    # Очистка временных таблиц CURRENCY, DATA
    # Это те таблицы куда произошла вставка данных
    # Используйте truncate
    sql_query('DELETE FROM currency', sqlite3.connect(path))
    sql_query('DELETE FROM data', sqlite3.connect(path))

    # Получение данных из таблицы JOIN_DATA за конкретный день
    # Используйте date при формировании запроса select * from ... where date ='...'
    report = sql_query(f"""
                        SELECT * 
                          FROM join_data 
                         WHERE date ='{date}'
                   """, sqlite3.connect(path))
    send_report()

dates_list = pd.date_range(
    min(start_date, end_date),
    max(start_date, end_date)
).strftime('%Y-%m-%d').tolist()
print(dates_list)

for date in dates_list:
    main(date, 'email', conn=sqlite3.connect('airflow_course.db'))

