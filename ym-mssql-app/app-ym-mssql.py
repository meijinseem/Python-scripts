import pandas as pd
import pyodbc
import time
from decimal import Decimal
import json
import io
import requests
from datetime import datetime, timedelta
import numpy as np


#<-------------------------------------- Определяем время и дату-------------------------------------------->


start = time.time()
today = datetime.today().date()   # Определяем сегодняшнюю дату
date1 = datetime.today().date() - timedelta(days=1) # Определяем вчерашнюю дату
date2 = datetime.today().date() - timedelta(days=1) # Определяем позавчерашнюю дату

#<-------------------------------------- Определяем токен и IP которые могут изменяться-------------------------------------------->

token = requests.get('http://999.999.99.99/token/').text.split(',')[0]  
ip_adress = requests.get('http://999.999.99.99/token/').text.split(',')[1]

#<--------------------------------------Данные для подключения к Metrica API Первый запрос-------------------------------------------->

header_first = {'Authorization': f'OAuth {token}'}   #Прописываем Токен. Дата окончания  12.10.2024 
payload_first = {
    'date1': f'{date1}',
    'date2': f'{date2}',
    'metrics': 'ym:s:visits',
    'ids': 12345678,      # id счетчика 
    'dimensions': 'ym:s:clientID, ym:s:firstTrafficSourceName, ym:s:firstSearchEngineRootName, ym:s:firstVisitDateTime, ym:s:firstUTMSource, ym:s:firstUTMCampaign, ym:s:firstUTMContent, ym:s:firstUTMTerm, ym:s:referer, ym:s:startURL',
    'accuracy': 'full',   # Семплирование: Все данные
    'limit': 100000,      # Лимит строк
    'filters' : "ym:s:isNewUser=='Yes'"   # Только новые пользователи
}

#<--------------------------------------Данные для подключения к Metrica API Второй запрос запрос-------------------------------------------->

header_second = {'Authorization': f'OAuth {token}'}  #Прописываем Токен. Дата окончания  12.10.2024 
payload_second = {
    'date1': f'{date1}',
    'date2': f'{date2}',
    'metrics': 'ym:s:visits',
    'ids': 12345678,      # id счетчика 
    'dimensions': 'ym:s:clientID, ym:s:firstVisitDateTime, ym:s:regionCity, ym:s:deviceCategory',
    'accuracy': 'full',   # Семплирование: Все данные
    'limit': 100000,      # Лимит строк
    'filters' : "ym:s:isNewUser=='Yes'"   # Только новые пользователи
}





#<--------------------------------------Данные для подключения к БД-------------------------------------------->

server = ip_adress     # Имя сервера
database = 'db_metrica'   # Название БД
username = 'username'   # Имя пользователя
password = 'password'  # Пароль для подключения


#<--------------------------------------Функция для замены значений Refferer там, где они не нужны-------------------------------------------->

def change_value_condition(df, column_to_change, condition_column, condition_value, new_value):
    mask = df[condition_column].isin(condition_value)      # Проверяем, есть ли значение в условной ячейке
    df.loc[mask, column_to_change] = new_value        # Заменяем значение в указанном столбце, если условие выполняется
    return df


#<--------------------------------------Функция, которая режет параметры в URL для чистоты-------------------------------------------->

def get_prefix(input_str):      
    parts = input_str.split("?", 1)  
    return parts[0] if len(parts) > 0 else input_str

#<--------------------------------------Функция, которая делит колонку на две, т.к у нас в колонке данные в формате "Название компании|48454221212 -------------------------------------------->

def split_column_by_pipe(df, column_name, new_column_name):
    try:
        df[new_column_name] = df[column_name].str.split('|', n=1, expand=True)
        return df
    except Exception as e:    #в любом случае возвращаем датасет
        return df



#<--------------------------------------Функция для подключения к БД и формировании SQL Запроса-------------------------------------------->
def db_insert(date_insert,count_data,max_date,min_date):
 
    # Прописываем данные для подключения
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

    # Пишем запрос, указывая колонки в которых будет добавляться значение и переменные в VALUES()
    sql_insert_query = '''INSERT INTO [dbo].[yaMetrica] (ClientID, FirstTrafficSource, FirstSearchEngine, DateAndTimeFirst, UTMSource,  UTMCampaignName, UTMCampaignID, UTMContent, UTMTerm, Referrer, LandingPage, City, DeviceType)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
    # Подключаемся
    conn = pyodbc.connect(connection_string)

    try:
        cursor = conn.cursor()   # Создаем объект cursor который обеспечивает интерфейс взаимодействия с БД
        cursor.executemany(sql_insert_query, date_insert)   # Выполняем sql запрос с использованием подготовленного запроса и значениями 
        conn.commit()     # Фиксация изменений в БД
        return (
            f"{count_data} strings were added to the dataset! \n"
            f"min date: {min_date}, max date: {max_date}"
            )

    except TypeError as ex:

        return f"Error: {ex}"
    
    finally:
        cursor.close()
        conn.close()   # В любом случае закрываем соденинение




#<--------------------------------------Главная функция-------------------------------------------->
def main():
    r_first = requests.get('https://api-metrika.yandex.net/stat/v1/data.csv', params=payload_first, headers=header_first)   # делаем запрос 1 на сервер метрики
    r_second = requests.get('https://api-metrika.yandex.net/stat/v1/data.csv', params=payload_second, headers=header_second)   # делаем запрос 2 на сервер метрики

    rawData_first = pd.read_csv(io.StringIO(r_first.text), dtype={'Sessions': str})     #Конвертируем данные в DataFrame
    rawData_second = pd.read_csv(io.StringIO(r_second.text), dtype={'Sessions': str})

    rawData_first = rawData_first[1:]    #Удаляем первую строку, которая отображает "итого"
    rawData_second = rawData_second[1:]

    # В данных есть особенность, они могут дублироваться. Удаляем дубли и оставляем только строки с самой первой датой визита
    rawData_first = rawData_first.sort_values(by=['Date and time of first visit']).drop_duplicates(subset='ClientID', keep='first')
    rawData_second = rawData_second.sort_values(by=['Date and time of first visit']).drop_duplicates(subset='ClientID', keep='first')

    #  Джоиним два датасета
    data = rawData_first.merge(rawData_second, on='ClientID', how='left')


    # Колонка 'UTM Campaign' разделиться на две:  ''UTM CampaignName' и 'UTM CampaignID'
    data['UTM CampaignName'] = data['UTM Campaign']
    data['UTM CampaignID'] = np.nan

    # делим колонку на две, т.к у нас в колонке данные в формате "Название компании|48454221212
    data = split_column_by_pipe(data, 'UTM CampaignName', ['UTM CampaignName', 'UTM CampaignID'])


    # оставляем только нужные колонки, которые будут передаваться в БД
    data = data[['ClientID', 'First traffic source', 'First search engine',
       'Date and time of first visit_x', 'UTM Source', 'UTM Content', 'UTM Term', 'Referrer', 
    'Landing page','City', 'Device type','UTM CampaignName','UTM CampaignID']]
    
    # Заполняем пропуски на дефолтное значение метрки 'Not specified'
    data = data.fillna('Not specified')

    # режем параметры в урлах для чистоты данных
    data['Landing page'] = data['Landing page'].apply(get_prefix)
    data['Referrer'] = data['Referrer'].apply(get_prefix)


    #Длина строк во всех колонках теперь не больше 60 символов. Чтоб не было конфликта при выгрузке в БД
    data = data.apply(lambda col: col.apply(lambda x: x[:60] if isinstance(x, str) else x))   

   # Форматируем дату в дату, чтоб не было конфликта при выгрузке в БД
    data['Date and time of first visit_x'] = pd.to_datetime(data['Date and time of first visit_x'])

    # Форматируем ClientID  в децимальный номер, чтоб не было конфликта при выгрузке в БД
    data['ClientID'] = data['ClientID'].apply(lambda x: Decimal(str(x)))


    # Меняем порядок колонок
    data = data.reindex(columns=['ClientID', 'First traffic source', 'First search engine',
       'Date and time of first visit_x', 'UTM Source','UTM CampaignName','UTM CampaignID', 'UTM Content', 'UTM Term', 'Referrer', 
    'Landing page','City', 'Device type'])


    #Задаем список, который содержит в себе те источники, реферер по которым нас не интересует
    dict_traffic_condition = ['Ad traffic','Internal traffic','Direct traffic','Search engine traffic']

    #Оставляем реферер только у важных источников
    data = change_value_condition(data, 'Referrer', 'First traffic source', dict_traffic_condition, 'Not specified')


    max_date = data['Date and time of first visit_x'].max()
    min_date = data['Date and time of first visit_x'].min()


    count_data = len(data)
    records = data.values.tolist()  # Превращаем DataFrame в список, для загрузки данных в БД
    
    
    return db_insert(records,count_data,max_date,min_date) #запускаем функцию
          
          
            




#<--------------------------------------Точка входа-------------------------------------------->
if __name__=='__main__':
    result = main()
    print(50*'_')
    print(50*' ')

    print(result)
    end = time.time()
    total = round(end - start, 1)
    print(f"The script was completed in {total} seconds")