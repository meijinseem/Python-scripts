import pandas as pd
import pyodbc
import time
from decimal import Decimal
import json
import io
import requests
from datetime import datetime, timedelta


#<-------------------------------------- Определяем время и дату-------------------------------------------->


start = time.time()
today = datetime.today().date()   # Определяем сегодняшнюю дату
yesterday = datetime.today().date() - timedelta(days=2) # Определяем вчерашнюю дату
after_yesterday = datetime.today().date() - timedelta(days=4) # Определяем позавчерашнюю дату


#<--------------------------------------Данные для подключения к Metrica API-------------------------------------------->

header = {'Authorization': 'OAuth YOU TOKEN'}   #Прописываем Токен. Дата окончания  12.10.2024 
payload = {
    'date1': f'{after_yesterday}',
    'date2': f'{yesterday}',
    'metrics': 'ym:s:visits',
    'ids': 123456789,      # id счетчика 
    'dimensions': 'ym:s:clientID, ym:s:firstTrafficSourceName, ym:s:dateTime, ym:s:firstSearchEngineRootName, ym:s:referer, ym:s:firstVisitDateTime, ym:s:firstUTMSource, ym:s:firstUTMCampaign, ym:s:firstUTMContent, ym:s:firstUTMTerm',
    'accuracy': 'full',   # Семплирование: Все данные
    'limit': 100000,      # Лимит строк
    'filters' : "ym:s:isNewUser=='Yes'"   # Только новые пользователи
}


#<--------------------------------------Данные для подключения к БД-------------------------------------------->

server = 'SERVERNAME'     # Имя сервера
database = 'DBNAME'   # Название БД
username = 'USERNAME'   # Имя пользователя
password = 'PASSWORD'  # Пароль для подключения




#<--------------------------------------Функция для подключения к БД и формировании SQL Запроса-------------------------------------------->
def db_insert(date_insert,count_data):
 
    # Прописываем данные для подключения
    connection_string = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

    # Пишем запрос, указывая колонки в которых будет добавляться значение и переменные в VALUES()
    sql_insert_query = '''INSERT INTO [dbo].[MetrciaSource] (ClientID, FirstTrafficSource, DateAndTime, FirstSearchEngine, Referrer, DateAndTimeFirst, UTMSource, UTMContent, UTMTerm, UTMCampaignName, UTMCampaignID)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''
    # Подключаемся
    conn = pyodbc.connect(connection_string)

    try:
        cursor = conn.cursor()   # Создаем объект cursor который обеспечивает интерфейс взаимодействия с БД
        cursor.executemany(sql_insert_query, date_insert)   # Выполняем sql запрос с использованием подготовленного запроса и значениями 
        conn.commit()     # Фиксация изменений в БД
        return f"{count_data} strings were added to the dataset!"

    except TypeError as ex:

        return f"Error: {ex}"
    
    finally:
        cursor.close()
        conn.close()   # В любом случае закрываем соденинение




#<--------------------------------------Главная функция-------------------------------------------->
def main():
    r = requests.get('https://api-metrika.yandex.net/stat/v1/data.csv', params=payload, headers=header)   # делаем запрос на сервер метрики
    rawData = pd.read_csv(io.StringIO(r.text), dtype={'Sessions': str})     #Конвертируем данные в DataFrame
    rawData = rawData[1:]    #Удаляем первую строку, которая отображает "итого"

    # В данных есть особенность, они могут дублироваться. Удаляем дубли и оставляем только строки с самой первой датой визита
    df_no_duplicates = rawData.sort_values(by=['Date and time of visit ']).drop_duplicates(subset='ClientID', keep='first')

    # Делим колонку на две, т.к у нас в колонке данные в формате "Название компании|48454221212"
    df_no_duplicates[['UTM CampaignName', 'UTM CampaignID']] = df_no_duplicates['UTM Campaign']. str.split('|', n=1 , expand= True )


    # оставляем только нужные колонки, которые будут передаваться в БД
    df_no_duplicates = df_no_duplicates[['ClientID', 'First traffic source',
       'Date and time of visit ', 'First search engine', 'Referrer', 'Date and time of first visit',
       'UTM Source', 'UTM Content', 'UTM Term','UTM CampaignName','UTM CampaignID']]
    
    # Заполняем пропуски на дефолтное значение метрки 'Not specified'
    df = df_no_duplicates.fillna('Not specified')


    #Длина строк во всех колонках теперь не больше 50 символов. Чтоб не было конфликта при выгрузке в БД
    df = df.apply(lambda col: col.apply(lambda x: x[:50] if isinstance(x, str) else x))


    # Форматируем дату в дату, чтоб не было конфликта при выгрузке в БД
    df['Date and time of visit '] = pd.to_datetime(df['Date and time of visit '])
    df['Date and time of first visit'] = pd.to_datetime(df['Date and time of first visit'])


    # Форматируем ClientID  в децимальный номер, чтоб не было конфликта при выгрузке в БД
    df['ClientID'] = df['ClientID'].apply(lambda x: Decimal(str(x)))

    count_data = len(df)
    records = df.values.tolist()  # Превращаем DataFrame в список, для загрузки данных в БД
    
    return db_insert(records,count_data)   # Запускаем функцию




#<--------------------------------------Точка входа-------------------------------------------->
if __name__=='__main__':
    result = main()
    print(50*'_')
    print(50*' ')
    print(result)
    end = time.time()
    total = round(end - start, 1)
    print(f"The script was completed in {total} seconds")
