import requests
import pandas as pd
import json
from datetime import datetime, timedelta
from datetime import date

def forma_date(x):
    x = datetime.strptime(x, "%Y-%m-%dT%H:%M:%S%z")
    formatted_date = x.strftime("%Y-%m-%d %H:%M:%S")
    return formatted_date



def join_source(x):
    result_string = ' → '.join(x)
    return result_string

###функция, которая строки с источниками и возвращает их в удобочинаемы для разделения вид.
def direct_separate(x):
    try:
        direct_cpc_key = 'Яндекс.Директ'
        seo_key = 'SEO'
        direct_key = 'Прямые визиты'
        yandex_cpc_key = 'yandex'
        maps_key = 'maps'
        if direct_cpc_key in x:
            separate_string = x.split(' → ')
            index_source = [0,2,-1]
            source_string = [separate_string[i] for i in index_source]
            return ','.join(source_string)
        elif seo_key in x:
            separate_string = x.split(' → ')
            index_source = [0,1]
            source_string = [separate_string[i] for i in index_source]
            return ' '.join(source_string)
        elif direct_key in x:
            separate_string = x.split(' → ')
            index_source = [0]
            source_string = [separate_string[i] for i in index_source]
            return ','.join(source_string)
        elif yandex_cpc_key in x and maps_key not in x:
            separate_string = x.split(' → ')
            index_source = [0,2]
            source_string = [separate_string[i] for i in index_source]
            source_string[0] = 'Яндекс.Директ'
            return ','.join(source_string)
        else:
            separate_string = x.split(' → ')
            return ','.join(separate_string)
    except IndexError:
        pass


def main():
###Проверяем какой сегодня день недели по счету, если понедельник, то диапазон дат для обновления увеличивается на +2 дня
###Это нужно для того, чтоб каждый понедельник не обновлять даты выгрузки в скрипте   

    today_weekday = datetime.today().date().weekday() 
    if today_weekday == 0:
        yesterday = datetime.today().date() - timedelta(days=1)
        after_yesterday = datetime.today().date() - timedelta(days=4)
    else:
        yesterday = datetime.today().date() - timedelta(days=1)
        after_yesterday = datetime.today().date() - timedelta(days=2)
    
###Пишем API запрос
    api_key = 'YOUR TOKEN KEY'
    project =12345
    api_url =f'https://cloud.roistat.com/api/v1/project/integration/order/list?project={project}&key={api_key}'
    payload = {"filters": {"and": [["creation_date",">",f"{after_yesterday}T23:59:59+0000"],["creation_date","<",f"{yesterday}T23:59:59+0000"]]}, "extend": [
            "visit"]}

 ###Форматируем API запрос в DataFrame    
    r = requests.post(api_url,json=payload)
    data = r.json()
    data = data['data']
    dataframe2 = pd.json_normalize(data)
    dataframe2 = dataframe2[['creation_date','visit_id','visit.source.display_name_by_level']]


    dataframe2['creation_date'] = dataframe2['creation_date'].apply(forma_date) ###Переводим дату в нужный формат
    dataframe2['creation_date'] = pd.to_datetime(dataframe2['creation_date'])  ###Переводим дату в нужный формат(2)

    dataframe2['visit.source.display_name_by_level'] = dataframe2['visit.source.display_name_by_level'].apply(join_source)  ###Переводим источники в нужный формат
    dataframe2['visit.source.display_name_by_level'] = dataframe2['visit.source.display_name_by_level'].apply(direct_separate) ###Переводим источники в нужный формат(2)

    dataframe2['visit_id'] = dataframe2['visit_id'].astype(str).astype(int)  ###Переводим номер визита в нужный формат



    with open(r'\\XXXXXXXX\XXXXXXX\XXXXXXXXXX\XXXXXXXXXXXXXXX\Roistat источники брать от сюда.xlsx', 'rb') as file: ###Читаем DataFrame со страыми данными
        dataframe = pd.read_excel(file, sheet_name='Лист1')

    count_dataframe = len(dataframe)  ###Строк в датасете до добавления
    main_columns_name = list(dataframe.columns)  ###Делаем список с основным названием колонок
    dataframe2.columns = main_columns_name  

###Объединяем датафреймы
    result_df = pd.concat([dataframe, dataframe2], ignore_index=True)
###Удаляем возможные дубликаты 
    result_df_no_duplicates = result_df.sort_values(by=['Дата визита']).drop_duplicates(subset='№ визита', keep='first')

  
    count_result_df_no_duplicates = len(result_df_no_duplicates) ###Считаем стровки в датасете после
    last_dataframe_date = result_df_no_duplicates['Дата визита'].max().date()
    
    #result_df_no_duplicates.to_excel(r'C:\Users\gav\Desktop\UA - datasets/roistat_source_db_1.xlsx', sheet_name='Лист1', index=False)
    try:
        result_df_no_duplicates.to_excel(r'\\XXXXXXXX\XXXXXXX\XXXXXXXXXX\XXXXXXXXXXXXXXX\Roistat источники брать от сюда.xlsx', sheet_name='Лист1', index=False)
        if last_dataframe_date == yesterday:
            print('_ '*25)
            print('All right!')
            print('_ '*25)
            print(f'{count_result_df_no_duplicates-count_dataframe} values were added to the dataset') 
        else:
            print('The last date in the dataset is not equal to yesterday...')
    except:
        print('Something went wrong when trying to save the datacet...')


if __name__=='__main__':
    main()



    


    


    
