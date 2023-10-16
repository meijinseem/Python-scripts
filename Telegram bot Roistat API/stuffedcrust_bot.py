
#!/usr/bin/python
# -*- coding: utf-8 -*-

import requests
import io
import pandas as pd
import json
from datetime import datetime, timedelta
import telebot


# Указываем токен бота
bot = telebot.TeleBot('YOUR TOKEN') 

# Задаем функцию которая забирает в качестве аргумента значение srt "2023-10-15T13:36:26.825545+03:00" и возвращает дату 2023-10-15
def str_to_date(x):
  x = x.split('T')[0]
  x = datetime.strptime(x, "%Y-%m-%d").date()
  return x

# Задаем основную функцию
def main():
	yesterday = datetime.today().date() - timedelta(days=1) # Определяем вчерашнюю дату
	after_yesterday = datetime.today().date() - timedelta(days=2) # Определяем позавчерашнюю дату

	api_key_roistat = 'YOUR TOKEN KEY' # Определяем токен для подключения к проекту по HTTP запросам
	project_roistat = 123456789 # Номер проекта
	api_url =f'https://cloud.roistat.com/api/v1/project/integration/order/list?project={project}&key={api_key}' # УРЛ для подключения
	payload = {"filters": {"and": [["creation_date",">",f"{after_yesterday}T23:59:59+0000"],["creation_date","<",f"{yesterday}T23:59:59+0000"]]}} # Входные параметры для филтьтрации 

	r = requests.post(api_url,json=payload) # Создаем POST запрос на сервер 
	data = r.json() # Забираем данные в JSON формате
	df = pd.DataFrame(data['data']) # Форматируем в DataFrame 
	count_leads = len(df) # Считаем кол-во лидов 
 
	max_date_dataset = str_to_date(df.creation_date.max()) # Находим последнюю дату в датасете за вчера
	min_date_dataset = str_to_date(df.creation_date.min()) # Находим первую дату в датасете за вчера

	result = f'За вчерашний день: {yesterday} Roistat насчитывает {count_leads} лида в проекте project.ru.' # Формируем результат ответа

	if max_date_dataset == min_date_dataset and min_date_dataset == yesterday: # прверяем условием, сходятся даты в датасете с объявленными переменными
		return result
	else:
		print('Даты не соотсветсвуют, проверьте датасет.')



if __name__ == '__main__':

	@bot.message_handler(commands=['start']) # Определяем входные команды для бота

	def start(message): # Задаем функцию которая отвечает в чате основной функцией main()
		try:
			reply = main() # Записываем ответ функции в переменную 
			bot.send_message(message.chat.id, reply) # Формируем ответ бота 
		except Exception as e:
			print(f"An error occurred: {e}")

	bot.polling(non_stop =True) # Запускаем бота

	
