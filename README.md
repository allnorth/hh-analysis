# Анализ рынка вакансий HH.ru

## Цели проекта

Создание ETL-процесса формирования витрин данных для анализа вакансий доступных на сайте hh.ru

## Используемые технологии

docker compose \
python \
airflow \
postgresql \
pgadmin

## Реализация
1. Для развертывания проекта используется docker compose
2. Для оркестрации ETL-процесса используется Apache Airflow
3. Для получения данных из API hh.ru и последующей загрузки в базу данных используется python-скрипт, добавленный в качестве Airflow-плагина
4. Для формирования хранилища данных используются PL/pgSQL-скрипты, вызываемые Airflow
5. Модель Data Vault, модель позволяет достаточно просто добавить новые источники
6. Для формироваяни необходимых витрин данных используются табличные функции на языке PL/pgSQL

## Установка и запуск проекта

### Запуск
1. Установить [docker](https://docs.docker.com/engine/install/)
2. Выполнить команду ```docker compose up -d```

### Управление
1. [Airflow](http://localhost:8080/)
2. [pgadmin](http://localhost)

## Получение данных
1. В pgadmin подключаемся к БД vacancy
2. Выполняем необходимый запрос:

   Запрос для получения по всем направлениям \
   ```SELECT * FROM mart.get_vacancies()```

   Запрос для получения витрины DE по регионам \
   ```SELECT * FROM mart.get_vacancies_by_region(ARRAY['Data Engineer'])```

   Запрос для получения витрины DA по регионам \
   ```SELECT * FROM mart.get_vacancies_by_region(ARRAY['Data Analyst'])```

   Запрос для получения витрины DS по регионам \
   ```SELECT * FROM mart.get_vacancies_by_region(ARRAY['Data Scientist'])```
   
   Запрос для получения витрины DE по компаниям \
   ```SELECT * FROM mart.get_vacancies_by_employer(ARRAY['Data Engineer'])```

   Запрос для получения витрины DA по компаниям \
   ```SELECT * FROM mart.get_vacancies_by_employer(ARRAY['Data Analyst'])```
   
   Запрос для получения витрины DS по компаниям \
   ```SELECT * FROM mart.get_vacancies_by_employer(ARRAY['Data Scientist']);```
   
   
