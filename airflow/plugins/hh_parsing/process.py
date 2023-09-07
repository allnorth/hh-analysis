import fake_useragent
import requests
import json
import time
import psycopg2

def get_headers():

    user = fake_useragent.UserAgent().random
    headers = {'user-agent': user}
    return headers

def get_page(filter, period, pg=0):

    params = {
        'text': filter,
        'page': pg,
        'per_page': 100,
        'period': period
    }
    url = 'https://api.hh.ru/vacancies'
    req = requests.get(url, params, headers=get_headers())
    data = req.content.decode()
    req.close()
    return data

def get_vacancies(period):
    filters = ['"Data Engineer" OR "Инженер данных" OR "Дата Инженер"'
               , '"Data Analyst" OR "Аналитик данных"'
               , '"Data Scientist"']

    raw_vacancies = []
    for filter in filters:
        for page in range(0, 25):
            page_dict = json.loads(get_page(filter, period, page))
            if page_dict.get('items') is not None:
                for vacancy in page_dict.get('items'):
                    raw_vacancies.append(vacancy)
                else:
                    break

            if (page_dict['pages'] - page) <= 1:
                break
            time.sleep(0.25)



    vacancies = set()
    for vacancy in raw_vacancies:
        vacancies.add((int(vacancy['id']),                                                                                                  #id
                          vacancy['name'],                                                                                                  #vacancy_name
                          vacancy['published_at'],                                                                                          #published_at
                          bool(True if vacancy['archived'] == 'true' else False),                                                           #is_archive
                          bool(True if vacancy['type']['id'] == 'open' else False),                                                         #is_open
                          (vacancy['employer']['id'] if 'id' in vacancy['employer'] else None),                                             #employer_id
                          (vacancy['employer']['name'] if 'name' in vacancy['employer'] else None),                                         #employer_name
                          bool(vacancy['employer']['accredited_it_employer'] if 'accredited_it_employer' in vacancy['employer'] else None), #is_accredited_it_employer
                          (vacancy['experience']['id'] if 'id' in vacancy['experience'] else None),                                         #experience_id
                          (vacancy['experience']['name'] if 'name' in vacancy['experience'] else None),                                     #experience_name
                          (vacancy['area']['id'] if 'id' in vacancy['experience'] else None),                                               #area_id
                          (vacancy['area']['name'] if 'name' in vacancy['experience'] else None),                                           #area_name
                          (vacancy['salary']['from'] if vacancy['salary'] is not None else None),                                           #salary_from
                          (vacancy['salary']['to'] if vacancy['salary'] is not None else None),                                             #salary_to
                          (vacancy['salary']['currency'] if vacancy['salary'] is not None else None),                                       #salary_currency
                          bool(vacancy['salary']['gross'] if vacancy['salary'] is not None else None)))                                     #is_gross

    return vacancies

def load_data(vacancies):

    conn = psycopg2.connect(host='host.docker.internal',
                            port=5430,
                            user='postgres', 
                            password='password',
                            dbname='test')

    with conn.cursor() as cur:
        sql = """INSERT INTO stage.vacancy (   id
                                             , vacancy_name
                                             , published_at
                                             , is_archive
                                             , is_open
                                             , employer_id
                                             , employer_name
                                             , is_accredited_it_employer
                                             , experience_id
                                             , experience_name
                                             , area_id
                                             , area_name
                                             , salary_from
                                             , salary_to
                                             , salary_currency
                                             , is_gross)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        cur.executemany(sql, vacancies)
        conn.commit()