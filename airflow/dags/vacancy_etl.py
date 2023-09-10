import pendulum
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import SQLExecuteQueryOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.empty import EmptyOperator
from airflow import DAG
from functools import partial
from hh_parsing.process import load_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'email': 'a@a.ru',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(dag_id='vacancy_etl',
         default_args=default_args,
         start_date=pendulum.datetime(2023,9,6, tz=pendulum.timezone('Europe/Moscow')),
         schedule='@daily',
         ) as dag:

    t_start = EmptyOperator(task_id='Start')

    t_truncate_stage = SQLExecuteQueryOperator(
        task_id = 'truncate_stage',
        conn_id='postgres_vacancy_db',
        sql =   """ TRUNCATE TABLE stage.vacancy;""")

    t_load_data = PythonOperator(
        task_id='load_data',
        python_callable = partial(load_data, conn_id='postgres_vacancy_db'))

    t_etl_core_hub_vacancy = SQLExecuteQueryOperator(
        task_id = 'load_hub_vacancy',
        conn_id='postgres_vacancy_db',
        sql =   """ INSERT INTO core.hub_vacancy (record_source, load_date, external_id)
                    SELECT        DISTINCT
                                    'hh' as record_source
                                  , now() as load_date
                                  , id as external_id
                    FROM        stage.vacancy AS v
                    LEFT JOIN   core.hub_vacancy AS hv ON hv.external_id = v.id::varchar
                    WHERE   hv.vacancy_id IS NULL;
                """)

    t_etl_core_hub_employer = SQLExecuteQueryOperator(
        task_id = 'load_hub_employer',
        conn_id='postgres_vacancy_db',
        sql =   """ INSERT INTO core.hub_employer (record_source, load_date, external_id)
                    SELECT      DISTINCT
                                  'hh' as record_source
                                , now() as load_date
                                , v.employer_id as external_id
                    FROM        stage.vacancy AS v
                    LEFT JOIN   core.hub_employer AS hv ON hv.external_id = v.employer_id::varchar
                    WHERE       hv.employer_id IS NULL
                                AND v.employer_id IS NOT NULL;
                """)

    t_etl_core_hub_experience = SQLExecuteQueryOperator(
        task_id = 'load_hub_experience',
        conn_id='postgres_vacancy_db',
        sql =   """ INSERT INTO core.hub_experience (record_source, load_date, external_id)
                    SELECT      DISTINCT
                                  'hh' as record_source
                                , now() as load_date
                                , v.experience_id as external_id
                    FROM        stage.vacancy AS v
                    LEFT JOIN   core.hub_experience AS hv ON hv.external_id = v.experience_id::varchar
                    WHERE       hv.experience_id IS NULL
                                AND v.experience_id IS NOT NULL;
                """)

    t_etl_core_hub_area = SQLExecuteQueryOperator(
        task_id = 'load_hub_area',
        conn_id='postgres_vacancy_db',
        sql =   """ INSERT INTO core.hub_area (record_source, load_date, external_id)
                    SELECT      DISTINCT
                                'hh' as record_source
                                , now() as load_date
                                , v.area_id as external_id
                    FROM        stage.vacancy AS v
                    LEFT JOIN   core.hub_area AS hv ON hv.external_id = v.area_id::varchar
                    WHERE       hv.area_id IS NULL
                                AND v.area_id IS NOT NULL;
                """)

    t_etl_core_hub_salary = SQLExecuteQueryOperator(
        task_id = 'load_hub_salary',
        conn_id='postgres_vacancy_db',
        sql =   """ INSERT INTO core.hub_salary (record_source, load_date, external_id)
                    SELECT        DISTINCT
                                  'hh' as record_source
                                , now() as load_date
                                , concat(coalesce(v.salary_from, 0.0)::varchar, coalesce(v.salary_to, 0.0)::varchar, coalesce(v.salary_currency::varchar, ''), coalesce(v.is_gross, FALSE))
                    FROM        stage.vacancy AS v
                    LEFT JOIN   core.hub_salary AS hv ON hv.external_id = concat(coalesce(v.salary_from, 0.0)::varchar, coalesce(v.salary_to, 0.0)::varchar, coalesce(v.salary_currency::varchar, ''), coalesce(v.is_gross, FALSE))
                    WHERE       hv.salary_id IS NULL
                                AND concat(coalesce(v.salary_from, 0.0)::varchar, coalesce(v.salary_to, 0.0)::varchar, coalesce(v.salary_currency::varchar, ''), coalesce(v.is_gross, FALSE)) IS NOT NULL;
                """)

    t_etl_core_sat_vacancy = SQLExecuteQueryOperator(
        task_id = 'load_sat_vacancy',
        conn_id='postgres_vacancy_db',
        sql =   """ MERGE INTO core.sat_vacancy AS trg
                    USING   (   SELECT  DISTINCT
                                          hv.vacancy_id
                                        , 'hh' as record_source
                                        , now() as load_date
                                        , v.vacancy_name
                                        , v.published_at
                                        , v.is_archive
                                        , v.is_open
                                        ,   CASE
                                                WHEN    v.vacancy_name LIKE ('%Data Engineer%')
                                                        OR v.vacancy_name LIKE ('%Инженер данных%')
                                                        OR v.vacancy_name LIKE ('%Дата Инженер%')
                                                THEN    'Data Engineer'

                                                WHEN    v.vacancy_name LIKE ('%Data Analyst%')
                                                        OR v.vacancy_name LIKE ('%Аналитик данных%')
                                                THEN    'Data Analyst'

                                                WHEN    v.vacancy_name LIKE ('%Data Scientist%')
                                                THEN    'Data Scientist'

                                                WHEN    v.vacancy_name LIKE ('%Data Scientist%')
                                                THEN    'Another'
                                            END AS type
                                        ,    CASE
                                                WHEN    v.vacancy_name LIKE ('%Стажер%')
                                                        OR v.vacancy_name LIKE ('%Intern%')
                                                THEN    'Intern'

                                                WHEN    v.vacancy_name LIKE ('%Младший%')
                                                        OR v.vacancy_name LIKE ('%Junior%')
                                                THEN    'Junior'

                                                WHEN    v.vacancy_name LIKE ('%Старший%')
                                                        OR v.vacancy_name LIKE ('%Middle%')
                                                THEN    'Middle'

                                                WHEN    v.vacancy_name LIKE ('%Ведущий%')
                                                        OR v.vacancy_name LIKE ('%Senior%')
                                                THEN    'Senior'

                                                WHEN    v.vacancy_name LIKE ('%Руководитель группы%')
                                                        OR v.vacancy_name LIKE ('%Team Lead%')
                                                THEN    'Team Lead'
                                            END AS grade

                                FROM    core.hub_vacancy AS hv
                                JOIN    stage.vacancy AS v ON v.id::varchar = hv.external_id
                            ) AS src
                    ON        trg.vacancy_id = src.vacancy_id
                            AND trg.record_source = src.record_source

                    WHEN    NOT MATCHED
                    THEN    INSERT  (     vacancy_id
                                        , record_source
                                        , load_date
                                        , vacancy_name
                                        , published_at
                                        , is_archive
                                        , is_open
                                        , type
                                        , grade
                                        , created_at
                                        , updated_at
                                        , deleted_at
                                    )
                            VALUES  (     src.vacancy_id
                                        , src.record_source
                                        , src.load_date
                                        , src.vacancy_name
                                        , src.published_at
                                        , src.is_archive
                                        , src.is_open
                                        , src.type
                                        , src.grade
                                        , now()
                                        , null::timestamp
                                        , null::timestamp
                                    )

                    WHEN    MATCHED
                            AND (   COALESCE(trg.vacancy_name, '') <> COALESCE(src.vacancy_name, '')
                                    OR COALESCE(trg.published_at, '1900-01-01') <> COALESCE(src.published_at, '1900-01-01')
                                    OR COALESCE(trg.is_archive, FALSE) <> COALESCE(src.is_archive, FALSE)
                                    OR COALESCE(trg.is_open, FALSE) <> COALESCE(src.is_open, FALSE)
                                    OR COALESCE(trg.type, '') <> COALESCE(src.type, '')
                                    OR COALESCE(trg.grade, '') <> COALESCE(src.grade, '')
                                )

                    THEN    UPDATE
                            SET       vacancy_name = src.vacancy_name
                                    , published_at = src.published_at
                                    , is_archive = src.is_archive
                                    , is_open = src.is_open
                                    , type = src.type
                                    , grade = src.grade
                                    , updated_at = now();

                    UPDATE  core.sat_vacancy
                    SET     deleted_at = now()
                    WHERE   extract(day from now() - sat_vacancy.published_at) > 30;
                """)

    t_etl_core_sat_employer = SQLExecuteQueryOperator(
        task_id = 'load_sat_employer',
        conn_id='postgres_vacancy_db',
        sql =   """ MERGE INTO core.sat_employer AS trg
                    USING    (    SELECT    DISTINCT
                                          he.employer_id
                                        , 'hh' as record_source
                                        , now() as load_date
                                        , v.employer_name
                                        , v.is_accredited_it_employer

                                FROM    core.hub_employer AS he
                                JOIN    stage.vacancy AS v ON v.employer_id::varchar = he.external_id
                            ) AS src
                    ON      trg.employer_id = src.employer_id
                            AND trg.record_source = src.record_source

                    WHEN    NOT MATCHED
                    THEN    INSERT    (   employer_id
                                        , record_source
                                        , load_date
                                        , employer_name
                                        , is_accredited_it_employer
                                        , created_at
                                        , updated_at
                                        , deleted_at
                                    )
                            VALUES    (   src.employer_id
                                        , src.record_source
                                        , src.load_date
                                        , src.employer_name
                                        , src.is_accredited_it_employer
                                        , now()
                                        , null::timestamp
                                        , null::timestamp
                                    )

                    WHEN    MATCHED
                            AND (   COALESCE(trg.employer_name, '') <> COALESCE(src.employer_name, '')
                                    OR COALESCE(trg.is_accredited_it_employer, FALSE) <> COALESCE(src.is_accredited_it_employer, FALSE)
                                )

                    THEN    UPDATE
                            SET       employer_name = src.employer_name
                                    , is_accredited_it_employer = src.is_accredited_it_employer
                                    , updated_at = now();
                """)

    t_etl_core_sat_experience = SQLExecuteQueryOperator(
        task_id = 'load_sat_experience',
        conn_id='postgres_vacancy_db',
        sql =   """ MERGE INTO core.sat_experience AS trg
                    USING   (   SELECT  DISTINCT
                                          he.experience_id
                                        , 'hh' as record_source
                                        , now() as load_date
                                        , v.experience_name

                                FROM    core.hub_experience AS he
                                JOIN    stage.vacancy AS v ON v.experience_id = he.external_id
                            ) AS src
                    ON      trg.experience_id = src.experience_id
                            AND trg.record_source = src.record_source

                    WHEN    NOT MATCHED
                    THEN    INSERT    (   experience_id
                                        , record_source
                                        , load_date
                                        , experience_name
                                        , created_at
                                        , updated_at
                                        , deleted_at
                                    )
                            VALUES    (   src.experience_id
                                        , src.record_source
                                        , src.load_date
                                        , src.experience_name
                                        , now()
                                        , null::timestamp
                                        , null::timestamp
                                    )

                    WHEN    MATCHED
                            AND COALESCE(trg.experience_name, '') <> COALESCE(src.experience_name, '')

                    THEN    UPDATE
                            SET       experience_name = src.experience_name
                                    , updated_at = now();
                """)

    t_etl_core_sat_area = SQLExecuteQueryOperator(
        task_id = 'load_sat_area',
        conn_id='postgres_vacancy_db',
        sql =   """ MERGE INTO core.sat_area AS trg
                    USING   (   SELECT    DISTINCT
                                          ha.area_id
                                        , 'hh' as record_source
                                        , now() as load_date
                                        , v.area_name

                                FROM    core.hub_area AS ha
                                JOIN    stage.vacancy AS v ON v.area_id::varchar = ha.external_id
                            ) AS src
                    ON      trg.area_id = src.area_id
                            AND trg.record_source = src.record_source

                    WHEN    NOT MATCHED
                    THEN    INSERT    (   area_id
                                        , record_source
                                        , load_date
                                        , area_name
                                        , created_at
                                        , updated_at
                                        , deleted_at
                                    )
                            VALUES    (   src.area_id
                                        , src.record_source
                                        , src.load_date
                                        , src.area_name
                                        , now()
                                        , null::timestamp
                                        , null::timestamp
                                    )

                    WHEN    MATCHED
                            AND COALESCE(trg.area_name, '') <> COALESCE(src.area_name, '')

                    THEN    UPDATE
                            SET       area_name = src.area_name
                                    , updated_at = now();
                """)

    t_etl_core_sat_salary = SQLExecuteQueryOperator(
        task_id = 'load_sat_salary',
        conn_id='postgres_vacancy_db',
        sql =   """ MERGE INTO core.sat_salary AS trg
                    USING   (   SELECT  DISTINCT
                                          hs.salary_id
                                        , 'hh' as record_source
                                        , now() as load_date
                                        , v.salary_from
                                        , v.salary_to
                                        , v.salary_currency
                                        , v.is_gross

                                FROM    core.hub_salary AS hs
                                JOIN    stage.vacancy AS v ON concat(coalesce(v.salary_from, 0.0)::varchar, coalesce(v.salary_to, 0.0)::varchar, coalesce(v.salary_currency::varchar, ''), coalesce(v.is_gross, FALSE)) = hs.external_id
                            ) AS src
                    ON      trg.salary_id = src.salary_id
                            AND trg.record_source = src.record_source

                    WHEN    NOT MATCHED
                    THEN    INSERT  (     salary_id
                                        , record_source
                                        , load_date
                                        , salary_from
                                        , salary_to
                                        , salary_currency
                                        , is_gross
                                        , created_at
                                        , updated_at
                                        , deleted_at
                                    )
                            VALUES  (     src.salary_id
                                        , src.record_source
                                        , src.load_date
                                        , COALESCE(src.salary_from, 0.0)
                                        , COALESCE(src.salary_to, 0.0)
                                        , src.salary_currency
                                        , src.is_gross
                                        , now()
                                        , null::timestamp
                                        , null::timestamp
                                    );
                """)

    t_etl_core_link_vacancy_employer = SQLExecuteQueryOperator(
        task_id = 'load_link_vacancy_employer',
        conn_id='postgres_vacancy_db',
        sql =   """ INSERT INTO core.link_vacancy_employer (vacancy_id, employer_id, date_from, date_to)
                    SELECT        hv.vacancy_id
                                , he.employer_id
                                , now()
                                , null::timestamp

                    FROM        stage.vacancy AS v
                    JOIN        core.hub_vacancy hv ON v.id::varchar = hv.external_id
                    JOIN        core.hub_employer he ON v.employer_id::varchar = he.external_id
                    LEFT JOIN   core.link_vacancy_employer lve on hv.vacancy_id = lve.vacancy_id
                                AND he.employer_id = lve.employer_id

                    WHERE       lve.link_id IS NULL;

                    UPDATE  core.link_vacancy_employer
                    SET     date_to = now()
                    WHERE   link_id IN  (   SELECT        lve.link_id

                                            FROM        core.link_vacancy_employer AS lve
                                            LEFT JOIN   core.hub_vacancy AS hv ON hv.vacancy_id = lve.vacancy_id
                                            LEFT JOIN   core.hub_employer AS he ON he.employer_id = lve.employer_id
                                            LEFT JOIN   stage.vacancy AS v ON v.id::varchar = hv.external_id
                                                        AND v.employer_id::varchar = he.external_id

                                            WHERE        v.employer_id IS NULL
                                                        OR v.id IS NULL
                                        );
                """)

    t_etl_core_link_vacancy_experience = SQLExecuteQueryOperator(
        task_id = 'load_link_vacancy_experience',
        conn_id='postgres_vacancy_db',
        sql =   """ INSERT INTO core.link_vacancy_experience (vacancy_id, experience_id, date_from, date_to)
                    SELECT        hv.vacancy_id
                                , he.experience_id
                                , now()
                                , null::timestamp

                    FROM        stage.vacancy AS v
                    JOIN        core.hub_vacancy hv ON v.id::varchar = hv.external_id
                    JOIN        core.hub_experience he ON v.experience_id::varchar = he.external_id
                    LEFT JOIN   core.link_vacancy_experience lve on hv.vacancy_id = lve.vacancy_id
                                AND he.experience_id = lve.experience_id

                    WHERE       lve.link_id IS NULL;

                    UPDATE  core.link_vacancy_experience
                    SET     date_to = now()
                    WHERE   link_id IN  (   SELECT      lve.link_id

                                            FROM        core.link_vacancy_experience AS lve
                                            LEFT JOIN   core.hub_vacancy AS hv ON hv.vacancy_id = lve.vacancy_id
                                            LEFT JOIN   core.hub_experience AS he ON he.experience_id = lve.experience_id
                                            LEFT JOIN   stage.vacancy AS v ON v.id::varchar = hv.external_id
                                                        AND v.experience_id::varchar = he.external_id

                                            WHERE       v.experience_id IS NULL
                                                        OR v.id IS NULL
                                        );
                """)

    t_etl_core_link_vacancy_area = SQLExecuteQueryOperator(
        task_id = 'load_link_vacancy_area',
        conn_id='postgres_vacancy_db',
        sql =   """ INSERT INTO core.link_vacancy_area (vacancy_id, area_id, date_from, date_to)
                    SELECT        hv.vacancy_id
                                , he.area_id
                                , now()
                                , null::timestamp

                    FROM        stage.vacancy AS v
                    JOIN        core.hub_vacancy hv ON v.id::varchar = hv.external_id
                    JOIN        core.hub_area he ON v.area_id::varchar = he.external_id
                    LEFT JOIN   core.link_vacancy_area lve on hv.vacancy_id = lve.vacancy_id
                                AND he.area_id = lve.area_id

                    WHERE       lve.link_id IS NULL;

                    UPDATE  core.link_vacancy_area
                    SET     date_to = now()
                    WHERE   link_id IN  (   SELECT      lve.link_id

                                            FROM        core.link_vacancy_area AS lve
                                            LEFT JOIN   core.hub_vacancy AS hv ON hv.vacancy_id = lve.vacancy_id
                                            LEFT JOIN   core.hub_area AS he ON he.area_id = lve.area_id
                                            LEFT JOIN   stage.vacancy AS v ON v.id::varchar = hv.external_id
                                                        AND v.area_id::varchar = he.external_id

                                            WHERE       v.area_id IS NULL
                                                        OR v.id IS NULL
                                        );
                """)

    t_etl_core_link_vacancy_salary = SQLExecuteQueryOperator(
        task_id = 'load_link_vacancy_salary',
        conn_id='postgres_vacancy_db',
        sql =   """ INSERT INTO core.link_vacancy_salary (vacancy_id, salary_id, date_from, date_to)
                    SELECT        hv.vacancy_id
                                , he.salary_id
                                , now()
                                , null::timestamp

                    FROM        stage.vacancy AS v
                    JOIN        core.hub_vacancy AS hv ON v.id::varchar = hv.external_id
                    JOIN        core.hub_salary AS he ON concat(coalesce(v.salary_from, 0.0)::varchar, coalesce(v.salary_to, 0.0)::varchar, coalesce(v.salary_currency::varchar, ''), coalesce(v.is_gross, FALSE)) = he.external_id
                    LEFT JOIN   core.link_vacancy_salary AS lve on hv.vacancy_id = lve.vacancy_id
                                AND he.salary_id = lve.salary_id

                    WHERE       lve.link_id IS NULL;

                    UPDATE  core.link_vacancy_salary
                    SET     date_to = now()
                    WHERE   link_id IN  (   SELECT        lve.link_id

                                            FROM        core.link_vacancy_salary AS lve
                                            LEFT JOIN   core.hub_vacancy AS hv ON hv.vacancy_id = lve.vacancy_id
                                            LEFT JOIN   core.hub_salary AS he ON he.salary_id = lve.salary_id
                                            LEFT JOIN   stage.vacancy AS v ON v.id::varchar = hv.external_id
                                                        AND he.external_id = concat(coalesce(v.salary_from, 0.0)::varchar, coalesce(v.salary_to, 0.0)::varchar, coalesce(v.salary_currency::varchar, ''), coalesce(v.is_gross, FALSE))

                                            WHERE       concat(coalesce(v.salary_from, 0.0)::varchar, coalesce(v.salary_to, 0.0)::varchar, coalesce(v.salary_currency::varchar, ''), coalesce(v.is_gross, FALSE)) IS NULL
                                                        OR v.id IS NULL
                                        );
                """)

    t_end = EmptyOperator(task_id='End')

    t_start >> t_truncate_stage >> t_load_data
    t_load_data >> t_etl_core_hub_vacancy >> t_etl_core_sat_vacancy
    t_load_data >> t_etl_core_hub_employer >> t_etl_core_sat_employer
    t_load_data >> t_etl_core_hub_experience >> t_etl_core_sat_experience
    t_load_data >> t_etl_core_hub_area >> t_etl_core_sat_area
    t_load_data >> t_etl_core_hub_salary >> t_etl_core_sat_salary
    [t_etl_core_hub_vacancy, t_etl_core_hub_employer] >> t_etl_core_link_vacancy_employer >> t_end
    [t_etl_core_hub_vacancy, t_etl_core_hub_experience] >> t_etl_core_link_vacancy_experience >> t_end
    [t_etl_core_hub_vacancy, t_etl_core_hub_area] >> t_etl_core_link_vacancy_area >> t_end
    [t_etl_core_hub_vacancy, t_etl_core_hub_salary] >> t_etl_core_link_vacancy_salary >> t_end