CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE IF NOT EXISTS stage.vacancy
( id int
, vacancy_name varchar
, published_at timestamp
, is_archive bool
, is_open bool
, employer_id int
, employer_name varchar
, is_accredited_it_employer bool
, experience_id varchar
, experience_name varchar
, area_id int
, area_name varchar
, salary_from decimal
, salary_to decimal
, salary_currency varchar
, is_gross bool
);

CREATE TABLE IF NOT EXISTS core.hub_vacancy
( vacancy_id serial NOT NULL PRIMARY KEY
, record_source varchar NOT NULL
, load_date timestamp NOT NULL
, external_id varchar NOT NULL

, UNIQUE (vacancy_id, record_source, external_id)
);

CREATE TABLE IF NOT EXISTS core.sat_vacancy
( vacancy_id int NOT NULL PRIMARY KEY
, record_source varchar NOT NULL
, load_date timestamp NOT NULL
, vacancy_name varchar
, published_at timestamp
, is_archive bool
, is_open bool
, type varchar
, grade varchar
, created_at timestamp
, updated_at timestamp
, deleted_at timestamp

, FOREIGN KEY (vacancy_id) REFERENCES core.hub_vacancy (vacancy_id)
);

CREATE TABLE IF NOT EXISTS core.hub_employer
( employer_id serial NOT NULL PRIMARY KEY
, record_source varchar NOT NULL
, load_date timestamp NOT NULL
, external_id varchar NOT NULL

, UNIQUE (employer_id, record_source, external_id)
);

CREATE TABLE IF NOT EXISTS core.sat_employer
( employer_id int NOT NULL PRIMARY KEY
, record_source varchar NOT NULL
, load_date timestamp NOT NULL
, employer_name varchar
, is_accredited_it_employer bool
, created_at timestamp
, updated_at timestamp
, deleted_at timestamp

, FOREIGN KEY (employer_id) REFERENCES core.hub_employer (employer_id)
);

CREATE TABLE IF NOT EXISTS core.hub_experience
( experience_id serial NOT NULL PRIMARY KEY
, record_source varchar NOT NULL
, load_date timestamp NOT NULL
, external_id varchar NOT NULL

, UNIQUE (experience_id, record_source, external_id)
);

CREATE TABLE IF NOT EXISTS core.sat_experience
( experience_id int NOT NULL PRIMARY KEY
, record_source varchar NOT NULL
, load_date timestamp NOT NULL
, experience_name varchar
, created_at timestamp
, updated_at timestamp
, deleted_at timestamp

, FOREIGN KEY (experience_id) REFERENCES core.hub_experience (experience_id)
);

CREATE TABLE IF NOT EXISTS core.hub_area
( area_id serial NOT NULL PRIMARY KEY
, record_source varchar NOT NULL
, load_date timestamp NOT NULL
, external_id varchar NOT NULL
, created_at timestamp
, updated_at timestamp
, deleted_at timestamp

, UNIQUE (area_id, record_source, external_id)
);

CREATE TABLE IF NOT EXISTS core.sat_area
( area_id int NOT NULL PRIMARY KEY
, record_source varchar NOT NULL
, load_date timestamp NOT NULL
, area_name varchar
, created_at timestamp
, updated_at timestamp
, deleted_at timestamp

, FOREIGN KEY (area_id) REFERENCES core.hub_area (area_id)
);

CREATE TABLE IF NOT EXISTS core.hub_salary
( salary_id serial NOT NULL PRIMARY KEY
, record_source varchar NOT NULL
, load_date timestamp NOT NULL
, external_id varchar NOT NULL

, UNIQUE (salary_id, record_source, external_id)
);

CREATE TABLE IF NOT EXISTS core.sat_salary
( salary_id int NOT NULL PRIMARY KEY
, record_source varchar NOT NULL
, load_date timestamp NOT NULL
, salary_from decimal
, salary_to decimal
, salary_currency varchar
, is_gross bool
, created_at timestamp
, updated_at timestamp
, deleted_at timestamp

, FOREIGN KEY (salary_id) REFERENCES core.hub_salary (salary_id)
);

CREATE TABLE IF NOT EXISTS core.link_vacancy_employer
( link_id serial NOT NULL PRIMARY KEY
, vacancy_id int NOT NULL REFERENCES core.hub_vacancy (vacancy_id)
, employer_id int NOT NULL REFERENCES core.hub_employer (employer_id)
, date_from timestamp NOT NULL
, date_to timestamp
);

CREATE TABLE IF NOT EXISTS core.link_vacancy_experience
( link_id serial NOT NULL PRIMARY KEY
, vacancy_id int NOT NULL REFERENCES core.hub_vacancy (vacancy_id)
, experience_id int NOT NULL REFERENCES core.hub_experience (experience_id)
, date_from timestamp NOT NULL
, date_to timestamp
);

CREATE TABLE IF NOT EXISTS core.link_vacancy_area
( link_id serial NOT NULL PRIMARY KEY
, vacancy_id int NOT NULL REFERENCES core.hub_vacancy (vacancy_id)
, area_id int NOT NULL REFERENCES core.hub_area (area_id)
, date_from timestamp NOT NULL
, date_to timestamp
);

CREATE TABLE IF NOT EXISTS core.link_vacancy_salary
( link_id serial NOT NULL PRIMARY KEY
, vacancy_id int NOT NULL REFERENCES core.hub_vacancy (vacancy_id)
, salary_id int NOT NULL REFERENCES core.hub_salary (salary_id)
, date_from timestamp NOT NULL
, date_to timestamp
);