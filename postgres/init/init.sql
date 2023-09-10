CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS proc;

CREATE TABLE IF NOT EXISTS proc.settings
( id int NOT NULL PRIMARY KEY
, name varchar
, value varchar
);

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

CREATE OR REPLACE FUNCTION mart.get_vacancies_by_region(filter varchar[])
RETURNS TABLE (	  id int
				, area_name varchar
				, cnt_open int
				, cnt_salary int
				, min_salary decimal
				, max_salary decimal
				, avg_salary decimal
				, ratio decimal
				, intern_cnt int
				, intern_avg decimal
				, intern_max decimal
				, intern_min decimal
				, junior_cnt int
				, junior_avg decimal
				, junior_min decimal
				, junior_max decimal
				, middle_cnt int
				, middle_avg decimal
				, middle_min decimal
				, middle_max decimal
				, senior_cnt int
				, senior_avg decimal
				, senior_min decimal
				, senior_max decimal
				, lead_cnt int
				, lead_avg decimal
				, lead_max decimal
				, lead_min decimal
				, no_exp_cnt int
				, no_exp_avg decimal
				, no_exp_min decimal
				, no_exp_max decimal
				, low_exp_cnt int
				, low_exp_avg decimal
				, low_exp_min decimal
				, low_exp_max decimal
				, mid_exp_cnt int
				, mid_exp_avg decimal
				, mid_exp_min decimal
				, mid_exp_max decimal
				, high_exp_cnt int
				, high_exp_avg decimal
				, high_exp_min decimal
				, high_exp_max decimal
			) AS $$

	WITH tbl AS (	SELECT		  sv.vacancy_id
								, sv.vacancy_name
								, sv.is_open
								, sv.grade
								, sa.area_id
								, sa.area_name
								, se.experience_name
								, ss.salary_from
								, ss.salary_to
					FROM		core.sat_vacancy AS sv
					JOIN		core.link_vacancy_area AS lva ON lva.vacancy_id = sv.vacancy_id
					JOIN		core.sat_area AS sa ON sa.area_id = lva.area_id
					JOIN		core.link_vacancy_experience AS lve ON lve.vacancy_id = sv.vacancy_id
					JOIN		core.sat_experience AS se ON se.experience_id = lve.experience_id
					LEFT JOIN	core.link_vacancy_salary as lvs ON lvs.vacancy_id = sv.vacancy_id
					LEFT JOIN	core.sat_salary as ss ON ss.salary_id = lvs.salary_id

					WHERE		sv.type = ANY(filter)
				)

		, avg_salary AS	(	SELECT	  sv.vacancy_id
									, coalesce((coalesce(salary_from, salary_to) + coalesce(salary_to, salary_from)/2), 0.0) AS avg_salary

							FROM		core.sat_vacancy AS sv
							JOIN		core.link_vacancy_salary AS lvs ON lvs.vacancy_id = sv.vacancy_id
							JOIN		core.sat_salary AS ss ON ss.salary_id = lvs.salary_id)

		, grade AS (	SELECT		  t.area_name
									, t.grade
									, count(DISTINCT t.vacancy_id) AS cnt
									, avg(avg_salary) AS avg
									, min(avg_salary) AS min
									, max(avg_salary) AS max
						FROM		tbl AS t
						JOIN		avg_salary AS av ON av.vacancy_id = t.vacancy_id
						GROUP BY	  t.area_name
									, t.grade)

		, expirience AS (	SELECT		  t.area_name
										, t.experience_name
										, count(distinct t.vacancy_id) AS cnt
										, avg(avg_salary) AS avg
										, min(avg_salary) AS min
										, max(avg_salary) AS max
							FROM		tbl AS t
							JOIN		avg_salary AS av ON av.vacancy_id = t.vacancy_id
							GROUP BY	  t.area_name
										, t.experience_name)

	select	  row_number() over () as id
			, tbl.area_name
			, tbl.cnt_open
			, tbl.cnt_salary
			, tbl.min_salary
			, tbl.max_salary
			, tbl.avg_salary
			, tbl.avg_salary/(case when tbl.max_salary = 0 then 1 else tbl.max_salary end) as ratio
			, coalesce(intern.cnt, 0.0) AS intern_cnt
			, coalesce(intern.avg, 0.0) AS intern_avg
			, coalesce(intern.min, 0.0) AS intern_min
			, coalesce(intern.max, 0.0) AS intern_max
			, coalesce(junior.cnt, 0.0) AS junior_cnt
			, coalesce(junior.avg, 0.0) AS junior_avg
			, coalesce(junior.min, 0.0) AS junior_min
			, coalesce(junior.max, 0.0) AS junior_max
			, coalesce(middle.cnt, 0.0) AS middle_cnt
			, coalesce(middle.avg, 0.0) AS middle_avg
			, coalesce(middle.min, 0.0) AS middle_min
			, coalesce(middle.max, 0.0) AS middle_max
			, coalesce(senior.cnt, 0.0) AS senior_cnt
			, coalesce(senior.avg, 0.0) AS senior_avg
			, coalesce(senior.min, 0.0) AS senior_min
			, coalesce(senior.max, 0.0) AS senior_max
			, coalesce(lead.cnt, 0.0) AS lead_cnt
			, coalesce(lead.avg, 0.0) AS lead_avg
			, coalesce(lead.min, 0.0) AS lead_min
			, coalesce(lead.max, 0.0) AS lead_max
			, coalesce(no_exp.cnt, 0.0) AS no_exp_cnt
			, coalesce(no_exp.avg, 0.0) AS no_exp_avg
			, coalesce(no_exp.min, 0.0) AS no_exp_min
			, coalesce(no_exp.max, 0.0) AS no_exp_max
			, coalesce(low_exp.cnt, 0.0) AS low_exp_cnt
			, coalesce(low_exp.avg, 0.0) AS low_exp_avg
			, coalesce(low_exp.min, 0.0) AS low_exp_min
			, coalesce(low_exp.max, 0.0) AS low_exp_max
			, coalesce(mid_exp.cnt, 0.0) AS mid_exp_cnt
			, coalesce(mid_exp.avg, 0.0) AS mid_exp_avg
			, coalesce(mid_exp.min, 0.0) AS mid_exp_min
			, coalesce(mid_exp.max, 0.0) AS mid_exp_max
			, coalesce(high_exp.cnt, 0.0) AS high_exp_cnt
			, coalesce(high_exp.avg, 0.0) AS high_exp_avg
			, coalesce(high_exp.min, 0.0) AS high_exp_min
			, coalesce(high_exp.max, 0.0) AS high_exp_max

	FROM	(	SELECT t.area_name
						, count(case when t.is_open = True then 1 end) as cnt_open
						, count(case when t.salary_from <> 0 or salary_from <> 0 then 1 end) as cnt_salary
						, coalesce(min(av.avg_salary), 0.0) as min_salary
						, coalesce(max(av.avg_salary), 0.0) as max_salary
						, coalesce(avg(av.avg_salary), 0.0) as avg_salary
				FROM tbl as t
				LEFT JOIN avg_salary as av ON av.vacancy_id = t.vacancy_id
				GROUP BY	t.area_name
			) AS tbl

	LEFT JOIN (select * from grade where grade = 'Intern') AS intern ON intern.area_name = tbl.area_name
	LEFT JOIN (select * from grade where grade = 'Junior') AS junior ON junior.area_name = tbl.area_name
	LEFT JOIN (select * from grade where grade = 'Middle') AS middle ON middle.area_name = tbl.area_name
	LEFT JOIN (select * from grade where grade = 'Senior') AS senior ON senior.area_name = tbl.area_name
	LEFT JOIN (select * from grade where grade = 'Team Lead') AS lead ON lead.area_name = tbl.area_name

	LEFT JOIN (select * from expirience where experience_name = 'Нет опыта') AS no_exp ON no_exp.area_name = tbl.area_name
	LEFT JOIN (select * from expirience where experience_name = 'От 1 года до 3 лет') AS low_exp ON low_exp.area_name = tbl.area_name
	LEFT JOIN (select * from expirience where experience_name = 'От 3 до 6 лет') AS mid_exp ON mid_exp.area_name = tbl.area_name
	LEFT JOIN (select * from expirience where experience_name = 'Более 6 лет') AS high_exp ON high_exp.area_name = tbl.area_name

	ORDER BY tbl.area_name;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION mart.get_vacancies_by_employer(filter varchar[])
RETURNS TABLE (	  id int
				, employer_name varchar
				, cnt_open int
				, cnt_salary int
				, min_salary decimal
				, max_salary decimal
				, avg_salary decimal
				, ratio decimal
				, intern_cnt int
				, intern_avg decimal
				, intern_max decimal
				, intern_min decimal
				, junior_cnt int
				, junior_avg decimal
				, junior_min decimal
				, junior_max decimal
				, middle_cnt int
				, middle_avg decimal
				, middle_min decimal
				, middle_max decimal
				, senior_cnt int
				, senior_avg decimal
				, senior_min decimal
				, senior_max decimal
				, lead_cnt int
				, lead_avg decimal
				, lead_max decimal
				, lead_min decimal
				, no_exp_cnt int
				, no_exp_avg decimal
				, no_exp_min decimal
				, no_exp_max decimal
				, low_exp_cnt int
				, low_exp_avg decimal
				, low_exp_min decimal
				, low_exp_max decimal
				, mid_exp_cnt int
				, mid_exp_avg decimal
				, mid_exp_min decimal
				, mid_exp_max decimal
				, high_exp_cnt int
				, high_exp_avg decimal
				, high_exp_min decimal
				, high_exp_max decimal
			) AS $$

	WITH tbl AS (	SELECT		  sv.vacancy_id
								, sv.vacancy_name
								, sv.is_open
								, sv.grade
								, semp.employer_name
								, semp.is_accredited_it_employer
								, se.experience_name
								, ss.salary_from
								, ss.salary_to
					FROM		core.sat_vacancy AS sv
					JOIN		core.link_vacancy_employer AS lva ON lva.vacancy_id = sv.vacancy_id
					JOIN		core.sat_employer AS semp ON semp.employer_id = lva.employer_id
					JOIN		core.link_vacancy_experience AS lve ON lve.vacancy_id = sv.vacancy_id
					JOIN		core.sat_experience AS se ON se.experience_id = lve.experience_id
					LEFT JOIN	core.link_vacancy_salary as lvs ON lvs.vacancy_id = sv.vacancy_id
					LEFT JOIN	core.sat_salary as ss ON ss.salary_id = lvs.salary_id

					WHERE		sv.type = ANY(filter)
				)

		, avg_salary AS	(	SELECT	  sv.vacancy_id
									, coalesce((coalesce(salary_from, salary_to) + coalesce(salary_to, salary_from)/2), 0.0) AS avg_salary

							FROM		core.sat_vacancy AS sv
							JOIN		core.link_vacancy_salary AS lvs ON lvs.vacancy_id = sv.vacancy_id
							JOIN		core.sat_salary AS ss ON ss.salary_id = lvs.salary_id)

		, grade AS (	SELECT		  t.employer_name
									, t.grade
									, count(DISTINCT t.vacancy_id) AS cnt
									, avg(avg_salary) AS avg
									, min(avg_salary) AS min
									, max(avg_salary) AS max
						FROM		tbl AS t
						JOIN		avg_salary AS av ON av.vacancy_id = t.vacancy_id
						GROUP BY	  t.employer_name
									, t.grade)

		, expirience AS (	SELECT		  t.employer_name
										, t.experience_name
										, count(distinct t.vacancy_id) AS cnt
										, avg(avg_salary) AS avg
										, min(avg_salary) AS min
										, max(avg_salary) AS max
							FROM		tbl AS t
							JOIN		avg_salary AS av ON av.vacancy_id = t.vacancy_id
							GROUP BY	  t.employer_name
										, t.experience_name)

	select	  row_number() over () as id
			, tbl.employer_name
			, tbl.cnt_open
			, tbl.cnt_salary
			, tbl.min_salary
			, tbl.max_salary
			, tbl.avg_salary
			, tbl.avg_salary/(case when tbl.max_salary = 0 then 1 else tbl.max_salary end) as ratio
			, coalesce(intern.cnt, 0.0) AS intern_cnt
			, coalesce(intern.avg, 0.0) AS intern_avg
			, coalesce(intern.min, 0.0) AS intern_min
			, coalesce(intern.max, 0.0) AS intern_max
			, coalesce(junior.cnt, 0.0) AS junior_cnt
			, coalesce(junior.avg, 0.0) AS junior_avg
			, coalesce(junior.min, 0.0) AS junior_min
			, coalesce(junior.max, 0.0) AS junior_max
			, coalesce(middle.cnt, 0.0) AS middle_cnt
			, coalesce(middle.avg, 0.0) AS middle_avg
			, coalesce(middle.min, 0.0) AS middle_min
			, coalesce(middle.max, 0.0) AS middle_max
			, coalesce(senior.cnt, 0.0) AS senior_cnt
			, coalesce(senior.avg, 0.0) AS senior_avg
			, coalesce(senior.min, 0.0) AS senior_min
			, coalesce(senior.max, 0.0) AS senior_max
			, coalesce(lead.cnt, 0.0) AS lead_cnt
			, coalesce(lead.avg, 0.0) AS lead_avg
			, coalesce(lead.min, 0.0) AS lead_min
			, coalesce(lead.max, 0.0) AS lead_max
			, coalesce(no_exp.cnt, 0.0) AS no_exp_cnt
			, coalesce(no_exp.avg, 0.0) AS no_exp_avg
			, coalesce(no_exp.min, 0.0) AS no_exp_min
			, coalesce(no_exp.max, 0.0) AS no_exp_max
			, coalesce(low_exp.cnt, 0.0) AS low_exp_cnt
			, coalesce(low_exp.avg, 0.0) AS low_exp_avg
			, coalesce(low_exp.min, 0.0) AS low_exp_min
			, coalesce(low_exp.max, 0.0) AS low_exp_max
			, coalesce(mid_exp.cnt, 0.0) AS mid_exp_cnt
			, coalesce(mid_exp.avg, 0.0) AS mid_exp_avg
			, coalesce(mid_exp.min, 0.0) AS mid_exp_min
			, coalesce(mid_exp.max, 0.0) AS mid_exp_max
			, coalesce(high_exp.cnt, 0.0) AS high_exp_cnt
			, coalesce(high_exp.avg, 0.0) AS high_exp_avg
			, coalesce(high_exp.min, 0.0) AS high_exp_min
			, coalesce(high_exp.max, 0.0) AS high_exp_max

	FROM	(	SELECT t.employer_name
						, count(case when t.is_open = True then 1 end) as cnt_open
						, count(case when t.salary_from <> 0 or salary_from <> 0 then 1 end) as cnt_salary
						, coalesce(min(av.avg_salary), 0.0) as min_salary
						, coalesce(max(av.avg_salary), 0.0) as max_salary
						, coalesce(avg(av.avg_salary), 0.0) as avg_salary
				FROM tbl as t
				LEFT JOIN avg_salary as av ON av.vacancy_id = t.vacancy_id
				GROUP BY	t.employer_name
			) AS tbl

	LEFT JOIN (SELECT * FROM grade WHERE grade = 'Intern') AS intern ON intern.employer_name = tbl.employer_name
	LEFT JOIN (SELECT * FROM grade WHERE grade = 'Junior') AS junior ON junior.employer_name = tbl.employer_name
	LEFT JOIN (SELECT * FROM grade WHERE grade = 'Middle') AS middle ON middle.employer_name = tbl.employer_name
	LEFT JOIN (SELECT * FROM grade WHERE grade = 'Senior') AS senior ON senior.employer_name = tbl.employer_name
	LEFT JOIN (SELECT * FROM grade WHERE grade = 'Team Lead') AS lead ON lead.employer_name = tbl.employer_name

	LEFT JOIN (SELECT * FROM expirience WHERE experience_name = 'Нет опыта') AS no_exp ON no_exp.employer_name = tbl.employer_name
	LEFT JOIN (SELECT * FROM expirience WHERE experience_name = 'От 1 года до 3 лет') AS low_exp ON low_exp.employer_name = tbl.employer_name
	LEFT JOIN (SELECT * FROM expirience WHERE experience_name = 'От 3 до 6 лет') AS mid_exp ON mid_exp.employer_name = tbl.employer_name
	LEFT JOIN (SELECT * FROM expirience WHERE experience_name = 'Более 6 лет') AS high_exp ON high_exp.employer_name = tbl.employer_name

	ORDER BY tbl.employer_name;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION mart.get_vacancies()
RETURNS TABLE (	  id int
				, vacancy_type varchar
				, cnt_open int
				, cnt_salary int
				, min_salary decimal
				, max_salary decimal
				, avg_salary decimal
				, ratio decimal
				, intern_cnt int
				, intern_avg decimal
				, intern_max decimal
				, intern_min decimal
				, junior_cnt int
				, junior_avg decimal
				, junior_min decimal
				, junior_max decimal
				, middle_cnt int
				, middle_avg decimal
				, middle_min decimal
				, middle_max decimal
				, senior_cnt int
				, senior_avg decimal
				, senior_min decimal
				, senior_max decimal
				, lead_cnt int
				, lead_avg decimal
				, lead_max decimal
				, lead_min decimal
				, no_exp_cnt int
				, no_exp_avg decimal
				, no_exp_min decimal
				, no_exp_max decimal
				, low_exp_cnt int
				, low_exp_avg decimal
				, low_exp_min decimal
				, low_exp_max decimal
				, mid_exp_cnt int
				, mid_exp_avg decimal
				, mid_exp_min decimal
				, mid_exp_max decimal
				, high_exp_cnt int
				, high_exp_avg decimal
				, high_exp_min decimal
				, high_exp_max decimal
			) AS $$

	WITH tbl AS (	SELECT		  sv.vacancy_id
								, sv.vacancy_name
								, sv.is_open
								, sv.grade
								, sv.type
								, sa.area_id
								, sa.area_name
								, se.experience_name
								, ss.salary_from
								, ss.salary_to
					FROM		core.sat_vacancy AS sv
					JOIN		core.link_vacancy_area AS lva on lva.vacancy_id = sv.vacancy_id
					JOIN		core.sat_area AS sa on sa.area_id = lva.area_id
					JOIN		core.link_vacancy_experience AS lve on lve.vacancy_id = sv.vacancy_id
					JOIN		core.sat_experience AS se on se.experience_id = lve.experience_id
					LEFT JOIN	core.link_vacancy_salary as lvs on lvs.vacancy_id = sv.vacancy_id
					LEFT JOIN	core.sat_salary as ss ON ss.salary_id = lvs.salary_id

					WHERE		sv.type <> 'Another'
				)

		, avg_salary AS	(	SELECT	  sv.vacancy_id
									, coalesce((coalesce(salary_from, salary_to) + coalesce(salary_to, salary_from)/2), 0.0) AS avg_salary

							FROM		core.sat_vacancy AS sv
							JOIN		core.link_vacancy_salary AS lvs ON lvs.vacancy_id = sv.vacancy_id
							JOIN		core.sat_salary AS ss ON ss.salary_id = lvs.salary_id)

		, grade AS (	SELECT		  t.type
									, t.grade
									, count(DISTINCT t.vacancy_id) AS cnt
									, avg(avg_salary) AS avg
									, min(avg_salary) AS min
									, max(avg_salary) AS max
						FROM		tbl AS t
						JOIN		avg_salary AS av on av.vacancy_id = t.vacancy_id
						GROUP BY	  t.type
									, t.grade)

		, expirience AS (	SELECT		  t.type
										, t.experience_name
										, count(distinct t.vacancy_id) AS cnt
										, avg(avg_salary) AS avg
										, min(avg_salary) AS min
										, max(avg_salary) AS max
							FROM		tbl AS t
							JOIN		avg_salary AS av on av.vacancy_id = t.vacancy_id
							GROUP BY	  t.type
										, t.experience_name)

	select	  row_number() over () as id
			, tbl.type
			, tbl.cnt_open
			, tbl.cnt_salary
			, tbl.min_salary
			, tbl.max_salary
			, tbl.avg_salary
			, tbl.avg_salary/(case when tbl.max_salary = 0 then 1 else tbl.max_salary end) as ratio
			, coalesce(intern.cnt, 0.0) AS intern_cnt
			, coalesce(intern.avg, 0.0) AS intern_avg
			, coalesce(intern.min, 0.0) AS intern_min
			, coalesce(intern.max, 0.0) AS intern_max
			, coalesce(junior.cnt, 0.0) AS junior_cnt
			, coalesce(junior.avg, 0.0) AS junior_avg
			, coalesce(junior.min, 0.0) AS junior_min
			, coalesce(junior.max, 0.0) AS junior_max
			, coalesce(middle.cnt, 0.0) AS middle_cnt
			, coalesce(middle.avg, 0.0) AS middle_avg
			, coalesce(middle.min, 0.0) AS middle_min
			, coalesce(middle.max, 0.0) AS middle_max
			, coalesce(senior.cnt, 0.0) AS senior_cnt
			, coalesce(senior.avg, 0.0) AS senior_avg
			, coalesce(senior.min, 0.0) AS senior_min
			, coalesce(senior.max, 0.0) AS senior_max
			, coalesce(lead.cnt, 0.0) AS lead_cnt
			, coalesce(lead.avg, 0.0) AS lead_avg
			, coalesce(lead.min, 0.0) AS lead_min
			, coalesce(lead.max, 0.0) AS lead_max
			, coalesce(no_exp.cnt, 0.0) AS no_exp_cnt
			, coalesce(no_exp.avg, 0.0) AS no_exp_avg
			, coalesce(no_exp.min, 0.0) AS no_exp_min
			, coalesce(no_exp.max, 0.0) AS no_exp_max
			, coalesce(low_exp.cnt, 0.0) AS low_exp_cnt
			, coalesce(low_exp.avg, 0.0) AS low_exp_avg
			, coalesce(low_exp.min, 0.0) AS low_exp_min
			, coalesce(low_exp.max, 0.0) AS low_exp_max
			, coalesce(mid_exp.cnt, 0.0) AS mid_exp_cnt
			, coalesce(mid_exp.avg, 0.0) AS mid_exp_avg
			, coalesce(mid_exp.min, 0.0) AS mid_exp_min
			, coalesce(mid_exp.max, 0.0) AS mid_exp_max
			, coalesce(high_exp.cnt, 0.0) AS high_exp_cnt
			, coalesce(high_exp.avg, 0.0) AS high_exp_avg
			, coalesce(high_exp.min, 0.0) AS high_exp_min
			, coalesce(high_exp.max, 0.0) AS high_exp_max

	FROM	(	SELECT t.type
						, count(case when t.is_open = True then 1 end) as cnt_open
						, count(case when t.salary_from <> 0 or salary_from <> 0 then 1 end) as cnt_salary
						, coalesce(min(av.avg_salary), 0.0) as min_salary
						, coalesce(max(av.avg_salary), 0.0) as max_salary
						, coalesce(avg(av.avg_salary), 0.0) as avg_salary
				FROM tbl as t
				LEFT JOIN avg_salary as av ON av.vacancy_id = t.vacancy_id
				GROUP BY	t.type
			) AS tbl

	LEFT JOIN (select * from grade where grade = 'Intern') AS intern on intern.type = tbl.type
	LEFT JOIN (select * from grade where grade = 'Junior') AS junior on junior.type = tbl.type
	LEFT JOIN (select * from grade where grade = 'Middle') AS middle on middle.type = tbl.type
	LEFT JOIN (select * from grade where grade = 'Senior') AS senior on senior.type = tbl.type
	LEFT JOIN (select * from grade where grade = 'Team Lead') AS lead on lead.type = tbl.type

	LEFT JOIN (select * from expirience where experience_name = 'Нет опыта') AS no_exp on no_exp.type = tbl.type
	LEFT JOIN (select * from expirience where experience_name = 'От 1 года до 3 лет') AS low_exp on low_exp.type = tbl.type
	LEFT JOIN (select * from expirience where experience_name = 'От 3 до 6 лет') AS mid_exp on mid_exp.type = tbl.type
	LEFT JOIN (select * from expirience where experience_name = 'Более 6 лет') AS high_exp on high_exp.type = tbl.type

	ORDER BY tbl.type;
$$ LANGUAGE SQL;