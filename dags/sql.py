create_schema_stage =  """
CREATE SCHEMA IF NOT EXISTS "staging";
"""

create_schema_mart =  """
CREATE SCHEMA IF NOT EXISTS "mart";
"""

stage_character = """DROP TABLE IF EXISTS staging.stage_character;
            CREATE UNLOGGED TABLE staging.stage_character (
                id                  INTEGER,
                name                VARCHAR(50),
                status              VARCHAR(10),
                species             VARCHAR(30),
                type                VARCHAR(40),
                gender              VARCHAR(15),
                id_origin_location  INTEGER,
                id_last_location    INTEGER,
                image_url           VARCHAR(60),
                url                 VARCHAR(45)
            );
        """

stage_lcoation = """DROP TABLE IF EXISTS staging.stage_location;
            CREATE UNLOGGED TABLE staging.stage_location (
                id                  INTEGER,
                name                VARCHAR(50),
                type                VARCHAR(40),
                dimension           VARCHAR(40),
                url                 VARCHAR(50)
            );
        """

stage_episode = """DROP TABLE IF EXISTS staging.stage_episode;
            CREATE UNLOGGED TABLE staging.stage_episode (
                id                  INTEGER,
                name                VARCHAR(50),
                air_date            VARCHAR(20),
                episode             VARCHAR(20),
                url                 VARCHAR(50)
            );
        """


join_character_episode = """DROP TABLE IF EXISTS staging.join_character_episode;
            CREATE UNLOGGED TABLE staging.join_character_episode (
                id_character                  INTEGER,
                id_episode                    INTEGER

            );
        """
    

join_character_location = """DROP TABLE IF EXISTS staging.join_character_location;
            CREATE UNLOGGED TABLE staging.join_character_location (
                id_location                  INTEGER,
                id_character                 INTEGER
            );
        """

table_month_creat = """DROP TABLE IF EXISTS mart.month_russian;
                CREATE TABLE mart.month_russian (
                    id_month INTEGER PRIMARY KEY,
                     name_month varchar(20)
                     );
                """
table_month_data = [(1, 'Январь'),
                    (2, 'Февраль'),
                    (3, 'Март'),
                    (4, 'Апрель'),
                    (5, 'Май'),
                    (6, 'Июнь'),
                    (7, 'Июль'),
                    (8, 'Август'),
                    (9, 'Сентрябрь'),
                    (10, 'Октябрь'),
                    (11, 'Ноябрь'),
                    (12, 'Декабрь')]

table_month_insert = """INSERT INTO mart.month_russian VALUES (%s, %s);"""


table_mart_only_month = """DROP TABLE IF EXISTS mart.human_episode_month;
            CREATE  TABLE mart.human_episode_month (
                cnt                  INTEGER,
				name_month           varchar(20)
            );
            """

table_mart_month_year = """DROP TABLE IF EXISTS mart.human_episode_month_year;
            CREATE  TABLE mart.human_episode_month_year (
                cnt                  INTEGER,
				name_month           varchar(20),
	            year                 INTEGER
            );
            """

view_month = """
                SELECT COALESCE(tmp.cnt,0), rus.name_month FROM
                (SELECT count(sc.id) cnt, EXTRACT(month FROM ce.air_date::date) AS month
                    FROM staging.stage_character sc, staging.join_character_episode j, staging.stage_episode ce
                    WHERE sc.id = j.id_character and j.id_episode = ce.id
                    AND sc.species = 'Human'
                    group by EXTRACT(month FROM ce.air_date::date)) tmp
                    RIGHT JOIN mart.month_russian rus ON tmp.month = rus.id_month
                    order by rus.id_month;
                """

view_month_year = """
                SELECT tmp.cnt, rus.name_month, tmp.year FROM
                (SELECT count(sc.id) cnt, EXTRACT(month FROM ce.air_date::date) AS month, EXTRACT(year FROM ce.air_date::date) AS year
                    FROM staging.stage_character sc, staging.join_character_episode j, staging.stage_episode ce
                    WHERE sc.id = j.id_character and j.id_episode = ce.id
                    AND sc.species = 'Human'
                    group by EXTRACT(month FROM ce.air_date::date), EXTRACT(year FROM ce.air_date::date)) tmp
                    JOIN mart.month_russian rus ON tmp.month = rus.id_month
                    order by tmp.year, rus.id_month;
                    """