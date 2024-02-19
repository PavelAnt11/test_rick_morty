from connection import Database
from airflow.models import DAG
from airflow.operators.python import PythonOperator
import datetime as dt
import requests
import psycopg2
from sql import stage_character, stage_lcoation, stage_episode, join_character_episode, join_character_location, create_schema_stage
from sql import table_month_creat, table_mart_only_month, table_mart_month_year, create_schema_mart, view_month, view_month_year, table_month_insert, table_month_data


postgresDB = Database('postgresql', host='database', port='5432', db_name='rickmorty', user_name='postgres', password='postgres')


# базовые аргументы DAG
args = {
   'owner': 'airflow',  # Информация о владельце DAG
   'start_date': dt.datetime(2024, 2, 17),  # Время начала выполнения пайплайна
}


dag = DAG(
    dag_id='rick_and_morty',  # Имя DAG
    schedule_interval=None,  # Периодичность запуска
    default_args=args,  # Базовые аргументы
)


def iter_from_api(api_name: str):
    session = requests.Session()
    page = 1
    while True:
        response = session.get(f'https://rickandmortyapi.com/api/{api_name}?page={page}')

        data = response.json()
        if response.status_code != 200:
            break

        yield from data['results']

        page += 1

def insert_execute_batch_character(connection, characters) -> None:
    with connection.cursor() as cursor:

        all_characters = [{**hero,
                           'image_url': hero['image'],
                           'id_origin_location': -1 if hero['origin']['name'] == 'unknown' else hero['origin']['url'].rsplit('/', 1)[-1],
                           'id_last_location': -1 if hero['location']['name'] == 'unknown' else hero['location']['url'].rsplit('/', 1)[-1],
                           'join_id': [(hero['id'], int(i.rsplit('/', 1)[-1])) for i in hero['episode']]
        } for hero in characters]

        all_join = []
        for character in [hero['join_id'] for hero in all_characters]:
            for k, v in character:
                all_join.append({'id_character': k, 'id_episode':v})

        psycopg2.extras.execute_batch(cursor, """
            INSERT INTO staging.join_character_episode VALUES (
                %(id_character)s,                  
                %(id_episode)s                       
            );
        """, all_join)
        
        psycopg2.extras.execute_batch(cursor, """
            INSERT INTO staging.stage_character VALUES (
                %(id)s,                  
                %(name)s,                 
                %(status)s,               
                %(species)s,              
                %(type)s,           
                %(gender)s,             
                %(id_origin_location)s,   
                %(id_last_location)s,     
                %(image_url)s,         
                %(url)s             
            );
        """, all_characters)

def insert_execute_batch_episode(connection, episodes) -> None:
    with connection.cursor() as cursor:

        all_episodes = [{'id': episode['id'],
                         'name': episode['name'],
                         'air_date': episode['air_date'],
                         'episode': episode['episode'],
                         'url': episode['url'],
                            } for episode in episodes]

        
        psycopg2.extras.execute_batch(cursor, """
            INSERT INTO staging.stage_episode VALUES (
                %(id)s,                  
                %(name)s,                 
                %(air_date)s,               
                %(episode)s,              
                %(url)s             
            );
        """, all_episodes)

def insert_execute_batch_location(connection, locations) -> None:
    with connection.cursor() as cursor:

        all_locations = [{'id': loc['id'],
                         'name': loc['name'],
                         'type': loc['type'],
                         'dimension': loc['dimension'],
                         'url': loc['url'],                        
                         'join_id': [(loc['id'], int(i.rsplit('/', 1)[-1])) for i in loc['residents']]
        } for loc in locations]

        all_join = []
        for loc in [loc['join_id'] for loc in all_locations]:
            for k, v in loc:
                all_join.append({'id_location':k, 'id_character': v, })

        psycopg2.extras.execute_batch(cursor, """
            INSERT INTO staging.join_character_location VALUES (
                %(id_location)s,                  
                %(id_character)s           
            );
        """, all_join)
        
        psycopg2.extras.execute_batch(cursor, """
            INSERT INTO staging.stage_location VALUES (
                %(id)s,                  
                %(name)s,                             
                %(type)s,           
                %(dimension)s,                    
                %(url)s             
            );
        """, all_locations)

def download_data_character():
    characters = iter_from_api('character')
    connection = postgresDB.conn

    insert_execute_batch_character(connection, characters)

def download_data_episode():
    episodes = iter_from_api('episode')
    connection = postgresDB.conn

    insert_execute_batch_episode(connection, episodes)

def download_data_location():
    locations = iter_from_api('location')
    connection = postgresDB.conn

    insert_execute_batch_location(connection, locations)

def download_all_data_from_api_to_db():
    download_data_character()
    download_data_episode()
    download_data_location()


def create_tables_stage():
    connection = postgresDB.conn
    with connection.cursor() as cursor:
        cursor.execute(create_schema_stage)

        cursor.execute(stage_character)

        cursor.execute(stage_character)

        cursor.execute(stage_lcoation)

        cursor.execute(stage_episode)

        cursor.execute(join_character_episode)

        cursor.execute(join_character_location)



def create_tables_mart():
    connection = postgresDB.conn
    with connection.cursor() as cursor:
        cursor.execute(create_schema_mart)

        cursor.execute(table_month_creat)

        cursor.execute(table_mart_only_month)

        cursor.execute(table_mart_month_year)

        psycopg2.extras.execute_batch(cursor, table_month_insert, table_month_data)
        

def create_and_load_mart():
    create_tables_mart()
    connection = postgresDB.conn
    with connection.cursor() as cursor:

        cursor.execute(view_month)
        data = cursor.fetchall()

        psycopg2.extras.execute_batch(cursor, """
            INSERT INTO mart.human_episode_month (cnt, name_month) VALUES (
                %s,                  
                %s                           
            );
        """, data)

        cursor.execute(view_month_year)
        data = cursor.fetchall()

        psycopg2.extras.execute_batch(cursor, """
            INSERT INTO mart.human_episode_month_year (cnt, name_month, year) VALUES (
                %s,                  
                %s,
                %s                                                 
            );
        """, data)


create_tables_stg = PythonOperator(
    task_id='create_tables_stg',
    python_callable=create_tables_stage,
    dag=dag,
)

# create_tables_mrt = PythonOperator(
#     task_id='create_tables_mart',
#     python_callable=create_and_load_mart,
#     dag=dag,
# )

# запись данных из API
download = PythonOperator(
    task_id='download',
    python_callable=download_all_data_from_api_to_db,
    dag=dag,
)

download_mart = PythonOperator(
    task_id='create_and_load_mart',
    python_callable=create_and_load_mart,
    dag=dag,)


create_tables_stg >> download >> download_mart
