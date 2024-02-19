## _Тестовое задание на позицию Data Engineer_
## Требования к системе 
  Установлены
- docker
- docker compose  

Память
- RAM 4096 MB
- Disk 40 GB

## Инструкция по сборке:
1) клонировать репозиторий
```sh
git clone https://github.com/PavelAnt11/test_rick_morty
```
2) Из папки куда склонировали репо запустить последовательно
```sh
echo -e "AIRFLOW_UID=$(id -u)" > .env
make setup
make start
```
3) Чтобы запустить DAG нужно
 - перейти  http://localhost:8080/login/
-- login: airflow
-- пароль: airflow
- Запустить DAG rick_and_morty
4) Чтобы посмотреть результаты
- Перейти на http://localhost:5050/login
-- Логин: pgadmin4@pgadmin.org
-- Пароль: admin1234
- Создать подключение к серверу
-- Дать название серверу (например PostgresLocal)
- На вкладке соединение в поле Имя/адрес сервера указать **database**
 -- порт 5432
 -- Логин и пароль postgres.
5) После соединения можно выполнить запрос к построенным витринам в схеме mart
```sh
select * from mart.human_episode_month_year
select * from mart.human_episode_month
```
