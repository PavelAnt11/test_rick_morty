# test_rick_morty
Тестовое задание на позицию Data Engineer
Инструкция по сборке:
1) клонировать репозиторий
2) Из папки запустить последовательно
echo -e "AIRFLOW_UID=$(id -u)" > .env
make setup
make start
3) Чтобы запустить GAG нужно
перейти http://localhost:8080/login/
login и пароль: airflow
Запустить даг rick_and_morty
4) Чтобы посмотреть результаты:
Перейти на http://localhost:5050/login
Логин pgadmin4@pgadmin.org  пароль  admin1234
Создать подключение к серверу
Дать название серверу
На вкладке соединение в поле Имя/адрес сервера указать database
порт 5432
Логин и пароль postgres.
5) После соединения можно выполнить запрос к построенным витринам в схеме mart
select * from mart.human_episode_month_year (с разбивкой по годам)
select * from mart.human_episode_month (разбивка по месяцам)
