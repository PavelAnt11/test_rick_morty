import psycopg2

class Database:
    def __init__(self, database_type: str, host: str, port: str, db_name: str, user_name: str, password: str):
        self.host = host
        self.port = port
        self.db_name = db_name
        self._user_name = user_name
        self._password = password
        self.database_type = database_type
        self.conn = self.establish_connection()
        self.cursor = self.conn.cursor()

    def establish_connection(self):
        """
        Создание подключения к локальной базе данных
        :return: Connection object
        """
        try:
            if self.database_type == 'postgresql':
                conn = psycopg2.connect(host=self.host, port=self.port, dbname=self.db_name, user=self._user_name,
                                        password=self._password)
                conn.set_session(autocommit=True)

                conn.autocommit = True
            print(f'Успешно подключились к  {self.db_name}!')

            return conn
        except:
            print(f'Ошибка при подключении к {self.database_type} базе данных! Проверть докер контейнер и убедитесь, что база запущена')
          
