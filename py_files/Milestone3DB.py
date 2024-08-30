import psycopg2
from psycopg2 import sql, pool, extras


class Milestone3DB:
    def __init__(self, host, dbname, user, password, port=5432):
        self.host = host
        self.dbname = dbname
        self.user = user
        self.password = password
        self.port = port
        self.connection_pool = None

    def create_connection_pool(self, num_threads):
        self.connection_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=num_threads,
            host=self.host,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            port=self.port
        )

    def get_connection(self):
        if self.connection_pool:
            return self.connection_pool.getconn()
        else:
            raise Exception('Connection pool not created')

    def release_connection(self, connection):
        if self.connection_pool:
            self.connection_pool.putconn(connection)

    def get_checkin_day_fk(self, connection, day, fk_business_id):
        with connection.cursor() as cursor:
            cursor.execute("""
                select day_id from checkin_day
                where fk_business_id = %s and day = %s
            """, (fk_business_id, day))
            result = cursor.fetchone()
            if result is not None:
                return result[0]
            return None

    def insert_into_table(self, connection, table, data):
        columns = data.keys()
        values = [data[column] for column in columns]

        insert_statement = sql.SQL(
            'INSERT INTO {table} ({columns}) VALUES ({values})'
        ).format(
            table=sql.Identifier(table),
            columns=sql.SQL(', ').join(map(sql.Identifier, columns)),
            values=sql.SQL(', ').join(sql.Placeholder() * len(values))
        )
        with connection.cursor() as cursor:
            cursor.execute(insert_statement, values)

    def insert_into_table_conflicts(self, connection, table, data, conflict_columns=None):
        columns = data.keys()
        values = [data[column] for column in columns]

        insert_statement = sql.SQL(
            'INSERT INTO {table} ({columns}) VALUES ({values})'
        ).format(
            table=sql.Identifier(table),
            columns=sql.SQL(', ').join(map(sql.Identifier, columns)),
            values=sql.SQL(', ').join(sql.Placeholder() * len(values))
        )
        if conflict_columns:
            conflict_statement = sql.SQL(
                ' ON CONFLICT ({conflict_columns}) DO NOTHING'
            ).format(
                conflict_columns=sql.SQL(', ').join(map(sql.Identifier, conflict_columns))
            )
            insert_statement = sql.Composed([insert_statement, conflict_statement])

        with connection.cursor() as cursor:
            cursor.execute(insert_statement, values)

    def insert_batch(self, connection, table, data_list, conflict_columns=None, max_retries=3):
        columns = data_list[0].keys()
        flattened_values = [tuple([d[key] for key in columns]) for d in data_list]

        columns_str = ', '.join(columns)
        values_template = ', '.join(['%s'] * len(columns))

        if conflict_columns:
            update_str = ', '.join([f"{col}=EXCLUDED.{col}" for col in columns if col not in conflict_columns])
            conflict_str = ', '.join(conflict_columns)
            insert_query = f"""
                insert into {table} ({columns_str}) values %s
                on conflict ({conflict_str}) 
                do nothing
            """
        else:
            insert_query = f"""
                insert into {table} ({columns_str}) values %s
            """

        attempt = 0
        while attempt < max_retries:
            try:
                with connection.cursor() as cursor:
                    psycopg2.extras.execute_values(cursor, insert_query, flattened_values, template=None, page_size=100)
                connection.commit()
                break
            except psycopg2.errors.DeadlockDetected as e:
                print(f"Deadlock detected: {e}, retrying {attempt + 1}/{max_retries}")
                connection.rollback()
                attempt += 1
                if attempt == max_retries:
                    raise e
            except psycopg2.DatabaseError as e:
                connection.rollback()
                attempt += 1
                if attempt == max_retries:
                    raise e
