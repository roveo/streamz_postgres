import random

from psycopg2.sql import SQL, Identifier, Literal


class Writer:
    def __init__(self, connection, table):
        self.table = Identifier(table)
        self.connection = connection
        self.size = 0

    def create_table(self):
        with self.connection.cursor() as cur:
            cur.execute(
                SQL(
                    "create table {table} ("
                    "id serial not null primary key,"
                    "x int not null"
                    ");"
                ).format(table=self.table)
            )
            self.connection.commit()

    def update(self, n):
        ids = random.sample(range(1, self.size + 1), n)
        with self.connection.cursor() as cur:
            for _id in ids:
                cur.execute(
                    SQL("update {table} set x = 2 where id = {_id};").format(
                        table=self.table, _id=Literal(_id)
                    )
                )
            self.connection.commit()

    def insert(self, n):
        with self.connection.cursor() as cur:
            for _ in range(n):
                cur.execute(
                    SQL("insert into {table} (x) values (1);").format(table=self.table)
                )
            self.connection.commit()
            self.size += n
