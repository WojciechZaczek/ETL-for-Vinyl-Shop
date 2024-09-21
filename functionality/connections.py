from airflow.models import Connection
from airflow.utils.db import provide_session
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine


@provide_session
def create_conn_airflow(session=None):

    conn_id = "mssql_conneciton"
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()

    if existing_conn is None:
        conn = Connection(
            conn_id=conn_id,
            conn_type='mssql',
            host='localhost',
            login='admin',
            password='admin',
            port=1433
        )
        session.add(conn)
        session.commit()


def create_conn_engine(conn_id):
    conn = BaseHook.get_connection(conn_id)
    if conn.conn_type == "mssql":
        connection_string = (
            f"mssql+pyodbc://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
            f"?driver=ODBC+Driver+17+for+SQL+Server")
        engine = create_engine(connection_string)
    return engine