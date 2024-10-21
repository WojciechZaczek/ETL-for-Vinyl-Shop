from airflow.hooks.base import BaseHook
from airflow.models import Connection
from airflow.utils.db import provide_session
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from typing import Union, Optional


@provide_session
def create_conn_airflow(session=None) -> None:
    """
    Create a new MSSQL connection in Airflow if it does not already exist.

    This function checks if a connection with the specified conn_id already exists
    in the Airflow metadata database. If it does not exist, it creates a new MSSQL
    connection with the provided details.

    :param session: SQLAlchemy ORM session (provided by Airflow)
    """

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


def create_conn_engine(conn_id: str) -> Union[Engine, None]:
    """
    Create a SQLAlchemy engine using the connection details from Airflow's connection.

    This function retrieves the connection details using the given conn_id and creates
    a SQLAlchemy engine for connecting to a Microsoft SQL Server database.

    :param conn_id: The connection ID to retrieve the connection details from Airflow.
    :return: SQLAlchemy Engine instance if the connection is of type 'mssql', otherwise None.
    """
    conn = BaseHook.get_connection(conn_id)
    if conn.conn_type == "mssql":
        connection_string = (
            f"mssql+pyodbc://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
            f"?driver=ODBC+Driver+17+for+SQL+Server")
        engine = create_engine(connection_string)
    return engine


def create_conn_engine_trust(servername: str = 'localhost', dbname: str = 'master') -> Engine:
    """
    Create a SQLAlchemy engine using a trusted connection to Microsoft SQL Server.

    This function creates an engine that uses Windows authentication (trusted connection)
    to connect to the specified database on the given server.

    :param servername: The name of the SQL Server (default: 'localhost').
    :param dbname: The name of the database to connect to (default: 'master').
    :return: SQLAlchemy Engine instance for the specified SQL Server.
    """
    engine = create_engine(
        'mssql+pyodbc://@' + servername + '/' + dbname + '?trusted_connection=yes&driver=ODBC+Driver+13+for+SQL+Server'
    )
    return engine
