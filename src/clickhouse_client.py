import clickhouse_connect

from src.config import get_settings

_client = None


def get_ch_client():
    global _client
    if _client is None:
        s = get_settings()
        _client = clickhouse_connect.get_client(
            host=s.clickhouse_host,
            port=s.clickhouse_port,
            username=s.clickhouse_user,
            password=s.clickhouse_password,
            database=s.clickhouse_database,
        )
    return _client


def execute_query(query, parameters=None):
    return get_ch_client().query(query, parameters=parameters).result_rows


def execute_query_df(query, parameters=None):
    return get_ch_client().query_df(query, parameters=parameters)


def insert_dataframe(table, df):
    get_ch_client().insert_df(table=table, df=df)
    return len(df)


def execute_command(command):
    get_ch_client().command(command)
