import sqlite3

import pandas as pd

db = 'chinook.db'

def run_query(query):
    """
    Connects to the database
    and runs a given query

    Arguments: query

    Returns a pandas dataframe
    """
    with sqlite3.connect(db) as conn:
        return pd.read_sql(query, conn)

def run_command(command):
    """
    Connects to the database
    and executes a command

    Arguments: command
    """
    with sqlite3.connect(db) as conn:
        conn.isolation_level = None
        conn.execute(command)

def show_tables():
    """
    Shows tables

    Returns a pandas dataframe
    """
    q = '''
        SELECT
            name
        FROM sqlite_master
        WHERE type IN ("table", "view")
        '''
    return run_query(q)

def get_table_row_count(tablename):
    """
    Returns number of rows of 
    a given tablename

    Arguments: table name
    """
    q = '''
        SELECT
            COUNT(1)
        FROM %s;
        '''% tablename
    return run_query(q)["COUNT(1)"][0]


tables = show_tables()
tables["row_count"] = [get_table_row_count(t) for t in tables["name"]]

print(tables)