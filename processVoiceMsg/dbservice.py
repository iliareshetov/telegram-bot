import logging

import psycopg2

from processVoiceMsg.config import DATABASE_HOST, DATABASE, USER, PASSWORD


def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (
        """
        CREATE TABLE IF NOT EXISTS messages (
            msg_id SERIAL PRIMARY KEY,
            processed_msg text NOT NULL,
             processed_msg text NOT NULL
        )
        """,
        """CREATE TABLE IF NOT EXISTS parts (
                part_id SERIAL PRIMARY KEY,
                part_name VARCHAR(255) NOT NULL
                )
        """
    )
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(
            host=DATABASE_HOST,
            database=DATABASE,
            user=USER,
            password=PASSWORD)
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error("db error", error)
    finally:
        if conn is not None:
            conn.close()


def insert_msg(processed_msg):
    """ insert a new vendor into the vendors table """
    sql = """INSERT INTO messages(processed_msg)
             VALUES(%s) RETURNING msg_id;"""
    conn = None
    msg_id = None
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(
            host=DATABASE_HOST,
            database=DATABASE,
            user=USER,
            password=PASSWORD)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql, (processed_msg,))
        # get the generated id back
        msg_id = cur.fetchone()[0]
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error("insert error", error)
    finally:
        if conn is not None:
            conn.close()

    return msg_id
