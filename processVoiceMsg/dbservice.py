import logging

import psycopg2

from processVoiceMsg.config import DATABASE_HOST, DATABASE, USER, PASSWORD


def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id bigint PRIMARY KEY,
            first_name text NOT NULL,
            real_name text,
            phone_number integer,
            create_time timestamp NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS bookings (
            booking_id SERIAL PRIMARY KEY,
            user_id bigint NOT NULL,
            create_time timestamp NOT NULL,
            CONSTRAINT fk_user
                FOREIGN KEY(user_id) 
	            REFERENCES users(user_id)
	            ON DELETE CASCADE
	            ON UPDATE CASCADE
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS messages (
            msg_id SERIAL PRIMARY KEY,
            processed_msg text NOT NULL,
             processed_msg text NOT NULL
        )
        """,
        # """CREATE TABLE IF NOT EXISTS botusetracker (
        #
        #         user_id SERIAL PRIMARY KEY,
        #         part_name VARCHAR(255) NOT NULL
        #         )
        # """
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


def insert_booking(user_id, first_name, booking_timestamp):
    sqluser = """INSERT INTO users (user_id, first_name, create_time)
                VALUES(%s, %s, now()) 
                ON CONFLICT (user_id) 
                DO NOTHING;
            """

    sqlbooking = """
                INSERT INTO bookings (user_id, create_time)
                VALUES(%s, %s) RETURNING booking_id;
            """

    conn = None
    inserted_id = None
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
        cur.execute(sqluser, (user_id, first_name))
        cur.execute(sqlbooking, (user_id, booking_timestamp))
        # get the generated id back
        inserted_id = cur.fetchone()[0]
        # commit the changes to the database
        conn.commit()
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error("insert error", error)
    finally:
        if conn is not None:
            conn.close()

    return inserted_id


def fetch_all_bookings_for_user(user_id):
    sql = """SELECT b.create_time
                FROM users u
                INNER JOIN bookings b 
                ON u.user_id = b.user_id
                WHERE u.user_id = %s
                ORDER BY b.booking_id;
            """

    conn = None
    fetched_bookings = None
    try:
        # connect to the PostgreSQL database
        conn = psycopg2.connect(
            host=DATABASE_HOST,
            database=DATABASE,
            user=USER,
            password=PASSWORD)
        # create a new cursor
        cur = conn.cursor()

        cur.execute(sql, (user_id,))

        fetched_bookings = cur.fetchall()

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        if conn is not None:
            conn.close()

    return fetched_bookings
