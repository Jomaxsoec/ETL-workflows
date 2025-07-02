"""
**************************************************************
*                                                            *
*  Title     : Incremental Data Load with Control Table      *
*  Author    : Joshua Henry                            *
*  Date      : 2025-07-02                                    *
*  Purpose   : Demonstrates loading data from source to      *
*              staging to target using control table logic   *
*              in Python with MySQL connection abstraction   *
*                                                            *
**************************************************************
"""

import pymysql
from pymysql import Error  
import pymysql.cursors as cursor
import logging

# -------------------------------------------------------------
# Configure logging
# -------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# -------------------------------------------------------------
# Connect to the MySQL database
# -------------------------------------------------------------
def connect_to_database():
    """
    Establishes a connection to the database
    using the custom sql.connection module.
    """
    try:
        conn = pymysql.connect(
            host='localhost',
            user='root',
            password='SpaceX0203',
            database='Madhan',
            port=3306
        )
        logging.info("Connected to MySQL database.")
        return conn
    except Error as err:
        logging.error(f"Connection error: {err}")
        return None


# -------------------------------------------------------------
# Insert data from source to staging
# -------------------------------------------------------------
def insert_data_to_staging(conn):
    """
    Loads data from the source table to the staging table.
    """
    try:
        cursor_obj = conn.cursor()
        query_str = """
            INSERT INTO stg_jun25 (col1_id, col2_desc, col3_desc)
            SELECT col1_id, col2_desc, col3_desc FROM src_jun25;
        """
        cursor_obj.execute(query_str)
        conn.commit()
        logging.info("Data inserted into staging table successfully.")
        print("-------------------------------------------------------------")
        print("Staging Table: stg_jun25")
        cursor_obj.execute("SELECT * FROM stg_jun25;")
        rows = cursor_obj.fetchall()
        for row in rows:
            print(row)
        print("-------------------------------------------------------------")
    except Exception as e:
        logging.error(f"Error inserting data into staging: {e}")
        conn.rollback()


# -------------------------------------------------------------
# Insert data to control table initially
# -------------------------------------------------------------
def insert_data_to_control(conn):
    """
    Inserts a new entry into the control table with timestamps.
    """
    try:
        cursor_obj = conn.cursor()
        query_str = """
            INSERT INTO control_table (table_name, max_created_date, max_modified_date)
            VALUES ('tgt_jun25', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
        """
        cursor_obj.execute(query_str)
        conn.commit()
        logging.info("Data inserted into control table successfully.")
        print("-------------------------------------------------------------")
        print("Control Table: control_table")
        cursor_obj.execute("SELECT * FROM control_table;")
        rows = cursor_obj.fetchall()
        for row in rows:
            print(row)
        print("-------------------------------------------------------------")
    except Exception as e:
        logging.error(f"Error inserting data into control table: {e}")
        conn.rollback()


# -------------------------------------------------------------
# Update control table after staging load
# -------------------------------------------------------------
def update_control_table_stg(conn):
    """
    Updates the control table's max dates based on the staging data,
    and simulates an update on the source table.
    """
    try:
        cursor_obj = conn.cursor()
        # update control_table with new max values
        query_str = """
            UPDATE control_table
            SET
                max_created_date = (SELECT MAX(created_date) FROM stg_jun25),
                max_modified_date = (SELECT MAX(modified_date) FROM stg_jun25)
            WHERE table_name = 'tgt_jun25';
        """
        cursor_obj.execute(query_str)
        conn.commit()
        logging.info("Control table updated after staging load.")
        print("-------------------------------------------------------------")
        print("Control Table: control_table Updated")
        cursor_obj.execute("SELECT * FROM control_table;")
        rows = cursor_obj.fetchall()
        for row in rows:
            print(row)
        print("-------------------------------------------------------------")

        # simulate a source table change
        query_update = """
            UPDATE src_jun25
            SET col2_desc = 'Joshua', col3_desc = 'Pro player'
            WHERE col1_id = 4;
        """
        cursor_obj.execute(query_update)
        print("-------------------------------------------------------------")
        print("Source Table: src_jun25 Updated")
        cursor_obj.execute("SELECT * FROM src_jun25;")
        rows = cursor_obj.fetchall()
        for row in rows:
            print(row)
        print("-------------------------------------------------------------")
        conn.commit()
        logging.info("Source table updated to simulate a change.")
    except Exception as e:
        logging.error(f"Error updating control table after staging: {e}")
        conn.rollback()


# -------------------------------------------------------------
# Insert new data into staging after source update
# -------------------------------------------------------------
def insert_into_stg(conn):
    """
    Loads new or changed records from the source to staging
    based on max dates from the control table.
    """
    try:
        cursor_obj = conn.cursor()
        query_str = """
            INSERT INTO stg_jun25 (col1_id, col2_desc, col3_desc)
            SELECT s.col1_id, s.col2_desc, s.col3_desc
            FROM src_jun25 s
            JOIN control_table c
              ON c.table_name = 'tgt_jun25'
            WHERE s.created_date > c.max_created_date
               OR s.modified_date > c.max_modified_date;
        """
        cursor_obj.execute(query_str)
        conn.commit()
        logging.info("New or changed data inserted into staging table successfully.")
        print("-------------------------------------------------------------")
        print("Staging Table: stg_jun25 Updated")
        cursor_obj.execute("SELECT * FROM stg_jun25;")
        rows = cursor_obj.fetchall()
        for row in rows:
            print(row)
        print("-------------------------------------------------------------")
    except Exception as e:
        logging.error(f"Error inserting new data into staging: {e}")
        conn.rollback()


# -------------------------------------------------------------
# Push data from staging to target with upsert logic
# -------------------------------------------------------------
def push_to_target_table(conn):
    """
    Inserts or updates data from staging to target using
    ON DUPLICATE KEY logic.
    """
    try:
        cursor_obj = conn.cursor()
        query_str = """
            INSERT INTO tgt_jun25 (col1_id, col2_desc, col3_desc)
            SELECT col1_id, col2_desc, col3_desc
            FROM stg_jun25
            ON DUPLICATE KEY UPDATE
                col2_desc = VALUES(col2_desc),
                col3_desc = VALUES(col3_desc);
        """
        cursor_obj.execute(query_str)
        conn.commit()
        logging.info("Data pushed to target table successfully.")
        print("-------------------------------------------------------------")
        print("Target Table: tgt_jun25")
        cursor_obj.execute("SELECT * FROM tgt_jun25;")
        rows = cursor_obj.fetchall()
        for row in rows:
            print(row)
        print("-------------------------------------------------------------")
    except Exception as e:
        logging.error(f"Error pushing data to target table: {e}")
        conn.rollback()


# -------------------------------------------------------------
# Final update of control table after pushing to target
# -------------------------------------------------------------
def update_control_table_tgt(conn):
    """
    Updates the control table based on the target table’s
    new max dates after loading is done.
    """
    try:
        cursor_obj = conn.cursor()
        query_str = """
            UPDATE control_table
            SET
                max_created_date = (SELECT MAX(created_date) FROM tgt_jun25),
                max_modified_date = (SELECT MAX(modified_date) FROM tgt_jun25)
            WHERE table_name = 'tgt_jun25';
        """
        cursor_obj.execute(query_str)
        conn.commit()
        logging.info("Control table updated after target load.")
    except Exception as e:
        logging.error(f"Error updating control table after target load: {e}")
        conn.rollback()


# -------------------------------------------------------------# 
#                   Main execution flow
# -------------------------------------------------------------#
if __name__ == "__main__":
    # Connect to the database
    conn = connect_to_database()
    if conn:
        try:
            # Step 1: Insert data into staging
            insert_data_to_staging(conn)

            # Step 2: Insert initial data into control table
            insert_data_to_control(conn)

            # Step 3: Update control table after staging load
            update_control_table_stg(conn)

            # Step 4: Insert new data into staging after source update
            insert_into_stg(conn)

            # Step 5: Push data from staging to target
            push_to_target_table(conn)

            # Step 6: Final update of control table after target load
            update_control_table_tgt(conn)

        finally:
            conn.close()
            logging.info("Database connection closed.")
    else:
        logging.error("Failed to connect to the database.")