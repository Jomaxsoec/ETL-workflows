"""
**************************************************************
*                                                            *
*  Title     : SCD Type 2 Demo in Python                     *
*  Author    : Joshua Henry                            *
*  Date      : 2025-07-02                                    *
*  Purpose   : Implements Slowly Changing Dimension (SCD2)    *
*              logic in Python using sqlite3 for demo        *
*                                                            *
**************************************************************

PSEUDO CODE:

1. Setup in-memory SQLite database
2. Create source, target, and staging tables
3. Insert initial data into source
4. Load staging with source data (with start_date = modified_date)
5. Insert new rows into target from staging if:
     - no existing record exists in target
     - or phone number has changed
6. Simulate an update in source (e.g., changing phone number)
7. Reload staging with fresh source data
8. Update target rows to set end_date where:
     - active target records (end_date is NULL)
     - phone number has changed compared to staging
9. Insert new rows into target from staging with new changes
10. Display final target dimension table

This simulates a typical SCD Type 2 flow, tracking changes
to a dimension attribute over time.

"""

import sqlite3
from datetime import datetime

def scd2_demo():
    """
    Function to simulate SCD Type 2 dimension loading
    using sqlite3 for demonstration.
    """

    # -------------------------------------------------------
    # Step 1: Connect to in-memory SQLite database
    # -------------------------------------------------------
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()

    # -------------------------------------------------------
    # Step 2: Drop and recreate source, target, staging tables
    # -------------------------------------------------------
    cursor.executescript("""
        DROP TABLE IF EXISTS src;
        DROP TABLE IF EXISTS target;
        DROP TABLE IF EXISTS staging;

        CREATE TABLE src (
            cust_id INTEGER PRIMARY KEY,
            phone_no INTEGER,
            created_date TEXT DEFAULT CURRENT_TIMESTAMP,
            modified_date TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE target (
            cust_id INTEGER,
            phone_no INTEGER,
            created_date TEXT,
            modified_date TEXT,
            start_date TEXT,
            end_date TEXT DEFAULT NULL
        );

        CREATE TABLE staging (
            cust_id INTEGER,
            phone_no INTEGER,
            created_date TEXT,
            modified_date TEXT,
            start_date TEXT,
            end_date TEXT DEFAULT NULL
        );
    """)
    conn.commit()

    # -------------------------------------------------------
    # Step 3: Insert initial data into source
    # -------------------------------------------------------
    initial_data = [
        (1, 9876543210),
        (2, 9123456780),
        (3, 9012345678),
        (4, 9345678901),
        (5, 9988776655),
        (6, 666666)
    ]
    cursor.executemany("""
        INSERT INTO src (cust_id, phone_no)
        VALUES (?, ?)
    """, initial_data)
    conn.commit()

    # -------------------------------------------------------
    # Step 4: Load staging from source
    # (start_date is set as modified_date, end_date NULL)
    # -------------------------------------------------------
    cursor.execute("""
        INSERT INTO staging (cust_id, phone_no, created_date, modified_date, start_date, end_date)
        SELECT
            cust_id,
            phone_no,
            created_date,
            modified_date,
            modified_date,
            NULL
        FROM src
    """)
    conn.commit()

    # -------------------------------------------------------
    # Step 5: Initial insert into target (no matching active records)
    # -------------------------------------------------------
    cursor.execute("""
        INSERT INTO target (cust_id, phone_no, created_date, modified_date, start_date, end_date)
        SELECT
            s.cust_id,
            s.phone_no,
            s.created_date,
            s.modified_date,
            s.start_date,
            s.end_date
        FROM staging s
        LEFT JOIN target t
            ON s.cust_id = t.cust_id AND t.end_date IS NULL
        WHERE
            t.cust_id IS NULL
            OR t.phone_no != s.phone_no
    """)
    conn.commit()

    # -------------------------------------------------------
    # Step 6: Simulate data change in source (cust_id=2)
    # -------------------------------------------------------
    cursor.execute("""
        UPDATE src
        SET phone_no = 666
        WHERE cust_id = 2
    """)
    conn.commit()

    # -------------------------------------------------------
    # Step 7: Reload staging with updated source data
    # -------------------------------------------------------
    cursor.execute("DELETE FROM staging")
    cursor.execute("""
        INSERT INTO staging (cust_id, phone_no, created_date, modified_date, start_date, end_date)
        SELECT
            cust_id,
            phone_no,
            created_date,
            modified_date,
            modified_date,
            NULL
        FROM src
    """)
    conn.commit()

    # -------------------------------------------------------
    # Step 8: Close the current active target records
    # for customers where phone_no has changed
    # by setting end_date to staging.modified_date
    # -------------------------------------------------------
    cursor.execute("""
        UPDATE target
        SET end_date = (
            SELECT s.modified_date
            FROM staging s
            WHERE s.cust_id = target.cust_id
              AND s.phone_no != target.phone_no
        )
        WHERE
            end_date IS NULL
            AND EXISTS (
                SELECT 1
                FROM staging s
                WHERE s.cust_id = target.cust_id
                  AND s.phone_no != target.phone_no
            )
    """)
    conn.commit()

    # -------------------------------------------------------
    # Step 9: Insert new target records for changed phone_no
    # -------------------------------------------------------
    cursor.execute("""
        INSERT INTO target (cust_id, phone_no, created_date, modified_date, start_date, end_date)
        SELECT
            s.cust_id,
            s.phone_no,
            s.created_date,
            s.modified_date,
            s.start_date,
            s.end_date
        FROM staging s
        LEFT JOIN target t
            ON s.cust_id = t.cust_id AND t.end_date IS NULL
        WHERE
            t.cust_id IS NULL
            OR t.phone_no != s.phone_no
    """)
    conn.commit()

    # -------------------------------------------------------
    # Step 10: Show final target dimension
    # -------------------------------------------------------
    print("\nFinal TARGET dimension table:\n")
    for row in cursor.execute("""
        SELECT * FROM target ORDER BY cust_id ASC
    """):
        print(row)

    # -------------------------------------------------------
    # Clean up connection
    # -------------------------------------------------------
    conn.close()


# Run the demo
if __name__ == "__main__":
    scd2_demo()
