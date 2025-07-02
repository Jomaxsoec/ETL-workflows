# SCD Type 2 Demo in Python

## Overview

This repository demonstrates **Slowly Changing Dimension Type 2 (SCD2)** logic using Python with an in-memory SQLite database. It is a lightweight, educational example showing how to track historical changes in dimension tables (commonly needed in data warehousing) by maintaining full change history for attributes like a customer’s phone number.

The script follows a classic SCD Type 2 approach:
- Inserts new dimension records when changes occur
- Closes previous dimension records by updating their `end_date`
- Preserves the entire history of changes

## Features

✅ Pure Python — no external dependencies  
✅ In-memory SQLite — no need for a separate DB  
✅ Demonstrates end-to-end SCD2 logic  
✅ Clear and modular structure  
✅ Easy to adapt to other RDBMS (Postgres, MySQL, etc.)

## How it works

The script:

1. Creates **source**, **staging**, and **target** tables in SQLite.  
2. Loads initial data into the source.  
3. Populates the target table with the current snapshot.  
4. Simulates a change in the source (changing a customer’s phone number).  
5. Reloads the staging area and updates the target table to reflect this change while preserving historical records.

The final output prints the **target dimension table** with both the current and historical versions of the data.

## Project Structure

```plaintext
.
├── SCD2.py          # Python script demonstrating SCD Type 2
└── README.md        # This file
```
# To run #
``` python SCD2.py ```
