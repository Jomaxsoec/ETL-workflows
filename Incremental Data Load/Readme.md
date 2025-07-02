# Incremental Data Load with Control Table

## Overview

This project demonstrates an **incremental data loading pipeline** using a **control table** approach in Python with MySQL. The script automates the extraction of new or changed records from a source table, loads them into a staging area, and then pushes them to the target table with upsert logic. The control table maintains checkpoint information (`max_created_date` and `max_modified_date`) to support efficient delta loads.

The pattern is commonly used in enterprise ETL (Extract, Transform, Load) pipelines to avoid full reloads and optimize performance.

## Features

✅ Python-based incremental data loader  
✅ Uses MySQL as the database  
✅ Control table to track max created/modified dates  
✅ Staging area for intermediate processing  
✅ Supports **upsert** logic in the target table  
✅ Modular, clear, and educational

## How It Works

The process flow is as follows:

1. Loads data from the source table (`src_jun25`) to the staging table (`stg_jun25`).  
2. Inserts a checkpoint in the control table to track `max_created_date` and `max_modified_date`.  
3. Updates the control table after initial load.  
4. Simulates a data change in the source to demonstrate incremental detection.  
5. Reloads only new or changed data from the source to staging, using the control table.  
6. Pushes the data to the target table (`tgt_jun25`) using an upsert (`ON DUPLICATE KEY UPDATE`) strategy.  
7. Updates the control table after the target is refreshed.

Each step logs messages to track progress, making the code easy to follow.

## Project Structure

```plaintext
.
├── staging.py        # Python script implementing incremental loading
└── README.md         # This file
```

## To Run
``` python staging.py```
