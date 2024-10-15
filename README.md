## Python Script interacting with Databricks 
### By Tursunai Turumbekova
[![CI](https://github.com/nogibjj/SQL_Query_Databricks1_Tursunai/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/SQL_Query_Databricks1_Tursunai/actions/workflows/cicd.yml)

## Project Overview

This project demonstrates how to connect to a SQL database using Python, perform CRUD operations (Create, Read, Update, and Delete), and execute SQL queries. The project also implements a CI/CD pipeline to ensure database operations work correctly and continuously. All operations can be performed via the command-line interface (CLI).

### Key Features:
- **Database Connection**: Establishes a connection to a SQLite database.
- **CRUD Operations**: Supports creating, reading, updating, and deleting records in the database.
- **SQL Queries**: Includes at least two distinct SQL queries to manipulate and retrieve data.
- **CI/CD Pipeline**: Automatically tests the correctness of database operations through the CI/CD pipeline.
- **Unit Tests**: Includes unit tests to verify the correct functionality of each CRUD operation.
- **CLI-Based Operations**: All CRUD operations and SQL queries can be executed via the command-line interface (CLI).

## File Structure

The project is organized into the following structure:
```bash
py_script_with_SQLDatabase/
│
├── mylib/
│   ├── extract.py           # Contains the function to extract data
│   ├── transform_load.py     # Contains the function to load and transform data into the SQLite database
│   ├── query.py              # Contains functions for CRUD operations and SQL queries
│
├── data/
│   └── urbanization.csv      # Sample dataset used for loading into the database
│
├── .github/
│   └── workflows/            # CI/CD pipeline configurations
│
├── main.py                   # Main script for executing CRUD operations via CLI
├── test_main.py              # Unit tests for the CRUD operations
├── Makefile                  # Automation for testing, linting, and formatting
├── requirements.txt          # Python dependencies
└── README.md                 # Project documentation
```
## Purpose of project
The goal of this project is to create an ETL-Query pipeline utilizing a cloud service like Databricks. This pipeline will involve tasks such as extracting data on Urbanization index from from FiveThirtyEight's public dataset, cleaning and transforming the data, then loading it into Databricks SQL Warehouse. Once the data is in place, we'll be able to run complex queries that may involve tasks like joining tables, aggregating data, and sorting results. This will be accomplished by establishing a database connection to Databricks. 
## Preparation
1. open codespaces 
2. wait for container to be built and virtual environment to be activated with requirements.txt installed 
3. make your own .env file to store your Databricks' secrets as it requires a conncection to be established to Databricks
3. extract: run `make extract`
4. transform and load: run `make transform_load`
4. query: run `make query` or alternatively write your own query using `python main.py general_query <insert query>`
