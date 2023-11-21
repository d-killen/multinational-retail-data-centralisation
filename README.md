# Mulinational Retail Data Centralisation

## Contents

 1. Description
 1. Skills Covered
 1. Installation Instructions
 1. Usage Instructions
 1. Project File Structure
 1. License Information

## Description

This project takes data from multiple disparate sources, cleans it up and uploads to a single database

It then runs queries of the database to answer specific questions and prints the results

## Skills Covered

Learning achieved through this project includes:
- Experience with Python: 
    - function definition
    - class definitions
    - class attributes
    - SQLAlchemy module
- Experience with SQL
- Experience with git & GitHub:
    - Making local changes
    - Committing and pushing changes back to the cloud repository
- README creation and editing

## Installation Instructions

1. Download the Mulinational Retail Data Centralisation project directory
1. Create the necessary PostgreSQL database named sales_data 
1. Create a local YAML file named db_creds.yaml and locate it in the project directory and within it enter the following data:

        RDS_HOST: <host address of the data stored in the RDS database>
        RDS_PASSWORD: <password to the data stored in the RDS database>
        RDS_USER: <username to the data stored in the RDS database>
        RDS_DATABASE: <name of the RDS database>
        RDS_PORT: <port to the data stored in the RDS database>

        SD_HOST: <your sales_data host>
        SD_PASSWORD: <password to your sales_data database>
        SD_USER: <username for your sales_data database>
        SD_DATABASE: <name of your sales_data database>
        SD_PORT: <port to your sales_data database>

## Usage Instructions

Once installed, run the milestones.py file

NOTE: Running the milestones.py file twice will produce an error without first removing all tables from your sales_data database

## Project File Structure

- Project Directory
    - README.md
    - milestones.py
    - data_extraction.py
    - data_cleaning.py
    - database_utils.py

## License Information

Mulinational Retail Data Centralisation
Copyright (C) 2023 Daniel Killen

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.
