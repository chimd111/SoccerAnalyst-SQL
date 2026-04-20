# SoccerAnalyst-SQL

## Requirements
- Java
- Microsoft SQL Server
- JDBC SQL Server driver

## Setup
1. Create a SQL Server database named `SoccerDB`.
2. Copy `auth.cfg.example` to `auth.cfg`.
3. Edit `auth.cfg` with your SQL Server username and password.
4. Run the program.
5. From the maintenance menu:
   - create tables
   - repopulate database

## auth.cfg format
db.host=localhost
db.port=1433
db.name=SoccerDB
db.user=your_username
db.password=your_password


## How to Run

1. Install Java and Microsoft SQL Server.

   - [Download **SQL Server 2022 Express Edition** from Microsoft.](https://www.microsoft.com/en-us/download/details.aspx?id=104781)
   - When you open the installer, choose basic
   - Complete Installation and install SSMS if prompted (recommended)

2. Create a database named `SoccerDB` in SQL Server.

    - New Query then run "CREATE DATABASE SoccerDB;"
    - Execute (or press F5)

3. In the project root, create `auth.cfg` with:

db.host=localhost\SQLEXPRESS
db.port=1433
db.name=SoccerDB

4. Compile and run the program:
   javac futbolsoccer.java
   java futbolsoccer

5. In the program menu, choose:
   1. Create Database for new Users

This will create all tables from `sql/schema.txt` and populate them using the SQL files in the `sql/` folder.