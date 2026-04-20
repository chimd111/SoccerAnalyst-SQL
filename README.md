# SoccerAnalyst-SQL

## Requirements

* Java (JDK 11+)
* Microsoft SQL Server (Express is fine)
* SQL Server Management Studio (SSMS)
* Microsoft JDBC Driver for SQL Server

---

## Setup Instructions

### 1. Install SQL Server

* Download **SQL Server Express**
* Install using **Basic setup**
* Install **SSMS** if prompted

---

### 2. Create the Database

Open SSMS → New Query → run:

```sql
CREATE DATABASE SoccerDB;
```

---

### 3. Configure `auth.cfg`

Create a file in the project root:

```properties
db.host=localhost
db.port=62201
db.name=SoccerDB
```

> ⚠️ Note: This project uses **Windows Authentication**, not username/password.

---

### 4. Add JDBC Driver

Place the following in your project folder:

* `mssql-jdbc-13.4.0.jre11.jar`
* `mssql-jdbc_auth-13.4.0.x64.dll`

---

### 5. Compile and Run

```bash
javac -cp ".;mssql-jdbc-13.4.0.jre11.jar" futbolsoccer.java
java -cp ".;mssql-jdbc-13.4.0.jre11.jar" -Djava.library.path="." futbolsoccer
```

Or using Makefile:

```bash
make clean
make
make run
```

---

## First Time Use

When the program starts:

1. Select:

```
1. Create Database for new Users
```

2. Type:

```
CREATE
```

This will:

* Create all tables
* Populate data from SQL files in `/sql`

---

## Features

The application provides:

### Raw Data Access

* View all tables with pagination

### Standard Queries

* Top players by market value
* Clubs with most wins/losses
* Transfer activity
* Competition participation

### Advanced Queries

* Squad value vs win percentage
* Financial health of competitions
* High-spending clubs with poor performance

### Maintenance Tools

* Delete all data
* Repopulate database

---

## Notes

* Uses **SQL Server (T-SQL)**
* Uses **JDBC with integrated security**
* Requires SQL Server to be running
* Port (`62201`) must match your SQL Server configuration

---

## Troubleshooting

### "Database connection failed"

* Ensure SQL Server is running
* Confirm port number in SSMS:

  * SQL Server Configuration Manager → TCP/IP → IPAll → TCP Port

### "Unable to load authentication DLL"

* Make sure:

```
mssql-jdbc_auth-13.4.0.x64.dll
```

is in the project root

---

## Project Structure

```
project/
│
├── futbolsoccer.java
├── auth.cfg
├── Makefile
├── mssql-jdbc-13.4.0.jre11.jar
├── mssql-jdbc_auth-13.4.0.x64.dll
└── sql/
    ├── schema.txt
    ├── *.txt (data files)
```

---
