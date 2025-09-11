# Database Structure Documentation

## Overview
The `client_data.db` is a SQLite database containing customer and order information. You will need to query information from this database to complete the task.
We **think** that the data looks as follows:

## Database Schema

### `customers` Table
```sql
CREATE TABLE customers (
    customer_id TEXT PRIMARY KEY,
    name TEXT,
    email TEXT,
    city_name TEXT
)
```

**Columns:**
- `customer_id`: Primary key
- `name`: Customer full name (First Last format)
- `email`: Customer email address
- `city_name`: Customer city

### `orders` Table
```sql
CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id TEXT,
    order_date TEXT,
    order_total REAL,
    FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
)
```

**Columns:**
- `order_id`: Auto-incrementing primary key
- `customer_id`: Foreign key to customers table
- `order_date`: Order date
- `order_total`: Order total amount in decimal format

## Data Characteristics

### Volume
- **~1000 customers** total
- **~5000 orders** total
- **200+ Manchester customers since 1 Sep 2024** (guaranteed for the task)

### Data Quality Issues
There may be data quality issues in the database.