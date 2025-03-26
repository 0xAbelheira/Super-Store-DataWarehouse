#!/bin/bash

# MySQL credentials
MYSQL_USER="root"
MYSQL_PASSWORD="root"
DATABASE_SCRIPT="setup_database.sql"
DATABASE_NAME="superstore_dw"

# Run the SQL script
mysql -u "$MYSQL_USER" -p"$MYSQL_PASSWORD" < "$DATABASE_SCRIPT"

echo "✅ Database and tables created successfully!"