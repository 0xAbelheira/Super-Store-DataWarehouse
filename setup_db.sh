#!/bin/bash

# Load environment variables from .env file
export $(grep -v '^#' .env | xargs)

# Run the SQL script
mysql -u "$DB_USER" -p"$DB_PASSWORD" < "$DATABASE_SCRIPT"

echo "✅ Database and tables created successfully!"