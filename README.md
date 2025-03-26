# **Super-Store Data Warehouse**

## **Setup Database**

**1.** For MacOS, use the following command to start the MySQL server.

    ```bash
    brew services start mysql
    ```

To stop the MySQL server, use the following command.

    ```bash
    brew services stop mysql
    ```

**2.** Run the `setup_db.sh` script to create the database and tables.

    ```bash
    ./setup_db.sh
    ```

**3.** Run the `etl.py` script to populate the database.

    ```bash
    python etl.py
    ```

