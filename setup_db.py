#!/usr/bin/env python3

import os
import platform
import subprocess
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DATABASE_SCRIPT = os.getenv("DATABASE_SCRIPT")

print(f"Setting up database using {DATABASE_SCRIPT}...")

# Determine platform-specific MySQL command
if platform.system() == "Windows":
    # Windows command
    mysql_cmd = ["mysql", f"-u{DB_USER}", f"-p{DB_PASSWORD}", f"< {DATABASE_SCRIPT}"]
    # Windows requires cmd shell to process the redirect
    complete_cmd = " ".join(mysql_cmd)
    subprocess.run(complete_cmd, shell=True)
else:
    # macOS/Linux command
    mysql_cmd = ["mysql", "-u", DB_USER, f"-p{DB_PASSWORD}"]
    with open(DATABASE_SCRIPT, "r") as sql_file:
        process = subprocess.Popen(mysql_cmd, stdin=subprocess.PIPE, text=True)
        process.communicate(input=sql_file.read())

print("✅ Database and tables created successfully!")
