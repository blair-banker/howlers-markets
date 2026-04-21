#!/usr/bin/env python3
import psycopg2
import os

def check_db():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="markets",
            user="postgres",
            password=os.getenv("POSTGRES_PASSWORD", "your_password_here")
        )
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM ontology.variables")
        count = cur.fetchone()[0]
        print(f"DB healthy, variables: {count}")
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"DB check failed: {e}")
        return False

if __name__ == "__main__":
    if check_db():
        print("Health check passed")
    else:
        print("Health check failed")
        exit(1)