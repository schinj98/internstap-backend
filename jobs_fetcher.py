from flask import Flask, jsonify
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv("config.env")

app = Flask(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")

def get_connection():
    return psycopg2.connect(DATABASE_URL)

@app.get("/jobs")
def get_jobs():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, logo_link, job_title, batch, location,
               qualification, salary, posted_date, raw
        FROM job_postings
        ORDER BY posted_date DESC
        LIMIT 50
    """)

    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]

    data = [dict(zip(cols, row)) for row in rows]

    cur.close()
    conn.close()
    return jsonify(data)
