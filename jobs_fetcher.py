from flask import Flask, jsonify, request
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
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 50))
    offset = (page - 1) * limit

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, logo_link, job_title, batch, location,
               qualification, salary, apply_link, posted_date, raw
        FROM job_postings
        ORDER BY posted_date DESC
        LIMIT %s OFFSET %s
    """, (limit, offset))

    rows = cur.fetchall()

    cur.execute("SELECT COUNT(*) FROM job_postings")
    total_jobs = cur.fetchone()[0]

    cols = [desc[0] for desc in cur.description]
    jobs = [dict(zip(cols, row)) for row in rows]

    cur.close()
    conn.close()

    return jsonify({
        "page": page,
        "limit": limit,
        "total": total_jobs,
        "jobs": jobs
    })
