from flask import Flask, jsonify, request
import psycopg2
import os
from dotenv import load_dotenv
from flask_cors import CORS

load_dotenv("config.env")

app = Flask(__name__)

# ---- Load allowed origins from config.env ----
allowed_origins_env = os.getenv("ALLOWED_ORIGINS", "")
allowed_origins = [origin.strip() for origin in allowed_origins_env.split(",") if origin.strip()]

# Apply CORS with env-based origins
CORS(app, resources={r"/*": {"origins": allowed_origins}})

# ---- Database ----
DATABASE_URL = os.getenv("DATABASE_URL")

def get_connection():
    return psycopg2.connect(DATABASE_URL)

# ---- API ----
# ---- API ----
@app.get("/jobs")
def get_jobs():
    page = int(request.args.get("page", 1))
    limit = int(request.args.get("limit", 50))
    offset = (page - 1) * limit

    conn = get_connection()
    cur = conn.cursor()

    # 1. Execute the main job postings query
    cur.execute("""
        SELECT id, logo_link, job_title, batch, location,
               qualification, salary, apply_link, posted_date, raw
        FROM job_postings
        ORDER BY posted_date DESC
        LIMIT %s OFFSET %s
    """, (limit, offset))

    rows = cur.fetchall()

    # 2. <<< FIX IS HERE >>> Get column names *now* before the next query
    cols = [desc[0] for desc in cur.description]
    jobs = [dict(zip(cols, row)) for row in rows] # Map data with correct columns

    # 3. Execute the total count query
    cur.execute("SELECT COUNT(*) FROM job_postings")
    total_jobs = cur.fetchone()[0]

    cur.close()
    conn.close()

    return jsonify({
        "page": page,
        "limit": limit,
        "total": total_jobs,
        "jobs": jobs
    })