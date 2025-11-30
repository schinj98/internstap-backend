#!/usr/bin/env python3

import os
import json
import datetime
import re
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Date, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError
from dotenv import load_dotenv
import google.generativeai as genai
import requests

# -------------------------
# LOAD CONFIG.ENV
# -------------------------
if os.path.exists("config.env"):
    load_dotenv("config.env")
else:
    raise RuntimeError("config.env file missing. Create one first.")

# -------------------------
# ENV VARIABLES
# -------------------------
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/jobs_db")
ALL_MESSAGES_PATH = os.getenv("ALL_MESSAGES_PATH", "./all_messages.txt")

if not GEMINI_API_KEY:
    raise RuntimeError("GEMINI_API_KEY missing in config.env")

# Gemini Init
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel("gemini-2.5-flash")

# -------------------------
# DATABASE SCHEMA
# -------------------------
engine = create_engine(DATABASE_URL, echo=False)
metadata = MetaData()

job_postings = Table(
    "job_postings",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("logo_link", String(1024)),
    Column("job_title", String(512), nullable=False),
    Column("batch", String(128)),
    Column("location", String(256)),
    Column("qualification", Text),
    Column("salary", String(128)),
    Column("apply_link", String(1024)),
    Column("posted_date", Date, nullable=False),
    Column("raw", JSONB)
)

def ensure_tables():
    metadata.create_all(engine)
    print("[OK] Database table ensured")


# ----------------------------------------
# LOGO FETCHER (Clearbit)
# ----------------------------------------
def get_company_logo(company_name: str) -> str:
    if not company_name:
        return ""
    clean = re.sub(r"[^a-zA-Z0-9]", "", company_name.replace(" ", "")).lower()
    guesses = [f"{clean}.com", f"{clean}.in", f"{clean}.co", f"{clean}.io"]
    for d in guesses:
        url = f"https://logo.clearbit.com/{d}"
        try:
            if requests.head(url, timeout=4).status_code == 200:
                return url
        except:
            pass
    return ""


# ----------------------------------------
# PROMPT
# ----------------------------------------
def build_prompt(text: str) -> str:
    return f"""
Extract ALL job postings from the text below and return STRICT JSON only.
NO MARKDOWN. NO COMMENTS. ONLY JSON ARRAY.

Each JSON object MUST contain:
- company_name
- job_title
- batch
- location
- qualification
- salary
- apply_link   <-- NEW FIELD REQUIRED

Rules:
- If company unknown → ""
- If location missing → "Remote"
- If qualification missing → "Any Graduate"
- If salary missing → "INR 3-6 LPA"
- If batch missing → "any"
- job_title should be simple & clean
- If apply link FOUND → return as-is
- If apply link is EMAIL (example: careers@company.com) → convert to "mailto:careers@company.com"
- If apply link missing → "" (empty string)

Return STRICT JSON array like:

[
  {{
    "company_name": "Google",
    "job_title": "Software Engineer",
    "batch": "2022",
    "location": "Bangalore",
    "qualification": "B.Tech",
    "salary": "INR 10-20 LPA",
    "apply_link": "https://google.com/careers/job"
  }}
]

Now extract from:

{text}
"""



# ----------------------------------------
# CHUNK READER
# ----------------------------------------
def read_chunks(path, lines_per_chunk=10):
    with open(path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    chunks = []
    current = []

    for line in lines:
        current.append(line)
        if len(current) == lines_per_chunk:
            chunks.append("".join(current))
            current = []

    if current:
        chunks.append("".join(current))

    return chunks


# ----------------------------------------
# SAFE GEMINI CALL
# ----------------------------------------
def ask_gemini(text):
    response = model.generate_content(
        contents=build_prompt(text),
        generation_config=genai.types.GenerationConfig(
            temperature=0.0,
            max_output_tokens=6000,
        )
    )

    try:
        return response.candidates[0].content.parts[0].text.strip()
    except:
        print("❌ Gemini returned no text.")
        return ""


# ----------------------------------------
# INSERT INTO DB  (FIXED WITH engine.begin)
# ----------------------------------------
def insert_jobs(jobs: list):
    conn = engine.connect()
    trans = conn.begin()  # Start transaction explicitly
    today = datetime.date.today()
    count = 0

    try:
        for j in jobs:
            company = j.get("company_name", "")
            logo = get_company_logo(company)

            apply_link = j.get("apply_link", "")

            # Convert email → mailto
            if apply_link and "@" in apply_link and not apply_link.startswith("http"):
                apply_link = f"mailto:{apply_link}"

            row = {
                "logo_link": logo,
                "job_title": j.get("job_title", "")[:512],
                "batch": j.get("batch", "any"),
                "location": j.get("location", "Remote"),
                "qualification": j.get("qualification", "Any Graduate"),
                "salary": j.get("salary", "Not Disclosed"),
                "apply_link": apply_link,
                "posted_date": today,
                "raw": j,
            }


            try:
                conn.execute(job_postings.insert().values(**row))
                count += 1
            except IntegrityError:
                pass
        
        trans.commit()  # ✅ Commit the transaction
        print(f"[OK] Inserted: {count} new jobs")
    except Exception as e:
        trans.rollback()  # Rollback on error
        print(f"❌ Insert failed: {e}")
    finally:
        conn.close()


# ----------------------------------------
# DELETE OLD DATA (FIXED WITH engine.begin)
# ----------------------------------------
def delete_old():
    cutoff = datetime.date.today() - datetime.timedelta(days=30)
    conn = engine.connect()
    trans = conn.begin()  # Start transaction explicitly
    
    try:
        result = conn.execute(
            job_postings.delete().where(job_postings.c.posted_date < cutoff)
        )
        trans.commit()  # ✅ Commit the transaction
        print(f"[OK] Deleted {result.rowcount} old records")
    except Exception as e:
        trans.rollback()
        print(f"❌ Delete failed: {e}")
    finally:
        conn.close()

# clearing all_messages file func
# ----------------------------------------
# CLEAR ALL_MESSAGES AFTER PROCESSING
# ----------------------------------------
def clear_all_messages(path):
    try:
        open(path, "w").close()  # truncate file
        print(f"[OK] Cleared: {path}")
    except Exception as e:
        print(f"❌ Error clearing file: {e}")

# ----------------------------------------
# MAIN
# ----------------------------------------
def main():
    ensure_tables()

    if not os.path.exists(ALL_MESSAGES_PATH):
        print("all_messages.txt not found.")
        return

    chunks = read_chunks(ALL_MESSAGES_PATH, lines_per_chunk=10)
    print(f"Total chunks: {len(chunks)}")

    all_jobs = []

    for idx, chunk in enumerate(chunks):
        print(f"\n🔹 Processing chunk {idx+1}/{len(chunks)}")
        raw = ask_gemini(chunk)
        if not raw:
            continue

        # Extract JSON
        start = raw.find("[")
        end = raw.rfind("]")

        if start == -1 or end == -1:
            print("⚠️ JSON missing in output")
            continue

        try:
            jobs = json.loads(raw[start:end+1])
            all_jobs.extend(jobs)
        except Exception as e:
            print("⚠️ JSON parse error:", e)
            continue

    print(f"\nTotal extracted jobs: {len(all_jobs)}")

    if all_jobs:
        insert_jobs(all_jobs)
        delete_old()
        print("✔ DONE")
    else:
        print("❌ No jobs extracted.")
    
    # clear the all_messages file
    clear_all_messages(ALL_MESSAGES_PATH)



if __name__ == "__main__":
    main()
