#!/usr/bin/env python3

import os
import json
import datetime
import re
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Date, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError
from dotenv import load_dotenv
# import google.generativeai as genai
from groq import Groq

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
# GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/jobs_db")
ALL_MESSAGES_PATH = os.getenv("ALL_MESSAGES_PATH", "./all_messages.txt")

# if not GEMINI_API_KEY:
#     raise RuntimeError("GEMINI_API_KEY missing in config.env")

# Gemini Init
# genai.configure(api_key=GEMINI_API_KEY)
# model = genai.GenerativeModel("gemini-2.5-flash")

# groq api
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
client = Groq(api_key=GROQ_API_KEY)


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
    Column("company_name", Text),
    Column("location", String(256)),
    Column("qualification", Text),
    Column("salary", String(128)),
    Column("apply_link", String(1024)),
    Column("more_details", String(5000)),
    Column("posted_date", Date, nullable=False)
)


def ensure_tables():
    metadata.create_all(engine)
    print("[OK] Database table ensured")




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
- apply_link
- logo_url
- more_details
- posting_date

IMPORTANT LOGO RULE:
- Find the best possible company logo URL by *simulating a Google search mentally*.
- or return "" if hard to find

Rules:
- If company unknown → ""
- If location missing → "Remote"
- If qualification missing → "Any Graduate"
- If salary missing → "try to find that company's average salary for that role" or "INR 3-6 LPA"
- If batch missing → "current year - 1" if experieced role or if intern give current year or multiple years
- job_title should be simple & clean
- If apply link FOUND → return as-is
- If apply link is EMAIL (example: careers@company.com) → convert to "careers@company.com"
- If apply link missing → "" (empty string)
- fetch more details from the data for every job postings, and if not available add few details or points for that specific company about roles, culture, eligibility, and required skills.
- for posting_date add the current time indian standard time of current and in each jobs make at least difference of 10-5 minutes and todays date in the standard format as date-month-year-time
Return STRICT JSON array like:

[
  {{
    "company_name": "Google",
    "job_title": "Google is hiring Software Engineer ",
    "batch": "2022/2025/2026",
    "location": "Bangalore, india",
    "qualification": "B.Tech / BCA / any Stream",
    "salary": "INR 10-20 LPA",
    "apply_link": "https://google.com/careers/job",
    "logo_url": "https://logo.clearbit.com/google.com",
    "more_details": "Google is well known software company and most of it reviews are positive. it hires freshers as well experienced. The required skills for this specific job, eligibility, etc.",
    "posting_date": "date-month-year-time"
  }}
]

Now extract from:

{text}
"""



# ----------------------------------------
# CHUNK READER
# ----------------------------------------
def read_chunks(path, lines_per_chunk=20):
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
# def ask_gemini(text):
#     response = model.generate_content(
#         contents=build_prompt(text),
#         generation_config=genai.types.GenerationConfig(
#             temperature=0.0,
#             max_output_tokens=6000,
#         )
#     )

#     try:
#         return response.candidates[0].content.parts[0].text.strip()
#     except:
#         print("❌ Gemini returned no text.")
#         return ""

# groq api call
def ask_groq(text):
    prompt = build_prompt(text)

    try:
        response = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[
                {"role": "system", "content": "Return ONLY valid JSON array. No text, no commentary."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.0,
            max_tokens=100000,
        )
        return response.choices[0].message.content
    except Exception as e:
        print("❌ Groq error:", e)
        return ""



# ----------------------------------------
# INSERT INTO DB  (FIXED WITH engine.begin)
# ----------------------------------------
def insert_jobs(jobs: list):
    conn = engine.connect()
    trans = conn.begin()
    count = 0

    try:
        for j in jobs:

            # --------------------------
            # Extract fields from AI JSON
            # --------------------------
            logo = j.get("logo_url", "")
            apply_link = j.get("apply_link", "")
            more_details = j.get("more_details", "")

            # FIX 1 → email ko mailto nahi banaya tha
            if apply_link and "@" in apply_link and not apply_link.startswith("http"):
                apply_link = f"mailto:{apply_link}"

            # FIX 2 → AI se aaya posting_date use karna tha
            posting_date_raw = j.get("posting_date", "")

            try:
                # convert to proper python datetime (DD-MM-YYYY-HH:MM format expected)
                posting_date = datetime.datetime.strptime(
                    posting_date_raw.split(" ")[0],  # date-time split
                    "%d-%m-%Y"
                ).date()
            except:
                posting_date = datetime.date.today()  # fallback

            # --------------------------
            # Final row
            # --------------------------
            row = {
                "logo_link": logo,
                "job_title": j.get("job_title", "")[:512],
                "batch": j.get("batch", "any"),
                "company_name": j.get("company_name", ""),
                "location": j.get("location", "Remote"),
                "qualification": j.get("qualification", "Any Graduate"),
                "salary": j.get("salary", "Not Disclosed"),
                "apply_link": apply_link,
                "posted_date": posting_date,
                "more_details": more_details
            }


            try:
                conn.execute(job_postings.insert().values(**row))
                count += 1
            except IntegrityError:
                pass

        trans.commit()
        print(f"[OK] Inserted: {count} new jobs")
    except Exception as e:
        trans.rollback()
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

    chunks = read_chunks(ALL_MESSAGES_PATH, lines_per_chunk=20)
    print(f"Total chunks: {len(chunks)}")

    all_jobs = []

    for idx, chunk in enumerate(chunks):
        print(f"\n🔹 Processing the chunk {idx+1}/{len(chunks)}")
        # raw = ask_gemini(chunk)
        raw = ask_groq(chunk)
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
        # delete_old()
        print("✔ DONE")
    else:
        print("❌ No jobs extracted.")
    
    # clear the all_messages file
    clear_all_messages(ALL_MESSAGES_PATH)

if __name__ == "__main__":
    main()
