import pdfplumber
import re
import pandas as pd
import mysql.connector
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# =========================
# DB CONNECTION
# =========================
def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )

# =========================
# HELPERS
# =========================
def clean_text(s):
    return re.sub(r"\s+", " ", str(s)).strip()

def extract_date(text):
    m = re.search(r'\d{2}/\d{2}/\d{4}', text)
    return m.group() if m else None

def extract_time(text):
    m = re.search(r'\d{4}', text)
    if m:
        t = m.group()
        return f"{t[:2]}:{t[2:]}"
    return None

def convert_date(d):
    if not d:
        return None
    try:
        return datetime.strptime(d, "%m/%d/%Y").strftime("%Y-%m-%d")
    except:
        return None

def normalize_disposition(text):
    text = clean_text(text)

    if "CBA" in text or "Cleared by Arrest" in text:
        return "Cleared by Arrest"
    elif "Inactive" in text:
        return "Inactive"
    elif "Active" in text:
        return "Active"
    return "Unknown"



# =========================
# TEXT PARSER
# =========================
def process_pdf(pdf_path):

    print(f"Processing {pdf_path}...")

    lines = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                lines.extend(text.split("\n"))
    lines = [l for l in lines if "PD" not in l or "VT POLICE" in l]
    
    records = []
    current = None
    case_buffer = None

    records = []
    current = None

    print("\n========== RAW LINES SAMPLE ==========")
    for i, l in enumerate(lines[:40]):
        print(f"{i}: {repr(l)}")
    print("=====================================\n")

    for i in range(len(lines)):
        line = clean_text(lines[i])

        # =========================
        # CASE START (line 1)
        # =========================
        match = re.match(r'^(\d{4}-\d{2})\s+(.*)', line)
        if match:
            case_prefix = match.group(1)
            rest = match.group(2)

            current = {
                "case": case_prefix,
                "offense": rest
            }

            records.append(current)
            print(f"🟡 START: {case_prefix}")
            continue

        # =========================
        # CASE CONTINUATION (line 2)
        # =========================
        match2 = re.match(r'^(\d{5,7})\s*(.*)', line)
        if match2 and current and len(current["case"]) <= 7:
            # only allow ONE completion

            suffix = match2.group(1)
            extra = match2.group(2)

            current["case"] += suffix
            current["offense"] += " " + extra

            print(f"🟢 COMPLETE: {current['case']}")
            continue

        # =========================
        # CONTINUATION TEXT
        # =========================
        if current:
            current["offense"] += " " + line

    # =========================
    # BUILD DF
    # =========================
    df = pd.DataFrame(records)

    if df.empty:
        print("❌ NO RECORDS PARSED")
        return df

    df["case"] = df["case"].apply(clean_text)
    df["offense"] = df["offense"].apply(clean_text)
    df["location"] = df["offense"]
    # extract fields AFTER full merge
    df["date_reported"] = df["offense"].apply(extract_date)
    df["occ_date"] = df["offense"].apply(extract_date)
    df["occ_time"] = df["offense"].apply(extract_time)
    df["disposition"] = df["offense"].apply(normalize_disposition)

    # keep valid cases
    df = df[df["case"].str.match(r'\d{4}-\d{6,}', na=False)]

    print(f"Rows after validation: {len(df)}")

    return df

# =========================
# LOAD TO DB
# =========================
def load_clean_data(df):

    conn = get_db_connection()
    cursor = conn.cursor(buffered=True)
    conn.autocommit = False

    success = 0

    try:
        for _, row in df.iterrows():

            case = row["case"]
            date_reported = convert_date(row["date_reported"])
            occ_date = convert_date(row["occ_date"])
            occ_time = row["occ_time"]
            location = row["location"] or "Unknown Location"
            disposition = row["disposition"] or "Unknown"

            if not case or not date_reported:
                continue

            cursor.execute("SELECT incident_id FROM incident WHERE case_number=%s", (case,))
            if cursor.fetchone():
                continue

            # location
            cursor.execute("INSERT IGNORE INTO location (location_name) VALUES (%s)", (location,))
            cursor.execute("SELECT location_id FROM location WHERE location_name=%s", (location,))
            loc = cursor.fetchone()
            if not loc:
                continue

            # disposition
            cursor.execute("INSERT IGNORE INTO disposition (disposition_type) VALUES (%s)", (disposition,))
            cursor.execute("SELECT disposition_id FROM disposition WHERE disposition_type=%s", (disposition,))
            disp = cursor.fetchone()
            if not disp:
                continue

            cursor.execute("""
                INSERT INTO incident
                (case_number, date_reported, occurrence_start_date, occurrence_start_time, location_id, agency_id, disposition_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (case, date_reported, occ_date, occ_time, loc[0], 1, disp[0]))

            success += 1

        conn.commit()
        print(f"\n✅ SUCCESS: Inserted {success} rows")

    except Exception as e:
        conn.rollback()
        print("\n❌ FAILED — FULL ROLLBACK")
        print("ERROR:", e)

    finally:
        cursor.close()
        conn.close()

# =========================
# RUN
# =========================
all_data = []

base_folder = "2025Logs"

for file in os.listdir(base_folder):
    if file.endswith(".pdf"):
        path = os.path.join(base_folder, file)
        df = process_pdf(path)
        all_data.append(df)

final_df = pd.concat(all_data, ignore_index=True)

print(f"\nTotal rows across all PDFs: {len(final_df)}")

load_clean_data(final_df)