import camelot
import pandas as pd
import re
import json
import mysql.connector
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()


## 2026Crime Load, Clean, and Insert file


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
def convert_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%m/%d/%Y").strftime("%Y-%m-%d")
    except:
        return None

def safe_fetch(cursor, msg):
    result = cursor.fetchone()
    if not result:
        raise Exception(msg)
    return result[0]

def clean_text(val):
    return re.sub(r"\s+", " ", str(val)).strip()

def clean_case(case):
    case = str(case).strip().rstrip("-")
    return case if len(case) >= 6 else None

def format_time(t):
    t = str(t).strip()
    if len(t) == 4 and t.isdigit():
        return f"{t[:2]}:{t[2:]}"
    return None

def parse_time_range(val):
    if "-" in val:
        s, e = val.split("-")
        return format_time(s), format_time(e)
    return format_time(val), None

def parse_date_range(val):
    if "-" in val:
        s, e = val.split("-")
        return s.strip(), e.strip()
    return val.strip(), None

# =========================
# CLEANING LOGIC
# =========================
def normalize_disposition(val):
    val = clean_text(val)
    if "Cleared by Arrest" in val:
        return "Cleared by Arrest - Referred to Student Conduct"
    if "Inactive" in val:
        return "Inactive - Referred to Student Conduct"
    if "Active" in val:
        return "Active"
    return val

def normalize_disposition_for_db(d):
    if not d:
        return None
    if "Cleared by Arrest" in d:
        return "Cleared by Arrest"
    if "Inactive" in d:
        return "Inactive"
    if "Active" in d:
        return "Active"
    return d

def normalize_offense(off):
    parts = re.split(r',|\n', off)
    parts = [clean_text(p) for p in parts if p.strip()]

    cleaned = []
    for p in parts:
        p = p.rstrip("-")
        p = re.sub(r"\s*-\s*", " - ", p)

        if p.startswith("Underage Possession"):
            p = "Underage Possession of Alcohol"

        cleaned.append(p)

    return list(set(cleaned))

def clean_location(loc):
    loc = clean_text(loc)
    loc = loc.replace("(", "").replace(")", "")
    loc = loc.replace("Street Street", "Street")
    return loc

# =========================
# PROCESS ONE PDF
# =========================
def process_pdf(pdf_path):

    tables = camelot.read_pdf(
        pdf_path,
        pages='all',
        flavor='stream',
        edge_tol=500,
        row_tol=10,
        strip_text='\n',
    )

    print(f"Tables found in {pdf_path}: {tables.n}")

    df = pd.concat([t.df for t in tables], ignore_index=True)

    df.columns = ["case", "date_reported", "offense", "location", "occ_date", "occ_time", "disposition"]

    df = df[df["case"].notna()]
    df = df[df["case"].str.contains(r"\d", na=False)]
    df = df[df["case"] != "Case #"]
    df["case"] = df["case"].ffill()
    df = df.fillna("")

    merged = df.groupby("case").agg({
        "date_reported": "first",
        "offense": lambda x: " ".join(x),
        "location": lambda x: " ".join(x),
        "occ_date": lambda x: " ".join(x),
        "occ_time": lambda x: " ".join(x),
        "disposition": lambda x: " ".join(x),
    }).reset_index()

    merged["case"] = merged["case"].apply(clean_case)
    merged = merged[merged["case"].notna()]

    merged["offense_list"] = merged["offense"].apply(normalize_offense)
    merged["location"] = merged["location"].apply(clean_location)
    merged["disposition"] = merged["disposition"].apply(normalize_disposition)

    merged[["occ_start_date", "occ_end_date"]] = merged["occ_date"].apply(
        lambda x: pd.Series(parse_date_range(x))
    )

    merged[["occ_start_time", "occ_end_time"]] = merged["occ_time"].apply(
        lambda x: pd.Series(parse_time_range(x))
    )

    final_df = merged[[
        "case",
        "date_reported",
        "location",
        "occ_start_date",
        "occ_start_time",
        "disposition",
        "offense_list"
    ]]

    final_df["offense_list"] = final_df["offense_list"].apply(json.dumps)


    # =========================
    # REMOVE INVALID ROWS
    # =========================
    final_df = final_df[
        final_df["case"].notna() &
        final_df["date_reported"].notna() &
        (final_df["date_reported"] != "")
    ]
    #final_df = final_df.where(pd.notnull(final_df), None)

    print(f"Rows after validation: {len(final_df)}")

    print(f"Clean rows from {pdf_path}: {len(final_df)}")

    return final_df



# =========================
# LOAD TO DATABASE (ALL OR NOTHING)
# =========================
def load_clean_data(df):
    conn = get_db_connection()
    cursor = conn.cursor(buffered=True)
    conn.autocommit = False

    success = 0
    failed = 0

    try:
        for _, row in df.iterrows():

            def clean_sql_value(val):
                if val is None:
                    return None
                if isinstance(val, float) and pd.isna(val):
                    return None
                return val

            case_number = clean_sql_value(row["case"])
            date_reported = convert_date(clean_sql_value(row["date_reported"]))
            occurrence_date = convert_date(clean_sql_value(row["occ_start_date"]))
            occurrence_time = clean_sql_value(row["occ_start_time"])
            location_name = clean_sql_value(row["location"])
            disposition_raw = clean_sql_value(row["disposition"])

            if not case_number or not date_reported:
                raise Exception("Invalid row")

            # Prevent duplicates
            cursor.execute("SELECT incident_id FROM incident WHERE case_number=%s", (case_number,))
            if cursor.fetchone():
                raise Exception(f"Duplicate case: {case_number}")

            offense_list = json.loads(row["offense_list"])

            # LOCATION
            cursor.execute("INSERT IGNORE INTO location (location_name) VALUES (%s)", (location_name,))
            cursor.execute("SELECT location_id FROM location WHERE location_name=%s", (location_name,))
            location_id = safe_fetch(cursor, "Location failed")

            # DISPOSITION
            # DISPOSITION (robust version)
            disposition_clean = normalize_disposition_for_db(disposition_raw)

            cursor.execute(
                "INSERT IGNORE INTO disposition (disposition_type) VALUES (%s)",
                (disposition_clean,)
            )

            cursor.execute(
                "SELECT disposition_id FROM disposition WHERE disposition_type=%s",
                (disposition_clean,)
            )

            disposition_id = safe_fetch(cursor, f"Disposition failed: {disposition_clean}")

            # INCIDENT
            cursor.execute("""
                INSERT INTO incident
                (case_number, date_reported, occurrence_start_date, occurrence_start_time, location_id, agency_id, disposition_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                case_number,
                date_reported,
                occurrence_date,
                occurrence_time,
                location_id,
                1,
                disposition_id
            ))

            incident_id = cursor.lastrowid

            # OFFENSES
            for offense in offense_list:
                cursor.execute("INSERT IGNORE INTO offense (offense_name) VALUES (%s)", (offense,))
                cursor.execute("SELECT offense_id FROM offense WHERE offense_name=%s", (offense,))
                offense_id = safe_fetch(cursor, "Offense failed")

                cursor.execute(
                    "INSERT INTO incident_offense (incident_id, offense_id) VALUES (%s, %s)",
                    (incident_id, offense_id)
                )

            success += 1

        conn.commit()
        print(f"\n✅ SUCCESS: Inserted {success} rows (0 failures)")

    except Exception as e:
        conn.rollback()
        print("\n❌ FAILED — FULL ROLLBACK")
        print("ERROR:", e)

    finally:
        cursor.close()
        conn.close()

# =========================
# RUN MULTI-FILE PIPELINE
# =========================
all_data = []

import os

base_folder = os.path.join(os.path.dirname(__file__), "..", "2026Logs")
base_folder = os.path.abspath(base_folder)

for file in os.listdir(base_folder):
    if file.endswith(".pdf") and file.startswith("file_2026"):

        pdf_path = os.path.join(base_folder, file)

        print(f"\nProcessing {pdf_path}...")

        try:
            df = process_pdf(pdf_path)
            all_data.append(df)

        except Exception as e:
            print(f"❌ Error in {file}: {e}")
            exit()

# Combine all files
final_df = pd.concat(all_data, ignore_index=True)
final_df = final_df.where(pd.notnull(final_df), None)

print(f"\nTotal rows across all PDFs: {len(final_df)}")

# Load into DB
load_clean_data(final_df)