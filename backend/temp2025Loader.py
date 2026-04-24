import mysql.connector
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )

OFFENSES = [
    "Underage Possession of Alcohol", "Public Intoxication", "Petit Larceny",
    "Larceny", "Vandalism", "Trespassing", "Disorderly Conduct", "Fake ID",
    "Driving Under the Influence", "Assault", "Identity Fraud",
    "Credit Card Fraud", "Fraud", "Hit and Run", "Reckless Driving",
    "Motor Vehicle Theft", "Grand Larceny", "Burglary", "Drug Possession",
    "Underage Possession of Marijuana", "Extortion", "Assault and Battery",
    "Violation of Protective Order", "Theft from Motor Vehicle",
    "Credit Card Theft", "Urinating in Public",
    "Unauthorized Use of Vehicle", "Harassment", "Stalking",
    "Damage to Property"
]

def parse_line(line):
    parts = line.strip().split()

    case_number = parts[0]
    date_reported = datetime.strptime(parts[1], "%m/%d/%Y").strftime("%Y-%m-%d")

    occ_idx = None
    for i in range(2, len(parts)):
        try:
            datetime.strptime(parts[i], "%m/%d/%Y")
            occ_idx = i
            break
        except:
            continue

    if occ_idx is None:
        raise ValueError("No occurrence date found")

    occurrence_date = datetime.strptime(parts[occ_idx], "%m/%d/%Y").strftime("%Y-%m-%d")
    occurrence_time = parts[occ_idx + 1]
    disposition = " ".join(parts[occ_idx + 2:])

    offense_name = None
    for offense in sorted(OFFENSES, key=len, reverse=True):
        if offense in line:
            offense_name = offense
            break

    if not offense_name:
        raise ValueError("Offense not found")

    after_offense = line.split(offense_name, 1)[1].strip()
    split_index = after_offense.find(parts[occ_idx])

    if split_index == -1:
        raise ValueError("Could not isolate location")

    location_name = after_offense[:split_index].strip()

    return (
        case_number,
        date_reported,
        offense_name,
        location_name,
        occurrence_date,
        occurrence_time,
        disposition
    )

def load_txt(file_path):
    conn = get_db_connection()
    cursor = conn.cursor(buffered=True)
    conn.autocommit = False

    success_count = 0
    failure_count = 0

    with open(file_path, "r", encoding="utf-8") as f:
        lines = [line.strip() for line in f if line.strip()]

    for line in lines:
        try:
            (
                case_number,
                date_reported,
                offense_name,
                location_name,
                occurrence_date,
                occurrence_time,
                disposition
            ) = parse_line(line)

            print(f"{case_number} | {offense_name} | {location_name}")

            # LOCATION
            cursor.execute("INSERT IGNORE INTO location (location_name) VALUES (%s)", (location_name,))
            cursor.execute("SELECT location_id FROM location WHERE location_name=%s", (location_name,))
            location_id = cursor.fetchone()[0]

            # DISPOSITION
            cursor.execute("INSERT IGNORE INTO disposition (disposition_type) VALUES (%s)", (disposition,))
            cursor.execute("SELECT disposition_id FROM disposition WHERE disposition_type=%s", (disposition,))
            disposition_id = cursor.fetchone()[0]

            # OFFENSE
            cursor.execute("INSERT IGNORE INTO offense (offense_name) VALUES (%s)", (offense_name,))
            cursor.execute("SELECT offense_id FROM offense WHERE offense_name=%s", (offense_name,))
            offense_id = cursor.fetchone()[0]

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

            # LINK TABLE
            cursor.execute(
                "INSERT INTO incident_offense (incident_id, offense_id) VALUES (%s, %s)",
                (incident_id, offense_id)
            )

            success_count += 1

        except Exception as e:
            failure_count += 1
            print("\n--- ERROR ---")
            print("LINE:", line)
            print("ERROR:", e)

    if success_count > 0:
        conn.commit()
    else:
        conn.rollback()

    print("\n===== SUMMARY =====")
    print("Inserted:", success_count)
    print("Failed:", failure_count)

    cursor.close()
    conn.close()

if __name__ == "__main__":
    load_txt("../2025Logs/example2025Logs.txt")