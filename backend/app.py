from flask import Flask, render_template, request
import mysql.connector
from collections import Counter
import os
from flask import session, redirect, url_for, request, render_template, flash
import bcrypt

from dotenv import load_dotenv
load_dotenv()

app = Flask(__name__, template_folder='../frontend/templates')
app.secret_key = os.getenv("SECRET_KEY")


# -----------------------------
# DB CONNECTION
# -----------------------------
def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )

from functools import wraps
#helper login 
def login_required(f):
    @wraps(f)
    def wrap(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return wrap


from functools import wraps

def admin_required(f):
    @wraps(f)
    def wrap(*args, **kwargs):
        if session.get('role') != 'admin':
            return "Access denied", 403
        return f(*args, **kwargs)
    return wrap

# -----------------------------
# SORTING FUNCTIONS
# -----------------------------
def get_sorting():
    sort = request.args.get("sort", "date_reported")
    order = request.args.get("order", "desc").lower()

    valid_columns = {
        "case_number": "i.case_number",
        "date_reported": "i.date_reported",
        "offense_name": "o.offense_name",
        "location_name": "l.location_name",
        "occurrence_start_date": "i.occurrence_start_date",
        "occurrence_start_time": "i.occurrence_start_time",
        "disposition_type": "d.disposition_type",
    }

    sort_column = valid_columns.get(sort, "i.date_reported")
    order = "ASC" if order == "asc" else "DESC"

    return sort, order, sort_column


def next_order(current_sort, column_name, current_order):
    if current_sort == column_name and current_order == "asc":
        return "desc"
    return "asc"

# -----------------------------
# ROUTES
# -----------------------------
@app.route("/")
def root():
    if 'user_id' in session:
        return redirect(url_for('home'))  # your existing main page
    else:
        return redirect(url_for('welcome'))  # new landing page


from datetime import datetime
@app.route('/welcome')
def welcome():
    if 'user_id' in session:
        return redirect(url_for('home'))

    current_date = datetime.now().strftime("%B %d, %Y")

    return render_template('welcome.html', current_date=current_date)

@app.route('/home')
@login_required
def home():
    return render_template('home.html')  

@app.route('/incidents_offenses')
def incidents():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    # 🔥 ENABLE SORTING
    sort, order, sort_column = get_sorting()

    query = f"""
    SELECT 
        i.case_number,
        i.date_reported,
        o.offense_name,
        l.location_name,
        i.occurrence_start_date,
        i.occurrence_start_time,
        d.disposition_type
    FROM incident i
    JOIN incident_offense io ON i.incident_id = io.incident_id
    JOIN offense o ON io.offense_id = o.offense_id
    LEFT JOIN location l ON i.location_id = l.location_id
    LEFT JOIN disposition d ON i.disposition_id = d.disposition_id
    ORDER BY {sort_column} {order};
    """

    cursor.execute(query)
    data = cursor.fetchall()

    total_incidents = len(data)

    # ======================
    # TOP OFFENSES
    # ======================
    cursor.execute("""
    SELECT o.offense_name, COUNT(*) as count
    FROM incident i
    JOIN incident_offense io ON i.incident_id = io.incident_id
    JOIN offense o ON io.offense_id = o.offense_id
    GROUP BY o.offense_name
    ORDER BY count DESC
    LIMIT 3;
    """)
    top_offenses = [(row['offense_name'], row['count']) for row in cursor.fetchall()]

    # ======================
    # TIME BLOCKS
    # ======================
    cursor.execute("""
    SELECT 
        CASE
            WHEN HOUR(i.occurrence_start_time) BETWEEN 0 AND 3 THEN '12AM-4AM'
            WHEN HOUR(i.occurrence_start_time) BETWEEN 4 AND 7 THEN '4AM-8AM'
            WHEN HOUR(i.occurrence_start_time) BETWEEN 8 AND 11 THEN '8AM-12PM'
            WHEN HOUR(i.occurrence_start_time) BETWEEN 12 AND 15 THEN '12PM-4PM'
            WHEN HOUR(i.occurrence_start_time) BETWEEN 16 AND 19 THEN '4PM-8PM'
            ELSE '8PM-12AM'
        END AS block,
        COUNT(*) as count
    FROM incident i
    GROUP BY block
    ORDER BY count DESC
    LIMIT 3;
    """)
    top_time_blocks = [(row['block'], row['count']) for row in cursor.fetchall()]

    # ======================
    # QUARTER CHART
    # ======================
    cursor.execute("""
    SELECT CONCAT('Q', QUARTER(i.date_reported)) AS quarter, COUNT(*) as count
    FROM incident i
    GROUP BY quarter
    ORDER BY quarter;
    """)
    quarter_data = cursor.fetchall()
    quarter_labels = [row['quarter'] for row in quarter_data]
    quarter_values = [row['count'] for row in quarter_data]

    # ======================
    # OFFENSE CHART
    # ======================
    cursor.execute("""
    SELECT o.offense_name, COUNT(*) as count
    FROM incident i
    JOIN incident_offense io ON i.incident_id = io.incident_id
    JOIN offense o ON io.offense_id = o.offense_id
    GROUP BY o.offense_name
    ORDER BY count DESC
    LIMIT 5;
    """)
    offense_data = cursor.fetchall()
    offense_labels = [row['offense_name'] for row in offense_data]
    offense_values = [row['count'] for row in offense_data]

    cursor.close()
    conn.close()

    return render_template(
        "incidents.html",
        data=data,
        total_incidents=total_incidents,
        top_offenses=top_offenses,
        top_time_blocks=top_time_blocks,
        quarter_labels=quarter_labels,
        quarter_values=quarter_values,
        offense_labels=offense_labels,
        offense_values=offense_values,
        year=None,
        current_sort=sort,
        current_order=order,
        next_order=next_order
    )















@app.route('/year/<int:year>')
def incidents_by_year(year):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    # 🔥 ENABLE SORTING
    sort, order, sort_column = get_sorting()

    # ======================
    # MAIN TABLE DATA
    # ======================
    query = f"""
    SELECT 
        i.case_number,
        i.date_reported,
        o.offense_name,
        l.location_name,
        i.occurrence_start_date,
        i.occurrence_start_time,
        d.disposition_type
    FROM incident i
    JOIN incident_offense io ON i.incident_id = io.incident_id
    JOIN offense o ON io.offense_id = o.offense_id
    LEFT JOIN location l ON i.location_id = l.location_id
    LEFT JOIN disposition d ON i.disposition_id = d.disposition_id
    WHERE YEAR(i.date_reported) = %s
    ORDER BY {sort_column} {order};
    """

    cursor.execute(query, (year,))
    data = cursor.fetchall()

    total_incidents = len(data)

    # ======================
    # TOP OFFENSES (FILTERED BY YEAR)
    # ======================
    cursor.execute("""
    SELECT o.offense_name, COUNT(*) as count
    FROM incident i
    JOIN incident_offense io ON i.incident_id = io.incident_id
    JOIN offense o ON io.offense_id = o.offense_id
    WHERE YEAR(i.date_reported) = %s
    GROUP BY o.offense_name
    ORDER BY count DESC
    LIMIT 3;
    """, (year,))
    top_offenses = [(row['offense_name'], row['count']) for row in cursor.fetchall()]

    # ======================
    # TIME BLOCKS (FILTERED BY YEAR)
    # ======================
    cursor.execute("""
    SELECT 
        CASE
            WHEN HOUR(i.occurrence_start_time) BETWEEN 0 AND 3 THEN '12AM-4AM'
            WHEN HOUR(i.occurrence_start_time) BETWEEN 4 AND 7 THEN '4AM-8AM'
            WHEN HOUR(i.occurrence_start_time) BETWEEN 8 AND 11 THEN '8AM-12PM'
            WHEN HOUR(i.occurrence_start_time) BETWEEN 12 AND 15 THEN '12PM-4PM'
            WHEN HOUR(i.occurrence_start_time) BETWEEN 16 AND 19 THEN '4PM-8PM'
            ELSE '8PM-12AM'
        END AS block,
        COUNT(*) as count
    FROM incident i
    WHERE YEAR(i.date_reported) = %s
    GROUP BY block
    ORDER BY count DESC
    LIMIT 3;
    """, (year,))
    top_time_blocks = [(row['block'], row['count']) for row in cursor.fetchall()]

    # ======================
    # QUARTER CHART (FILTERED)
    # ======================
    cursor.execute("""
    SELECT CONCAT('Q', QUARTER(i.date_reported)) AS quarter, COUNT(*) as count
    FROM incident i
    WHERE YEAR(i.date_reported) = %s
    GROUP BY quarter
    ORDER BY quarter;
    """, (year,))
    quarter_data = cursor.fetchall()
    quarter_labels = [row['quarter'] for row in quarter_data]
    quarter_values = [row['count'] for row in quarter_data]

    # ======================
    # OFFENSE CHART (FILTERED)
    # ======================
    cursor.execute("""
    SELECT o.offense_name, COUNT(*) as count
    FROM incident i
    JOIN incident_offense io ON i.incident_id = io.incident_id
    JOIN offense o ON io.offense_id = o.offense_id
    WHERE YEAR(i.date_reported) = %s
    GROUP BY o.offense_name
    ORDER BY count DESC
    LIMIT 5;
    """, (year,))
    offense_data = cursor.fetchall()
    offense_labels = [row['offense_name'] for row in offense_data]
    offense_values = [row['count'] for row in offense_data]

    cursor.close()
    conn.close()

    return render_template(
        "incidents.html",
        data=data,
        total_incidents=total_incidents,
        top_offenses=top_offenses,
        top_time_blocks=top_time_blocks,
        quarter_labels=quarter_labels,
        quarter_values=quarter_values,
        offense_labels=offense_labels,
        offense_values=offense_values,
        year=year,
        current_sort=sort,
        current_order=order,
        next_order=next_order
    )









@app.route('/compare_years')
def compare_years():
    year1 = request.args.get('year1')
    year2 = request.args.get('year2')

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    ALCOHOL_OFFENSES = (
        "Underage Possession of Alcohol",
        "Public Intoxication",
        "Fake ID",
        "Driving Under the Influence"
    )

    def get_year_stats(year):
        # ============================
        # TOTAL INCIDENTS
        # ============================
        cursor.execute("""
            SELECT COUNT(*) as total
            FROM incident
            WHERE YEAR(date_reported) = %s
        """, (year,))
        total = cursor.fetchone()['total']

        # ============================
        # OFFENSE DISTRIBUTION
        # ============================
        cursor.execute("""
            SELECT o.offense_name, COUNT(*) as count
            FROM incident i
            JOIN incident_offense io ON i.incident_id = io.incident_id
            JOIN offense o ON io.offense_id = o.offense_id
            WHERE YEAR(i.date_reported) = %s
            GROUP BY o.offense_name
            ORDER BY count DESC
        """, (year,))
        offenses = cursor.fetchall()

        for offense in offenses:
            offense["percent"] = round((offense["count"] / total) * 100, 1) if total > 0 else 0

        # ============================
        # TIME BLOCK DISTRIBUTION
        # ============================
        cursor.execute("""
            SELECT
                CASE
                    WHEN HOUR(occurrence_start_time) BETWEEN 0 AND 3 THEN '12AM–4AM'
                    WHEN HOUR(occurrence_start_time) BETWEEN 4 AND 7 THEN '4AM–8AM'
                    WHEN HOUR(occurrence_start_time) BETWEEN 8 AND 11 THEN '8AM–12PM'
                    WHEN HOUR(occurrence_start_time) BETWEEN 12 AND 15 THEN '12PM–4PM'
                    WHEN HOUR(occurrence_start_time) BETWEEN 16 AND 19 THEN '4PM–8PM'
                    ELSE '8PM–12AM'
                END AS time_block,
                COUNT(*) as count
            FROM incident
            WHERE YEAR(date_reported) = %s
            GROUP BY time_block
            ORDER BY count DESC
        """, (year,))
        time_blocks = cursor.fetchall()

        for t in time_blocks:
            t["percent"] = round((t["count"] / total) * 100, 1) if total > 0 else 0

        top_time = time_blocks[0] if time_blocks else {"time_block": "N/A", "percent": 0}

        # ============================
        # ALCOHOL-RELATED SHARE
        # ============================
        placeholders = ",".join(["%s"] * len(ALCOHOL_OFFENSES))
        cursor.execute(f"""
            SELECT COUNT(*) as alcohol_total
            FROM incident i
            JOIN incident_offense io ON i.incident_id = io.incident_id
            JOIN offense o ON io.offense_id = o.offense_id
            WHERE YEAR(i.date_reported) = %s
            AND o.offense_name IN ({placeholders})
        """, (year, *ALCOHOL_OFFENSES))
        alcohol_total = cursor.fetchone()["alcohol_total"]
        alcohol_percent = round((alcohol_total / total) * 100, 1) if total > 0 else 0

        # ============================
        # DISPOSITION DISTRIBUTION
        # ============================
        cursor.execute("""
            SELECT d.disposition_type, COUNT(*) as count
            FROM incident i
            JOIN disposition d ON i.disposition_id = d.disposition_id
            WHERE YEAR(i.date_reported) = %s
            GROUP BY d.disposition_type
            ORDER BY count DESC
        """, (year,))
        dispositions = cursor.fetchall()

        for d in dispositions:
            d["percent"] = round((d["count"] / total) * 100, 1) if total > 0 else 0

        return {
            "total": total,
            "offenses": offenses,
            "time_blocks": time_blocks,
            "top_time": top_time,
            "alcohol_percent": alcohol_percent,
            "dispositions": dispositions
        }

    stats1 = get_year_stats(year1)
    stats2 = get_year_stats(year2)

    # ============================
    # OFFENSE MAP
    # ============================
    offense_map = {}
    for o in stats1["offenses"]:
        offense_map[o["offense_name"]] = [o["percent"], 0]
    for o in stats2["offenses"]:
        if o["offense_name"] in offense_map:
            offense_map[o["offense_name"]][1] = o["percent"]
        else:
            offense_map[o["offense_name"]] = [0, o["percent"]]

    biggest_change = None
    max_diff = 0
    for offense, (p1, p2) in offense_map.items():
        diff = abs(p2 - p1)
        if diff > max_diff:
            max_diff = diff
            biggest_change = (offense, p1, p2, round(p2 - p1, 1))

    labels = list(offense_map.keys())[:5]
    year1_values = [offense_map[o][0] for o in labels]
    year2_values = [offense_map[o][1] for o in labels]

    # ============================
    # TIME CHART
    # ============================
    time_labels = ["12AM–4AM", "4AM–8AM", "8AM–12PM", "12PM–4PM", "4PM–8PM", "8PM–12AM"]

    def build_time_map(time_data):
        tmap = {t["time_block"]: t["percent"] for t in time_data}
        return [tmap.get(label, 0) for label in time_labels]

    time_year1 = build_time_map(stats1["time_blocks"])
    time_year2 = build_time_map(stats2["time_blocks"])

    # ============================
    # DISPOSITION CHART
    # ============================
    disp_labels = sorted(list(set(
        [d["disposition_type"] for d in stats1["dispositions"]] +
        [d["disposition_type"] for d in stats2["dispositions"]]
    )))

    def build_disp_map(disp_data):
        dmap = {d["disposition_type"]: d["percent"] for d in disp_data}
        return [dmap.get(label, 0) for label in disp_labels]

    disp_year1 = build_disp_map(stats1["dispositions"])
    disp_year2 = build_disp_map(stats2["dispositions"])

    cursor.close()
    conn.close()

    return render_template(
        "compare.html",
        year1=year1,
        year2=year2,
        stats1=stats1,
        stats2=stats2,
        biggest_change=biggest_change,

        chart_labels=labels,
        chart_year1=year1_values,
        chart_year2=year2_values,

        time_labels=time_labels,
        time_year1=time_year1,
        time_year2=time_year2,

        disp_labels=disp_labels,
        disp_year1=disp_year1,
        disp_year2=disp_year2
    )











#signup route
@app.route('/signup', methods=['GET', 'POST'])
def signup():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                INSERT INTO users (username, password_hash)
                VALUES (%s, %s)
            """, (username, hashed))
            conn.commit()
        except:
            flash("Username already exists")
            return redirect(url_for('signup'))

        return redirect(url_for('login'))

    return render_template('signup.html')

#login route
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT * FROM users WHERE username = %s", (username,))
        user = cursor.fetchone()

        if user and bcrypt.checkpw(password.encode('utf-8'), user['password_hash'].encode('utf-8')):
            session['user_id'] = user['user_id']
            session['role'] = user['role']
            session['username'] = user['username']
            return redirect(url_for('home'))

        flash("Invalid credentials")
        return redirect(url_for('login'))

    return render_template('login.html')

#logout route
@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/change_password', methods=['GET', 'POST'])
def change_password():
    # Must be logged in
    if 'user_id' not in session:
        return redirect(url_for('login'))

    if request.method == 'POST':
        current_password = request.form['current_password']
        new_password = request.form['new_password']
        confirm_password = request.form['confirm_password']

        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        # Get current user
        cursor.execute("SELECT * FROM users WHERE user_id = %s", (session['user_id'],))
        user = cursor.fetchone()

        # Check current password
        if not bcrypt.checkpw(current_password.encode('utf-8'), user['password_hash'].encode('utf-8')):
            flash("Current password is incorrect")
            return redirect(url_for('change_password'))

        # Check new password match
        if new_password != confirm_password:
            flash("New passwords do not match")
            return redirect(url_for('change_password'))

        # Hash new password
        new_hashed = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt())

        # Update DB
        cursor.execute("""
            UPDATE users
            SET password_hash = %s
            WHERE user_id = %s
        """, (new_hashed, session['user_id']))
        conn.commit()

        flash("Password updated successfully")
        return redirect(url_for('home'))

    return render_template('change_password.html')

@app.route('/create_user', methods=['GET', 'POST'])
def create_user():
    # Only admins allowed
    if 'role' not in session or session['role'] != 'admin':
        flash("Admin access required")
        return redirect(url_for('home'))

    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        role = request.form['role']

        hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                INSERT INTO users (username, password_hash, role)
                VALUES (%s, %s, %s)
            """, (username, hashed, role))
            conn.commit()
            flash("User created successfully")
        except:
            flash("Username already exists")
            return redirect(url_for('create_user'))

        return redirect(url_for('home'))

    return render_template('create_user.html')




#demo load button
import subprocess
from flask import redirect, url_for, flash, session

@app.route('/load_2026')
def load_2026():
    if session.get('role') != 'admin':
        flash("Unauthorized access.")
        return redirect(url_for('home'))

    try:
        subprocess.run(['python', 'LCI2026pdfs.py'], check=True)
        flash("2026 incidents successfully loaded.")
    except Exception as e:
        flash(f"Error loading data: {e}")

    return redirect(url_for('incidents', year=2026))

#demo load button
import subprocess
from flask import redirect, url_for, flash, session

@app.route('/load_2025')
def load_2025():
    if session.get('role') != 'admin':
        flash("Unauthorized access.")
        return redirect(url_for('home'))

    try:
        subprocess.run(['python', 'temp2025Loader.py'], check=True)
        flash("2025 incidents successfully loaded.")
    except Exception as e:
        flash(f"Error loading data: {e}")

    return redirect(url_for('incidents', year=2025))




@app.route('/add_incident', methods=['GET', 'POST'])
@login_required
@admin_required
def add_incident():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True, buffered=True)
    # Load data (optional now, but fine to keep)
    cursor.execute("SELECT * FROM offense")
    offenses = cursor.fetchall()

    cursor.execute("SELECT * FROM location")
    locations = cursor.fetchall()

    cursor.execute("SELECT * FROM disposition")
    dispositions = cursor.fetchall()

    if request.method == 'POST':
        # -------------------------
        # BASIC FORM DATA
        # -------------------------
        case_number = request.form.get('case_number')
        date_reported = request.form.get('date_reported')
        occurrence_date = request.form.get('occurrence_date')
        occurrence_time = request.form.get('occurrence_time')

        location_name = request.form.get('location_name')
        offense_name = request.form.get('offense_name')
        disposition_type = request.form.get('disposition_type')

        # -------------------------
        # LOCATION (find or insert)
        # -------------------------
        cursor.execute(
            "SELECT location_id FROM location WHERE location_name = %s",
            (location_name,)
        )
        loc = cursor.fetchone()

        if loc:
            location_id = loc['location_id']
        else:
            cursor.execute(
            "INSERT INTO location (location_name, on_campus, state, campus_id) VALUES (%s, %s, %s, %s)",
            (location_name, 1, "VA", 1)
        )
            location_id = cursor.lastrowid

        # -------------------------
        # OFFENSE (find or insert)
        # -------------------------
        cursor.execute(
            "SELECT offense_id FROM offense WHERE offense_name = %s",
            (offense_name,)
        )
        off = cursor.fetchone()

        if off:
            offense_id = off['offense_id']
        else:
            cursor.execute(
                "INSERT INTO offense (offense_name, is_clery_reportable) VALUES (%s, %s)",
                (offense_name, 1)
            )
            offense_id = cursor.lastrowid

        # -------------------------
        # DISPOSITION (find or insert)
        # -------------------------
        cursor.execute(
            "SELECT disposition_id FROM disposition WHERE disposition_type = %s",
            (disposition_type,)
        )
        disp = cursor.fetchone()

        if disp:
            disposition_id = disp['disposition_id']
        else:
            cursor.execute(
                "INSERT INTO disposition (disposition_type) VALUES (%s)",
                (disposition_type,)
            )
            disposition_id = cursor.lastrowid

        # -------------------------
        # INSERT INCIDENT
        # -------------------------
        cursor.execute("""
            INSERT INTO incident 
            (case_number, date_reported, occurrence_start_date, occurrence_start_time, location_id, disposition_id, agency_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            case_number,
            date_reported,
            occurrence_date,
            occurrence_time,
            location_id,
            disposition_id,
            1   # agency_id
        ))

        incident_id = cursor.lastrowid

        # -------------------------
        # LINK OFFENSE
        # -------------------------
        cursor.execute("""
            INSERT INTO incident_offense (incident_id, offense_id)
            VALUES (%s, %s)
        """, (incident_id, offense_id))

        conn.commit()

        return redirect(url_for('home'))

    return render_template(
        'add_incident.html',
        offenses=offenses,
        locations=locations,
        dispositions=dispositions
    )

@app.route('/edit_incident/<int:incident_id>', methods=['GET', 'POST'])
@login_required
@admin_required
def edit_incident(incident_id):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True, buffered=True)

    if request.method == 'POST':
        # -------------------------
        # FORM DATA
        # -------------------------
        date_reported = request.form.get('date_reported')
        occurrence_date = request.form.get('occurrence_date')
        occurrence_time = request.form.get('occurrence_time')

        location_name = request.form.get('location_name')
        offense_name = request.form.get('offense_name')
        disposition_type = request.form.get('disposition_type')

        # -------------------------
        # LOCATION (find or insert)
        # -------------------------
        cursor.execute(
            "SELECT location_id FROM location WHERE location_name = %s",
            (location_name,)
        )
        loc = cursor.fetchone()

        if loc:
            location_id = loc['location_id']
        else:
            cursor.execute(
                "INSERT INTO location (location_name, on_campus, state, campus_id) VALUES (%s, %s, %s, %s)",
                (location_name, 1, "VA", 1)
            )
            location_id = cursor.lastrowid

        # -------------------------
        # OFFENSE (find or insert)
        # -------------------------
        cursor.execute(
            "SELECT offense_id FROM offense WHERE offense_name = %s",
            (offense_name,)
        )
        off = cursor.fetchone()

        if off:
            offense_id = off['offense_id']
        else:
            cursor.execute(
                "INSERT INTO offense (offense_name, is_clery_reportable) VALUES (%s, %s)",
                (offense_name, 1)
            )
            offense_id = cursor.lastrowid

        # -------------------------
        # DISPOSITION
        # -------------------------
        cursor.execute(
            "SELECT disposition_id FROM disposition WHERE disposition_type = %s",
            (disposition_type,)
        )
        disposition_id = cursor.fetchone()['disposition_id']

        # -------------------------
        # UPDATE INCIDENT
        # -------------------------
        cursor.execute("""
            UPDATE incident
            SET date_reported = %s,
                occurrence_start_date = %s,
                occurrence_start_time = %s,
                location_id = %s,
                disposition_id = %s
            WHERE incident_id = %s
        """, (
            date_reported,
            occurrence_date,
            occurrence_time,
            location_id,
            disposition_id,
            incident_id
        ))

        # -------------------------
        # UPDATE OFFENSE LINK
        # -------------------------
        cursor.execute("""
            UPDATE incident_offense
            SET offense_id = %s
            WHERE incident_id = %s
        """, (offense_id, incident_id))

        conn.commit()
        return redirect(url_for('home'))

    # -------------------------
    # LOAD EXISTING DATA
    # -------------------------
    cursor.execute("""
        SELECT i.*, l.location_name, d.disposition_type
        FROM incident i
        LEFT JOIN location l ON i.location_id = l.location_id
        LEFT JOIN disposition d ON i.disposition_id = d.disposition_id
        WHERE i.incident_id = %s
    """, (incident_id,))
    incident = cursor.fetchone()

    cursor.execute("""
        SELECT o.offense_name
        FROM incident_offense io
        JOIN offense o ON io.offense_id = o.offense_id
        WHERE io.incident_id = %s
    """, (incident_id,))
    offense = cursor.fetchone()

    incident['offense_name'] = offense['offense_name']

    return render_template('edit_incident.html', incident=incident)


@app.route('/edit_incident_select')
@login_required
@admin_required
def edit_incident_select():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT incident_id, case_number, date_reported
        FROM incident
        ORDER BY date_reported DESC
    """)

    incidents = cursor.fetchall()

    return render_template('edit_select.html', incidents=incidents)

@app.route('/delete_incident/<int:incident_id>', methods=['POST'])
@login_required
@admin_required
def delete_incident(incident_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    # delete dependent records first (FK safety)
    cursor.execute(
        "DELETE FROM incident_offense WHERE incident_id = %s",
        (incident_id,)
    )

    # delete main incident
    cursor.execute(
        "DELETE FROM incident WHERE incident_id = %s",
        (incident_id,)
    )

    conn.commit()

    return redirect(url_for('home'))




# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    app.run(debug=False)