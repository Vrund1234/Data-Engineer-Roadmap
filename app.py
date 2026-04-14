import streamlit as st
import psycopg2
import pandas as pd
import hashlib
from quiz import run_quiz

# -----------------------------
# CONFIG
# -----------------------------
st.set_page_config(
    page_title="Data Engineer Roadmap",
    page_icon="🚀",
    layout="wide"
)

ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "vrund@&2024"

# -----------------------------
# DATABASE CONNECTION
# -----------------------------
@st.cache_resource
def get_connection():
    conn = psycopg2.connect(
        host=st.secrets["DB_HOST"],
        database=st.secrets["DB_NAME"],
        user=st.secrets["DB_USER"],
        password=st.secrets["DB_PASS"],
        port="5432",
        sslmode="require"
    )
    conn.autocommit = True
    return conn


def get_safe_connection():
    try:
        conn = get_connection()
        conn.cursor().execute("SELECT 1")
    except:
        get_connection.clear()
        conn = get_connection()
    return conn


# ✅ ADD THIS
conn = get_safe_connection()
cursor = conn.cursor()


# -----------------------------
# CREATE TABLES
# -----------------------------
def init_db():
    conn = get_safe_connection()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY,
        password TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS progress (
        username TEXT,
        task_key TEXT,
        completed INTEGER,
        PRIMARY KEY (username, task_key)
    )
    """)

    conn.commit()
    cursor.close()

init_db()

# -----------------------------
# HELPERS
# -----------------------------
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def create_user(username, password):
    conn = get_safe_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            "INSERT INTO users (username, password) VALUES (%s, %s)",
            (username, hash_password(password))
        )
        conn.commit()
        return True
    except:
        return False
    finally:
        cursor.close()

def login_user(username, password):
    if username == ADMIN_USERNAME and password == ADMIN_PASSWORD:
        return {"username": ADMIN_USERNAME, "role": "admin"}

    conn = get_safe_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT * FROM users WHERE username=%s AND password=%s",
        (username, hash_password(password))
    )
    user = cursor.fetchone()
    cursor.close()

    if user:
        return {"username": username, "role": "user"}
    return None

def load_progress(user):
    conn = get_safe_connection()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT task_key, completed FROM progress WHERE username=%s",
        (user,)
    )
    rows = cursor.fetchall()
    cursor.close()

    return {row[0]: bool(row[1]) for row in rows}

def save_progress(user, task_key, completed):
    conn = get_safe_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO progress (username, task_key, completed)
        VALUES (%s, %s, %s)
        ON CONFLICT (username, task_key)
        DO UPDATE SET completed = EXCLUDED.completed
    """, (user, task_key, int(completed)))

    conn.commit()
    cursor.close()

def get_leaderboard():
    conn = get_safe_connection()

    return pd.read_sql("""
        SELECT username, COUNT(*) as completed_tasks
        FROM progress
        WHERE completed = 1
        GROUP BY username
        ORDER BY completed_tasks DESC
    """, conn)

def get_user_detailed_progress(user):
    conn = get_safe_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT task_key, completed
        FROM progress
        WHERE username=%s
    """, (user,))

    rows = cursor.fetchall()
    cursor.close()

    return pd.DataFrame(rows, columns=["Task", "Completed"])

# -----------------------------
# ADMIN FUNCTIONS
# -----------------------------
def delete_user(target_user):
    conn = get_safe_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM users WHERE username=%s", (target_user,))
    cursor.execute("DELETE FROM progress WHERE username=%s", (target_user,))

    conn.commit()
    cursor.close()


def reset_all_progress():
    conn = get_safe_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM progress")

    conn.commit()
    cursor.close()


def delete_all_data():
    conn = get_safe_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM users")
    cursor.execute("DELETE FROM progress")

    conn.commit()
    cursor.close()

# -----------------------------
# AUTH STATE
# -----------------------------
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

# -----------------------------
# AUTH UI
# -----------------------------
st.sidebar.title("Authentication")

menu = st.sidebar.selectbox("Select Option", ["Login", "Signup"])

if not st.session_state.authenticated:

    username = st.sidebar.text_input("Username")
    password = st.sidebar.text_input("Password", type="password")

    if menu == "Signup":
        if st.sidebar.button("Create Account"):
            if create_user(username, password):
                st.success("Account created!")
            else:
                st.error("Username exists")

    elif menu == "Login":
        if st.sidebar.button("Login"):
            user = login_user(username, password)
            if user:
                st.session_state.authenticated = True
                st.session_state.username = user["username"]
                st.session_state.role = user["role"]
                st.session_state.completed = load_progress(user["username"])
                st.success("Logged in")
                st.rerun()
            else:
                st.error("Invalid credentials")

    st.stop()

# -----------------------------
# USER
# -----------------------------
username = st.session_state.username
role = st.session_state.role

if st.sidebar.button("Logout"):
    st.session_state.clear()
    st.rerun()

# -----------------------------
# ROADMAP
# -----------------------------
roadmap = {
    "Phase 1: SQL + Python Foundation": {
        "SQL": [
            "SQL Setup & Basics", "Filtering Data (WHERE)",
            "Aggregations (COUNT, SUM, AVG)", "GROUP BY & ORDER BY",
            "SQL Joins", "SQL Practice", "Complete SQL Tutorial from W3 School"
        ],
        "Python": [
            "Python Basics", " Ptyhon Data Types",
            "Pandas Introduction", "Pandas & Numpy", "Complete Python Tutorial from W3 School"
        ]
    },
    "Phase 2: SQL Mastery and Databases": {
        "Databases": [
            "PostgreSQL Basics", "MySQL Basics",
            "SQL Server (T-SQL)", "Cloud Databases"
        ],
        "Advanced SQL": [
            "Window Functions", "CTE (WITH Clause)", "Subqueries",
            "Indexes and Performance", "Query Optimization",
            "Views and Materialized Views", "Stored Procedures",
            "Transactions", "Advanced SQL Practice"
        ]
    },
        "Phase 3: Data Architecture": {
        "Core Concepts": [
            "What is a Database",
            "What is a Data Warehouse",
            "What is a Data Lake",
            "What is a Data Lakehouse"
        ],
        "Comparisons": [
            "Database vs Data Warehouse",
            "Data Lake vs Data Warehouse",
            "Lakehouse vs Warehouse",
            "OLTP vs OLAP"
        ],
        "Modeling": [
            "Data Modeling Basics",
            "Star vs Snowflake Schema",
            "Architecture Diagram Task"
        ]
    },
    "Phase 4: Data Pipelines and Big Data": {
        "Pipeline Basics": [
            "ETL vs ELT",
            "Data Pipeline Basics",
            "Batch vs Streaming",
            "Data Orchestration",
            "Data Quality Checks",
            "Pipeline Design"
        ],
        "Spark": [
            "Introduction to Spark",
            "Spark DataFrames",
            "Transformations vs Actions",
            "Joins in Spark",
            "Aggregations",
            "Performance Optimization",
            "ETL Pipeline Project"
        ]
    },
    "Phase 5: Databricks, Snowflake and Cloud": {
        "Databricks": [
            "Workspace and Notebooks",
            "Cluster Management",
            "DBFS",
            "Delta Lake",
            "Delta Table Operations",
            "Optimization and Z-Ordering",
            "Medallion Architecture"
        ],
        "Snowflake": [
            "Snowflake Basics",
            "Databases and Schemas",
            "Virtual Warehouses",
            "Snowflake SQL",
            "Time Travel",
            "Zero Copy Cloning",
            "Snowpipe"
        ],
        "Cloud": [
            "Cloud Basics",
            "Azure Basics",
            "AWS Basics",
            "Storage Services",
            "IAM (Security)"
        ],
        "Integration": [
            "Databricks with Azure",
            "Snowflake with Storage",
            "ETL Between Systems",
            "Integration Project"
        ]
    },
    "Phase 6: Data Visualization": {
        "Power BI": [
            "Power BI Basics",
            "Power Query",
            "Data Modeling",
            "DAX Basics",
            "Advanced DAX"
        ],
        "Visualization": [
            "Charts and Visuals",
            "Dashboard Design",
            "Filters and Drilldown",
            "KPIs and Metrics"
        ],
        "Advanced": [
            "Performance Optimization",
            "Data Refresh",
            "Row Level Security",
            "Publishing"
        ],
        "Integration": [
            "Connect to Databricks",
            "Connect to Snowflake",
            "Live Dashboards",
            "Visualization Project"
        ]
    }
}
st.sidebar.title("Navigation")
menu_option = st.sidebar.radio("Navigation", ["Roadmap", "SQL Quiz"])

if menu_option == "SQL Quiz":
    run_quiz(username, role)
    st.stop()

selected_phase = st.sidebar.radio("Select Phase", list(roadmap.keys()))

st.title("Data Engineer Roadmap")
st.markdown(f"Logged in as: **{username}**")

phase_data = roadmap[selected_phase]

total = 0
done = 0

for category, topics in phase_data.items():
    st.subheader(category)
    for topic in topics:
        key = f"{selected_phase}-{category}-{topic}"
        total += 1

        checked = st.checkbox(
            topic,
            value=st.session_state.completed.get(key, False),
            key=key
        )

        save_progress(username, key, checked)

        if checked:
            done += 1

progress = int((done / total) * 100)
st.progress(progress / 100)
st.write(f"{progress}% completed")

# -----------------------------
# ADMIN
# -----------------------------
if role == "admin":
    st.subheader("Leaderboard")
    st.dataframe(get_leaderboard())