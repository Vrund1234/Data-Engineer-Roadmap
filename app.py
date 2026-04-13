import streamlit as st
import sqlite3
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

# 🔐 ADMIN CREDENTIALS (FIXED)
ADMIN_USERNAME = "admin"
ADMIN_PASSWORD = "vrund@&2024"

# -----------------------------
# DATABASE SETUP
# -----------------------------
conn = sqlite3.connect("progress.db", check_same_thread=False)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    username TEXT PRIMARY KEY,
    password TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS progress (
    user TEXT,
    task_key TEXT,
    completed INTEGER,
    PRIMARY KEY (user, task_key)
)
""")

conn.commit()

# -----------------------------
# HELPER FUNCTIONS
# -----------------------------
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()


def create_user(username, password):
    if username == ADMIN_USERNAME:
        return False  # prevent creating admin manually
    try:
        cursor.execute(
            "INSERT INTO users (username, password) VALUES (?, ?)",
            (username, hash_password(password))
        )
        conn.commit()
        return True
    except:
        return False

def get_user_detailed_progress(user):
    cursor.execute("""
        SELECT task_key, completed
        FROM progress
        WHERE user = ?
    """, (user,))
    rows = cursor.fetchall()

    return pd.DataFrame(rows, columns=["Task", "Completed"])


def login_user(username, password):
    # ✅ ADMIN LOGIN (HARDCODED & SECURE)
    if username == ADMIN_USERNAME and password == ADMIN_PASSWORD:
        return {"username": ADMIN_USERNAME, "role": "admin"}

    # Normal user login
    cursor.execute(
        "SELECT * FROM users WHERE username=? AND password=?",
        (username, hash_password(password))
    )
    user = cursor.fetchone()

    if user:
        return {"username": username, "role": "user"}

    return None


def load_progress(user):
    cursor.execute("SELECT task_key, completed FROM progress WHERE user = ?", (user,))
    rows = cursor.fetchall()
    return {key: bool(val) for key, val in rows}


def save_progress(user, task_key, completed):
    cursor.execute("""
    INSERT OR REPLACE INTO progress (user, task_key, completed)
    VALUES (?, ?, ?)
    """, (user, task_key, int(completed)))
    conn.commit()


def get_leaderboard():
    query = """
    SELECT user, COUNT(*) as completed_tasks
    FROM progress
    WHERE completed = 1
    GROUP BY user
    ORDER BY completed_tasks DESC
    """
    return pd.read_sql_query(query, conn)


# -----------------------------
# ADMIN FUNCTIONS
# -----------------------------
def delete_user(target_user):
    cursor.execute("DELETE FROM users WHERE username = ?", (target_user,))
    cursor.execute("DELETE FROM progress WHERE user = ?", (target_user,))
    conn.commit()


def reset_all_progress():
    cursor.execute("DELETE FROM progress")
    conn.commit()


def delete_all_data():
    cursor.execute("DELETE FROM users")
    cursor.execute("DELETE FROM progress")
    conn.commit()


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
                st.success("Account created! Please login.")
            else:
                st.error("Username not allowed or already exists.")

    elif menu == "Login":
        if st.sidebar.button("Login"):
            user = login_user(username, password)
            if user:
                st.session_state.authenticated = True
                st.session_state.username = user["username"]
                st.session_state.role = user["role"]

                st.session_state.completed = load_progress(user["username"])
                st.session_state.current_user = user["username"]

                st.success("Logged in successfully")
                st.rerun()
            else:
                st.error("Invalid username or password")

    st.stop()

# -----------------------------
# LOGGED IN USER
# -----------------------------
username = st.session_state.username
role = st.session_state.role

# Logout
if st.sidebar.button("Logout"):
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.rerun()

# -----------------------------
# LOAD USER DATA
# -----------------------------
if (
    "completed" not in st.session_state
    or "current_user" not in st.session_state
    or st.session_state.current_user != username
):
    st.session_state.completed = load_progress(username)
    st.session_state.current_user = username

# Reset own progress
# if st.sidebar.button("Reset My Progress"):
#     cursor.execute("DELETE FROM progress WHERE user = ?", (username,))
#     conn.commit()
#     st.session_state.completed = {}
#     st.rerun()

# -----------------------------
# ROADMAP
# -----------------------------
roadmap = {
    "Phase 1: SQL + Python Foundation": {
        "SQL": [
            "SQL Setup & Basics", "Filtering Data (WHERE)",
            "Aggregations (COUNT, SUM, AVG)", "GROUP BY & ORDER BY",
            "SQL Joins", "SQL Practice"
        ],
        "Python": [
            "Python Basics", "Python Data Structures",
            "Pandas Introduction", "Pandas Transformations"
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

# -----------------------------
# NAVIGATION
# -----------------------------
st.sidebar.title("Navigation")

menu_option = st.sidebar.radio(
    "Navigation",
    ["Roadmap", "SQL Quiz"]
)

# 👉 QUIZ MODE
if menu_option == "SQL Quiz":
    run_quiz(username, role)
    st.stop()

# 👉 ROADMAP MODE (ONLY HERE define selected_phase)
selected_phase = st.sidebar.radio(
    "Select Phase",
    list(roadmap.keys())
)
# -----------------------------
# UI
# -----------------------------
st.title("Data Engineer Roadmap")
st.markdown(f"Logged in as: **{username}**")

phase_data = roadmap[selected_phase]

phase_total = 0
phase_completed = 0

for category, topics in phase_data.items():
    st.subheader(category)

    cols = st.columns(2)
    for i, topic in enumerate(topics):
        key = f"{selected_phase}-{category}-{topic}"
        phase_total += 1

        checked = cols[i % 2].checkbox(
            topic,
            value=st.session_state.completed.get(key, False),
            key=key
        )

        st.session_state.completed[key] = checked
        save_progress(username, key, checked)

        if checked:
            phase_completed += 1

# -----------------------------
# PROGRESS
# -----------------------------
st.markdown("---")
st.subheader("Progress")

progress = int((phase_completed / phase_total) * 100)
st.progress(progress / 100)
st.write(f"{phase_completed}/{phase_total} tasks completed ({progress}%)")

# -----------------------------
# ADMIN SECTION
# -----------------------------
if role == "admin":

    st.markdown("---")
    st.subheader("🏆 Leaderboard")

    df = get_leaderboard()
    if not df.empty:
        df.index = df.index + 1
        st.dataframe(df, use_container_width=True)
    else:
        st.info("No data yet")

    # -------------------------
    # USER PROGRESS VIEW (NEW)
    # -------------------------
    st.markdown("---")
    st.subheader("📊 User Detailed Progress")

    users_df = pd.read_sql_query("SELECT username FROM users", conn)

    if not users_df.empty:
        user_list = users_df["username"].tolist()

        selected_user_progress = st.selectbox(
            "Select user to view progress",
            user_list,
            key="progress_view"
        )

        if selected_user_progress:
            progress_df = get_user_detailed_progress(selected_user_progress)

            if not progress_df.empty:
                progress_df["Phase"] = progress_df["Task"].apply(lambda x: x.split("-")[0])
                progress_df["Category"] = progress_df["Task"].apply(lambda x: x.split("-")[1])
                progress_df["Topic"] = progress_df["Task"].apply(lambda x: x.split("-")[2])

                progress_df["Completed"] = progress_df["Completed"].apply(
                    lambda x: "✅ Completed" if x == 1 else "❌ Not Completed"
                )

                progress_df = progress_df[["Phase", "Category", "Topic", "Completed"]]

                st.dataframe(progress_df, use_container_width=True)
                
            if not progress_df.empty:
                progress_df["Completed"] = progress_df["Completed"].apply(
                    lambda x: "✅ Completed" if x == 1 else "❌ Not Completed"
                )
                st.dataframe(progress_df, use_container_width=True)
            else:
                st.info("No progress data for this user")

    # -------------------------
    # ADMIN PANEL
    # -------------------------
    st.markdown("---")
    st.subheader("⚙️ Admin Panel")

    users_df = pd.read_sql_query("SELECT username FROM users", conn)

    if not users_df.empty:
        user_list = users_df["username"].tolist()

        st.markdown("### 🗑️ Delete User")
        selected_user = st.selectbox("Select user", user_list)

        if st.button("Delete User"):
            delete_user(selected_user)
            st.success(f"{selected_user} deleted")
            st.rerun()

    st.markdown("### 🔄 Reset All Progress")
    if st.button("Reset All Users Progress"):
        reset_all_progress()
        st.success("All progress reset")
        st.rerun()

    st.markdown("### 💀 Danger Zone")
    confirm = st.text_input("Type DELETE to confirm")

    if confirm == "DELETE":
        if st.button("Delete ALL Data"):
            delete_all_data()
            st.success("Everything deleted")
            st.rerun()