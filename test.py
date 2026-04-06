import streamlit as st
import sqlite3
import pandas as pd

# -----------------------------
# PAGE CONFIG
# -----------------------------
st.set_page_config(
    page_title="Data Engineer Roadmap",
    page_icon="🚀",
    layout="wide"
)

# -----------------------------
# DATABASE SETUP
# -----------------------------
conn = sqlite3.connect("progress.db", check_same_thread=False)
cursor = conn.cursor()

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
# DB FUNCTIONS
# -----------------------------
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
# USER LOGIN
# -----------------------------
st.sidebar.title("User Login")
username = st.sidebar.text_input("Enter your username")

if not username:
    st.warning("Please enter a username to continue.")
    st.stop()

# -----------------------------
# LOAD USER DATA
# -----------------------------
if "completed" not in st.session_state:
    st.session_state.completed = load_progress(username)

# -----------------------------
# RESET BUTTON
# -----------------------------
if st.sidebar.button("Reset My Progress"):
    cursor.execute("DELETE FROM progress WHERE user = ?", (username,))
    conn.commit()
    st.session_state.completed = {}
    st.rerun()

# -----------------------------
# ROADMAP DATA
# -----------------------------
roadmap = {
    "Phase 1: SQL + Python Foundation": {
        "SQL": [
            "SQL Setup & Basics",
            "Filtering Data (WHERE)",
            "Aggregations (COUNT, SUM, AVG)",
            "GROUP BY & ORDER BY",
            "SQL Joins",
            "SQL Practice"
        ],
        "Python": [
            "Python Basics",
            "Python Data Structures",
            "Pandas Introduction",
            "Pandas Transformations"
        ]
    },
    "Phase 2: SQL Mastery and Databases": {
        "Databases": [
            "PostgreSQL Basics",
            "MySQL Basics",
            "SQL Server (T-SQL)",
            "Cloud Databases"
        ],
        "Advanced SQL": [
            "Window Functions",
            "CTE (WITH Clause)",
            "Subqueries",
            "Indexes and Performance",
            "Query Optimization",
            "Views and Materialized Views",
            "Stored Procedures",
            "Transactions",
            "Advanced SQL Practice"
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
# SIDEBAR NAVIGATION
# -----------------------------
st.sidebar.title("Navigation")
selected_phase = st.sidebar.radio("Select Phase", list(roadmap.keys()))

# -----------------------------
# MAIN HEADER
# -----------------------------
st.title("Data Engineer Roadmap")
st.markdown(f"Tracking progress for: **{username}**")

# -----------------------------
# DISPLAY PHASE
# -----------------------------
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
# PHASE PROGRESS
# -----------------------------
st.markdown("---")
st.subheader("Phase Progress")

phase_progress = int((phase_completed / phase_total) * 100)
st.progress(phase_progress / 100)

col1, col2 = st.columns(2)
col1.metric("Completed Tasks", f"{phase_completed}/{phase_total}")
col2.metric("Completion Percentage", f"{phase_progress}%")

# -----------------------------
# OVERALL PROGRESS
# -----------------------------
total_tasks = len(st.session_state.completed)
completed_tasks = sum(st.session_state.completed.values())

st.markdown("---")
st.subheader("Overall Progress")

overall_progress = int((completed_tasks / total_tasks) * 100) if total_tasks else 0
st.progress(overall_progress / 100)

col1, col2 = st.columns(2)
col1.metric("Total Completed", f"{completed_tasks}/{total_tasks}")
col2.metric("Overall Percentage", f"{overall_progress}%")

# -----------------------------
# LEADERBOARD
# -----------------------------
st.markdown("---")
st.subheader("Leaderboard")

leaderboard_df = get_leaderboard()

if not leaderboard_df.empty:
    leaderboard_df.index = leaderboard_df.index + 1
    st.dataframe(leaderboard_df, use_container_width=True)
else:
    st.info("No data available yet.")


i also want password with username