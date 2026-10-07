import streamlit as st
import psycopg2
import pandas as pd
import hashlib
from datetime import datetime

# Local modules
from quiz import run_quiz
from python_quiz import run_python_quiz
from de_quiz import run_de_quiz
import roadmap_data

# -----------------------------
# PAGE CONFIGURATION
# -----------------------------
st.set_page_config(
    page_title="Data Engineer Roadmap 2026",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# -----------------------------
# CUSTOM MODERN CSS STYLING
# -----------------------------
CUSTOM_CSS = """
<style>
/* Global Styling & Font */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap');

html, body, [class*="css"] {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
}

code, pre {
    font-family: 'JetBrains Mono', monospace !important;
}

/* Card & Metric Container */
.de-kpi-card {
    background: linear-gradient(135deg, rgba(30, 41, 59, 0.7) 0%, rgba(15, 23, 42, 0.8) 100%);
    border: 1px solid rgba(255, 255, 255, 0.1);
    border-radius: 12px;
    padding: 16px 20px;
    box-shadow: 0 8px 24px -4px rgba(0, 0, 0, 0.2);
    min-height: 122px;
    display: flex;
    flex-direction: column;
    justify-content: space-between;
    transition: transform 0.2s ease, border-color 0.2s ease;
}
.de-kpi-card:hover {
    transform: translateY(-2px);
    border-color: rgba(99, 102, 241, 0.4);
}

/* Prevent buttons from truncating */
button[data-testid*="baseButton"] {
    white-space: nowrap !important;
}

/* Make selectbox behave as a pure dropdown without search caret */
div[data-baseweb="select"] input {
    caret-color: transparent !important;
    cursor: pointer !important;
}

.de-kpi-title {
    font-size: 0.85rem;
    font-weight: 500;
    color: #94A3B8;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    margin-bottom: 6px;
}

.de-kpi-val {
    font-size: 1.8rem;
    font-weight: 800;
    color: #F8FAFC;
    line-height: 1.2;
}

.de-kpi-subtitle {
    font-size: 0.8rem;
    color: #64748B;
    margin-top: 4px;
}

/* Topic Card */
.de-topic-card {
    background: rgba(30, 41, 59, 0.4);
    border: 1px solid rgba(255, 255, 255, 0.08);
    border-radius: 10px;
    padding: 14px 18px;
    margin-bottom: 12px;
    transition: all 0.2s ease;
}
.de-topic-card:hover {
    background: rgba(30, 41, 59, 0.6);
    border-color: rgba(99, 102, 241, 0.3);
}

/* Badges */
.badge-pill {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 9999px;
    font-size: 0.72rem;
    font-weight: 600;
    letter-spacing: 0.03em;
    margin-right: 6px;
}
.badge-beginner {
    background-color: rgba(16, 185, 129, 0.15);
    color: #34D399;
    border: 1px solid rgba(16, 185, 129, 0.3);
}
.badge-intermediate {
    background-color: rgba(245, 158, 11, 0.15);
    color: #FBBF24;
    border: 1px solid rgba(245, 158, 11, 0.3);
}
.badge-advanced {
    background-color: rgba(239, 68, 68, 0.15);
    color: #F87171;
    border: 1px solid rgba(239, 68, 68, 0.3);
}
.badge-must-learn {
    background-color: rgba(99, 102, 241, 0.18);
    color: #818CF8;
    border: 1px solid rgba(99, 102, 241, 0.35);
}
.badge-recommended {
    background-color: rgba(14, 165, 233, 0.15);
    color: #38BDF8;
    border: 1px solid rgba(14, 165, 233, 0.3);
}

/* Resource Link */
.resource-tag {
    display: inline-flex;
    align-items: center;
    background: rgba(255, 255, 255, 0.05);
    padding: 4px 12px;
    border-radius: 6px;
    font-size: 0.8rem;
    color: #93C5FD;
    text-decoration: none;
    margin-right: 8px;
    margin-top: 6px;
    border: 1px solid rgba(255, 255, 255, 0.1);
    transition: background 0.15s ease;
}
.resource-tag:hover {
    background: rgba(99, 102, 241, 0.2);
    color: #FFFFFF;
}

/* Pipeline Flow visual boxes */
.flow-step-box {
    background: rgba(15, 23, 42, 0.6);
    border: 1px solid rgba(99, 102, 241, 0.25);
    border-radius: 8px;
    padding: 14px;
    text-align: center;
}

/* Hide browser default password reveal button to prevent duplicate eye icons */
input[type="password"]::-ms-reveal,
input[type="password"]::-ms-clear,
input::-ms-reveal,
input::-ms-clear {
    display: none !important;
    width: 0 !important;
    height: 0 !important;
}
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

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
        sslmode="require",
        connect_timeout=6
    )
    conn.autocommit = True
    return conn


def get_safe_connection():
    try:
        conn = get_connection()
        conn.cursor().execute("SELECT 1")
    except Exception:
        get_connection.clear()
        conn = get_connection()
    return conn


def init_db():
    try:
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
    except Exception as e:
        st.error(f"Database initialization error: {e}")

init_db()

# -----------------------------
# AUTH & HELPERS
# -----------------------------
def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


def create_user(username: str, password: str) -> bool:
    if username == ADMIN_USERNAME:
        return False
    conn = get_safe_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO users (username, password) VALUES (%s, %s)",
            (username, hash_password(password))
        )
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        cursor.close()


def login_user(username: str, password: str):
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


def load_progress(user: str) -> dict:
    conn = get_safe_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT task_key, completed FROM progress WHERE username=%s",
        (user,)
    )
    rows = cursor.fetchall()
    cursor.close()
    return {row[0]: bool(row[1]) for row in rows}


def save_progress(user: str, task_key: str, completed: bool):
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
    df = pd.read_sql("""
        SELECT username, COUNT(*) as completed_tasks
        FROM progress
        WHERE completed = 1
        GROUP BY username
        ORDER BY completed_tasks DESC
    """, conn)
    return df


def delete_user(target_user: str):
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
# AUTH STATE & LOGIN UI
# -----------------------------
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

st.sidebar.title("🚀 Data Engineer Portal")

if not st.session_state.authenticated:
    st.sidebar.subheader("Authentication")
    auth_mode = st.sidebar.selectbox("Choose Mode", ["Login", "Sign Up"], filter_mode=None)
    u_input = st.sidebar.text_input("Username", key="auth_user")
    p_input = st.sidebar.text_input("Password", type="password", key="auth_pass")

    if auth_mode == "Sign Up":
        if st.sidebar.button("Create Account", type="primary"):
            if u_input and p_input:
                if create_user(u_input.strip(), p_input):
                    st.sidebar.success("🎉 Account created successfully! Please log in.")
                else:
                    st.sidebar.error("Username already taken or invalid.")
            else:
                st.sidebar.warning("Please fill in both fields.")
    else:
        if st.sidebar.button("Login", type="primary"):
            if u_input and p_input:
                user_info = login_user(u_input.strip(), p_input)
                if user_info:
                    st.session_state.authenticated = True
                    st.session_state.username = user_info["username"]
                    st.session_state.role = user_info["role"]
                    st.session_state.completed = load_progress(user_info["username"])
                    st.rerun()
                else:
                    st.sidebar.error("Invalid credentials.")
            else:
                st.sidebar.warning("Please enter username and password.")

    # Splash presentation for unauthenticated visitors
    st.title("🌟 Data Engineer Roadmap (2026 Edition)")
    st.markdown("""
    ### 🎯 What you'll master:
    - **Stage 1**: Python & SQL Foundations
    - **Stage 2**: Advanced SQL, Query Tuning & Relational Internals
    - **Stage 3**: Data Architecture, Lakehouses & Kimball Modeling
    - **Stage 4**: Distributed Computing with Apache Spark (PySpark)
    - **Stage 5**: Modern Cloud Warehouses (Databricks, Snowflake & Cloud)
    - **Stage 6**: Business Intelligence & Serving (Power BI, DAX)
    - **Stage 7**: Workflow Orchestration with Airflow, Docker & DataOps
    - **Stage 8**: Event Streaming & Messaging with Apache Kafka
    - **Stage 9**: Modular Analytics Transformation with dbt Core
    - **Stage 10**: Production Portfolio Projects & Technical Interview Preparation
    
    👉 **Please log in or create an account in the sidebar to track your progress and take interactive quizzes!**
    """)
    st.stop()

# -----------------------------
# AUTHENTICATED USER SESSION
# -----------------------------
username = st.session_state.username
role = st.session_state.role

# Sync progress from DB if needed
if "completed" not in st.session_state:
    st.session_state.completed = load_progress(username)

# Calculate user metrics
metrics = roadmap_data.calculate_progress(st.session_state.completed)
completed_set = metrics["completed_keys_set"]

# Sidebar Profile Card
st.sidebar.markdown(f"""
<div style="background: rgba(30, 41, 59, 0.6); padding: 12px 16px; border-radius: 8px; border: 1px solid rgba(255,255,255,0.1); margin-bottom: 12px;">
    <div style="font-size: 0.8rem; color: #94A3B8;">LOGGED IN AS</div>
    <div style="font-size: 1.1rem; font-weight: 700; color: #F8FAFC;">{username} <span style="font-size: 0.75rem; background: #4F46E5; color: white; padding: 2px 6px; border-radius: 4px;">{role.upper()}</span></div>
    <div style="font-size: 0.85rem; color: {metrics['rank']['color']}; font-weight: 600; margin-top: 4px;">{metrics['rank']['title']}</div>
</div>
""", unsafe_allow_html=True)

if st.sidebar.button("🚪 Logout", use_container_width=True):
    st.session_state.clear()
    st.rerun()

# Sidebar Main Navigation
st.sidebar.markdown("### 🧭 Navigation")
nav_selection = st.sidebar.radio(
    "Go To",
    [
        "🗺️ Roadmap Tracker",
        "📊 Visual Pipeline Flow",
        "🛠️ Portfolio Projects",
        "📜 Certification Guide",
        "🎯 SQL Quiz",
        "🐍 Python Quiz",
        "⚡ DE Core Quiz",
        "🏆 Leaderboard",
        "⚙️ Admin Panel"
    ],
    label_visibility="collapsed"
)

# -----------------------------
# QUIZ ROUTING
# -----------------------------
if nav_selection == "🎯 SQL Quiz":
    for key in list(st.session_state.keys()):
        if key.startswith("py_") or key.startswith("de_"):
            del st.session_state[key]
    run_quiz(username, role)
    st.stop()

elif nav_selection == "🐍 Python Quiz":
    for key in list(st.session_state.keys()):
        if key.startswith("mcq_") or key.startswith("query_") or key.startswith("de_") or key in ["mcq_set", "query_set", "submitted"]:
            del st.session_state[key]
    run_python_quiz(username, role)
    st.stop()

elif nav_selection == "⚡ DE Core Quiz":
    for key in list(st.session_state.keys()):
        if key.startswith("mcq_") or key.startswith("query_") or key.startswith("py_") or key in ["mcq_set", "query_set", "submitted"]:
            del st.session_state[key]
    run_de_quiz(username, role)
    st.stop()

elif nav_selection == "🏆 Leaderboard":
    st.title("🏆 Data Engineer Leaderboard & Community")
    st.markdown("Track and celebrate learning progress across all enrolled engineers.")
    
    df_lead = get_leaderboard()
    if not df_lead.empty:
        total_de_topics = metrics["total_topics"]
        df_lead["Progress %"] = df_lead["completed_tasks"].apply(
            lambda x: f"{round(min(100.0, (x / total_de_topics) * 100), 1)}%"
        )
        df_lead["Rank Status"] = df_lead["completed_tasks"].apply(
            lambda x: "👑 Architect" if x >= 90 else ("💎 Senior" if x >= 60 else ("🚀 Pipeline" if x >= 35 else ("⚡ Apprentice" if x >= 15 else "🌱 Novice")))
        )
        df_lead.index = df_lead.index + 1
        df_lead.columns = ["Username", "Tasks Completed", "Progress %", "Rank Status"]
        st.dataframe(df_lead, use_container_width=True)
    else:
        st.info("No completed tasks yet. Check topics in the roadmap to climb the leaderboard!")
    st.stop()

elif nav_selection == "⚙️ Admin Panel":
    if role != "admin":
        st.error("🔒 Access Denied. Admin privileges required.")
        st.stop()
    
    st.title("⚙️ System Administrator Dashboard")
    st.markdown("Manage users, track detailed task metrics, and maintain database integrity.")

    st.subheader("👥 User Management")
    conn = get_safe_connection()
    cur = conn.cursor()
    cur.execute("SELECT username FROM users WHERE username != %s", (ADMIN_USERNAME,))
    all_users = [r[0] for r in cur.fetchall()]
    cur.close()

    if all_users:
        selected_user = st.selectbox("Inspect User Progress", all_users)
        if selected_user:
            cur = conn.cursor()
            cur.execute("SELECT task_key, completed FROM progress WHERE username=%s", (selected_user,))
            user_tasks = cur.fetchall()
            cur.close()
            st.write(f"**Total tasks tracked:** {len(user_tasks)} | **Completed:** {sum(1 for t in user_tasks if t[1] == 1)}")
            
            if st.button(f"🗑️ Delete User '{selected_user}'"):
                delete_user(selected_user)
                st.success(f"User '{selected_user}' removed.")
                st.rerun()
    else:
        st.info("No standard users registered yet.")

    st.markdown("---")
    st.subheader("⚠️ Dangerous Operations")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🧹 Reset All User Progress"):
            reset_all_progress()
            st.warning("All progress data cleared.")
            st.rerun()
    with col2:
        if st.button("🚨 Purge All Data (Users + Progress)"):
            delete_all_data()
            st.error("All user accounts and progress purged.")
            st.rerun()
    st.stop()

elif nav_selection == "📊 Visual Pipeline Flow":
    st.title("📊 End-to-End Data Engineering Pipeline Flow")
    st.markdown("""
    Visual representation of the **Modern Data Engineering Lifecycle**.
    Every skill in this roadmap connects directly to a stage in this production flow.
    """)

    st.markdown("""
    ```mermaid
    graph LR
        subgraph Sources ["1. Data Sources"]
            A1["APIs & Webhooks"]
            A2["OLTP Databases (Postgres/MySQL)"]
            A3["Event Streams (Logs / Clickstream)"]
        end

        subgraph Ingestion ["2. Ingestion & Transport"]
            B1["Batch Ingestion (Airbyte / Fivetran)"]
            B2["CDC (Debezium)"]
            B3["Event Broker (Apache Kafka)"]
        end

        subgraph Storage ["3. Lakehouse Storage"]
            C1["Raw Bronze Lake (AWS S3 / ADLS)"]
            C2["Open Table Format (Delta Lake / Iceberg)"]
        end

        subgraph Processing ["4. Transformation Engine"]
            D1["Distributed Processing (Apache Spark / PySpark)"]
            D2["Analytics Engineering (dbt Core)"]
        end

        subgraph Serving ["5. Serving & Warehouse"]
            E1["Cloud Warehouse (Snowflake / BigQuery)"]
            E2["BI & Reporting (Power BI / Tableau)"]
            E3["Reverse ETL (Census / Hightouch)"]
        end

        A1 --> B1 --> C1
        A2 --> B2 --> B3 --> C1
        A3 --> B3 --> C1
        C1 --> C2 --> D1 --> D2
        D2 --> E1 --> E2
        E1 --> E3
    ```
    """)

    st.markdown("### 🔄 Lifecycle Stages Breakdown")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("""
        **1. Generation & Ingestion**
        - Capturing data from transactional DBs, APIs, and IoT.
        - Using **CDC (Debezium)** and **Kafka** for event-driven latency.
        """)
    with col2:
        st.markdown("""
        **2. Storage & Processing**
        - Storing petabyte-scale data in **S3 / ADLS**.
        - ACID transactions via **Delta Lake / Apache Iceberg**.
        - Distributed processing with **PySpark** & modular SQL with **dbt**.
        """)
    with col3:
        st.markdown("""
        **3. Serving & Governance**
        - Centralized query engines like **Snowflake** & **BigQuery**.
        - Interactive dashboards in **Power BI**.
        - Orchestrated via **Apache Airflow** DAGs with data quality gates.
        """)
    st.stop()

elif nav_selection == "🛠️ Portfolio Projects":
    st.title("🛠️ High-Impact Data Engineering Portfolio Projects")
    st.markdown("""
    Hiring managers look for practical proof of end-to-end data pipeline mastery. 
    Below are 4 production-grade project blueprints designed to demonstrate core competencies on GitHub and resumes.
    """)

    for p in roadmap_data.PORTFOLIO_PROJECTS:
        with st.container():
            st.markdown(f"### {p['title']}")
            st.markdown(f"**Difficulty Level:** `{p['level']}` | **Key Stack:** {', '.join([f'`{t}`' for t in p['tech_stack']])}")
            st.write(p["summary"])
            
            with st.expander("🔍 Architecture & Implementation Steps"):
                for step in p["architecture_steps"]:
                    st.markdown(f"- {step}")
                st.markdown(f"🔗 [Explore Reference Implementations on GitHub]({p['github_template']})")
            st.markdown("---")
    st.stop()

elif nav_selection == "📜 Certification Guide":
    st.title("📜 Industry Data Engineering Certifications (2026)")
    st.markdown("Validate your skills with globally recognized cloud and data engineering certifications.")

    for cert in roadmap_data.CERTIFICATIONS_GUIDE:
        st.markdown(f"### 🎖️ {cert['title']}")
        st.markdown(f"**Issued By:** `{cert['provider']}` | **Difficulty:** `{cert['difficulty']}`")
        st.markdown(f"**Domains Covered:** {', '.join([f'`{t}`' for t in cert['topics_covered']])}")
        st.markdown(f"👉 [Official Certification Blueprint & Registration]({cert['url']})")
        st.markdown("---")
    st.stop()

# -----------------------------
# MAIN: ROADMAP TRACKER
# -----------------------------
st.title("🗺️ Modern Data Engineer Roadmap (2026)")
st.markdown("Systematic curriculum with interactive progress tracking.")

# Top Metrics Banner
col_m1, col_m2, col_m3, col_m4 = st.columns(4)

with col_m1:
    st.markdown(f"""
    <div class="de-kpi-card">
        <div class="de-kpi-title">Overall Completion</div>
        <div class="de-kpi-val" style="color: #6366F1;">{metrics['percentage']}%</div>
        <div class="de-kpi-subtitle">{metrics['completed_count']} of {metrics['total_topics']} topics done</div>
    </div>
    """, unsafe_allow_html=True)

with col_m2:
    st.markdown(f"""
    <div class="de-kpi-card">
        <div class="de-kpi-title">Current DE Rank</div>
        <div class="de-kpi-val" style="color: {metrics['rank']['color']};">{metrics['rank']['title']}</div>
        <div class="de-kpi-subtitle">{metrics['rank']['desc']}</div>
    </div>
    """, unsafe_allow_html=True)

with col_m3:
    pending_count = metrics['total_topics'] - metrics['completed_count']
    st.markdown(f"""
    <div class="de-kpi-card">
        <div class="de-kpi-title">Pending Topics</div>
        <div class="de-kpi-val" style="color: #F59E0B;">{pending_count}</div>
        <div class="de-kpi-subtitle">~{pending_count * 2.5:.0f} learning hours left</div>
    </div>
    """, unsafe_allow_html=True)

with col_m4:
    st.markdown(f"""
    <div class="de-kpi-card">
        <div class="de-kpi-title">Assessments Status</div>
        <div class="de-kpi-val" style="color: #10B981;">3 Quizzes</div>
        <div class="de-kpi-subtitle">SQL • Python • DE Core</div>
    </div>
    """, unsafe_allow_html=True)

st.write("")
st.progress(metrics["percentage"] / 100.0)

# Filter & Search Controls
st.markdown("---")
col_s1, col_s2, col_s3 = st.columns([2.5, 1, 1])

with col_s1:
    search_query = st.text_input("🔍 Search Topics", placeholder="e.g. Spark, Kafka, Airflow, Window Functions, dbt...")

with col_s2:
    filter_status = st.selectbox("Status", ["All Topics", "Pending", "Completed"], filter_mode=None)

with col_s3:
    filter_difficulty = st.selectbox("Difficulty", ["All Levels", "Beginner", "Intermediate", "Advanced"], filter_mode=None)

# Clean stage display titles mapping
PHASE_SHORT_TITLES = {
    "Phase 1: SQL + Python Foundation": "Phase 1: SQL & Python Foundation",
    "Phase 2: SQL Mastery and Databases": "Phase 2: SQL & Databases",
    "Phase 3: Data Architecture": "Phase 3: Data Architecture",
    "Phase 4: Data Pipelines and Big Data": "Phase 4: Pipelines & Spark",
    "Phase 5: Databricks, Snowflake and Cloud": "Phase 5: Databricks & Cloud",
    "Phase 6: Data Visualization": "Phase 6: Power BI & Analytics",
    "Phase 7: Modern Orchestration & DataOps": "Phase 7: Airflow & DataOps",
    "Phase 8: Streaming & Real-Time Data (Kafka & Flink)": "Phase 8: Streaming & Kafka",
    "Phase 9: Transformation with dbt & Open Formats": "Phase 9: dbt & Lakehouses",
    "Phase 10: Capstone Projects & DE Interview Masterclass": "Phase 10: Capstones & Interviews"
}

phase_options = list(roadmap_data.ROADMAP_DATA.keys())
phase_labels = []
for p in phase_options:
    p_meta = metrics["phase_metrics"].get(p, {"percentage": 0.0, "icon": "📌"})
    short_title = PHASE_SHORT_TITLES.get(p, p)
    phase_labels.append(f"{p_meta['icon']} {short_title} ({p_meta['percentage']:.0f}%)")

if "cur_phase_idx" not in st.session_state:
    st.session_state.cur_phase_idx = 0

# Professional compact sidebar stage selector (pure dropdown, search disabled)
st.sidebar.markdown("---")
st.sidebar.markdown("### 📚 Learning Stages")
selected_phase_idx = st.sidebar.selectbox(
    "Select Stage",
    range(len(phase_options)),
    index=st.session_state.cur_phase_idx,
    format_func=lambda i: phase_labels[i],
    key="phase_selector_sidebar",
    filter_mode=None
)
st.session_state.cur_phase_idx = selected_phase_idx

# Sidebar compact stage progress bar
cur_p_stat = metrics["phase_metrics"].get(phase_options[selected_phase_idx], {"percentage": 0.0, "completed": 0, "total": 0})
st.sidebar.progress(cur_p_stat["percentage"] / 100.0)
st.sidebar.caption(f"Stage Progress: **{cur_p_stat['percentage']}%** ({cur_p_stat['completed']}/{cur_p_stat['total']} topics)")

selected_phase_name = phase_options[selected_phase_idx]
selected_phase_info = roadmap_data.ROADMAP_DATA[selected_phase_name]

# -----------------------------
# TOPIC TOGGLE CALLBACKS
# -----------------------------
def toggle_topic_status(task_key: str, phase_name: str, cat_name: str, aliases: list):
    widget_k = f"chk_{task_key}"
    new_val = st.session_state.get(widget_k, False)
    save_progress(username, task_key, new_val)
    st.session_state.completed[task_key] = new_val
    st.session_state[f"search_{task_key}"] = new_val
    for alias in aliases:
        ak = f"{phase_name}-{cat_name}-{alias}"
        save_progress(username, ak, new_val)
        st.session_state.completed[ak] = new_val
        st.session_state[f"chk_{ak}"] = new_val
        st.session_state[f"search_{ak}"] = new_val


def toggle_search_status(task_key: str, phase_name: str, cat_name: str, aliases: list):
    search_k = f"search_{task_key}"
    new_val = st.session_state.get(search_k, False)
    save_progress(username, task_key, new_val)
    st.session_state.completed[task_key] = new_val
    st.session_state[f"chk_{task_key}"] = new_val
    for alias in aliases:
        ak = f"{phase_name}-{cat_name}-{alias}"
        save_progress(username, ak, new_val)
        st.session_state.completed[ak] = new_val
        st.session_state[f"chk_{ak}"] = new_val
        st.session_state[f"search_{ak}"] = new_val


# If search is active, show matching results across all phases
if search_query.strip():
    st.subheader(f"🔍 Search Results for '{search_query.strip()}'")
    matching_topics = roadmap_data.search_topics(
        query=search_query,
        phase_filter="All",
        difficulty_filter=filter_difficulty if filter_difficulty != "All Levels" else "All",
        status_filter=filter_status if filter_status != "All Topics" else "All",
        completed_keys=completed_set
    )
    
    if not matching_topics:
        st.info("No matching topics found. Try a different keyword.")
    else:
        st.caption(f"Found {len(matching_topics)} topics matching your criteria:")
        for t in matching_topics:
            k = t["task_key"]
            is_checked = k in completed_set
            search_widget_k = f"search_{k}"
            if search_widget_k not in st.session_state:
                st.session_state[search_widget_k] = is_checked

            with st.container():
                c1, c2 = st.columns([0.05, 0.95])
                with c1:
                    st.checkbox(
                        t["name"],
                        key=search_widget_k,
                        label_visibility="collapsed",
                        on_change=toggle_search_status,
                        args=(k, t["phase"], t["category"], t.get("aliases", []))
                    )
                with c2:
                    diff_badge = f"<span class='badge-pill badge-{t['difficulty'].lower()}'>{t['difficulty']}</span>"
                    must_badge = f"<span class='badge-pill badge-{t['importance'].lower().replace(' ', '-')}'>{t['importance']}</span>"
                    st.markdown(f"**{t['name']}** in *{t['phase']}* &nbsp; {diff_badge}{must_badge} `{t['est_time']}`", unsafe_allow_html=True)
                    st.write(t["summary"])
                    with st.expander("Details, Snippet & Resources"):
                        if t.get("interview_question"):
                            st.markdown(f"💬 **Interview Question:** {t['interview_question']}")
                        if t.get("code_snippet"):
                            st.code(t["code_snippet"])
                        if t.get("resources"):
                            res_html = "".join([f"<a href='{r['url']}' target='_blank' class='resource-tag'>🔗 {r['title']}</a>" for r in t["resources"]])
                            st.markdown(res_html, unsafe_allow_html=True)
                st.write("")
    st.stop()

# Standard Phase View
p_stat = metrics["phase_metrics"].get(selected_phase_name, {"percentage": 0.0, "completed": 0, "total": 0})

# Stage Stepper Navigator
nav_col1, nav_col2, nav_col3 = st.columns([1, 2, 1])
with nav_col1:
    if selected_phase_idx > 0:
        if st.button("⬅️ Previous Stage", key="prev_top_btn", use_container_width=True):
            st.session_state.cur_phase_idx = selected_phase_idx - 1
            st.rerun()
    else:
        st.button("⬅️ Previous Stage", key="prev_top_dis", disabled=True, use_container_width=True)

with nav_col2:
    st.markdown(
        f"<div style='text-align: center; font-size: 0.95rem; font-weight: 600; color: #94A3B8; padding-top: 6px;'>"
        f"Stage <b>{selected_phase_idx + 1}</b> of <b>{len(phase_options)}</b>"
        f"</div>",
        unsafe_allow_html=True
    )

with nav_col3:
    if selected_phase_idx < len(phase_options) - 1:
        if st.button("Next Stage ➡️", key="next_top_btn", use_container_width=True):
            st.session_state.cur_phase_idx = selected_phase_idx + 1
            st.rerun()
    else:
        st.button("Next Stage ➡️", key="next_top_dis", disabled=True, use_container_width=True)


st.markdown(f"""
### {selected_phase_info.get('icon', '📌')} {selected_phase_name}
*{selected_phase_info.get('description', '')}* &nbsp; • &nbsp; **Est. Duration:** `{selected_phase_info.get('est_time', '')}` &nbsp; • &nbsp; **Completion:** **{p_stat['percentage']}%** ({p_stat['completed']}/{p_stat['total']} completed)
""")

# Quick Bulk Actions for Phase
col_b1, col_b2, _ = st.columns([1.5, 1.5, 3])
with col_b1:
    if st.button("✅ Mark Phase Done", key=f"mark_all_{selected_phase_name}", use_container_width=True):
        for cat_name, t_list in selected_phase_info["categories"].items():
            for t in t_list:
                k = f"{selected_phase_name}-{cat_name}-{t['name']}"
                save_progress(username, k, True)
                st.session_state.completed[k] = True
                st.session_state[f"chk_{k}"] = True
                st.session_state[f"search_{k}"] = True
                for alias in t.get("aliases", []):
                    ak = f"{selected_phase_name}-{cat_name}-{alias}"
                    save_progress(username, ak, True)
                    st.session_state.completed[ak] = True
                    st.session_state[f"chk_{ak}"] = True
                    st.session_state[f"search_{ak}"] = True
        st.success("All topics in this phase marked complete!")
        st.rerun()

with col_b2:
    if st.button("🔄 Reset Phase", key=f"unmark_all_{selected_phase_name}", use_container_width=True):
        for cat_name, t_list in selected_phase_info["categories"].items():
            for t in t_list:
                k = f"{selected_phase_name}-{cat_name}-{t['name']}"
                save_progress(username, k, False)
                st.session_state.completed[k] = False
                st.session_state[f"chk_{k}"] = False
                st.session_state[f"search_{k}"] = False
                for alias in t.get("aliases", []):
                    ak = f"{selected_phase_name}-{cat_name}-{alias}"
                    save_progress(username, ak, False)
                    st.session_state.completed[ak] = False
                    st.session_state[f"chk_{ak}"] = False
                    st.session_state[f"search_{ak}"] = False
        st.warning("All topics in this phase reset to incomplete.")
        st.rerun()

st.write("")

# Render Category sections and interactive topic cards
canonical_map = roadmap_data.get_canonical_key_mapping()

for category_name, topic_list in selected_phase_info["categories"].items():
    st.markdown(f"#### 📁 {category_name}")
    
    for topic in topic_list:
        canon_key = f"{selected_phase_name}-{category_name}-{topic['name']}"
        
        # Determine if completed (checking canonical key or any historical alias)
        is_completed = canon_key in completed_set
        if not is_completed:
            for alias in topic.get("aliases", []):
                alias_key = f"{selected_phase_name}-{category_name}-{alias}"
                if st.session_state.completed.get(alias_key, False):
                    is_completed = True
                    break

        # Filter check
        if filter_status == "Completed" and not is_completed:
            continue
        if filter_status == "Pending" and is_completed:
            continue
        if filter_difficulty != "All Levels" and topic["difficulty"] != filter_difficulty:
            continue

        widget_k = f"chk_{canon_key}"
        if widget_k not in st.session_state:
            st.session_state[widget_k] = is_completed

        diff_class = topic["difficulty"].lower()
        imp_class = topic["importance"].lower().replace(" ", "-")

        with st.container():
            c_check, c_body = st.columns([0.04, 0.96])
            with c_check:
                st.checkbox(
                    topic["name"],
                    key=widget_k,
                    label_visibility="collapsed",
                    on_change=toggle_topic_status,
                    args=(canon_key, selected_phase_name, category_name, topic.get("aliases", []))
                )

            with c_body:
                diff_pill = f"<span class='badge-pill badge-{diff_class}'>{topic['difficulty']}</span>"
                imp_pill = f"<span class='badge-pill badge-{imp_class}'>{topic['importance']}</span>"
                time_badge = f"<span style='color: #94A3B8; font-size: 0.75rem; font-weight: 500;'>⏱️ {topic['est_time']}</span>"
                
                is_checked_current = st.session_state.get(widget_k, is_completed)
                name_style = "text-decoration: line-through; opacity: 0.65;" if is_checked_current else ""
                st.markdown(f"<span style='font-size: 1.05rem; font-weight: 600; {name_style}'>{topic['name']}</span> &nbsp; {diff_pill}{imp_pill} {time_badge}", unsafe_allow_html=True)
                st.write(topic["summary"])

                # Expandable Deep Dive
                with st.expander("📘 Concept Deep-Dive, Interview Q&A & Code"):
                    if topic.get("skills"):
                        skills_str = " • ".join(topic["skills"])
                        st.markdown(f"🎯 **Core Skills:** `{skills_str}`")

                    if topic.get("interview_question"):
                        st.markdown(f"💬 **Top Interview Question:**")
                        st.info(topic["interview_question"])

                    if topic.get("code_snippet"):
                        st.markdown("💻 **Practical Snippet / Syntax:**")
                        st.code(topic["code_snippet"])

                    if topic.get("resources"):
                        st.markdown("🔗 **Curated Resources & Documentation:**")
                        res_html = "".join([f"<a href='{r['url']}' target='_blank' class='resource-tag'>🔗 {r['title']}</a>" for r in topic["resources"]])
                        st.markdown(res_html, unsafe_allow_html=True)

            st.markdown("<div style='height: 6px;'></div>", unsafe_allow_html=True)
    
    st.markdown("---")

# Bottom Stage Navigation
st.write("")
b_col1, b_col2, b_col3 = st.columns([1, 2, 1])
with b_col1:
    if selected_phase_idx > 0:
        if st.button("⬅️ Previous Stage", key="prev_bot_btn", use_container_width=True):
            st.session_state.cur_phase_idx = selected_phase_idx - 1
            st.rerun()

with b_col2:
    st.markdown(
        f"<div style='text-align: center; color: #64748B; font-size: 0.9rem; padding-top: 8px;'>"
        f"Stage <b>{selected_phase_idx + 1} of {len(phase_options)}</b> completed? Proceed to next stage!"
        f"</div>",
        unsafe_allow_html=True
    )

with b_col3:
    if selected_phase_idx < len(phase_options) - 1:
        if st.button("Next Stage ➡️", key="next_bot_btn", use_container_width=True):
            st.session_state.cur_phase_idx = selected_phase_idx + 1
            st.rerun()