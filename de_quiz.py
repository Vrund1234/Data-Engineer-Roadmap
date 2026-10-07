import streamlit as st
import psycopg2
import pandas as pd
from datetime import datetime
import random

# -----------------------------
# DB CONNECTION
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
        connect_timeout=5
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


# -----------------------------
# INIT TABLE
# -----------------------------
def init_db():
    try:
        conn = get_safe_connection()
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS de_quiz_results (
            id SERIAL PRIMARY KEY,
            username TEXT,
            score INTEGER,
            total INTEGER,
            percentage REAL,
            submitted_at TEXT
        )
        """)
        cursor.close()
    except Exception as e:
        print(f"Error initializing de_quiz table: {e}")

init_db()

# -----------------------------
# QUESTION POOL (ROADMAP.SH ALIGNED)
# -----------------------------
DE_QUESTIONS_POOL = [
    {
        "question": "Which file format uses columnar storage with dictionary encoding and snappy compression, making it ideal for OLAP queries?",
        "options": ["CSV", "JSON", "Apache Parquet", "Apache Avro"],
        "answer": "Apache Parquet",
        "explanation": "Apache Parquet is an open-source columnar storage format designed for efficient analytical queries, column pruning, and high compression rates."
    },
    {
        "question": "In Ralph Kimball's dimensional modeling, what characterizes an SCD Type 2 (Slowly Changing Dimension)?",
        "options": [
            "Overwrites old values without keeping history",
            "Creates a new record with effective date ranges and an is_current flag",
            "Adds a new column for the previous value only",
            "Deletes the historical dimension table entirely"
        ],
        "answer": "Creates a new record with effective date ranges and an is_current flag",
        "explanation": "SCD Type 2 preserves the full history of attribute changes by inserting a new row with timestamp ranges (e.g., start_date, end_date) and a current flag."
    },
    {
        "question": "What is the primary difference between a Transformation and an Action in Apache Spark?",
        "options": [
            "Transformations execute immediately; Actions are evaluated lazily",
            "Transformations create a new DataFrame/RDD lazily; Actions trigger actual execution and return results",
            "Transformations only run on Driver; Actions only run on Workers",
            "Transformations modify data on disk; Actions modify memory only"
        ],
        "answer": "Transformations create a new DataFrame/RDD lazily; Actions trigger actual execution and return results",
        "explanation": "Spark uses lazy evaluation. Transformations (like select, filter, groupBy) build an execution DAG, while Actions (like count, collect, write) trigger the computation."
    },
    {
        "question": "In an Apache Kafka topic, how is horizontal consumer scalability achieved across a single Consumer Group?",
        "options": [
            "Multiple consumers within the same group read the same partition simultaneously",
            "Each partition is assigned to at most one consumer instance within the consumer group",
            "By increasing the number of Zookeeper nodes",
            "Kafka automatically duplicates messages across all consumers in the group"
        ],
        "answer": "Each partition is assigned to at most one consumer instance within the consumer group",
        "explanation": "A partition in Kafka can only be consumed by one consumer in a consumer group at any given time. If you have more consumers than partitions, the extra consumers remain idle."
    },
    {
        "question": "Which architecture pattern decouples compute from storage and utilizes an ACID transaction log over Parquet files on object storage?",
        "options": ["Traditional RDBMS", "Data Lakehouse (Delta Lake / Apache Iceberg)", "Standard Hadoop HDFS", "OLTP NoSQL"],
        "answer": "Data Lakehouse (Delta Lake / Apache Iceberg)",
        "explanation": "The Data Lakehouse combines low-cost cloud object storage with table formats (like Delta Lake or Iceberg) that maintain an ACID transaction log for reliability and time travel."
    },
    {
        "question": "In Apache Airflow, why should you prefer sensor mode='reschedule' over mode='poke' for tasks that wait for hours?",
        "options": [
            "'poke' mode crashes after 10 minutes",
            "'reschedule' frees up worker slot resources and sleeps until the next check interval",
            "'poke' requires root Linux permissions",
            "'reschedule' automatically creates the missing file"
        ],
        "answer": "'reschedule' frees up worker slot resources and sleeps until the next check interval",
        "explanation": "In 'poke' mode, the worker slot remains blocked continuously. In 'reschedule' mode, the task yields the worker slot and is rescheduled only when the poke interval arrives."
    },
    {
        "question": "What does the dbt `{{ ref('model_name') }}` macro do when compiling SQL?",
        "options": [
            "Imports a Python library into PostgreSQL",
            "Interpolates the exact database schema/table name and automatically builds the dependency DAG",
            "Creates a foreign key constraint between tables",
            "Deletes the referenced table before running"
        ],
        "answer": "Interpolates the exact database schema/table name and automatically builds the dependency DAG",
        "explanation": "`{{ ref() }}` enables modularity in dbt by resolving table names according to the environment and constructing the directed acyclic graph (DAG) of transformations."
    },
    {
        "question": "In Snowflake, what is 'Zero-Copy Cloning'?",
        "options": [
            "Exporting data to an external FTP server without a copy command",
            "Creating an instant copy of a database/table by copying metadata pointers without duplicating storage bytes",
            "Running queries with zero virtual warehouses running",
            "A feature to disable table replication across regions"
        ],
        "answer": "Creating an instant copy of a database/table by copying metadata pointers without duplicating storage bytes",
        "explanation": "Zero-Copy Cloning replicates metadata pointers pointing to existing micro-partitions, allowing instant staging/dev copies at zero additional storage cost until data is modified."
    },
    {
        "question": "What strategy is most effective to eliminate network shuffle overhead when joining a massive Fact table with a small Dimension table in Spark?",
        "options": [
            "Sort-Merge Join",
            "Broadcast Hash Join (broadcast(small_df))",
            "Cartesian Cross Join",
            "Increasing spark.sql.shuffle.partitions to 20,000"
        ],
        "answer": "Broadcast Hash Join (broadcast(small_df))",
        "explanation": "A Broadcast Hash Join copies the small DataFrame to all executor nodes, allowing the join to happen locally on each partition without shuffling the massive fact table across the network."
    },
    {
        "question": "According to the CAP Theorem, what two properties must a distributed database choose between during a network partition (P)?",
        "options": [
            "Consistency (C) or Availability (A)",
            "Concurrency or Atomicity",
            "Durability or Scalability",
            "Security or Performance"
        ],
        "answer": "Consistency (C) or Availability (A)",
        "explanation": "Network partitions are inevitable in distributed systems. When a partition occurs, the system must either sacrifice availability (return an error) or consistency (return potentially stale data)."
    },
    {
        "question": "In the Medallion Architecture (Bronze -> Silver -> Gold), what is the primary purpose of the Silver layer?",
        "options": [
            "Store raw, unmodified logs and CDC payloads",
            "Provide cleaned, deduplicated, and schema-enforced enterprise views of data",
            "Serve direct exports for executive PowerPoint decks",
            "Store expired historical data ready for permanent deletion"
        ],
        "answer": "Provide cleaned, deduplicated, and schema-enforced enterprise views of data",
        "explanation": "Bronze stores raw data, Silver cleans and enriches data with consistent schemas and deduplication, and Gold aggregates business-level marts for BI reporting."
    },
    {
        "question": "What is the primary role of Change Data Capture (CDC) with tools like Debezium in modern data pipelines?",
        "options": [
            "Capturing row-level database changes directly from transaction logs (WAL/Binlog) without polling the production database",
            "Compressing CSV files into Zip format",
            "Encrypting passwords before saving to users table",
            "Generating dummy data for unit testing"
        ],
        "answer": "Capturing row-level database changes directly from transaction logs (WAL/Binlog) without polling the production database",
        "explanation": "CDC reads transaction logs (such as PostgreSQL WAL or MySQL binlog) to stream inserts, updates, and deletes in real-time with near-zero impact on the operational database."
    },
    {
        "question": "What causes 'data skew' in an Apache Spark join or aggregation?",
        "options": [
            "An unequal distribution of records across partition keys where a few keys contain most of the data",
            "Using Python instead of Scala",
            "Writing to S3 instead of local disk",
            "Having too many executor cores enabled"
        ],
        "answer": "An unequal distribution of records across partition keys where a few keys contain most of the data",
        "explanation": "Data skew occurs when one or more keys dominate the dataset (e.g., NULL values or heavy categories), causing a single executor task to process 90% of the data while others sit idle."
    },
    {
        "question": "What is the key advantage of Apache Iceberg's 'Hidden Partitioning' over traditional Hive partitioning?",
        "options": [
            "It hides tables from unauthorized users",
            "Users query against original columns without needing to know the partition transform formula (e.g., year(event_time))",
            "It automatically converts Parquet files into MySQL tables",
            "It prevents tables from having more than 100 rows"
        ],
        "answer": "Users query against original columns without needing to know the partition transform formula (e.g., year(event_time))",
        "explanation": "In traditional Hive partitioning, users had to remember explicit partition columns (like dt='2026-05-14') or scan full tables. Iceberg translates filters automatically, preventing accidental full table scans."
    },
    {
        "question": "Why is 'Idempotency' considered a mandatory requirement for production data pipelines?",
        "options": [
            "It guarantees that re-running a pipeline for a specific date produces identical output without duplicating data",
            "It reduces the size of files stored in cloud storage",
            "It automatically fixes corrupted SQL queries",
            "It converts batch pipelines into streaming pipelines"
        ],
        "answer": "It guarantees that re-running a pipeline for a specific date produces identical output without duplicating data",
        "explanation": "Pipelines inevitably fail and need backfilling. An idempotent pipeline ensures that running a job multiple times for the same time partition yields the exact same clean state."
    }
]


def run_de_quiz(username: str, role: str):
    st.title("⚡ Core Data Engineering Assessment")
    st.markdown(f"Test your knowledge on **Architecture, Spark, Kafka, Airflow, Warehousing & Lakehouses**. | Candidate: **{username}**")

    # Session state initialization
    if "de_quiz_set" not in st.session_state:
        st.session_state.de_quiz_set = random.sample(DE_QUESTIONS_POOL, min(10, len(DE_QUESTIONS_POOL)))
        st.session_state.de_submitted = False
        st.session_state.de_answers = ["--"] * len(st.session_state.de_quiz_set)

    def reset_de_quiz():
        st.session_state.de_quiz_set = random.sample(DE_QUESTIONS_POOL, min(10, len(DE_QUESTIONS_POOL)))
        st.session_state.de_submitted = False
        st.session_state.de_answers = ["--"] * len(st.session_state.de_quiz_set)
        if "de_score" in st.session_state:
            del st.session_state.de_score
        if "de_pct" in st.session_state:
            del st.session_state.de_pct

    questions = st.session_state.de_quiz_set
    is_submitted = st.session_state.get("de_submitted", False)

    st.markdown("### 📋 Multiple Choice Questions (10 Questions)")

    for i, q in enumerate(questions):
        st.markdown(f"**Q{i+1}: {q['question']}**")
        options = ["--"] + q["options"]
        current_ans = st.session_state.de_answers[i]
        idx = options.index(current_ans) if current_ans in options else 0

        selected = st.radio(
            f"Select answer for Q{i+1}",
            options,
            index=idx,
            key=f"de_q_{i}",
            label_visibility="collapsed",
            disabled=is_submitted
        )
        st.session_state.de_answers[i] = selected
        st.write("")

    col1, col2 = st.columns([1, 4])
    with col1:
        if not is_submitted:
            if st.button("🚀 Submit Assessment", type="primary", use_container_width=True):
                if "--" in st.session_state.de_answers:
                    st.error("⚠️ Please answer all questions before submitting!")
                else:
                    score = 0
                    for idx, q in enumerate(questions):
                        if st.session_state.de_answers[idx] == q["answer"]:
                            score += 1
                    total = len(questions)
                    pct = round((score / total) * 100, 1)

                    st.session_state.de_score = score
                    st.session_state.de_total = total
                    st.session_state.de_pct = pct
                    st.session_state.de_submitted = True

                    # Save to DB
                    try:
                        conn = get_safe_connection()
                        cur = conn.cursor()
                        cur.execute("""
                            INSERT INTO de_quiz_results (username, score, total, percentage, submitted_at)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (username, score, total, pct, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                        conn.commit()
                        cur.close()
                    except Exception as e:
                        print(f"Error saving DE quiz result: {e}")

                    st.rerun()
        else:
            if st.button("🔄 Retake Assessment", use_container_width=True):
                reset_de_quiz()
                st.rerun()

    # Results & Explanations
    if is_submitted:
        st.markdown("---")
        score = st.session_state.get("de_score", 0)
        total = st.session_state.get("de_total", 10)
        pct = st.session_state.get("de_pct", 0.0)

        if pct >= 80:
            st.success(f"🎉 **Outstanding! Score: {score}/{total} ({pct}%)** — You have demonstrated strong Data Engineering acumen!")
        elif pct >= 60:
            st.info(f"👍 **Good Job! Score: {score}/{total} ({pct}%)** — Solid foundation, review key distributed systems topics!")
        else:
            st.warning(f"📚 **Keep Learning! Score: {score}/{total} ({pct}%)** — Review the roadmap topics to strengthen your foundation.")

        st.markdown("### 🔍 Answer Review & Explanations")
        for i, q in enumerate(questions):
            user_ans = st.session_state.de_answers[i]
            correct_ans = q["answer"]
            is_correct = user_ans == correct_ans

            with st.expander(f"Q{i+1}: {'✅ Correct' if is_correct else '❌ Incorrect'} — {q['question'][:75]}...", expanded=not is_correct):
                if is_correct:
                    st.markdown(f"**Your Answer:** `{user_ans}` ✅")
                else:
                    st.markdown(f"**Your Answer:** `{user_ans}` ❌")
                    st.markdown(f"**Correct Answer:** `{correct_ans}` ✅")
                st.markdown(f"💡 **Explanation:** {q['explanation']}")

    # Quiz History
    st.markdown("---")
    st.subheader("📜 Assessment History")
    try:
        conn = get_safe_connection()
        df = pd.read_sql_query("""
            SELECT score, total, percentage, submitted_at
            FROM de_quiz_results
            WHERE username = %s
            ORDER BY id DESC
            LIMIT 10
        """, conn, params=(username,))
        if not df.empty:
            st.dataframe(df, use_container_width=True)
        else:
            st.info("No attempts recorded yet. Submit the assessment above!")
    except Exception as e:
        st.info("History will be available after your first saved attempt.")

    # Admin view
    if role == "admin":
        st.markdown("---")
        st.subheader("👑 All DE Quiz Results (Admin View)")
        try:
            conn = get_safe_connection()
            df_admin = pd.read_sql_query("""
                SELECT username, score, total, percentage, submitted_at
                FROM de_quiz_results
                ORDER BY id DESC
                LIMIT 50
            """, conn)
            if not df_admin.empty:
                st.dataframe(df_admin, use_container_width=True)
        except Exception as e:
            st.error(f"Error loading admin results: {e}")
