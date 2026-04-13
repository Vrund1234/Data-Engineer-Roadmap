import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
import time
import random

# -----------------------------
# DB CONNECTION
# -----------------------------
conn = sqlite3.connect("progress.db", check_same_thread=False)
cursor = conn.cursor()

# -----------------------------
# CREATE TABLE
# -----------------------------
cursor.execute("""
CREATE TABLE IF NOT EXISTS quiz_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT,
    score INTEGER,
    total INTEGER,
    percentage REAL,
    submitted_at TEXT
)
""")
conn.commit()

# -----------------------------
# MCQ QUESTIONS (POOL)
# -----------------------------
mcq_questions = [
    {
        "question": "Which SQL category does CREATE belong to?",
        "options": ["DML", "DDL", "DCL", "TCL"],
        "answer": "DDL"
    },
    {
        "question": "Which command removes a table permanently?",
        "options": ["DELETE", "TRUNCATE", "DROP", "REMOVE"],
        "answer": "DROP"
    },
    {
        "question": "Which constraint ensures uniqueness + NOT NULL?",
        "options": ["UNIQUE", "PRIMARY KEY", "CHECK", "FOREIGN KEY"],
        "answer": "PRIMARY KEY"
    },
    {
        "question": "What does NULL = NULL return?",
        "options": ["TRUE", "FALSE", "NULL", "ERROR"],
        "answer": "NULL"
    },
    {
        "question": "Which clause executes first?",
        "options": ["SELECT", "WHERE", "FROM", "ORDER BY"],
        "answer": "FROM"
    },
    {
        "question": "Which JOIN returns all rows from left table?",
        "options": ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"],
        "answer": "LEFT JOIN"
    },
    {
        "question": "Which removes duplicates?",
        "options": ["GROUP BY", "DISTINCT", "ORDER BY", "UNION ALL"],
        "answer": "DISTINCT"
    },
    {
        "question": "Which is faster?",
        "options": ["UNION", "UNION ALL", "Same", "Depends"],
        "answer": "UNION ALL"
    },
    {
        "question": "COUNT(column) does what?",
        "options": ["Counts all rows", "Counts non-null values", "Counts duplicates", "Counts unique"],
        "answer": "Counts non-null values"
    },
    {
        "question": "Which function returns first non-null value?",
        "options": ["COUNT", "COALESCE", "AVG", "SUM"],
        "answer": "COALESCE"
    },
    {
        "question": "Which clause filters groups?",
        "options": ["WHERE", "HAVING", "GROUP BY", "ORDER BY"],
        "answer": "HAVING"
    },
    {
        "question": "What is a foreign key?",
        "options": [
            "Unique column",
            "Reference to another table",
            "Primary key",
            "Index"
        ],
        "answer": "Reference to another table"
    },
    {
        "question": "Which SQL keyword removes duplicate rows?",
        "options": ["DISTINCT", "GROUP BY", "ORDER BY", "FILTER"],
        "answer": "DISTINCT"
    },
    {
        "question": "Which index improves read performance?",
        "options": ["Constraint", "Trigger", "Index", "Primary Key"],
        "answer": "Index"
    },
    {
        "question": "Which is transaction control?",
        "options": ["SELECT", "COMMIT", "JOIN", "CREATE"],
        "answer": "COMMIT"
    },
    {
        "question": "Which function gives ranking without gaps?",
        "options": ["RANK", "ROW_NUMBER", "DENSE_RANK", "COUNT"],
        "answer": "DENSE_RANK"
    },
    {
        "question": "Which clause is used for grouping?",
        "options": ["WHERE", "GROUP BY", "HAVING", "ORDER BY"],
        "answer": "GROUP BY"
    },
    {
        "question": "Which SQL removes specific rows?",
        "options": ["DROP", "TRUNCATE", "DELETE", "REMOVE"],
        "answer": "DELETE"
    },
    {
        "question": "Which keyword combines results and removes duplicates?",
        "options": ["UNION", "UNION ALL", "JOIN", "MERGE"],
        "answer": "UNION"
    },
    {
        "question": "Which join returns only matching rows?",
        "options": ["LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "FULL JOIN"],
        "answer": "INNER JOIN"
    },
    {
        "question": "Which is NOT a constraint?",
        "options": ["PRIMARY KEY", "INDEX", "FOREIGN KEY", "CHECK"],
        "answer": "INDEX"
    },
    {
        "question": "What is normalization?",
        "options": [
            "Data duplication",
            "Data cleaning",
            "Reducing redundancy",
            "Sorting data"
        ],
        "answer": "Reducing redundancy"
    },
    {
        "question": "Which query finds duplicates?",
        "options": [
            "SELECT * FROM users",
            "SELECT email, COUNT(*) FROM users GROUP BY email HAVING COUNT(*) > 1",
            "SELECT DISTINCT email FROM users",
            "SELECT COUNT(*) FROM users"
        ],
        "answer": "SELECT email, COUNT(*) FROM users GROUP BY email HAVING COUNT(*) > 1"
    },
    {
        "question": "Which SQL clause sorts data?",
        "options": ["GROUP BY", "ORDER BY", "WHERE", "HAVING"],
        "answer": "ORDER BY"
    },
    {
        "question": "Which ACID property ensures all or nothing?",
        "options": ["Consistency", "Atomicity", "Isolation", "Durability"],
        "answer": "Atomicity"
    }
]

# -----------------------------
# QUERY QUESTIONS (5)
# -----------------------------
query_questions = [
    {
        "question": "Find employees hired after 2023",
        "schema": """
Table: employees
Columns:
- emp_id (INT)
- name (TEXT)
- department (TEXT)
- salary (INT)
- hire_date (DATE)
""",
        "answer_keywords": ["select", "from employees", "hire_date", "2023"]
    },
    {
        "question": "Find duplicate emails from users table",
        "schema": """
Table: users
Columns:
- user_id (INT)
- name (TEXT)
- email (TEXT)
""",
        "answer_keywords": ["group by", "email", "count", "having"]
    },
    {
        "question": "Find 3rd highest salary",
        "schema": """
Table: employees
Columns:
- emp_id (INT)
- name (TEXT)
- salary (INT)
""",
        "answer_keywords": ["salary", "order by", "desc"]
    },
    {
        "question": "Count employees in each department",
        "schema": """
Table: employees
Columns:
- emp_id (INT)
- name (TEXT)
- department (TEXT)
""",
        "answer_keywords": ["group by", "department", "count"]
    },
    {
        "question": "Find beverages with fruit percentage between 35 and 40",
        "schema": """
Table: beverages
Columns:
- id (INT)
- name (TEXT)
- fruit_pct (INT)
""",
        "answer_keywords": ["between", "35", "40", "fruit_pct"]
    }
]

# -----------------------------
# QUIZ FUNCTION
# -----------------------------
def run_quiz(username, role):

    st.title("🧠 SQL Quiz (Anti-Cheat Mode)")

    # -----------------------------
    # ANTI-CHEAT JS
    # -----------------------------
    st.markdown("""
    <script>
    document.addEventListener('paste', e => {
        e.preventDefault();
        alert('Paste disabled!');
    });

    document.addEventListener('visibilitychange', function() {
        if (document.hidden) {
            alert('Tab switch detected!');
        }
    });
    </script>
    """, unsafe_allow_html=True)

    # -----------------------------
    # TIMER
    # -----------------------------
    QUIZ_DURATION = 600  # 10 mins

    if "start_time" not in st.session_state:
        st.session_state.start_time = time.time()

    elapsed = int(time.time() - st.session_state.start_time)
    remaining = QUIZ_DURATION - elapsed

    if remaining <= 0:
        st.error("⏰ Time's up!")
        st.stop()

    st.warning(f"⏳ Time Left: {remaining} sec")

    # -----------------------------
    # RANDOMIZE QUESTIONS
    # -----------------------------
    if "mcq_set" not in st.session_state:
        st.session_state.mcq_set = random.sample(mcq_questions, 10)
        st.session_state.query_set = random.sample(query_questions, 3)

    mcq_set = st.session_state.mcq_set
    query_set = st.session_state.query_set

    mcq_answers = []
    query_answers = []

    # -----------------------------
    # MCQ
    # -----------------------------
    st.header("📘 MCQs")

    for i, q in enumerate(mcq_set):
        st.subheader(f"Q{i+1}. {q['question']}")
        ans = st.radio("Select:", ["--"] + q["options"], key=f"mcq_{i}")
        mcq_answers.append(ans)

    # -----------------------------
    # QUERY
    # -----------------------------
    st.header("💻 SQL Queries")

    for i, q in enumerate(query_set):
        st.subheader(f"Q{i+1}. {q['question']}")
        st.info(f"Schema: {q['schema']}")
        ans = st.text_area("Write query:", key=f"query_{i}")
        query_answers.append(ans)

    # -----------------------------
    # SUBMIT
    # -----------------------------
    if st.button("Submit"):

        score = 0
        total = len(mcq_set) + len(query_set)

        # MCQ
        for i, q in enumerate(mcq_set):
            if mcq_answers[i] == q["answer"]:
                score += 1

        # QUERY
        for i, q in enumerate(query_set):
            user = query_answers[i].lower()
            if all(k in user for k in q["answer_keywords"]):
                score += 1

        percent = round((score / total) * 100, 2)

        st.success(f"Score: {score}/{total}")
        st.info(f"Percentage: {percent}%")

        cursor.execute("""
        INSERT INTO quiz_results VALUES (NULL, ?, ?, ?, ?, ?)
        """, (username, score, total, percent, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        conn.commit()

        # reset timer
        del st.session_state.start_time

    # -----------------------------
    # USER HISTORY VIEW
    # -----------------------------
    st.markdown("---")
    st.subheader("📊 Your Quiz History")

    user_df = pd.read_sql_query(
        """
        SELECT score, total, percentage, submitted_at 
        FROM quiz_results 
        WHERE username = ?
        ORDER BY id DESC
        """,
        conn,
        params=(username,)
    )

    if not user_df.empty:
        st.dataframe(user_df, use_container_width=True)

        # 🔥 Highlight latest attempt
        latest = user_df.iloc[0]

        st.markdown("### 🏆 Latest Performance")
        st.success(f"Score: {latest['score']}/{latest['total']}")
        st.info(f"Percentage: {latest['percentage']}%")
    else:
        st.info("No quiz attempts yet. Take your first quiz 🚀")

    # -----------------------------
    # ADMIN VIEW
    # -----------------------------
    if role == "admin":
        st.markdown("---")
        st.subheader("📊 All Quiz Attempts")

        df = pd.read_sql_query(
            "SELECT username, score, total, percentage, submitted_at FROM quiz_results ORDER BY id DESC",
            conn
        )

        if not df.empty:
            st.dataframe(df, use_container_width=True)
        else:
            st.info("No attempts yet")