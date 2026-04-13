import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime

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
# MCQ QUESTIONS (25)
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

    st.title("🧠 SQL Quiz")

    mcq_answers = []
    query_answers = []

    # -----------------------------
    # MCQ SECTION
    # -----------------------------
    st.header("📘 MCQs")

    for i, q in enumerate(mcq_questions):
        st.subheader(f"Q{i+1}. {q['question']}")

        options = ["-- Select --"] + q["options"]

        ans = st.radio(
            "Select answer:",
            options,
            key=f"mcq_{i}"
        )

        mcq_answers.append(ans)

    # -----------------------------
    # QUERY SECTION
    # -----------------------------
    st.header("💻 SQL Query Questions")

    for i, q in enumerate(query_questions):
        st.subheader(f"Q{i+1}. {q['question']}")

        st.info(q["schema"])  # 👈 SHOW TABLE STRUCTURE

        user_query = st.text_area(
            "Write SQL query:",
            key=f"query_{i}"
        )

        query_answers.append(user_query)

    # -----------------------------
    # SUBMIT
    # -----------------------------
    if st.button("Submit Quiz"):

        if "-- Select --" in mcq_answers:
            st.error("Answer all MCQs first")
            return

        score = 0
        total = len(mcq_questions) + len(query_questions)

        st.markdown("## 🧾 Results")

        # MCQ Evaluation
        for i, q in enumerate(mcq_questions):
            if mcq_answers[i] == q["answer"]:
                st.success(f"MCQ {i+1} ✅ Correct")
                score += 1
            else:
                st.error(f"MCQ {i+1} ❌ Wrong | Correct: {q['answer']}")

        # Query Evaluation (keyword-based match)
        for i, q in enumerate(query_questions):
            user = query_answers[i].strip().lower()

            if all(keyword in user for keyword in q["answer_keywords"]):
                st.success(f"Query {i+1} ✅ Correct")
                score += 1
            else:
                st.error(f"Query {i+1} ❌ Incorrect")

        percentage = round((score / total) * 100, 2)

        st.markdown("---")
        st.success(f"🎯 Score: {score}/{total}")
        st.info(f"📊 Percentage: {percentage}%")

        # SAVE
        cursor.execute("""
        INSERT INTO quiz_results (username, score, total, percentage, submitted_at)
        VALUES (?, ?, ?, ?, ?)
        """, (username, score, total, percentage, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        conn.commit()

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