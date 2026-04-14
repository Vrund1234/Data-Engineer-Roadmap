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


# -----------------------------
# INIT TABLE
# -----------------------------
def init_db():
    conn = get_safe_connection()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS quiz_results (
        id SERIAL PRIMARY KEY,
        username TEXT,
        score INTEGER,
        total INTEGER,
        percentage REAL,
        submitted_at TEXT
    )
    """)

    cursor.close()

init_db()


# -----------------------------
# QUESTIONS
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
    },
    # ---------------- NEW (FROM YOUR LIST) ----------------
    {
        "question": "Which SQL statement is used to retrieve data?",
        "options": ["GET", "SELECT", "FETCH", "EXTRACT"],
        "answer": "SELECT"
    },
    {
        "question": "Which clause is used to filter records?",
        "options": ["ORDER BY", "WHERE", "GROUP BY", "SELECT"],
        "answer": "WHERE"
    },
    {
        "question": "What does COUNT(*) do?",
        "options": [
            "Counts columns",
            "Counts rows including NULLs",
            "Counts only non-null values",
            "Returns total columns"
        ],
        "answer": "Counts rows including NULLs"
    },
    {
        "question": "Which keyword removes duplicate rows?",
        "options": ["UNIQUE", "DISTINCT", "REMOVE", "FILTER"],
        "answer": "DISTINCT"
    },
    {
        "question": "What is the default sort order of ORDER BY?",
        "options": ["DESC", "ASC", "RANDOM", "NONE"],
        "answer": "ASC"
    },
    {
        "question": "Which clause is used to group rows?",
        "options": ["WHERE", "GROUP BY", "ORDER BY", "HAVING"],
        "answer": "GROUP BY"
    },
    {
        "question": "Which function returns the highest value?",
        "options": ["TOP()", "MAX()", "HIGH()", "UPPER()"],
        "answer": "MAX()"
    },
    {
        "question": "What does NULL represent?",
        "options": [
            "Zero",
            "Empty string",
            "Unknown or missing value",
            "False"
        ],
        "answer": "Unknown or missing value"
    },
    {
        "question": "Which operator is used for pattern matching?",
        "options": ["=", "LIKE", "IN", "BETWEEN"],
        "answer": "LIKE"
    },
    {
        "question": "What does IN operator do?",
        "options": [
            "Matches pattern",
            "Checks range",
            "Matches multiple values",
            "Joins tables"
        ],
        "answer": "Matches multiple values"
    },
    {
        "question": "Which keyword is used to sort results?",
        "options": ["SORT", "ORDER", "ORDER BY", "ARRANGE"],
        "answer": "ORDER BY"
    },
    {
        "question": "Which join returns matching rows from both tables?",
        "options": ["LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "FULL JOIN"],
        "answer": "INNER JOIN"
    },
    {
        "question": "Which join returns all rows from left table?",
        "options": ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "CROSS JOIN"],
        "answer": "LEFT JOIN"
    },
    {
        "question": "What does HAVING do?",
        "options": [
            "Filters before grouping",
            "Filters after grouping",
            "Sorts data",
            "Joins tables"
        ],
        "answer": "Filters after grouping"
    },
    {
        "question": "Which command adds new data?",
        "options": ["ADD", "INSERT", "UPDATE", "CREATE"],
        "answer": "INSERT"
    },
    {
        "question": "Which command modifies existing data?",
        "options": ["MODIFY", "CHANGE", "UPDATE", "ALTER"],
        "answer": "UPDATE"
    },
    {
        "question": "Which command deletes data?",
        "options": ["REMOVE", "DELETE", "DROP", "CLEAR"],
        "answer": "DELETE"
    },
    {
        "question": "What is a primary key?",
        "options": [
            "Duplicate column",
            "Unique identifier",
            "Foreign column",
            "Sorted column"
        ],
        "answer": "Unique identifier"
    },
    {
        "question": "Which constraint ensures uniqueness?",
        "options": ["NOT NULL", "UNIQUE", "CHECK", "DEFAULT"],
        "answer": "UNIQUE"
    },
    {
        "question": "Which function calculates average?",
        "options": ["AVG()", "SUM()", "COUNT()", "MEAN()"],
        "answer": "AVG()"
    },
    {
        "question": "Which clause limits number of rows?",
        "options": ["LIMIT", "TOP", "FETCH", "All of the above (depends on DB)"],
        "answer": "All of the above (depends on DB)"
    },
    {
        "question": "What does BETWEEN do?",
        "options": [
            "Filters exact values",
            "Filters range",
            "Filters nulls",
            "Joins tables"
        ],
        "answer": "Filters range"
    },
    {
        "question": "What is a foreign key?",
        "options": [
            "Unique key",
            "Link between tables",
            "Primary column",
            "Sorted column"
        ],
        "answer": "Link between tables"
    },
    {
        "question": "Which statement creates a table?",
        "options": ["MAKE TABLE", "CREATE TABLE", "NEW TABLE", "BUILD TABLE"],
        "answer": "CREATE TABLE"
    }
]

# 🔥 UPDATED QUERY QUESTIONS (WITH FULL CONTEXT)
query_questions = [
    {
        "question": "Find employees hired after 2023",
        "table_name": "employees",
        "columns": [
            ("emp_id", "INT"),
            ("name", "TEXT"),
            ("department", "TEXT"),
            ("salary", "INT"),
            ("hire_date", "DATE")
        ],
        "sample_data": [
            (1, "Alice", "IT", 60000, "2022-05-10"),
            (2, "Bob", "HR", 50000, "2024-01-15"),
            (3, "Charlie", "IT", 70000, "2023-07-20")
        ],
        "answer_keywords": ["select", "from employees", "hire_date", "2023"],
        "correct_query": "SELECT * FROM employees WHERE hire_date > '2023-01-01';"
    },
    {
        "question": "Find duplicate emails",
        "table_name": "users",
        "columns": [
            ("user_id", "INT"),
            ("name", "TEXT"),
            ("email", "TEXT")
        ],
        "sample_data": [
            (1, "John", "john@gmail.com"),
            (2, "Jane", "jane@gmail.com"),
            (3, "Jake", "john@gmail.com")
        ],
        "answer_keywords": ["group by", "email", "count", "having"],
        "correct_query": "SELECT email, COUNT(*) FROM users GROUP BY email HAVING COUNT(*) > 1;"
    },
    {
        "question": "Find 3rd highest salary",
        "table_name": "employees",
        "columns": [
            ("emp_id", "INT"),
            ("name", "TEXT"),
            ("salary", "INT")
        ],
        "sample_data": [
            (1, "A", 50000),
            (2, "B", 70000),
            (3, "C", 60000),
            (4, "D", 80000)
        ],
        "answer_keywords": ["salary", "order by", "desc"],
        "correct_query": "SELECT salary FROM employees ORDER BY salary DESC LIMIT 1 OFFSET 2;"
    },
        {
        "question": "Select all employees",
        "table_name": "employees",
        "columns": [("id","INT"),("name","TEXT")],
        "sample_data": [(1,"A"),(2,"B")],
        "answer_keywords": ["select", "*", "from employees"],
        "correct_query": "SELECT * FROM employees;"
    },
    {
        "question": "Find employees with salary > 50000",
        "table_name": "employees",
        "columns": [("id","INT"),("salary","INT")],
        "sample_data": [(1,40000),(2,60000)],
        "answer_keywords": ["salary", "50000", "where"],
        "correct_query": "SELECT * FROM employees WHERE salary > 50000;"
    },
    {
        "question": "Count total orders",
        "table_name": "orders",
        "columns": [("id","INT")],
        "sample_data": [(1),(2),(3)],
        "answer_keywords": ["count", "from orders"],
        "correct_query": "SELECT COUNT(*) FROM orders;"
    },
    {
        "question": "Count employees per department",
        "table_name": "employees",
        "columns": [("id","INT"),("department","TEXT")],
        "sample_data": [(1,"IT"),(2,"HR"),(3,"IT")],
        "answer_keywords": ["group by", "count", "department"],
        "correct_query": "SELECT department, COUNT(*) FROM employees GROUP BY department;"
    },
    {
        "question": "Join employees with departments",
        "table_name": "employees/departments",
        "columns": [
            ("emp_id","INT"),
            ("name","TEXT"),
            ("dept_id","INT")
        ],
        "sample_data": [],
        "answer_keywords": ["join", "on", "dept_id"],
        "correct_query": "SELECT e.name, d.dept_name FROM employees e JOIN departments d ON e.dept_id = d.dept_id;"
    }
]


# -----------------------------
# RESET FUNCTION
# -----------------------------
def reset_quiz():
    keys_to_delete = ["mcq_set", "query_set", "submitted", "query_questions"]

    for key in keys_to_delete:
        if key in st.session_state:
            del st.session_state[key]

    for key in list(st.session_state.keys()):
        if key.startswith("mcq_") or key.startswith("query_"):
            del st.session_state[key]

    st.rerun()


# -----------------------------
# SUBMIT FUNCTION
# -----------------------------
def submit_quiz(username, mcq_set, query_set, mcq_answers, query_answers):
    score = 0
    total = len(mcq_set) + len(query_set)

    # MCQ
    for i, q in enumerate(mcq_set):
        if mcq_answers[i] == q["answer"]:
            score += 1

    # QUERY
    for i, q in enumerate(query_set):
        user = query_answers[i].lower().strip()
        if user and all(k in user for k in q["answer_keywords"]):
            score += 1

    percent = round((score / total) * 100, 2)

    conn = get_safe_connection()
    cursor = conn.cursor()

    cursor.execute("""
    INSERT INTO quiz_results (username, score, total, percentage, submitted_at)
    VALUES (%s, %s, %s, %s, %s)
    """, (username, score, total, percent,
          datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

    conn.commit()
    cursor.close()

    return score, total, percent

# -----------------------------
# MAIN QUIZ FUNCTION
# -----------------------------
def run_quiz(username, role):

    st.title("🧠 SQL Quiz")

    # RETAKE
    if st.button("🔄 Retake Quiz", key="retake_top"):
        reset_quiz()

    # LOAD QUESTIONS
    MCQ_LIMIT = 25
    QUERY_LIMIT = 3

    if "mcq_set" not in st.session_state:
        st.session_state.mcq_set = random.sample(
            mcq_questions, 
            min(MCQ_LIMIT, len(mcq_questions))
        )

    if "query_set" not in st.session_state:
        st.session_state.query_set = random.sample(
            query_questions, 
            min(QUERY_LIMIT, len(query_questions))
        )

    mcq_set = st.session_state.mcq_set
    query_set = st.session_state.query_set

    mcq_answers = []
    query_answers = []

    submitted = st.session_state.get("submitted", False)

    # -----------------------------
    # MCQ
    # -----------------------------
    st.header("📘 MCQs")

    # INIT SHUFFLED OPTIONS (ONLY ONCE)
    if "mcq_options" not in st.session_state:
        st.session_state.mcq_options = []

        for q in mcq_set:
            opts = q["options"].copy()
            random.shuffle(opts)
            st.session_state.mcq_options.append(opts)


    for i, q in enumerate(mcq_set):
        options = st.session_state.mcq_options[i]
        ans = st.radio(
            f"Q{i+1}. {q['question']}",
            ["--"] + options,
            key=f"mcq_{i}",
            disabled=submitted
        )
        mcq_answers.append(ans)

    # -----------------------------
    # QUERY
    # -----------------------------
    st.header("💻 SQL Queries")

    for i, q in enumerate(query_set):
        st.subheader(f"Q{i+1}. {q['question']}")

        st.dataframe(pd.DataFrame(q["columns"], columns=["Column", "Type"]))
        st.dataframe(pd.DataFrame(q["sample_data"], columns=[c[0] for c in q["columns"]]))

        ans = st.text_area(
            "Write SQL query:",
            key=f"query_{i}",
            disabled=submitted
        )
        query_answers.append(ans)


    # -----------------------------
    # SUBMIT
    # -----------------------------
    if st.button("Submit Quiz", key="submit_btn") and not submitted:

        if "--" in mcq_answers:
            st.error("Answer all MCQs first")
            return

        st.session_state.submitted = True

        score, total, percent = submit_quiz(
            username, mcq_set, query_set, mcq_answers, query_answers
        )

        # ✅ STORE RESULTS
        st.session_state.score = score
        st.session_state.total = total
        st.session_state.percent = percent
        st.session_state.mcq_answers = mcq_answers
        st.session_state.query_answers = query_answers

    # -----------------------------
    # SHOW RESULTS (PERSISTENT)
    # -----------------------------
    if st.session_state.get("submitted", False):

        st.success(f"🎯 Score: {st.session_state.score}/{st.session_state.total}")
        st.info(f"📊 Percentage: {st.session_state.percent}%")

        st.markdown("---")
        st.subheader("✅ Review Answers")

        # MCQ REVIEW
        st.markdown("### 📘 MCQs Review")
        for i, q in enumerate(mcq_set):
            user_ans = st.session_state.mcq_answers[i]
            correct_ans = q["answer"]

            if user_ans == correct_ans:
                st.success(f"Q{i+1}: Correct ✅")
            else:
                st.error(f"Q{i+1}: Wrong ❌")
                st.write(f"Your Answer: {user_ans}")
                st.write(f"Correct Answer: {correct_ans}")

        # QUERY REVIEW
        st.markdown("### 💻 SQL Query Review")
        for i, q in enumerate(query_set):
            user_ans = st.session_state.query_answers[i].lower().strip()

            if user_ans and all(k in user_ans for k in q["answer_keywords"]):
                st.success(f"Query {i+1}: Correct ✅")
            else:
                st.error(f"Query {i+1}: Wrong ❌")
                st.write("Expected keywords:")
                st.code(q["correct_query"], language="sql")

        st.button("🔄 Retake Quiz", key="retake_after_submit", on_click=reset_quiz)

    # -----------------------------
    # HISTORY
    # -----------------------------
    st.markdown("---")
    st.subheader("📊 Your Quiz History")

    conn = get_safe_connection()

    df = pd.read_sql_query("""
        SELECT score, total, percentage, submitted_at
        FROM quiz_results
        WHERE username = %s
        ORDER BY id DESC
    """, conn, params=(username,))

    if not df.empty:
        st.dataframe(df, use_container_width=True)
    else:
        st.info("No quiz attempts yet 🚀")

    # -----------------------------
    # ADMIN
    # -----------------------------
    if role == "admin":
        st.markdown("---")
        st.subheader("📊 All Quiz Attempts")

        df = pd.read_sql_query("""
            SELECT username, score, total, percentage, submitted_at
            FROM quiz_results
            ORDER BY id DESC
        """, conn)

        if not df.empty:
            st.dataframe(df, use_container_width=True)
        else:
            st.info("No attempts yet")