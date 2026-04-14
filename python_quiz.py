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
    CREATE TABLE IF NOT EXISTS python_quiz_results (
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
# PYTHON MCQs (50)
# -----------------------------
mcq_questions = [
    {"question": "What is the output of print(type(5))?", "options": ["int", "<class 'int'>", "number", "integer"], "answer": "<class 'int'>"},
    {"question": "Which keyword is used to define a function?", "options": ["func", "define", "def", "function"], "answer": "def"},
    {"question": "Which data type is immutable?", "options": ["List", "Dictionary", "Set", "Tuple"], "answer": "Tuple"},
    {"question": "What is len('hello')?", "options": ["4", "5", "6", "Error"], "answer": "5"},
    {"question": "Which operator is used for exponentiation?", "options": ["^", "**", "exp", "//"], "answer": "**"},

    {"question": "Which method adds element to list?", "options": ["insert()", "append()", "add()", "push()"], "answer": "append()"},
    {"question": "list.pop() does?", "options": ["Remove first", "Remove last", "Clear list", "Length"], "answer": "Remove last"},
    {"question": "Lists are?", "options": ["Immutable", "Ordered and mutable", "Unordered", "Fixed"], "answer": "Ordered and mutable"},
    {"question": "Index of first element?", "options": ["1", "0", "-1", "None"], "answer": "0"},
    {"question": "Output: [1,2,3][1]?", "options": ["1", "2", "3", "Error"], "answer": "2"},

    {"question": "Strings are?", "options": ["Mutable", "Immutable", "Numeric", "None"], "answer": "Immutable"},
    {"question": "hello.upper()?", "options": ["hello", "HELLO", "Hello", "error"], "answer": "HELLO"},
    {"question": "'abc'*2 ?", "options": ["abcabc", "abc2", "error", "None"], "answer": "abcabc"},
    {"question": "Split method?", "options": ["break()", "split()", "cut()", "divide()"], "answer": "split()"},
    {"question": "'hello'[0]?", "options": ["h", "e", "o", "error"], "answer": "h"},

    {"question": "Dictionary stores?", "options": ["values", "key-value pairs", "list", "tuple"], "answer": "key-value pairs"},
    {"question": "Access dict value?", "options": ["dict(key)", "dict[key]", "dict.value", "getvalue"], "answer": "dict[key]"},
    {"question": "keys() returns?", "options": ["keys", "values", "items", "get"], "answer": "keys"},
    {"question": "Dictionaries are?", "options": ["Ordered", "Mutable", "Immutable", "Numeric"], "answer": "Mutable"},
    {"question": "len({'a':1,'b':2})?", "options": ["1", "2", "3", "Error"], "answer": "2"},

    {"question": "Which loop repeats fixed times?", "options": ["while", "for", "loop", "repeat"], "answer": "for"},
    {"question": "Infinite loop condition?", "options": ["False", "True", "None", "0"], "answer": "True"},
    {"question": "Break does?", "options": ["skip", "stop loop", "continue", "repeat"], "answer": "stop loop"},
    {"question": "Continue does?", "options": ["stop", "skip iteration", "break", "restart"], "answer": "skip iteration"},
    {"question": "range(3)?", "options": ["1,2,3", "0,1,2", "0,1,2,3", "1,2"], "answer": "0,1,2"},

    {"question": "Function returns using?", "options": ["break", "return", "stop", "give"], "answer": "return"},
    {"question": "Default parameter?", "options": ["required", "optional", "fixed", "none"], "answer": "optional"},
    {"question": "Lambda is?", "options": ["class", "anonymous function", "loop", "module"], "answer": "anonymous function"},
    {"question": "Function without return gives?", "options": ["0", "None", "error", "False"], "answer": "None"},

    {"question": "Python is?", "options": ["compiled", "interpreted", "binary", "static"], "answer": "interpreted"},
    {"question": "Comment symbol?", "options": ["//", "#", "<!--", "**"], "answer": "#"},
    {"question": "Import keyword?", "options": ["include", "import", "using", "load"], "answer": "import"},
    {"question": "Boolean value?", "options": ["1", "True", "yes", "ok"], "answer": "True"},

    {"question": "Set is?", "options": ["ordered", "unordered unique", "list", "dict"], "answer": "unordered unique"},
    {"question": "in operator?", "options": ["assign", "membership", "compare", "loop"], "answer": "membership"},
    {"question": "== means?", "options": ["assign", "compare", "equal", "none"], "answer": "compare"},
    {"question": "is means?", "options": ["value compare", "identity compare", "assign", "none"], "answer": "identity compare"},
    {"question": "None means?", "options": ["zero", "empty", "no value", "false"], "answer": "no value"},

    {"question": "Pandas used for?", "options": ["ML", "data manipulation", "API", "UI"], "answer": "data manipulation"},
    {"question": "DataFrame is?", "options": ["list", "table", "dict", "tuple"], "answer": "table"},
    {"question": "Read CSV?", "options": ["open()", "pd.read_csv()", "csv.read()", "file.read()"], "answer": "pd.read_csv()"},
    {"question": "head() returns?", "options": ["last rows", "first rows", "middle", "all"], "answer": "first rows"},
    {"question": "tail() returns?", "options": ["first", "last rows", "none", "sorted"], "answer": "last rows"},

    {"question": "Open file?", "options": ["file()", "open()", "read()", "load()"], "answer": "open()"},
    {"question": "Mode 'r'?", "options": ["write", "read", "append", "delete"], "answer": "read"},
    {"question": "Mode 'w'?", "options": ["read", "write", "append", "none"], "answer": "write"},
    {"question": "Mode 'a'?", "options": ["read", "append", "write", "delete"], "answer": "append"},
    {"question": "Close file?", "options": ["end()", "close()", "stop()", "finish()"], "answer": "close()"},
]

# -----------------------------
# CODING QUESTIONS
# -----------------------------
coding_questions = [
    {
        "question": "Write a Python program to reverse a given string.\nExample: Input: 'hello' → Output: 'olleh'",
        "keywords": ["[::-1]"],
        "answer": "s[::-1]"
    },
    {
        "question": "Write a Python program to check whether a number is even or odd.\nExample: Input: 4 → Output: Even",
        "keywords": ["%", "2"],
        "answer": "num % 2 == 0"
    },
    {
        "question": "Write a Python program to find the largest number in a list.\nExample: Input: [1, 5, 3] → Output: 5",
        "keywords": ["max"],
        "answer": "max(lst)"
    },
    {
        "question": "Write a Python program to count the number of vowels in a string.\nExample: Input: 'hello' → Output: 2",
        "keywords": ["for", "in"],
        "answer": "Use a loop to count vowels"
    },
    {
        "question": "Write a Python program to remove duplicate elements from a list.\nExample: Input: [1,2,2,3] → Output: [1,2,3]",
        "keywords": ["set"],
        "answer": "list(set(lst))"
    },
    {
        "question": "Write a Python program to calculate the factorial of a number.\nExample: Input: 5 → Output: 120",
        "keywords": ["for", "*"],
        "answer": "Use loop or recursion"
    },
    {
        "question": "Write a Python program to check whether a string is a palindrome.\nExample: Input: 'madam' → Output: True",
        "keywords": ["[::-1]"],
        "answer": "s == s[::-1]"
    },
    {
        "question": "Write a Python program to count the number of words in a string.\nExample: Input: 'hello world' → Output: 2",
        "keywords": ["split"],
        "answer": "len(s.split())"
    },
    {
        "question": "Write a Python program to read a CSV file using pandas.\nExample: File: data.csv → Output: DataFrame",
        "keywords": ["read_csv"],
        "answer": "pd.read_csv()"
    },
    {
        "question": "Write a Python program to create a dictionary from two lists (keys and values).\nExample: keys=['a','b'], values=[1,2] → Output: {'a':1,'b':2}",
        "keywords": ["zip"],
        "answer": "dict(zip(keys, values))"
    },
]
# -----------------------------
# RESET
# -----------------------------
def reset_quiz():
    for key in list(st.session_state.keys()):
        if key.startswith("mcq_") or key.startswith("code_") or key in ["mcq_set", "code_set", "submitted"]:
            del st.session_state[key]
    # st.rerun()

# -----------------------------
# MAIN QUIZ
# -----------------------------
def run_python_quiz(username, role):

    st.title("🐍 Python Quiz")

    # if st.button("🔄 Retake Quiz"):
    #     reset_quiz()

    if "mcq_set" not in st.session_state:
        st.session_state.mcq_set = random.sample(mcq_questions, 25)

    if "code_set" not in st.session_state:
        st.session_state.code_set = random.sample(coding_questions, 3)

    mcq_set = st.session_state.mcq_set
    code_set = st.session_state.code_set

    mcq_answers = []
    code_answers = []

    submitted = st.session_state.get("submitted", False)

    # MCQ
    st.header("📘 MCQs")

    if "mcq_options" not in st.session_state:
        st.session_state.mcq_options = []
        for q in mcq_set:
            opts = q["options"].copy()
            random.shuffle(opts)
            st.session_state.mcq_options.append(opts)

    for i, q in enumerate(mcq_set):
        ans = st.radio(
            f"Q{i+1}. {q['question']}",
            ["--"] + st.session_state.mcq_options[i],
            key=f"mcq_{i}",
            disabled=submitted
        )
        mcq_answers.append(ans)

    # CODING
    st.header("💻 Coding")

    for i, q in enumerate(code_set):
        st.subheader(q["question"])
        ans = st.text_area("Write code:", key=f"code_{i}", disabled=submitted)
        code_answers.append(ans)

    # -----------------------------
    # SUBMIT
    # -----------------------------
    if st.button("Submit") and not submitted:

        if "--" in mcq_answers:
            st.error("Please answer all MCQs")
            return

        st.session_state.submitted = True

        score = 0
        total = len(mcq_set) + len(code_set)

        # MCQ scoring
        for i, q in enumerate(mcq_set):
            if mcq_answers[i] == q["answer"]:
                score += 1

        # Coding scoring
        for i, q in enumerate(code_set):
            user = code_answers[i].lower()
            if user and all(k in user for k in q["keywords"]):
                score += 1

        percent = round((score / total) * 100, 2)

        conn = get_safe_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO python_quiz_results (username, score, total, percentage, submitted_at)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            username,
            score,
            total,
            percent,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ))

        cursor.close()

        # Store results
        st.session_state.score = score
        st.session_state.total = total
        st.session_state.percent = percent
        st.session_state.mcq_answers = mcq_answers
        st.session_state.code_answers = code_answers


    # -----------------------------
    # RESULTS + REVIEW
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

        # CODING REVIEW
        st.markdown("### 💻 Coding Review")

        for i, q in enumerate(code_set):
            user_ans = st.session_state.code_answers[i].lower()

            if user_ans and all(k in user_ans for k in q["keywords"]):
                st.success(f"Question {i+1}: Correct ✅")
            else:
                st.error(f"Question {i+1}: Wrong ❌")
                st.write("Expected Answer:")
                st.code(q["answer"], language="python")

        if st.button("🔄 Retake Quiz"):
            reset_quiz()
            st.rerun()