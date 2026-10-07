"""
Comprehensive Data Engineer Roadmap 2026 (roadmap.sh aligned).
Preserves 100% backward compatibility with all existing database task keys
while providing modern 2026 curriculum, rich metadata, curated resources,
code snippets, and interview prep.
"""

from typing import Dict, List, Any, Optional

# -----------------------------------------------------------------------------
# ROADMAP PHASES & DETAILED TOPICS
# -----------------------------------------------------------------------------
# Backward compatibility: For all existing 6 phases, the exact original
# Phase name, Category name, and Topic name are preserved.
# Additional phases and topics seamlessly extend the curriculum to match roadmap.sh.

ROADMAP_DATA = {
    "Phase 1: SQL + Python Foundation": {
        "icon": "🐍",
        "description": "Foundational programming and SQL fundamentals essential for every data engineer.",
        "est_time": "3-4 Weeks",
        "color": "#3B82F6",
        "categories": {
            "SQL": [
                {
                    "name": "SQL Setup & Basics",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "2 hrs",
                    "summary": "Understand Relational Database concepts, SQL execution order, and client setup (DBeaver, psql, pgAdmin).",
                    "skills": ["Client tools", "RDBMS concepts", "Data types", "SQL syntax"],
                    "interview_question": "What is the difference between DDL, DML, DCL, and TCL in SQL?",
                    "code_snippet": "-- DDL: Define schema\nCREATE TABLE employees (\n    emp_id INT PRIMARY KEY,\n    name VARCHAR(100) NOT NULL,\n    department VARCHAR(50),\n    salary NUMERIC(10, 2)\n);",
                    "resources": [
                        {"title": "W3Schools SQL Introduction", "url": "https://www.w3schools.com/sql/sql_intro.asp"},
                        {"title": "roadmap.sh SQL Guide", "url": "https://roadmap.sh/sql"},
                        {"title": "PostgreSQL Tutorial", "url": "https://www.postgresqltutorial.com/"}
                    ],
                    "aliases": ["SQL Practice", "Aggregations (COUNT, SUM, AVG)"]
                },
                {
                    "name": "SELECT Statement",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "1 hr",
                    "summary": "Retrieve data from tables, use column aliases (AS), arithmetic operators, and expression evaluation.",
                    "skills": ["Projection", "Column aliasing", "Calculated columns"],
                    "interview_question": "Why is 'SELECT *' considered bad practice in production analytical queries?",
                    "code_snippet": "SELECT \n    emp_id,\n    name,\n    salary * 1.10 AS projected_salary\nFROM employees;",
                    "resources": [
                        {"title": "W3Schools SELECT", "url": "https://www.w3schools.com/sql/sql_select.asp"},
                        {"title": "Mode Analytics SQL SELECT", "url": "https://mode.com/sql-tutorial/sql-select-statement/"}
                    ]
                },
                {
                    "name": "Filtering Data (WHERE)",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "2 hrs",
                    "summary": "Filter records using comparison operators (=, !=, <, >), logical operators (AND, OR, NOT), IN, BETWEEN, and pattern matching (LIKE, ILIKE).",
                    "skills": ["Boolean logic", "LIKE wildcards", "BETWEEN ranges", "IN lists"],
                    "interview_question": "What is the performance difference between LIKE 'text%' and LIKE '%text'?",
                    "code_snippet": "SELECT * FROM employees\nWHERE department = 'Engineering'\n  AND salary BETWEEN 60000 AND 120000\n  AND name LIKE 'A%';",
                    "resources": [
                        {"title": "W3Schools WHERE Clause", "url": "https://www.w3schools.com/sql/sql_where.asp"}
                    ]
                },
                {
                    "name": "Sorting (ORDER BY)",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "1 hr",
                    "summary": "Sort query results in ascending (ASC) or descending (DESC) order, handle NULL sorting order (NULLS FIRST/LAST), and multi-column ordering.",
                    "skills": ["Multi-column sorting", "NULLS FIRST/LAST", "Collation"],
                    "interview_question": "Where does ORDER BY execute in the SQL logical query processing phase?",
                    "code_snippet": "SELECT name, department, salary\nFROM employees\nORDER BY department ASC, salary DESC NULLS LAST;",
                    "resources": [
                        {"title": "PostgreSQL ORDER BY Tutorial", "url": "https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-order-by/"}
                    ]
                },
                {
                    "name": "Aggregate Functions (COUNT, SUM, AVG, MAX)",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "2 hrs",
                    "summary": "Perform multi-row aggregations: COUNT(*), COUNT(col), COUNT(DISTINCT), SUM, AVG, MIN, and MAX.",
                    "skills": ["Aggregation", "COUNT(*) vs COUNT(column)", "Handling NULLs in AVG/SUM"],
                    "interview_question": "What is the difference between COUNT(*) and COUNT(column_name) when NULLs exist?",
                    "code_snippet": "SELECT \n    COUNT(*) AS total_rows,\n    COUNT(salary) AS non_null_salaries,\n    AVG(salary) AS avg_salary,\n    MAX(salary) AS top_salary\nFROM employees;",
                    "resources": [
                        {"title": "W3Schools SQL Aggregate Functions", "url": "https://www.w3schools.com/sql/sql_count_avg_sum.asp"}
                    ],
                    "aliases": ["Aggregations (COUNT, SUM, AVG)"]
                },
                {
                    "name": "GROUP BY & HAVING",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "2.5 hrs",
                    "summary": "Group rows by categorical keys and filter grouped metrics using HAVING instead of WHERE.",
                    "skills": ["Data grouping", "HAVING filter vs WHERE filter", "Aggregates per group"],
                    "interview_question": "Can you use aggregate functions in the WHERE clause? Why or why not?",
                    "code_snippet": "SELECT department, COUNT(*) AS emp_count, AVG(salary) AS avg_sal\nFROM employees\nGROUP BY department\nHAVING AVG(salary) > 75000;",
                    "resources": [
                        {"title": "W3Schools GROUP BY", "url": "https://www.w3schools.com/sql/sql_groupby.asp"},
                        {"title": "W3Schools HAVING", "url": "https://www.w3schools.com/sql/sql_having.asp"}
                    ],
                    "aliases": ["GROUP BY & ORDER BY"]
                },
                {
                    "name": "DISTINCT & NULL Handling",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "2 hrs",
                    "summary": "Remove duplicate rows using DISTINCT. Master Three-Valued Logic (True, False, Unknown), IS NULL, IS NOT NULL, COALESCE, and NULLIF.",
                    "skills": ["Three-valued logic", "COALESCE fallback", "NULLIF division safeguard"],
                    "interview_question": "What does the expression 'NULL = NULL' evaluate to in SQL?",
                    "code_snippet": "SELECT DISTINCT department,\n    COALESCE(bonus, 0) AS safe_bonus,\n    -- Prevent divide-by-zero error\n    revenue / NULLIF(units_sold, 0) AS unit_price\nFROM sales;",
                    "resources": [
                        {"title": "PostgreSQL COALESCE Guide", "url": "https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-coalesce/"}
                    ]
                },
                {
                    "name": "SQL Joins (INNER, LEFT, RIGHT)",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "3 hrs",
                    "summary": "Combine records across relational entities using INNER JOIN, LEFT (OUTER) JOIN, RIGHT (OUTER) JOIN, FULL OUTER JOIN, and CROSS JOIN.",
                    "skills": ["Relational integrity", "Join conditions", "Outer join mechanics", "Self-joins"],
                    "interview_question": "How do you emulate a FULL OUTER JOIN in databases (like MySQL) that don't support it natively?",
                    "code_snippet": "SELECT \n    e.name, d.department_name, d.location\nFROM employees e\nLEFT JOIN departments d ON e.dept_id = d.id;",
                    "resources": [
                        {"title": "Visual SQL Joins Guide", "url": "https://blog.codinghorror.com/a-visual-explanation-of-sql-joins/"},
                        {"title": "W3Schools SQL Joins", "url": "https://www.w3schools.com/sql/sql_join.asp"}
                    ],
                    "aliases": ["SQL Joins"]
                },
                {
                    "name": "UNION vs UNION ALL",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "1 hr",
                    "summary": "Combine results from multiple SELECT queries vertically. Understand performance implications: UNION deduplicates (implicit sort) vs UNION ALL preserves all rows.",
                    "skills": ["Set theory", "UNION ALL efficiency", "Data type alignment"],
                    "interview_question": "Why is UNION ALL significantly faster than UNION on large datasets?",
                    "code_snippet": "-- Fast: No sort or deduplication step\nSELECT customer_id, order_date FROM online_orders\nUNION ALL\nSELECT customer_id, order_date FROM retail_orders;",
                    "resources": [
                        {"title": "W3Schools SQL UNION", "url": "https://www.w3schools.com/sql/sql_union.asp"}
                    ]
                },
                {
                    "name": "Constraints (PRIMARY KEY, FOREIGN KEY, UNIQUE)",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "2 hrs",
                    "summary": "Enforce entity integrity and relational structure via PRIMARY KEY, FOREIGN KEY (CASCADE/RESTRICT), UNIQUE, NOT NULL, and CHECK constraints.",
                    "skills": ["Data integrity", "Referential integrity", "Foreign key cascades"],
                    "interview_question": "What is the difference between a PRIMARY KEY and a UNIQUE constraint?",
                    "code_snippet": "CREATE TABLE orders (\n    order_id SERIAL PRIMARY KEY,\n    user_id INT REFERENCES users(id) ON DELETE CASCADE,\n    amount NUMERIC(10,2) CHECK (amount > 0)\n);",
                    "resources": [
                        {"title": "PostgreSQL Constraints Docs", "url": "https://www.postgresql.org/docs/current/ddl-constraints.html"}
                    ]
                },
                {
                    "name": "SQL Practice (Basic Queries)",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "4 hrs",
                    "summary": "Solve 20+ foundational LeetCode/HackerRank SQL problems focusing on filtering, grouping, aggregate calculations, and multi-table joins.",
                    "skills": ["Query problem solving", "Translating business logic to SQL", "HackerRank/LeetCode"],
                    "interview_question": "Find all employees who earn more than the average salary of their department.",
                    "code_snippet": "SELECT e.name, e.salary, e.department\nFROM employees e\nWHERE e.salary > (\n    SELECT AVG(salary) FROM employees WHERE department = e.department\n);",
                    "resources": [
                        {"title": "LeetCode SQL 50 Study Plan", "url": "https://leetcode.com/studyplan/top-sql-50/"},
                        {"title": "HackerRank SQL Challenges", "url": "https://www.hackerrank.com/domains/sql"}
                    ]
                },
                {
                    "name": "Complete SQL Tutorial from W3 School",
                    "importance": "Recommended",
                    "difficulty": "Beginner",
                    "est_time": "3 hrs",
                    "summary": "Thorough walkthrough of standard ANSI SQL documentation, interactive editors, and syntax quizzes on W3Schools.",
                    "skills": ["Standard SQL", "Syntax fluency", "Broad conceptual coverage"],
                    "interview_question": "What are the common SQL dialect differences between PostgreSQL, MySQL, and BigQuery?",
                    "code_snippet": "-- Universal ANSI SQL check\nSELECT CURRENT_DATE, CURRENT_TIMESTAMP;",
                    "resources": [
                        {"title": "W3Schools Full SQL Tutorial", "url": "https://www.w3schools.com/sql/"}
                    ],
                    "aliases": ["Complete SQL basics from W3 School"]
                }
            ],
            "Python": [
                {
                    "name": "Python Basics",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "2 hrs",
                    "summary": "Python syntax, indentation rules, execution model (bytecode and interpreter), input/output, comments, and standard PEP 8 coding conventions.",
                    "skills": ["PEP 8", "Interpreter", "Variables", "CLI script execution"],
                    "interview_question": "How does Python manage memory and what is the role of the Garbage Collector (GC)?",
                    "code_snippet": "def greet(name: str) -> str:\n    return f\"Hello, {name}! Welcome to Data Engineering.\"\n\nif __name__ == '__main__':\n    print(greet('Engineer'))",
                    "resources": [
                        {"title": "Official Python 3 Tutorial", "url": "https://docs.python.org/3/tutorial/"},
                        {"title": "roadmap.sh Python Guide", "url": "https://roadmap.sh/python"}
                    ]
                },
                {
                    "name": "Python Data Types",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "2 hrs",
                    "summary": "Primitive vs Non-primitive types: int, float, str, bool, bytes, NoneType. Mutable vs Immutable types and object identity (id()).",
                    "skills": ["Type casting", "Immutability", "Memory references"],
                    "interview_question": "Which data types in Python are mutable and which are immutable? Why is this crucial for function arguments?",
                    "code_snippet": "val_int = 42\nval_float = 3.14159\nval_bool = True\nval_str = 'Data'\nprint(type(val_int), type(val_str))",
                    "resources": [
                        {"title": "Real Python Data Types", "url": "https://realpython.com/python-data-types/"}
                    ],
                    "aliases": [" Ptyhon Data Types", "Python Data Structures"]
                },
                {
                    "name": "Operators (==, is, in)",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "1 hr",
                    "summary": "Equality (==) vs Identity (is), membership operator (in), arithmetic, bitwise, and logical short-circuit evaluation.",
                    "skills": ["Object identity", "Short-circuit logic", "Membership checking"],
                    "interview_question": "Explain the difference between '==' and 'is' in Python with a list comparison example.",
                    "code_snippet": "a = [1, 2, 3]\nb = [1, 2, 3]\nprint(a == b)  # True (equal values)\nprint(a is b)  # False (distinct memory objects)",
                    "resources": [
                        {"title": "Real Python Python Operators", "url": "https://realpython.com/python-operators-expressions/"}
                    ]
                },
                {
                    "name": "Control Flow (if, loops)",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "2 hrs",
                    "summary": "if/elif/else statements, for loops over iterables, while loops, loop controls (break, continue, pass), and for-else constructs.",
                    "skills": ["Conditional execution", "Iterators", "Loop flow control"],
                    "interview_question": "How does the 'else' block work after a 'for' loop in Python?",
                    "code_snippet": "items = [10, 20, 30, 40]\nfor item in items:\n    if item == 30:\n        print('Found 30!')\n        break\nelse:\n    print('Item not found')",
                    "resources": [
                        {"title": "W3Schools Python For Loops", "url": "https://www.w3schools.com/python/python_for_loops.asp"}
                    ]
                },
                {
                    "name": "Functions & Lambda",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "2.5 hrs",
                    "summary": "Defining functions with def, return values, positional vs keyword arguments, *args and **kwargs, docstrings, type hinting, and lambda expressions.",
                    "skills": ["*args & **kwargs", "Lambda expressions", "Type hints", "Scope (LEGB)"],
                    "interview_question": "What is the danger of using a mutable default argument like `def append_to(item, target=[])` in Python?",
                    "code_snippet": "# Safe pattern using None as default\ndef process_batch(records, metadata=None):\n    if metadata is None:\n        metadata = {}\n    return len(records)\n\nsquare = lambda x: x ** 2",
                    "resources": [
                        {"title": "Real Python Defining Functions", "url": "https://realpython.com/defining-your-own-python-function/"}
                    ]
                },
                {
                    "name": "String Operations (split, slicing, upper)",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "2 hrs",
                    "summary": "String slicing [start:stop:step], f-strings formatting, methods (split, join, strip, replace, upper, lower), and regex basics (re module).",
                    "skills": ["Text parsing", "String slicing", "f-string formatting", "CSV/log parsing"],
                    "interview_question": "How do you parse a raw timestamp string '2026-05-14 10:30:00' into a datetime object?",
                    "code_snippet": "raw_log = '2026-01-01|ERROR|Database connection timeout'\nparts = raw_log.strip().split('|')\ntimestamp, level, message = parts\nprint(f'[{level}] on {timestamp}: {message}')",
                    "resources": [
                        {"title": "Real Python String Formatting", "url": "https://realpython.com/python-string-formatting/"}
                    ]
                },
                {
                    "name": "List Operations (append, pop, indexing)",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "2 hrs",
                    "summary": "Lists in Python: dynamic arrays, indexing, slicing, append, extend, insert, pop, remove, sort, and list comprehensions.",
                    "skills": ["Dynamic arrays", "List comprehensions", "O(1) vs O(N) list operations"],
                    "interview_question": "What is the time complexity of popping from the end of a list vs popping from index 0?",
                    "code_snippet": "nums = [1, 2, 3, 4, 5]\nsquares = [x**2 for x in nums if x % 2 == 0]  # [4, 16]\nlast_item = nums.pop()  # O(1)\nfirst_item = nums.pop(0)  # O(N)",
                    "resources": [
                        {"title": "Real Python Lists and Tuples", "url": "https://realpython.com/python-lists-tuples/"}
                    ]
                },
                {
                    "name": "Dictionary Operations",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "2.5 hrs",
                    "summary": "Hash maps in Python: keys(), values(), items(), get() with default, setdefault(), update(), dictionary comprehensions, and defaultdict/Counter.",
                    "skills": ["Hash maps", "Key lookups O(1)", "Nested JSON handling", "collections module"],
                    "interview_question": "How are Python dictionaries implemented under the hood and why are lookups O(1) average time?",
                    "code_snippet": "from collections import defaultdict\n\nword_counts = defaultdict(int)\nfor word in ['spark', 'kafka', 'spark', 'sql']:\n    word_counts[word] += 1\nprint(dict(word_counts))",
                    "resources": [
                        {"title": "Real Python Dictionaries", "url": "https://realpython.com/python-dicts/"}
                    ]
                },
                {
                    "name": "Sets in Python",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "1.5 hrs",
                    "summary": "Set data structure, uniqueness, mathematical set operations (union, intersection, difference, symmetric difference), and O(1) membership testing.",
                    "skills": ["Deduplication", "Set operations", "Fast membership testing"],
                    "interview_question": "Why is 'x in my_set' much faster than 'x in my_list' for 1,000,000 items?",
                    "code_snippet": "stream_a = {'id_1', 'id_2', 'id_3'}\nstream_b = {'id_2', 'id_3', 'id_4'}\n\nnew_ids = stream_b - stream_a  # {'id_4'}\ncommon_ids = stream_a & stream_b  # {'id_2', 'id_3'}",
                    "resources": [
                        {"title": "Real Python Sets", "url": "https://realpython.com/python-sets/"}
                    ]
                },
                {
                    "name": "File Handling (read/write modes)",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "2 hrs",
                    "summary": "Reading and writing files using context managers ('with open'), text vs binary modes ('r', 'w', 'a', 'rb'), working with CSV, JSON, and os/pathlib.",
                    "skills": ["Context managers", "Streaming large files line-by-line", "JSON/CSV serialization"],
                    "interview_question": "How do you read a 10 GB file in Python on a machine with only 4 GB RAM without crashing?",
                    "code_snippet": "# Process line-by-line to avoid memory overflow\nwith open('huge_dataset.csv', 'r', encoding='utf-8') as f:\n    for line in f:\n        process_record(line)",
                    "resources": [
                        {"title": "Real Python Reading and Writing Files", "url": "https://realpython.com/read-write-files-python/"}
                    ]
                },
                {
                    "name": "Pandas Introduction",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "4 hrs",
                    "summary": "Series and DataFrames, reading CSV/Excel/Parquet/SQL, exploratory data analysis (.head(), .info(), .describe()), filtering, grouping, and aggregations.",
                    "skills": ["DataFrames", "Data cleaning", "GroupBy & Agg", "Missing value imputation"],
                    "interview_question": "What is the memory limitation of Pandas and how does it compare to Apache Spark DataFrames?",
                    "code_snippet": "import pandas as pd\n\ndf = pd.read_csv('sales.csv')\nsummary = df.groupby('region').agg({\n    'revenue': 'sum',\n    'order_id': 'count'\n}).reset_index()\nprint(summary)",
                    "resources": [
                        {"title": "Pandas Official 10-Minute Guide", "url": "https://pandas.pydata.org/docs/user_guide/10min.html"}
                    ],
                    "aliases": ["Pandas & Numpy", "Pandas Transformations"]
                },
                {
                    "name": "Numpy Basics",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "2 hrs",
                    "summary": "NumPy n-dimensional arrays (ndarray), vectorized operations, broadcasting, array slicing, mathematical operations, and memory efficiency over Python lists.",
                    "skills": ["Vectorization", "Broadcasting", "ndarray memory buffers"],
                    "interview_question": "Why are NumPy vectorized operations dramatically faster than standard Python for-loops?",
                    "code_snippet": "import numpy as np\n\narr = np.array([10, 20, 30, 40])\n# Vectorized calculation executed in optimized C\nscaled = arr * 1.5 + 10\nprint(scaled)",
                    "resources": [
                        {"title": "NumPy Absolute Basics Guide", "url": "https://numpy.org/doc/stable/user/absolute_beginners.html"}
                    ]
                },
                {
                    "name": "Python Coding Practice",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "4 hrs",
                    "summary": "Real-world coding exercises for data engineering: reading unstructured logs, parsing JSON, transforming dictionaries, and building small ETL scripts.",
                    "skills": ["ETL scripts", "Data cleaning", "Error handling (try/except)"],
                    "interview_question": "Write a Python script that reads an API response and extracts all failed transactions.",
                    "code_snippet": "import json\n\ndef parse_transactions(payload: str) -> list:\n    data = json.loads(payload)\n    return [tx for tx in data.get('transactions', []) if tx.get('status') == 'FAILED']",
                    "resources": [
                        {"title": "Real Python Python Projects", "url": "https://realpython.com/intermediate-python-project-ideas/"}
                    ]
                },
                {
                    "name": "Complete Python Tutorial from W3 School",
                    "importance": "Recommended",
                    "difficulty": "Beginner",
                    "est_time": "3 hrs",
                    "summary": "Review core syntax, built-in functions, modules, pip packages, virtual environments, and standard library references.",
                    "skills": ["Python standard library", "Virtual environments", "Pip packaging"],
                    "interview_question": "Why should you always use a virtual environment (venv) for Python projects?",
                    "code_snippet": "# Terminal commands:\n# python -m venv venv\n# source venv/bin/activate  (Linux/Mac)\n# .\\venv\\Scripts\\activate  (Windows)",
                    "resources": [
                        {"title": "W3Schools Full Python Tutorial", "url": "https://www.w3schools.com/python/"}
                    ],
                    "aliases": ["Complete Python basics from W3 School"]
                }
            ]
        }
    },

    "Phase 2: SQL Mastery and Databases": {
        "icon": "🗄️",
        "description": "Deep dive into production database engines, advanced analytical SQL, indexing strategies, and query performance tuning.",
        "est_time": "3-4 Weeks",
        "color": "#6366F1",
        "categories": {
            "Databases": [
                {
                    "name": "PostgreSQL Basics",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "3 hrs",
                    "summary": "Architecture of PostgreSQL (MVCC, WAL, processes, buffer cache), schemas, tables, psql CLI, and JSONB document capabilities.",
                    "skills": ["PostgreSQL administration", "psql CLI", "JSONB data type", "MVCC"],
                    "interview_question": "How does Multi-Version Concurrency Control (MVCC) in PostgreSQL prevent read and write locks from blocking each other?",
                    "code_snippet": "-- Query nested JSONB directly in PostgreSQL\nSELECT \n    id, payload->>'customer_name' AS customer,\n    (payload->'cart'->0->>'price')::numeric AS first_item_price\nFROM orders_json;",
                    "resources": [
                        {"title": "PostgreSQL Official Documentation", "url": "https://www.postgresql.org/docs/"},
                        {"title": "PostgreSQL Tutorial", "url": "https://www.postgresqltutorial.com/"}
                    ]
                },
                {
                    "name": "MySQL Basics",
                    "importance": "Recommended",
                    "difficulty": "Beginner",
                    "est_time": "2 hrs",
                    "summary": "MySQL storage engines (InnoDB vs MyISAM), replication (binary logs, primary-replica), configuration, and differences from PostgreSQL.",
                    "skills": ["InnoDB engine", "Replication basics", "MySQL dialect"],
                    "interview_question": "Why is InnoDB the standard engine over MyISAM for transactions?",
                    "code_snippet": "SHOW VARIABLES LIKE '%innodb_buffer_pool_size%';\nSHOW PROCESSLIST;",
                    "resources": [
                        {"title": "MySQL Tutorial", "url": "https://www.mysqltutorial.org/"}
                    ]
                },
                {
                    "name": "SQL Server (T-SQL)",
                    "importance": "Recommended",
                    "difficulty": "Intermediate",
                    "est_time": "2 hrs",
                    "summary": "Microsoft SQL Server features, Transact-SQL (T-SQL) syntax, CROSS APPLY / OUTER APPLY, temporary tables (#tables), and table variables.",
                    "skills": ["T-SQL syntax", "CROSS APPLY", "Temp tables (#)", "SSMS"],
                    "interview_question": "What is the difference between CROSS APPLY and INNER JOIN in SQL Server?",
                    "code_snippet": "-- CROSS APPLY with a table-valued function\nSELECT c.CustomerID, o.OrderID, o.OrderDate\nFROM Customers c\nCROSS APPLY (\n    SELECT TOP 3 * FROM Orders WHERE CustomerID = c.CustomerID ORDER BY OrderDate DESC\n) o;",
                    "resources": [
                        {"title": "Microsoft Learn T-SQL Guide", "url": "https://learn.microsoft.com/en-us/sql/t-sql/language-reference"}
                    ]
                },
                {
                    "name": "Cloud Databases",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "3 hrs",
                    "summary": "Managed relational cloud databases: AWS RDS (PostgreSQL/MySQL), AWS Aurora (Distributed storage, read replicas), Azure SQL, and GCP Cloud SQL.",
                    "skills": ["Managed DBaaS", "Read replicas", "Automated backups", "Multi-AZ high availability"],
                    "interview_question": "How does AWS Aurora decouple compute and storage compared to standard Amazon RDS?",
                    "code_snippet": "# Cloud DB concepts:\n# - Automated snapshot lifecycle\n# - Auto-scaling storage\n# - Multi-AZ failover (<30 seconds)",
                    "resources": [
                        {"title": "AWS RDS Documentation", "url": "https://aws.amazon.com/rds/"},
                        {"title": "AWS Aurora Deep Dive", "url": "https://aws.amazon.com/rds/aurora/"}
                    ]
                }
            ],
            "Advanced SQL": [
                {
                    "name": "Window Functions (RANK, DENSE_RANK, ROW_NUMBER)",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "4 hrs",
                    "summary": "Analytical windowing: OVER(PARTITION BY ... ORDER BY ...), ROW_NUMBER, RANK, DENSE_RANK, LEAD, LAG, FIRST_VALUE, and running totals (SUM() OVER).",
                    "skills": ["Analytical functions", "Partitioning", "Running totals", "Deduplication via windowing"],
                    "interview_question": "What is the difference between ROW_NUMBER(), RANK(), and DENSE_RANK() when there are ties in values?",
                    "code_snippet": "-- Find top 2 highest earners in each department\nWITH RankedSalaries AS (\n    SELECT \n        emp_id, name, department, salary,\n        DENSE_RANK() OVER(PARTITION BY department ORDER BY salary DESC) AS rank_num\n    FROM employees\n)\nSELECT * FROM RankedSalaries WHERE rank_num <= 2;",
                    "resources": [
                        {"title": "PostgreSQL Window Functions Guide", "url": "https://www.postgresql.org/docs/current/tutorial-window.html"},
                        {"title": "Mode Analytics Window Functions", "url": "https://mode.com/sql-tutorial/sql-window-functions/"}
                    ],
                    "aliases": ["Window Functions"]
                },
                {
                    "name": "CTE (WITH Clause)",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "2 hrs",
                    "summary": "Common Table Expressions for readability and modularity. Non-recursive CTEs and Recursive CTEs for hierarchy/graph traversal.",
                    "skills": ["CTE modularization", "WITH RECURSIVE", "DAG traversal in SQL"],
                    "interview_question": "How does a RECURSIVE CTE work to traverse an organizational hierarchy tree?",
                    "code_snippet": "WITH RECURSIVE OrgTree AS (\n    -- Anchor member: CEO\n    SELECT emp_id, name, manager_id, 1 AS level\n    FROM employees WHERE manager_id IS NULL\n    UNION ALL\n    -- Recursive member: Direct reports\n    SELECT e.emp_id, e.name, e.manager_id, ot.level + 1\n    FROM employees e\n    JOIN OrgTree ot ON e.manager_id = ot.emp_id\n)\nSELECT * FROM OrgTree;",
                    "resources": [
                        {"title": "PostgreSQL CTEs Documentation", "url": "https://www.postgresql.org/docs/current/queries-with.html"}
                    ]
                },
                {
                    "name": "Subqueries",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "2 hrs",
                    "summary": "Scalar subqueries, row subqueries, table subqueries, correlated subqueries, EXISTS vs IN, and subquery optimization.",
                    "skills": ["Correlated subqueries", "EXISTS operator", "Subquery unnesting"],
                    "interview_question": "Why is 'EXISTS' often preferred over 'IN' when evaluating large subqueries containing NULLs?",
                    "code_snippet": "SELECT c.customer_name\nFROM customers c\nWHERE EXISTS (\n    SELECT 1 FROM orders o \n    WHERE o.customer_id = c.id AND o.status = 'COMPLETED'\n);",
                    "resources": [
                        {"title": "W3Schools SQL Subqueries", "url": "https://www.w3schools.com/sql/sql_subqueries.asp"}
                    ]
                },
                {
                    "name": "Indexes and Performance",
                    "importance": "Must Learn",
                    "difficulty": "Advanced",
                    "est_time": "3 hrs",
                    "summary": "Index structures: B-Tree, Hash, GIN, GiST, BRIN. Composite indexes (leftmost prefix rule), covering indexes, and index maintenance overhead.",
                    "skills": ["B-Tree indexing", "Covering indexes", "BRIN for large time-series", "Index trade-offs"],
                    "interview_question": "What is the leftmost prefix rule in composite B-Tree indexes?",
                    "code_snippet": "-- Create composite B-Tree index\nCREATE INDEX idx_orders_user_date ON orders(user_id, order_date DESC);\n\n-- BRIN index: Extremely lightweight for sorted append-only time series\nCREATE INDEX idx_logs_created_brin ON logs USING BRIN (created_at);",
                    "resources": [
                        {"title": "Use The Index, Luke (Guide to DB Performance)", "url": "https://use-the-index-luke.com/"}
                    ]
                },
                {
                    "name": "Query Optimization",
                    "importance": "Must Learn",
                    "difficulty": "Advanced",
                    "est_time": "4 hrs",
                    "summary": "Reading EXPLAIN and EXPLAIN ANALYZE execution plans. Identifying Seq Scans, Index Scans, Bitmap Index Scans, Hash Joins, Nested Loops, and Sort operations.",
                    "skills": ["EXPLAIN ANALYZE", "Execution plans", "SARGability", "Join algorithms"],
                    "interview_question": "What does 'SARGable' query mean, and why does wrapping an indexed column in a function (e.g. YEAR(date_col)) ruin performance?",
                    "code_snippet": "-- Analyze execution plan and timing in PostgreSQL\nEXPLAIN (ANALYZE, BUFFERS)\nSELECT customer_id, SUM(total)\nFROM orders\nWHERE order_date >= '2026-01-01'\nGROUP BY customer_id;",
                    "resources": [
                        {"title": "Depesz Postgres EXPLAIN Visualizer", "url": "https://explain.depesz.com/"},
                        {"title": "PostgreSQL EXPLAIN Docs", "url": "https://www.postgresql.org/docs/current/using-explain.html"}
                    ]
                },
                {
                    "name": "Views and Materialized Views",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "2 hrs",
                    "summary": "Standard Views (virtual stored queries) vs Materialized Views (persisted query result cache on disk), REFRESH MATERIALIZED VIEW, and incremental refresh.",
                    "skills": ["Data abstraction", "Materialized views", "Automated refresh pipelines"],
                    "interview_question": "When should you use a Materialized View over a regular View in a data pipeline?",
                    "code_snippet": "CREATE MATERIALIZED VIEW daily_revenue_mv AS\nSELECT date_trunc('day', order_date) AS day, SUM(total) AS daily_rev\nFROM orders\nGROUP BY 1;\n\n-- Refresh without blocking reads\nREFRESH MATERIALIZED VIEW CONCURRENTLY daily_revenue_mv;",
                    "resources": [
                        {"title": "PostgreSQL Materialized Views", "url": "https://www.postgresqltutorial.com/postgresql-views/postgresql-materialized-views/"}
                    ]
                },
                {
                    "name": "Stored Procedures",
                    "importance": "Recommended",
                    "difficulty": "Intermediate",
                    "est_time": "2 hrs",
                    "summary": "Writing user-defined functions (UDFs) and stored procedures in PL/pgSQL or T-SQL, procedural logic, error handling, and transaction management.",
                    "skills": ["PL/pgSQL", "Procedural database code", "UDFs"],
                    "interview_question": "Why are modern data teams moving away from stored procedures toward tools like dbt and Airflow?",
                    "code_snippet": "CREATE OR REPLACE PROCEDURE update_inventory(p_item_id INT, p_qty INT)\nLANGUAGE plpgsql\nAS $$\nBEGIN\n    UPDATE inventory SET stock = stock - p_qty WHERE item_id = p_item_id;\n    COMMIT;\nEND;\n$$;",
                    "resources": [
                        {"title": "PL/pgSQL Documentation", "url": "https://www.postgresql.org/docs/current/plpgsql.html"}
                    ]
                },
                {
                    "name": "Transactions (ACID Properties)",
                    "importance": "Must Learn",
                    "difficulty": "Advanced",
                    "est_time": "3 hrs",
                    "summary": "ACID deep dive: Atomicity, Consistency, Isolation, Durability. Isolation levels (Read Uncommitted, Read Committed, Repeatable Read, Serializable) and concurrency anomalies (Dirty Read, Non-repeatable Read, Phantom Read).",
                    "skills": ["ACID theory", "Transaction isolation levels", "BEGIN/COMMIT/ROLLBACK", "Locking"],
                    "interview_question": "Explain what a Phantom Read anomaly is and which isolation level prevents it.",
                    "code_snippet": "BEGIN;\nUPDATE accounts SET balance = balance - 500 WHERE account_id = 1;\nUPDATE accounts SET balance = balance + 500 WHERE account_id = 2;\nCOMMIT; -- Or ROLLBACK on error",
                    "resources": [
                        {"title": "PostgreSQL Transaction Isolation Docs", "url": "https://www.postgresql.org/docs/current/transaction-iso.html"}
                    ],
                    "aliases": ["Transactions"]
                },
                {
                    "name": "Advanced Joins",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "2 hrs",
                    "summary": "Anti-joins (NOT EXISTS / LEFT JOIN WHERE IS NULL), Semi-joins (EXISTS / IN), Lateral Joins (CROSS JOIN LATERAL), and Cartesian products.",
                    "skills": ["Anti-join patterns", "Semi-joins", "LATERAL joins"],
                    "interview_question": "What is a LATERAL join in PostgreSQL and what capability does it provide?",
                    "code_snippet": "-- LATERAL join: Subquery references column from previous table\nSELECT u.username, recent_order.order_id, recent_order.total\nFROM users u\nCROSS JOIN LATERAL (\n    SELECT order_id, total FROM orders \n    WHERE user_id = u.id ORDER BY order_date DESC LIMIT 1\n) recent_order;",
                    "resources": [
                        {"title": "PostgreSQL LATERAL Joins Guide", "url": "https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-LATERAL"}
                    ]
                },
                {
                    "name": "Real-world SQL Problems",
                    "importance": "Must Learn",
                    "difficulty": "Advanced",
                    "est_time": "4 hrs",
                    "summary": "Complex real-world data warehousing interview challenges: Gaps and Islands problem, Retention Rate calculation, User Churn, Month-over-Month (MoM) growth.",
                    "skills": ["Gaps & Islands", "Cohort analysis", "MoM calculations", "Rolling 7-day average"],
                    "interview_question": "How do you calculate a rolling 7-day average revenue using window frame clauses?",
                    "code_snippet": "SELECT \n    sale_date,\n    revenue,\n    AVG(revenue) OVER(\n        ORDER BY sale_date \n        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW\n    ) AS rolling_7d_avg\nFROM daily_sales;",
                    "resources": [
                        {"title": "Gaps and Islands Problem Guide", "url": "https://mode.com/sql-tutorial/sql-window-functions/"}
                    ]
                },
                {
                    "name": "SQL Query Practice (Intermediate + Advanced)",
                    "importance": "Must Learn",
                    "difficulty": "Advanced",
                    "est_time": "5 hrs",
                    "summary": "30+ Hard LeetCode / StrataScratch problems covering window functions, CTEs, self joins, and tricky aggregations.",
                    "skills": ["LeetCode Hard SQL", "StrataScratch", "Interview mock queries"],
                    "interview_question": "Find the top 3 products by revenue for each category, handling ties properly.",
                    "code_snippet": "WITH CategoryProductRev AS (\n    SELECT \n        category, product_name, SUM(revenue) AS total_rev,\n        DENSE_RANK() OVER(PARTITION BY category ORDER BY SUM(revenue) DESC) AS rank_pos\n    FROM sales\n    GROUP BY category, product_name\n)\nSELECT * FROM CategoryProductRev WHERE rank_pos <= 3;",
                    "resources": [
                        {"title": "StrataScratch Data Engineering SQL Questions", "url": "https://www.stratascratch.com/"},
                        {"title": "DataLemur Free SQL Interview Questions", "url": "https://datalemur.com/"}
                    ],
                    "aliases": ["Advanced SQL Practice"]
                }
            ]
        }
    },

    "Phase 3: Data Architecture": {
        "icon": "🏗️",
        "description": "Architectural principles, storage systems, data modeling (Kimball), OLTP vs OLAP, and data lakes/lakehouses.",
        "est_time": "3 Weeks",
        "color": "#10B981",
        "categories": {
            "Core Concepts": [
                {
                    "name": "What is a Database",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "1.5 hrs",
                    "summary": "General database concepts, ACID guarantees, relational vs non-relational storage, indexing, and serving low-latency CRUD operations.",
                    "skills": ["Database fundamentals", "CRUD operations", "Transactional data"],
                    "interview_question": "What is the primary difference in workload between a relational database and a data warehouse?",
                    "code_snippet": "# RDBMS Characteristics:\n# - High concurrency (thousands of queries/sec)\n# - Small reads and writes (single row lookups)\n# - Highly normalized to avoid update anomalies",
                    "resources": [
                        {"title": "roadmap.sh Database Fundamentals", "url": "https://roadmap.sh/data-engineer"}
                    ]
                },
                {
                    "name": "What is a Data Warehouse",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "2 hrs",
                    "summary": "Centralized analytical repository. Columnar storage, massive parallel processing (MPP), schema-on-write, and historical trend analysis (Snowflake, BigQuery, Redshift).",
                    "skills": ["Data warehousing", "Columnar format", "MPP engines", "Schema-on-write"],
                    "interview_question": "Why do Data Warehouses use columnar storage instead of row-oriented storage?",
                    "code_snippet": "# Columnar advantage:\n# Queries only scan requested columns (e.g. SUM(amount)),\n# reducing I/O by 90%+ and allowing massive compression (Snappy/ZSTD).",
                    "resources": [
                        {"title": "Amazon Redshift Architecture Guide", "url": "https://aws.amazon.com/redshift/"},
                        {"title": "Snowflake Architecture Overview", "url": "https://www.snowflake.com/en/data-cloud/architecture/"}
                    ]
                },
                {
                    "name": "What is a Data Lake",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "2 hrs",
                    "summary": "Repository for structured, semi-structured, and unstructured raw data in low-cost cloud object storage (AWS S3, ADLS Gen2, GCS) with schema-on-read.",
                    "skills": ["Data lakes", "Cloud object storage (S3/ADLS)", "Schema-on-read", "Parquet/ORC"],
                    "interview_question": "What is a 'Data Swamp' and what causes a data lake to deteriorate into one?",
                    "code_snippet": "# S3 / ADLS Lake Structure:\n# s3://company-datalake/\n#   ├── raw/year=2026/month=05/events.json.gz\n#   ├── staging/customers.parquet\n#   └── analytics/agg_revenue.parquet",
                    "resources": [
                        {"title": "AWS Data Lake on S3", "url": "https://aws.amazon.com/big-data/datalakes-and-analytics/"}
                    ]
                },
                {
                    "name": "What is a Data Lakehouse",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "2.5 hrs",
                    "summary": "Modern architecture combining the cheap storage and flexibility of Data Lakes with the ACID transactions, governance, and SQL performance of Data Warehouses (Delta Lake, Apache Iceberg).",
                    "skills": ["Lakehouse architecture", "Open Table Formats", "Delta Lake", "Apache Iceberg"],
                    "interview_question": "How do open table formats like Apache Iceberg and Delta Lake enable ACID transactions on object storage like S3?",
                    "code_snippet": "# Lakehouse stack:\n# [BI & ML Tools: SQL / Python]\n#       ↓ (Query Engine: Spark / Trino / Snowflake)\n# [Table Metadata: Delta Lake / Apache Iceberg ACID log]\n#       ↓ (Storage: Parquet files on AWS S3 / Azure ADLS)",
                    "resources": [
                        {"title": "What is a Lakehouse? (Databricks)", "url": "https://www.databricks.com/glossary/data-lakehouse"},
                        {"title": "Apache Iceberg Architecture", "url": "https://iceberg.apache.org/"}
                    ]
                }
            ],
            "Comparisons": [
                {
                    "name": "Database vs Data Warehouse",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "1.5 hrs",
                    "summary": "Comparison of OLTP databases (row-based, high write concurrency, 3NF normalized) vs OLAP Data Warehouses (columnar, batch/micro-batch, denormalized, heavy aggregations).",
                    "skills": ["OLTP vs OLAP", "Workload characteristics", "Row vs Column store"],
                    "interview_question": "Can you run complex analytical queries directly on your primary production PostgreSQL database? What are the dangers?",
                    "code_snippet": "# Trade-off Matrix:\n# Feature      | RDBMS (OLTP)         | Warehouse (OLAP)\n# Storage      | Row-oriented (B-Tree)| Columnar (Parquet/FSI)\n# Schema       | Highly Normalized    | Star / Snowflake Schema\n# Operations   | Fast point writes    | Large analytical scans",
                    "resources": [
                        {"title": "AWS OLTP vs OLAP Guide", "url": "https://aws.amazon.com/compare/the-difference-between-olap-and-oltp/"}
                    ]
                },
                {
                    "name": "Data Lake vs Data Warehouse",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "1.5 hrs",
                    "summary": "Comparison of Data Lake (raw storage, all formats, schema-on-read, low cost) vs Data Warehouse (curated, structured, schema-on-write, high performance SQL).",
                    "skills": ["Schema-on-read vs Schema-on-write", "Storage cost vs Query speed"],
                    "interview_question": "Explain the difference between Schema-on-Read and Schema-on-Write.",
                    "code_snippet": "# Lake: Ingest first, define schema when querying (Schema-on-Read)\n# Warehouse: Define table schema first, transform and validate before insert (Schema-on-Write)",
                    "resources": [
                        {"title": "Google Cloud Data Lake vs Data Warehouse", "url": "https://cloud.google.com/learn/data-lake-vs-data-warehouse"}
                    ]
                },
                {
                    "name": "Lakehouse vs Warehouse",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "2 hrs",
                    "summary": "Lakehouse (open storage formats, no vendor lock-in, supports ML & AI directly) vs Cloud Data Warehouse (proprietary storage, instant serverless SQL, zero infrastructure setup).",
                    "skills": ["Open standards vs Proprietary formats", "Vendor lock-in", "Unified ML & BI"],
                    "interview_question": "Why is the Lakehouse architecture gaining rapid adoption over standalone Data Warehouses in 2026?",
                    "code_snippet": "# Open formats (Parquet + Iceberg/Delta) allow multiple engines\n# (Spark, Trino, Flink, DuckDB) to read the same data files without copying.",
                    "resources": [
                        {"title": "Lakehouse vs Warehouse (Snowflake & Databricks comparison)", "url": "https://www.databricks.com/discover/data-lakehouse"}
                    ]
                },
                {
                    "name": "OLTP vs OLAP",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "2 hrs",
                    "summary": "Online Transaction Processing (OLTP - atomic single record CRUD) vs Online Analytical Processing (OLAP - complex queries over billions of records, aggregates).",
                    "skills": ["OLTP characteristics", "OLAP characteristics", "CAP Theorem implications"],
                    "interview_question": "What is the CAP Theorem and how does it apply to distributed databases?",
                    "code_snippet": "# CAP Theorem:\n# Consistency, Availability, Partition Tolerance.\n# In a network partition (P), a distributed system must choose\n# between Consistency (CP) or Availability (AP).",
                    "resources": [
                        {"title": "CAP Theorem Simplified", "url": "https://www.ibm.com/topics/cap-theorem"}
                    ]
                }
            ],
            "Modeling": [
                {
                    "name": "Data Modeling Basics",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "3 hrs",
                    "summary": "Fundamentals of dimensional modeling (Ralph Kimball). Fact tables (additive, semi-additive, non-additive metrics) and Dimension tables (descriptive attributes).",
                    "skills": ["Dimensional modeling", "Fact tables", "Dimension tables", "Surrogate keys"],
                    "interview_question": "What is the purpose of surrogate keys in dimension tables, and why should you avoid using natural business keys as primary keys?",
                    "code_snippet": "-- Fact table with surrogate keys referencing dimensions\nCREATE TABLE fact_sales (\n    sale_key BIGSERIAL PRIMARY KEY,\n    date_key INT REFERENCES dim_date(date_key),\n    customer_key INT REFERENCES dim_customer(customer_key),\n    product_key INT REFERENCES dim_product(product_key),\n    quantity INT,\n    total_revenue NUMERIC(12, 2)\n);",
                    "resources": [
                        {"title": "Kimball Group Dimensional Modeling Techniques", "url": "https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/"}
                    ]
                },
                {
                    "name": "Star vs Snowflake Schema",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "3 hrs",
                    "summary": "Star Schema (denormalized dimensions, single join to fact table, faster OLAP queries) vs Snowflake Schema (normalized dimensions, multiple joins, saves storage, cleaner hierarchies).",
                    "skills": ["Star schema design", "Snowflake schema design", "Join cost vs storage trade-offs"],
                    "interview_question": "Why is Star Schema almost always preferred over Snowflake Schema in modern columnar data warehouses?",
                    "code_snippet": "# Star Schema: Fact -> Dim (1 join, denormalized)\n# Snowflake: Fact -> Dim_Product -> Dim_Subcategory -> Dim_Category (3 joins, normalized)\n# In modern columnar warehouses, join latency outweighs minimal storage savings.",
                    "resources": [
                        {"title": "Star Schema vs Snowflake Schema Guide", "url": "https://www.guru99.com/star-snowflake-data-warehouse.html"}
                    ]
                },
                {
                    "name": "Architecture Diagram Task",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "3 hrs",
                    "summary": "Design an end-to-end data architecture diagram: Ingestion sources (APIs, CDC) -> Raw lake storage -> Silver cleansing -> Gold dimensional models -> BI reporting.",
                    "skills": ["System design for DE", "Data flow diagrams", "Medallion architecture"],
                    "interview_question": "Walk me through how data flows in the Medallion (Bronze/Silver/Gold) architecture.",
                    "code_snippet": "# Pipeline Flow:\n# [Sources] -> [Bronze: Raw Append-Only]\n#                ↓ (Cleaning, Deduplication, Schema Enforcement)\n#              [Silver: Cleaned Enriched Tables]\n#                ↓ (Aggregations, Kimball Star Schemas)\n#              [Gold: Business Marts for Power BI / Executive Dashboards]",
                    "resources": [
                        {"title": "Medallion Architecture (Databricks)", "url": "https://www.databricks.com/glossary/medallion-architecture"}
                    ]
                }
            ]
        }
    },

    "Phase 4: Data Pipelines and Big Data": {
        "icon": "⚡",
        "description": "ETL/ELT design, batch vs streaming pipelines, Apache Spark (PySpark), distributed computing, and pipeline reliability.",
        "est_time": "4 Weeks",
        "color": "#F59E0B",
        "categories": {
            "Pipeline Basics": [
                {
                    "name": "ETL vs ELT",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "2 hrs",
                    "summary": "Extract-Transform-Load (legacy, transformation done on compute engine before loading) vs Extract-Load-Transform (modern, raw data loaded into warehouse first, then transformed with dbt/SQL).",
                    "skills": ["ETL pipeline patterns", "ELT modern data stack", "In-warehouse transformations"],
                    "interview_question": "What technological advancements caused the industry shift from ETL to ELT over the last decade?",
                    "code_snippet": "# ETL: Raw -> [Compute Server: Python/Spark transforms] -> Warehouse\n# ELT: Raw -> Warehouse Raw Storage -> [dbt executes SQL inside Warehouse] -> Warehouse Analytics",
                    "resources": [
                        {"title": "dbt: What is ELT?", "url": "https://www.getdbt.com/analytics-engineering/what-is-elt"}
                    ]
                },
                {
                    "name": "Data Pipeline Basics",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "2 hrs",
                    "summary": "Core pipeline concepts: Extractors, Transformers, Loaders, DAGs (Directed Acyclic Graphs), state tracking, checkpointing, and error alerts.",
                    "skills": ["Pipeline components", "DAG concept", "Checkpointing"],
                    "interview_question": "What is pipeline idempotency and why is it essential in production data engineering?",
                    "code_snippet": "# Idempotent pipeline principle:\n# Running the pipeline for date '2026-05-14' multiple times\n# must produce the EXACT same state without creating duplicate records.",
                    "resources": [
                        {"title": "Designing Data Pipelines", "url": "https://roadmap.sh/data-engineer"}
                    ]
                },
                {
                    "name": "Batch vs Streaming",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "2.5 hrs",
                    "summary": "Batch processing (bounded data processed in scheduled intervals) vs Stream processing (unbounded data processed continuously with low latency via Kafka/Flink).",
                    "skills": ["Bounded vs Unbounded data", "Latency SLAs", "Micro-batch vs Event-driven"],
                    "interview_question": "When is batch processing preferred over stream processing?",
                    "code_snippet": "# Trade-off:\n# Batch: Hours/Days latency, High throughput, Lower compute cost, Easier deduplication\n# Streaming: Sub-second latency, Continuous processing, Higher cost, Complex state management",
                    "resources": [
                        {"title": "Streaming vs Batch Processing Guide", "url": "https://aws.amazon.com/compare/the-difference-between-batch-processing-and-stream-processing/"}
                    ]
                },
                {
                    "name": "Data Orchestration",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "3 hrs",
                    "summary": "Orchestrating complex dependency graphs: Scheduling, Retries, Task dependencies, SLA monitoring, and alerting using orchestrators like Apache Airflow.",
                    "skills": ["DAG scheduling", "Task dependencies", "Retries & Backoff", "SLAs"],
                    "interview_question": "Why shouldn't you schedule production data pipelines using basic Linux Cron?",
                    "code_snippet": "# Cron limitations vs Airflow:\n# - Cron has no dependency awareness (Job B starts before Job A finishes)\n# - Cron has no automatic retries or exponential backoff\n# - Cron has no visual execution graph or unified log monitoring",
                    "resources": [
                        {"title": "Apache Airflow Core Concepts", "url": "https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/index.html"}
                    ]
                },
                {
                    "name": "Data Quality Checks",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "2.5 hrs",
                    "summary": "Data assertions and validation: Null checks, Uniqueness constraints, Range validation, Schema drift detection, Great Expectations, and Soda Core.",
                    "skills": ["Data assertions", "Schema drift validation", "Great Expectations", "dbt tests"],
                    "interview_question": "How do you implement circuit breakers in a data pipeline when source data quality fails validation?",
                    "code_snippet": "# Great Expectations expectation example\n# validator.expect_column_values_to_not_be_null('customer_id')\n# validator.expect_column_values_to_be_between('age', min_value=18, max_value=120)",
                    "resources": [
                        {"title": "Great Expectations Official Docs", "url": "https://docs.greatexpectations.io/"}
                    ]
                },
                {
                    "name": "Pipeline Design",
                    "importance": "Must Learn",
                    "difficulty": "Advanced",
                    "est_time": "3.5 hrs",
                    "summary": "End-to-end design patterns: Lambda Architecture vs Kappa Architecture, Change Data Capture (CDC), dead-letter queues (DLQ), and reprocessing strategy.",
                    "skills": ["Lambda vs Kappa architecture", "CDC (Debezium)", "Dead Letter Queues", "Backfill strategy"],
                    "interview_question": "What is the difference between Lambda Architecture and Kappa Architecture?",
                    "code_snippet": "# Lambda: Dual path -> Batch Layer (Hadoop/Spark) + Speed Layer (Storm/Flink) -> Serving Layer\n# Kappa: Single path -> Everything is a stream processed by Kafka + Flink",
                    "resources": [
                        {"title": "Lambda vs Kappa Architecture", "url": "https://www.kai-waehner.de/blog/2021/09/23/comparison-data-architectures-lambda-kappa-kappa-plus/"}
                    ]
                }
            ],
            "Spark": [
                {
                    "name": "Introduction to Spark",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "3 hrs",
                    "summary": "Apache Spark architecture: Driver node, Cluster Manager (YARN/K8s/Standalone), Executor nodes, In-memory computing, and Spark vs MapReduce.",
                    "skills": ["Spark Architecture", "Driver & Executors", "In-memory processing", "Lazy evaluation"],
                    "interview_question": "What are the roles of the Driver program and Executors in an Apache Spark cluster?",
                    "code_snippet": "from pyspark.sql import SparkSession\n\nspark = SparkSession.builder \\\n    .appName(\"DE_Roadmap_Spark\") \\\n    .config(\"spark.executor.memory\", \"4g\") \\\n    .getOrCreate()\n\nprint(f\"Spark Version: {spark.version}\")",
                    "resources": [
                        {"title": "Apache Spark Official Overview", "url": "https://spark.apache.org/docs/latest/"},
                        {"title": "Spark by Examples PySpark Tutorial", "url": "https://sparkbyexamples.com/pyspark-tutorial/"}
                    ]
                },
                {
                    "name": "Spark DataFrames",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "3.5 hrs",
                    "summary": "PySpark DataFrames API: reading CSV/JSON/Parquet, printSchema(), select(), filter(), withColumn(), drop(), and schema definition with StructType/StructField.",
                    "skills": ["PySpark DataFrames", "StructType schemas", "Column transformations"],
                    "interview_question": "Why is it best practice to explicitly define a StructType schema instead of letting Spark infer it (inferSchema=True)?",
                    "code_snippet": "from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType\nfrom pyspark.sql.functions import col\n\nschema = StructType([\n    StructField(\"user_id\", IntegerType(), False),\n    StructField(\"action\", StringType(), True),\n    StructField(\"score\", DoubleType(), True)\n])\n\ndf = spark.read.schema(schema).parquet(\"s3://my-bucket/events/\")\nfiltered = df.filter(col(\"score\") > 50.0)",
                    "resources": [
                        {"title": "PySpark DataFrame API Reference", "url": "https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html"}
                    ]
                },
                {
                    "name": "Transformations vs Actions",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "2.5 hrs",
                    "summary": "Lazy evaluation in Spark: Transformations (Narrow: map/filter vs Wide: groupBy/join - causes shuffles) vs Actions (count, collect, write, show - triggers execution).",
                    "skills": ["Lazy evaluation", "Narrow vs Wide transformations", "Shuffling", "Catalyst Optimizer"],
                    "interview_question": "What is the difference between a narrow transformation and a wide transformation in Spark?",
                    "code_snippet": "# Narrow (no shuffle across nodes): map, filter, union\n# Wide (data shuffled across network): groupBy, distinct, join\n\n# Action (Triggers actual cluster execution):\ndf.write.mode(\"overwrite\").parquet(\"output/path/\")",
                    "resources": [
                        {"title": "Spark Transformations and Actions Explained", "url": "https://data-flair.training/blogs/spark-rdd-operations-transformations-actions/"}
                    ]
                },
                {
                    "name": "Joins in Spark",
                    "importance": "Must Learn",
                    "difficulty": "Advanced",
                    "est_time": "3.5 hrs",
                    "summary": "Spark Join strategies: Sort-Merge Join (default for large datasets), Broadcast Hash Join (broadcasting small dimension table to eliminate shuffle), and Shuffle Hash Join.",
                    "skills": ["Broadcast Hash Join", "Sort-Merge Join", "Broadcast threshold", "broadcast() function"],
                    "interview_question": "How does a Broadcast Join work in Spark and when should you use it to eliminate shuffle overhead?",
                    "code_snippet": "from pyspark.sql.functions import broadcast\n\n# Broadcast small dimension table to every executor\njoined_df = large_facts_df.join(\n    broadcast(small_dim_df),\n    large_facts_df.dim_id == small_dim_df.id,\n    \"left\"\n)",
                    "resources": [
                        {"title": "Spark Join Strategies Deep Dive", "url": "https://towardsdatascience.com/the-art-of-joining-in-spark-3-0-2831f28c298c"}
                    ]
                },
                {
                    "name": "Aggregations",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "3 hrs",
                    "summary": "PySpark aggregations using groupBy(), agg(), built-in functions (sum, avg, count, max), and PySpark Window functions (Window.partitionBy().orderBy()).",
                    "skills": ["PySpark groupBy", "agg() functions", "PySpark Window specifications"],
                    "interview_question": "How do you calculate a moving average in PySpark using the Window API?",
                    "code_snippet": "from pyspark.sql.window import Window\nfrom pyspark.sql.functions import avg, col\n\nwindow_spec = Window.partitionBy(\"category\").orderBy(\"date\").rowsBetween(-6, 0)\ndf_with_ma = df.withColumn(\"rolling_7d\", avg(col(\"sales\")).over(window_spec))",
                    "resources": [
                        {"title": "PySpark Window Functions Guide", "url": "https://sparkbyexamples.com/pyspark/pyspark-window-functions/"}
                    ]
                },
                {
                    "name": "Performance Optimization",
                    "importance": "Must Learn",
                    "difficulty": "Advanced",
                    "est_time": "4.5 hrs",
                    "summary": "Tuning Spark jobs: AQE (Adaptive Query Execution), Data Skew mitigation (salting keys), coalesce vs repartition, cache() vs persist(StorageLevel), and Spark UI analysis.",
                    "skills": ["Adaptive Query Execution (AQE)", "Handling data skew", "Salting technique", "Spark UI stages/tasks"],
                    "interview_question": "What causes data skew in Spark joins, and how does key salting fix it?",
                    "code_snippet": "# Enable Adaptive Query Execution (Spark 3+)\nspark.conf.set(\"spark.sql.adaptive.enabled\", \"true\")\nspark.conf.set(\"spark.sql.adaptive.skewJoin.enabled\", \"true\")\n\n# Coalesce (reduces partitions without shuffle) vs Repartition (full shuffle)\noptimized_df = df.coalesce(10)",
                    "resources": [
                        {"title": "Apache Spark Tuning Guide", "url": "https://spark.apache.org/docs/latest/tuning.html"}
                    ]
                },
                {
                    "name": "ETL Pipeline Project",
                    "importance": "Must Learn",
                    "difficulty": "Advanced",
                    "est_time": "6 hrs",
                    "summary": "Build a complete PySpark pipeline: Read multi-gigabyte raw JSON from S3/local, parse, clean, handle missing data, join with lookup dimension, and write partitioned Parquet.",
                    "skills": ["End-to-end PySpark project", "Partitioned writing", "Snappy compression", "Unit testing Spark"],
                    "interview_question": "How do you structure partition columns when writing Parquet datasets to S3 to balance read pruning and small file problems?",
                    "code_snippet": "# Partition by Year and Month for efficient partition pruning\nfinal_df.write \\\n    .partitionBy(\"year\", \"month\") \\\n    .mode(\"overwrite\") \\\n    .parquet(\"s3://lakehouse/curated/transactions/\")",
                    "resources": [
                        {"title": "Building Production Data Pipelines with PySpark", "url": "https://sparkbyexamples.com/"}
                    ]
                }
            ]
        }
    },

    "Phase 5: Databricks, Snowflake and Cloud": {
        "icon": "☁️",
        "description": "Modern cloud analytics platforms: Databricks, Snowflake, Delta Lake, and public cloud infrastructure (AWS/Azure/GCP).",
        "est_time": "4 Weeks",
        "color": "#06B6D4",
        "categories": {
            "Databricks": [
                {
                    "name": "Workspace and Notebooks",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "2 hrs",
                    "summary": "Navigating the Databricks Lakehouse Platform: Workspaces, interactive Notebooks (Python, SQL, Scala, R), Git folders integration, and Databricks Community Edition.",
                    "skills": ["Databricks UI", "Multi-language notebooks", "Repos integration", "Community edition"],
                    "interview_question": "What is Databricks Unity Catalog and how does it provide centralized governance across clusters?",
                    "code_snippet": "# %sql cell magic in Databricks\n# %python\n# display(spark.read.table(\"samples.nyctaxi.trips\"))",
                    "resources": [
                        {"title": "Databricks Documentation", "url": "https://docs.databricks.com/"},
                        {"title": "Databricks Free Community Edition", "url": "https://community.cloud.databricks.com/"}
                    ]
                },
                {
                    "name": "Cluster Management",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "2.5 hrs",
                    "summary": "Cluster modes (Single Node, Standard, Multi-node), Driver and Worker sizing, Auto-scaling, Auto-termination (cost saving), Spot instances, and Databricks Runtime (DBR).",
                    "skills": ["Cluster sizing", "Spot vs On-Demand", "Auto-termination", "DBR selection"],
                    "interview_question": "How do you configure Databricks clusters to balance cost optimization with performance SLAs?",
                    "code_snippet": "# Cost optimization best practice:\n# 1. Enable Auto-Termination (set to 15-20 mins)\n# 2. Use Single Node clusters for development/testing\n# 3. Use Spot instances for Worker nodes in fault-tolerant batch workloads",
                    "resources": [
                        {"title": "Databricks Compute Configuration", "url": "https://docs.databricks.com/compute/index.html"}
                    ]
                },
                {
                    "name": "DBFS",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "1.5 hrs",
                    "summary": "Databricks File System (DBFS): Virtual filesystem abstraction over cloud storage (AWS S3, ADLS), dbutils CLI (`dbutils.fs.ls()`, `dbutils.fs.cp()`), and Unity Catalog Volumes.",
                    "skills": ["DBFS abstraction", "dbutils utilities", "Mount points vs Volumes"],
                    "interview_question": "Why are Unity Catalog Volumes replacing legacy DBFS mount points in Databricks?",
                    "code_snippet": "# Inspect files using dbutils in Databricks\nfiles = dbutils.fs.ls(\"/databricks-datasets/\")\nfor f in files[:5]:\n    print(f.path, f.size)",
                    "resources": [
                        {"title": "Databricks DBFS Guide", "url": "https://docs.databricks.com/dbfs/index.html"}
                    ]
                },
                {
                    "name": "Delta Lake",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "3.5 hrs",
                    "summary": "Open-source storage framework: ACID transactions on Parquet files, transaction log (_delta_log JSON), schema enforcement, and schema evolution (mergeSchema).",
                    "skills": ["Delta Lake architecture", "_delta_log transaction log", "Schema enforcement & evolution"],
                    "interview_question": "How does the Delta transaction log (_delta_log) achieve ACID consistency when multiple jobs write concurrently?",
                    "code_snippet": "# Write DataFrame as a Delta table\ndf.write.format(\"delta\").mode(\"append\").save(\"/mnt/delta/events/\")\n\n# Read Delta table\ndelta_df = spark.read.format(\"delta\").load(\"/mnt/delta/events/\")",
                    "resources": [
                        {"title": "Delta Lake Official Documentation", "url": "https://docs.delta.io/latest/index.html"}
                    ]
                },
                {
                    "name": "Delta Table Operations",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "3 hrs",
                    "summary": "Delta SQL operations: MERGE INTO (Upserts for CDC data pipelines), UPDATE, DELETE, and Time Travel (RESTORE TABLE or `VERSION AS OF` / `TIMESTAMP AS OF`).",
                    "skills": ["MERGE INTO (Upserts)", "Time Travel querying", "RESTORE table", "Audit history"],
                    "interview_question": "Write a Delta Lake MERGE statement to perform an Upsert (Insert if new, Update if matching).",
                    "code_snippet": "%sql\nMERGE INTO target_customers AS target\nUSING source_updates AS source\nON target.customer_id = source.customer_id\nWHEN MATCHED THEN\n  UPDATE SET target.email = source.email, target.updated_at = source.updated_at\nWHEN NOT MATCHED THEN\n  INSERT (customer_id, email, updated_at) VALUES (source.customer_id, source.email, source.updated_at);",
                    "resources": [
                        {"title": "Delta Lake MERGE Documentation", "url": "https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge"}
                    ]
                },
                {
                    "name": "Optimization and Z-Ordering",
                    "importance": "Must Learn",
                    "difficulty": "Advanced",
                    "est_time": "3 hrs",
                    "summary": "Delta performance tuning: OPTIMIZE command (compaction of small files), Z-ORDER BY (multidimensional data clustering to skip irrelevant files), and VACUUM (cleaning up old snapshots).",
                    "skills": ["OPTIMIZE compaction", "Z-Ordering algorithm", "Data skipping", "VACUUM retention"],
                    "interview_question": "What is the small file problem in data lakes and how does Delta OPTIMIZE solve it?",
                    "code_snippet": "%sql\n-- Compact files and cluster by frequently filtered columns\nOPTIMIZE target_customers\nZORDER BY (customer_id, country);\n\n-- Remove old file versions older than 168 hours (7 days)\nVACUUM target_customers RETAIN 168 HOURS;",
                    "resources": [
                        {"title": "Databricks Z-Ordering Guide", "url": "https://docs.databricks.com/delta/optimizations/file-mgmt.html"}
                    ]
                },
                {
                    "name": "Medallion Architecture",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "3 hrs",
                    "summary": "Multi-hop data design: Bronze (raw, immutable ingest), Silver (cleansed, deduplicated, enriched tables), and Gold (business aggregates, star schema data marts).",
                    "skills": ["Bronze/Silver/Gold pipeline", "Data quality promotion", "Streaming ingestion (Auto Loader)"],
                    "interview_question": "Why should users and BI analysts NEVER query the Bronze layer directly?",
                    "code_snippet": "# Medallion flow in Databricks:\n# Bronze: Raw Kafka/JSON stream (Append-only)\n# Silver: Filter bad rows, enforce types, merge CDC upserts\n# Gold: Daily KPIs, user cohorts, executive metrics",
                    "resources": [
                        {"title": "Databricks Medallion Architecture Overview", "url": "https://www.databricks.com/glossary/medallion-architecture"}
                    ]
                }
            ],
            "Snowflake": [
                {
                    "name": "Snowflake Basics",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "2 hrs",
                    "summary": "Snowflake three-layer architecture: Database Storage (Centralized micro-partitions), Compute (Decoupled Virtual Warehouses), and Cloud Services (Optimization, security, metadata).",
                    "skills": ["Snowflake architecture", "Decoupled compute & storage", "Cloud services layer"],
                    "interview_question": "How does Snowflake's decoupled compute and storage architecture prevent query contention between ETL jobs and BI analysts?",
                    "code_snippet": "-- Scale up compute in 1 second without downtime\nALTER WAREHOUSE analytics_wh SET WAREHOUSE_SIZE = 'LARGE';\n-- Scale back down after job completes\nALTER WAREHOUSE analytics_wh SET WAREHOUSE_SIZE = 'XSMALL';",
                    "resources": [
                        {"title": "Snowflake Architecture Documentation", "url": "https://docs.snowflake.com/en/user-guide/intro-key-concepts"}
                    ]
                },
                {
                    "name": "Databases and Schemas",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "2 hrs",
                    "summary": "Object hierarchy in Snowflake: Organization -> Account -> Database -> Schema -> Table / View / Stage. Standard, Transient, and Temporary tables.",
                    "skills": ["Snowflake object hierarchy", "Transient vs Permanent tables", "RBAC roles (SYSADMIN, ACCOUNTADMIN)"],
                    "interview_question": "What is the difference between a Permanent table and a Transient table in Snowflake?",
                    "code_snippet": "CREATE OR REPLACE DATABASE raw_lake;\nCREATE OR REPLACE SCHEMA raw_lake.stripe_payments;\n-- Transient table (no fail-safe, lower storage cost)\nCREATE TRANSIENT TABLE raw_lake.stripe_payments.charges_stage (\n    charge_id STRING, amount NUMBER(10,2), status STRING\n);",
                    "resources": [
                        {"title": "Snowflake Databases & Schemas", "url": "https://docs.snowflake.com/en/sql-reference/ddl-database"}
                    ]
                },
                {
                    "name": "Virtual Warehouses",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "2.5 hrs",
                    "summary": "Virtual Warehouses: T-shirt sizes (X-Small to 6X-Large), Multi-cluster warehouses for concurrency auto-scaling, auto-suspend, and auto-resume settings.",
                    "skills": ["Virtual warehouse sizing", "Multi-cluster auto-scaling", "Credit consumption control"],
                    "interview_question": "Explain the difference between scaling UP (increasing warehouse size) and scaling OUT (multi-cluster warehouse) in Snowflake.",
                    "code_snippet": "CREATE OR REPLACE WAREHOUSE etl_wh WITH\n  WAREHOUSE_SIZE = 'MEDIUM'\n  AUTO_SUSPEND = 60       -- Suspend after 60 seconds of inactivity\n  AUTO_RESUME = TRUE\n  INITIALLY_SUSPENDED = TRUE;",
                    "resources": [
                        {"title": "Snowflake Virtual Warehouses Guide", "url": "https://docs.snowflake.com/en/user-guide/warehouses-overview"}
                    ]
                },
                {
                    "name": "Snowflake SQL",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "3 hrs",
                    "summary": "Snowflake SQL extensions: FLATTEN for nested JSON/ARRAYs, PARSE_JSON, VARIANT data type, QUALIFY clause (filter window functions directly!), and metadata functions.",
                    "skills": ["VARIANT data type", "FLATTEN function", "QUALIFY clause", "Semi-structured data querying"],
                    "interview_question": "What does the QUALIFY clause do in Snowflake and why is it superior to wrapping queries in a CTE?",
                    "code_snippet": "-- Deduplicate in a single statement using QUALIFY!\nSELECT emp_id, department, salary\nFROM employees\nQUALIFY ROW_NUMBER() OVER(PARTITION BY emp_id ORDER BY updated_at DESC) = 1;",
                    "resources": [
                        {"title": "Snowflake QUALIFY Clause Documentation", "url": "https://docs.snowflake.com/en/sql-reference/constructs/qualify"}
                    ]
                },
                {
                    "name": "Time Travel",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "2.5 hrs",
                    "summary": "Query historical data states up to 90 days (Enterprise): `AT (OFFSET => ...)`, `AT (TIMESTAMP => ...)`, `BEFORE (STATEMENT => ...)`, and UNDROP TABLE/DATABASE.",
                    "skills": ["Time Travel queries", "UNDROP table", "Data recovery SLAs"],
                    "interview_question": "How do you recover a dropped table in Snowflake if someone accidentally ran DROP TABLE orders?",
                    "code_snippet": "-- Query table exactly as it existed 10 minutes ago\nSELECT * FROM orders AT(OFFSET => -60*10);\n\n-- Instant accident recovery\nUNDROP TABLE orders;",
                    "resources": [
                        {"title": "Snowflake Time Travel Guide", "url": "https://docs.snowflake.com/en/user-guide/data-time-travel"}
                    ]
                },
                {
                    "name": "Zero Copy Cloning",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "2 hrs",
                    "summary": "Instant cloning of databases, schemas, and tables using `CLONE` keyword without duplicating storage bytes (only metadata pointers are cloned).",
                    "skills": ["Zero Copy Clone", "Dev/Staging environments from Prod", "Metadata pointers"],
                    "interview_question": "How does Zero Copy Cloning allow instant testing against production-sized data without incurring double storage costs?",
                    "code_snippet": "-- Create instant isolated dev replica of production database\nCREATE DATABASE prod_backup_dev CLONE production_db;\n-- Test destructive pipeline changes with zero risk to production!",
                    "resources": [
                        {"title": "Snowflake Cloning Overview", "url": "https://docs.snowflake.com/en/user-guide/object-clone"}
                    ]
                },
                {
                    "name": "Snowpipe",
                    "importance": "Must Learn",
                    "difficulty": "Advanced",
                    "est_time": "3.5 hrs",
                    "summary": "Continuous, event-driven data ingestion: Cloud storage events (AWS S3 SNS/SQS, Azure Event Grid) triggering micro-batch serverless loads into Snowflake tables.",
                    "skills": ["Snowpipe", "External Stages", "Storage Integrations", "Serverless ingestion"],
                    "interview_question": "What is the difference between batch COPY INTO commands and continuous Snowpipe ingestion?",
                    "code_snippet": "CREATE OR REPLACE PIPE raw_lake.sales_pipe\nAUTO_INGEST = TRUE\nAS\nCOPY INTO raw_lake.sales_table\nFROM @raw_lake.s3_sales_stage\nFILE_FORMAT = (TYPE = 'PARQUET');",
                    "resources": [
                        {"title": "Snowflake Snowpipe Documentation", "url": "https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro"}
                    ]
                }
            ],
            "Cloud": [
                {
                    "name": "Cloud Basics",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "2 hrs",
                    "summary": "Cloud computing fundamentals for data: IaaS, PaaS, SaaS, Regions, Availability Zones, Shared Responsibility Model, and Public Cloud providers.",
                    "skills": ["Cloud architecture", "Regions & AZs", "Shared Responsibility Model"],
                    "interview_question": "Explain the Shared Responsibility Model between cloud providers (AWS/Azure/GCP) and data engineering teams.",
                    "code_snippet": "# Cloud Provider Matrix:\n# Feature      | AWS               | Azure            | GCP\n# Object Store | S3                | ADLS Gen2 / Blob | Cloud Storage (GCS)\n# Warehouse    | Redshift          | Synapse Analytics| BigQuery\n# Orchestration| MWAA (Airflow)    | Data Factory     | Cloud Composer",
                    "resources": [
                        {"title": "AWS Cloud Computing Overview", "url": "https://aws.amazon.com/what-is-cloud-computing/"}
                    ]
                },
                {
                    "name": "Azure Basics",
                    "importance": "Recommended",
                    "difficulty": "Beginner",
                    "est_time": "2.5 hrs",
                    "summary": "Microsoft Azure ecosystem for data: Azure Resource Groups, Azure Data Lake Storage (ADLS Gen2), Azure Data Factory (ADF), and Azure Databricks.",
                    "skills": ["Azure portal", "ADLS Gen2 hierarchical namespace", "Azure Data Factory"],
                    "interview_question": "What is the key advantage of ADLS Gen2 Hierarchical Namespace over flat blob storage?",
                    "code_snippet": "# ADLS Gen2 URL scheme:\n# abfss://<container>@<storage_account>.dfs.core.windows.net/<path>",
                    "resources": [
                        {"title": "Azure Data Fundamentals", "url": "https://learn.microsoft.com/en-us/azure/data-explorer/"}
                    ]
                },
                {
                    "name": "AWS Basics",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "3 hrs",
                    "summary": "Amazon Web Services core services: S3 (object store), EC2 (compute instances), AWS Glue (serverless Spark & Catalog), AWS Athena (Serverless Presto/Trino SQL), and CloudWatch.",
                    "skills": ["AWS S3 buckets", "AWS Glue Data Catalog", "Amazon Athena queries"],
                    "interview_question": "How does AWS Athena work with the AWS Glue Data Catalog to query raw Parquet files in S3 with SQL?",
                    "code_snippet": "# AWS Athena query directly against S3 data via Glue Catalog:\nSELECT region, SUM(amount) FROM glue_db.s3_orders_table GROUP BY 1;",
                    "resources": [
                        {"title": "AWS Data Analytics Guide", "url": "https://aws.amazon.com/analytics/"}
                    ]
                },
                {
                    "name": "Storage Services",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "2 hrs",
                    "summary": "Comparing storage tiers: Standard, Infrequent Access (IA), Glacier / Archive. Storage lifecycle policies, access point policies, and cost optimization.",
                    "skills": ["Storage tiering", "Lifecycle transition rules", "Data archiving cost optimization"],
                    "interview_question": "How do S3 Lifecycle policies automatically save 80%+ on data lake storage costs for historical data?",
                    "code_snippet": "# S3 Lifecycle Rule:\n# Day 0-30:   S3 Standard (Fast daily pipeline reads)\n# Day 31-90:  S3 Standard-IA (Weekly/Monthly reporting)\n# Day 91+:    S3 Glacier Flexible Retrieval (Compliance / Deep Archive)",
                    "resources": [
                        {"title": "AWS S3 Storage Classes", "url": "https://aws.amazon.com/s3/storage-classes/"}
                    ]
                },
                {
                    "name": "IAM (Security)",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "3 hrs",
                    "summary": "Identity and Access Management (IAM): Users, Groups, Roles, Policies (Least Privilege), Role Assumption, Instance Profiles, and KMS encryption keys.",
                    "skills": ["IAM Roles", "Least Privilege Principle", "KMS envelope encryption", "Service principles"],
                    "interview_question": "Why should data pipelines authenticate using IAM Roles instead of hardcoded Access Key ID and Secret Keys?",
                    "code_snippet": "{\n  \"Version\": \"2012-10-17\",\n  \"Statement\": [{\n    \"Effect\": \"Allow\",\n    \"Action\": [\"s3:GetObject\", \"s3:PutObject\"],\n    \"Resource\": \"arn:aws:s3:::my-company-datalake/curated/*\"\n  }]\n}",
                    "resources": [
                        {"title": "AWS IAM Best Practices", "url": "https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html"}
                    ]
                }
            ],
            "Integration": [
                {
                    "name": "Databricks with Azure",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "2.5 hrs",
                    "summary": "Connecting Azure Databricks to ADLS Gen2 using Service Principals, Azure Key Vault secrets, and Unity Catalog External Locations.",
                    "skills": ["Azure Service Principals", "Databricks Key Vault secrets", "ADLS Gen2 credentials"],
                    "interview_question": "How do you securely pass credentials to Databricks using Azure Key Vault backed secret scopes?",
                    "code_snippet": "# Read secret securely from Databricks secret scope\nstorage_key = dbutils.secrets.get(scope=\"azure-keyvault\", key=\"adls-storage-key\")",
                    "resources": [
                        {"title": "Connect Azure Databricks to ADLS Gen2", "url": "https://learn.microsoft.com/en-us/azure/databricks/connect/storage/azure-storage"}
                    ]
                },
                {
                    "name": "Snowflake with Storage",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "3 hrs",
                    "summary": "Configuring Snowflake External Stages backed by AWS S3 or Azure Blob via Storage Integrations (cloud role trust relationship without static keys).",
                    "skills": ["Snowflake Storage Integration", "External Stages", "IAM Trust Policy"],
                    "interview_question": "Why does Snowflake recommend Storage Integrations over passing AWS credentials in plain text?",
                    "code_snippet": "CREATE STORAGE INTEGRATION s3_lake_int\n  TYPE = EXTERNAL_STAGE\n  STORAGE_PROVIDER = 'S3'\n  ENABLED = TRUE\n  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake_access_role'\n  STORAGE_ALLOWED_LOCATIONS = ('s3://company-lake/data/');",
                    "resources": [
                        {"title": "Snowflake S3 Storage Integration Guide", "url": "https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration"}
                    ]
                },
                {
                    "name": "ETL Between Systems",
                    "importance": "Must Learn",
                    "difficulty": "Advanced",
                    "est_time": "3.5 hrs",
                    "summary": "Designing multi-system cross-cloud ETL: Extracting raw data from Postgres/APIs -> Spark transformation in Databricks -> Ingestion into Snowflake / BigQuery.",
                    "skills": ["Cross-system integration", "JDBC connectors", "Format conversions", "Idempotent loads"],
                    "interview_question": "How do you handle schema evolution when source PostgreSQL tables add new columns during ETL to Snowflake?",
                    "code_snippet": "# Spark writing directly to Snowflake using Snowflake connector\ndf.write \\\n  .format(\"snowflake\") \\\n  .options(**sfOptions) \\\n  .option(\"dbtable\", \"ANALYTICS.PROCESSED_ORDERS\") \\\n  .mode(\"append\") \\\n  .save()",
                    "resources": [
                        {"title": "Snowflake Connector for Spark", "url": "https://docs.snowflake.com/en/user-guide/spark-connector"}
                    ]
                },
                {
                    "name": "Integration Project",
                    "importance": "Must Learn",
                    "difficulty": "Advanced",
                    "est_time": "6 hrs",
                    "summary": "Build a multi-cloud data pipeline: Upload real-world dataset to AWS S3, process and clean with PySpark, ingest into Snowflake via External Stage, and query with Snowflake SQL.",
                    "skills": ["Portfolio Integration Project", "S3 -> PySpark -> Snowflake", "Production deployment"],
                    "interview_question": "Walk me through how you monitored and tuned the Snowflake ingestion pipeline in your project.",
                    "code_snippet": "# End-to-end milestone: S3 Bucket -> Databricks Cleanse -> Snowflake Analytics -> Verified!",
                    "resources": [
                        {"title": "Modern Cloud Data Pipeline Architecture", "url": "https://roadmap.sh/data-engineer"}
                    ]
                }
            ]
        }
    },

    "Phase 6: Data Visualization": {
        "icon": "📊",
        "description": "Business Intelligence, Power BI, DAX, data modeling for reporting, dashboard design, and serving analytics to stakeholders.",
        "est_time": "3 Weeks",
        "color": "#EC4899",
        "categories": {
            "Power BI": [
                {
                    "name": "Power BI Basics",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "2 hrs",
                    "summary": "Power BI Desktop overview: Connecting to data sources (SQL, CSV, Snowflake, Web), navigating Report view, Data view, and Model view.",
                    "skills": ["Power BI Desktop", "Data connection types", "Report vs Data view"],
                    "interview_question": "What is the difference between 'Import Mode' and 'DirectQuery Mode' in Power BI?",
                    "code_snippet": "# Import Mode: Caches data in-memory (VertiPaq engine) -> Extremely fast, size limit\n# DirectQuery: Queries source DB in real-time -> No size limit, relies on DB performance",
                    "resources": [
                        {"title": "Microsoft Power BI Guided Learning", "url": "https://learn.microsoft.com/en-us/power-bi/fundamentals/desktop-getting-started"}
                    ]
                },
                {
                    "name": "Power Query",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "3 hrs",
                    "summary": "Power Query (M language) ETL editor: Removing columns, filtering rows, pivoting/unpivoting, changing data types, merging queries, and query folding.",
                    "skills": ["Power Query Editor", "M language", "Query Folding", "Unpivot columns"],
                    "interview_question": "What is Query Folding in Power Query and why is it critical for report refresh performance?",
                    "code_snippet": "# Query Folding: Power Query translates UI transformations into native SQL,\n# pushing processing down to the source database server instead of your laptop.",
                    "resources": [
                        {"title": "Query Folding in Power BI", "url": "https://learn.microsoft.com/en-us/power-bi/guidance/power-query-folding"}
                    ]
                },
                {
                    "name": "Data Modeling",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "3 hrs",
                    "summary": "Building relationships in Power BI: 1-to-Many (1:*), Many-to-Many (*:*), cross-filter direction (Single vs Both), and building a Star Schema model.",
                    "skills": ["Star schema modeling in BI", "Cross filter direction", "Active vs Inactive relationships"],
                    "interview_question": "Why should you avoid bidirectional cross-filtering in Power BI data models?",
                    "code_snippet": "# Modeling Best Practice:\n# - Always organize tables in a Star Schema (Fact in center, Dimensions surrounding)\n# - Use 1:* single directional relationships to prevent circular dependency bugs",
                    "resources": [
                        {"title": "Power BI Star Schema Guidance", "url": "https://learn.microsoft.com/en-us/power-bi/guidance/star-schema"}
                    ]
                },
                {
                    "name": "DAX Basics",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "3 hrs",
                    "summary": "Data Analysis Expressions (DAX): Calculated Columns (row-by-row, stored in memory) vs Measures (dynamic calculation at query time), SUM, COUNTROWS, DIVIDE.",
                    "skills": ["DAX Measures", "Calculated columns", "DIVIDE safe division", "Aggregation functions"],
                    "interview_question": "Explain the difference between a Calculated Column and a Measure in DAX.",
                    "code_snippet": "-- DAX Measure (Evaluated dynamically on visual filter context)\nTotal Revenue = SUM(fact_sales[revenue])\n\nProfit Margin = DIVIDE([Total Revenue] - SUM(fact_sales[cost]), [Total Revenue], 0)",
                    "resources": [
                        {"title": "DAX Basics in Power BI Desktop", "url": "https://learn.microsoft.com/en-us/power-bi/transform-model/desktop-quickstart-learn-dax-basics"}
                    ]
                },
                {
                    "name": "Advanced DAX",
                    "importance": "Must Learn",
                    "difficulty": "Advanced",
                    "est_time": "4 hrs",
                    "summary": "Mastering the CALCULATE function (context transition and filter modification), FILTER, ALL, ALLEXCEPT, and Time Intelligence (DATESYTD, SAMEPERIODLASTYEAR).",
                    "skills": ["CALCULATE function", "Evaluation contexts (Row vs Filter)", "Time intelligence", "ALL / FILTER"],
                    "interview_question": "How does CALCULATE modify the filter context of a measure in DAX?",
                    "code_snippet": "-- Revenue for Electronics category only, overriding visual filter\nElectronics Revenue = \nCALCULATE(\n    [Total Revenue],\n    dim_product[category] = \"Electronics\"\n)\n\n-- Year-Over-Year Revenue\nRevenue YoY = \n[Total Revenue] - CALCULATE([Total Revenue], SAMEPERIODLASTYEAR(dim_date[Date]))",
                    "resources": [
                        {"title": "SQLBI DAX Guide (Marco Russo & Alberto Ferrari)", "url": "https://www.sqlbi.com/"}
                    ]
                }
            ],
            "Visualization": [
                {
                    "name": "Charts and Visuals",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "2 hrs",
                    "summary": "Choosing the right visual: Bar/Column charts for comparisons, Line charts for time-series trends, Scatter plots for correlations, Matrix for tabular metrics, and Tree maps.",
                    "skills": ["Visual hierarchy", "Chart selection", "Data storytelling"],
                    "interview_question": "When should you use a Bar chart instead of a Pie chart for categorical distribution?",
                    "code_snippet": "# Chart Selection Guide:\n# - Trend over Time  -> Line Chart\n# - Comparison       -> Horizontal Bar Chart\n# - Part-to-Whole    -> Treemap / Donut (Max 5 slices)\n# - Correlation      -> Scatter Plot",
                    "resources": [
                        {"title": "Financial Times Visual Vocabulary", "url": "https://github.com/Financial-Times/chart-doctor/tree/master/visual-vocabulary"}
                    ]
                },
                {
                    "name": "Dashboard Design",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "2.5 hrs",
                    "summary": "UX/UI principles for analytical dashboards: Visual hierarchy, 5-second rule, color palettes for accessibility, clutter reduction, and responsive layout.",
                    "skills": ["Dashboard layout", "Z-pattern / F-pattern layout", "Accessibility"],
                    "interview_question": "What is the '5-second rule' in executive dashboard design?",
                    "code_snippet": "# Dashboard UX rules:\n# 1. Top row: Key KPIs (Total Revenue, MoM Growth, Active Users)\n# 2. Middle: Trends and categorical breakdown\n# 3. Bottom: Detailed drill-down tables",
                    "resources": [
                        {"title": "Power BI Dashboard Design Best Practices", "url": "https://learn.microsoft.com/en-us/power-bi/create-reports/service-dashboards-design-tips"}
                    ]
                },
                {
                    "name": "Filters and Drilldown",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "2 hrs",
                    "summary": "Interactive slicers, dropdown filters, Drill-down hierarchies (Year -> Quarter -> Month), Drill-through pages (viewing single customer details), and Bookmarks.",
                    "skills": ["Slicers", "Drill-through", "Drill-down hierarchies", "Bookmarks & Buttons"],
                    "interview_question": "How do Drill-through pages enhance dashboard usability?",
                    "code_snippet": "# Drill-down: Click on 2026 to expand into Q1, Q2, Q3, Q4 in the same visual.\n# Drill-through: Right click customer -> navigate to detailed 'Customer Profile' page.",
                    "resources": [
                        {"title": "Drill-through in Power BI", "url": "https://learn.microsoft.com/en-us/power-bi/create-reports/desktop-drillthrough"}
                    ]
                },
                {
                    "name": "KPIs and Metrics",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "2 hrs",
                    "summary": "KPI Card visuals, gauge charts, trend indicators, target variance tracking, and conditional formatting rules.",
                    "skills": ["KPI cards", "Variance analysis", "Conditional formatting"],
                    "interview_question": "What components make up an effective KPI card?",
                    "code_snippet": "# Effective KPI card elements:\n# [1] Actual Value ($1.4M)\n# [2] Target Goal ($1.2M)\n# [3] Variance Indicator (+16.7% ▲ Green)\n# [4] Mini Sparkline trend",
                    "resources": [
                        {"title": "Power BI KPI Visuals", "url": "https://learn.microsoft.com/en-us/power-bi/visuals/power-bi-visualization-kpi"}
                    ]
                }
            ],
            "Advanced": [
                {
                    "name": "Performance Optimization",
                    "importance": "Must Learn",
                    "difficulty": "Advanced",
                    "est_time": "3 hrs",
                    "summary": "Optimizing slow Power BI reports: Performance Analyzer tool, DAX Studio query tuning, removing unnecessary columns/high-cardinality keys, and aggregation tables.",
                    "skills": ["Performance Analyzer", "DAX Studio", "VertiPaq compression tuning", "Cardinality reduction"],
                    "interview_question": "How does column cardinality affect VertiPaq engine compression in Power BI?",
                    "code_snippet": "# High cardinality columns (e.g. detailed timestamps, GUIDs) kill compression.\n# Splitting Timestamp into Date (low cardinality) and Time (low cardinality)\n# can reduce dataset size by 70%+!",
                    "resources": [
                        {"title": "Power BI Performance Optimization Guide", "url": "https://learn.microsoft.com/en-us/power-bi/guidance/power-bi-optimization"}
                    ]
                },
                {
                    "name": "Data Refresh",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "2.5 hrs",
                    "summary": "Scheduled refresh in Power BI Service: On-Premises Data Gateway setup, incremental refresh configuration, and refresh failure alerting.",
                    "skills": ["On-premises Data Gateway", "Scheduled Refresh", "Incremental Refresh", "REST API triggers"],
                    "interview_question": "How does Incremental Refresh work in Power BI to avoid refreshing years of historical data?",
                    "code_snippet": "# RangeStart and RangeEnd parameters define incremental partitions:\n# Historical data: Read once and frozen\n# Recent data: Refreshed every few hours",
                    "resources": [
                        {"title": "Incremental Refresh in Power BI", "url": "https://learn.microsoft.com/en-us/power-bi/connect-data/incremental-refresh-overview"}
                    ]
                },
                {
                    "name": "Row Level Security",
                    "importance": "Must Learn",
                    "difficulty": "Advanced",
                    "est_time": "3 hrs",
                    "summary": "Row-Level Security (RLS): Static RLS (creating role per department) and Dynamic RLS using `USERPRINCIPALNAME()` so managers only see their own team's data.",
                    "skills": ["Static RLS", "Dynamic RLS", "USERPRINCIPALNAME() function", "Testing roles"],
                    "interview_question": "Write a dynamic DAX filter expression for Row-Level Security based on the logged-in user email.",
                    "code_snippet": "-- Dynamic RLS DAX filter on dim_sales_rep table\n[email] = USERPRINCIPALNAME()",
                    "resources": [
                        {"title": "Row-Level Security (RLS) with Power BI", "url": "https://learn.microsoft.com/en-us/power-bi/enterprise/service-admin-rls"}
                    ]
                },
                {
                    "name": "Publishing",
                    "importance": "Must Learn",
                    "difficulty": "Beginner",
                    "est_time": "2 hrs",
                    "summary": "Publishing to Power BI Service: Workspaces (Dev/Test/Prod), Apps distribution to end-users, managing permissions, and exporting reports to PDF/PowerPoint.",
                    "skills": ["Power BI Service Workspaces", "Power BI Apps distribution", "User access control"],
                    "interview_question": "What is the difference between sharing a Power BI Workspace vs distributing an App?",
                    "code_snippet": "# Workspaces are for creators/collaborators (Edit rights).\n# Apps are packaged, read-only bundles for executive stakeholders.",
                    "resources": [
                        {"title": "Publish from Power BI Desktop", "url": "https://learn.microsoft.com/en-us/power-bi/create-reports/desktop-upload-desktop-files"}
                    ]
                }
            ],
            "Integration": [
                {
                    "name": "Connect to Databricks",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "2 hrs",
                    "summary": "Connecting Power BI directly to Databricks SQL Warehouses via native Databricks connector, Partner Connect, personal access tokens, and SSO.",
                    "skills": ["Databricks SQL Warehouse connection", "DirectQuery vs Import", "Partner Connect"],
                    "interview_question": "What are the performance considerations when using DirectQuery against Databricks SQL?",
                    "code_snippet": "# Direct connection to Databricks SQL Warehouse:\n# Server Hostname: <workspace>.cloud.databricks.com\n# HTTP Path: /sql/1.0/warehouses/<warehouse-id>",
                    "resources": [
                        {"title": "Connect Power BI to Databricks", "url": "https://docs.databricks.com/bi/power-bi.html"}
                    ]
                },
                {
                    "name": "Connect to Snowflake",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "2 hrs",
                    "summary": "Connecting Power BI to Snowflake: Account locator URL, Role selection, Warehouse configuration, SSO authentication, and DirectQuery vs Import trade-offs.",
                    "skills": ["Snowflake native connector", "Role-based BI queries", "Snowflake query history monitoring"],
                    "interview_question": "How do you ensure Power BI reports don't accidentally keep a Snowflake virtual warehouse running 24/7?",
                    "code_snippet": "# Power BI connection config:\n# Server: <account_identifier>.snowflakecomputing.com\n# Warehouse: BI_REPORTING_WH (Configured with AUTO_SUSPEND = 60)",
                    "resources": [
                        {"title": "Connect Power BI to Snowflake", "url": "https://docs.snowflake.com/en/user-guide/ecosystem-powerbi"}
                    ]
                },
                {
                    "name": "Live Dashboards",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "2.5 hrs",
                    "summary": "Near real-time reporting: Automatic page refresh (APR), streaming datasets in Power BI, Pub/Sub integration, and direct query optimization.",
                    "skills": ["Automatic Page Refresh", "Streaming datasets", "Sub-minute refresh SLAs"],
                    "interview_question": "How does Automatic Page Refresh (APR) work with DirectQuery sources in Power BI Premium?",
                    "code_snippet": "# APR settings:\n# Configure page refresh interval (e.g. Every 15 seconds)\n# Monitor source database load to avoid connection pooling saturation.",
                    "resources": [
                        {"title": "Automatic Page Refresh in Power BI", "url": "https://learn.microsoft.com/en-us/power-bi/create-reports/desktop-automatic-page-refresh"}
                    ]
                },
                {
                    "name": "Visualization Project",
                    "importance": "Must Learn",
                    "difficulty": "Advanced",
                    "est_time": "5 hrs",
                    "summary": "Cap-stone BI Dashboard: Build an executive business intelligence report with Star Schema modeling, advanced DAX measures, dynamic RLS, and published App.",
                    "skills": ["End-to-End BI Portfolio Project", "Executive Presentation", "DAX calculation logic"],
                    "interview_question": "Present the executive business value and architecture of your portfolio dashboard.",
                    "code_snippet": "# Deliverables:\n# 1. Executive Summary page (Revenue, MoM variance)\n# 2. Product & Category Performance drilldown\n# 3. Dynamic RLS security configuration",
                    "resources": [
                        {"title": "Power BI Portfolio Project Inspiration", "url": "https://community.powerbi.com/t5/Data-Stories-Gallery/bd-p/DataStoriesGallery"}
                    ]
                }
            ]
        }
    },

    # -------------------------------------------------------------------------
    # NEW ROADMAP.SH COMPREHENSIVE PHASES (2026 EDITION)
    # -------------------------------------------------------------------------
    "Phase 7: Modern Orchestration & DataOps": {
        "icon": "🔄",
        "description": "Production workflow orchestration (Apache Airflow, Dagster, Prefect), Docker containerization, CI/CD, and Infrastructure as Code.",
        "est_time": "3 Weeks",
        "color": "#8B5CF6",
        "categories": {
            "Apache Airflow Deep Dive": [
                {
                    "name": "Airflow Architecture & DAGs",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "3.5 hrs",
                    "summary": "Core Airflow architecture: Webserver, Scheduler, Metadata DB, and Executors (Celery, Kubernetes, Local). Defining DAGs, default_args, and schedule intervals.",
                    "skills": ["Airflow architecture", "DAG authoring", "Executors (Celery/K8s)", "Schedule intervals"],
                    "interview_question": "How does the Airflow Scheduler monitor DAGs and transition tasks across states (queued, running, success, failed)?",
                    "code_snippet": "from datetime import datetime, timedelta\nfrom airflow import DAG\nfrom airflow.operators.empty import EmptyOperator\n\ndefault_args = {\n    'owner': 'data_team',\n    'retries': 3,\n    'retry_delay': timedelta(minutes=5),\n}\n\nwith DAG(\n    'daily_sales_pipeline',\n    default_args=default_args,\n    schedule_interval='@daily',\n    start_date=datetime(2026, 1, 1),\n    catchup=False\n) as dag:\n    start = EmptyOperator(task_id='start')\n    end = EmptyOperator(task_id='end')\n    start >> end",
                    "resources": [
                        {"title": "Apache Airflow Official Tutorial", "url": "https://airflow.apache.org/docs/apache-airflow/stable/tutorial/index.html"},
                        {"title": "roadmap.sh Airflow Guide", "url": "https://roadmap.sh/data-engineer"}
                    ]
                },
                {
                    "name": "TaskFlow API & XComs",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "3 hrs",
                    "summary": "Modern Airflow TaskFlow API (@task decorator), passing data between tasks using XComs (and custom XCom backends on S3/GCS), and dynamic task mapping.",
                    "skills": ["@task decorator", "XCom mechanics", "Dynamic task mapping (.expand())"],
                    "interview_question": "Why shouldn't you pass large data payloads (e.g. 500 MB Pandas DataFrames) directly through standard Airflow XComs?",
                    "code_snippet": "from airflow.decorators import task\n\n@task\ndef extract_data() -> dict:\n    return {'file_path': 's3://bucket/2026-05-14.parquet'}\n\n@task\ndef process_data(metadata: dict):\n    print(f\"Processing {metadata['file_path']}\")\n\n# Clean Pythonic DAG wiring\nmeta = extract_data()\nprocess_data(meta)",
                    "resources": [
                        {"title": "Airflow TaskFlow API Tutorial", "url": "https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html"}
                    ]
                },
                {
                    "name": "Sensors, Hooks & Operators",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "3 hrs",
                    "summary": "Standard and custom operators: BashOperator, PythonOperator, PostgresOperator, S3Hook, and Sensors (waiting for files/keys with poke vs reschedule mode).",
                    "skills": ["Airflow Operators", "Hooks & Connections", "Sensors (poke vs reschedule)"],
                    "interview_question": "What is the difference between sensor mode='poke' and mode='reschedule' in Airflow?",
                    "code_snippet": "from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor\n\n# Reschedule mode frees up worker slots between checks!\nwait_for_file = S3KeySensor(\n    task_id='wait_for_daily_drop',\n    bucket_name='partner-drop',\n    bucket_key='orders_*.csv',\n    mode='reschedule',\n    poke_interval=300\n)",
                    "resources": [
                        {"title": "Airflow Sensors Best Practices", "url": "https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html"}
                    ]
                }
            ],
            "Containers & CI/CD": [
                {
                    "name": "Docker for Data Engineering",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "4 hrs",
                    "summary": "Containerizing data workloads: Dockerfile syntax, multi-stage builds, container isolation, volume mounting, and docker-compose for spinning up local Kafka, Postgres, and Airflow.",
                    "skills": ["Dockerfile", "docker-compose.yml", "Multi-stage builds", "Local testing environments"],
                    "interview_question": "How does Docker solve the 'works on my machine' issue when deploying Python and Spark data pipelines?",
                    "code_snippet": "FROM python:3.11-slim\nWORKDIR /app\nCOPY requirements.txt .\nRUN pip install --no-cache-dir -r requirements.txt\nCOPY . .\nCMD [\"python\", \"pipeline_runner.py\"]",
                    "resources": [
                        {"title": "Docker for Data Engineers (FreeCodeCamp)", "url": "https://www.freecodecamp.org/news/docker-for-data-engineering/"},
                        {"title": "roadmap.sh Docker Guide", "url": "https://roadmap.sh/docker"}
                    ]
                },
                {
                    "name": "CI/CD & GitHub Actions for DE",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "3 hrs",
                    "summary": "Continuous Integration & Deployment for data pipelines: automated linting (flake8, black, ruff), pytest unit tests, SQLFluff for SQL models, and automated deployment.",
                    "skills": ["GitHub Actions workflows", "pytest for data pipelines", "SQLFluff linting", "Automated deployments"],
                    "interview_question": "What testing checks should be mandatory in a Data Engineering CI pipeline before code is merged into main?",
                    "code_snippet": "# .github/workflows/data_pipeline_ci.yml\nname: Data Pipeline CI\non: [push, pull_request]\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/checkout@v4\n      - uses: actions/setup-python@v5\n        with: { python-version: '3.11' }\n      - run: pip install -r requirements.txt pytest sqlfluff\n      - run: pytest tests/\n      - run: sqlfluff lint models/",
                    "resources": [
                        {"title": "GitHub Actions Documentation", "url": "https://docs.github.com/en/actions"}
                    ]
                },
                {
                    "name": "Infrastructure as Code (Terraform)",
                    "importance": "Recommended",
                    "difficulty": "Advanced",
                    "est_time": "4 hrs",
                    "summary": "Managing cloud data infrastructure reproducibly with Terraform (HCL): Provisioning S3 buckets, Snowflake warehouses, IAM roles, and BigQuery datasets as code.",
                    "skills": ["Terraform syntax", "State management", "Provisioning S3/Snowflake/GCP resources"],
                    "interview_question": "Why is Infrastructure as Code (IaC) critical for data engineering compliance and disaster recovery?",
                    "code_snippet": "resource \"aws_s3_bucket\" \"datalake\" {\n  bucket = \"production-de-lakehouse-2026\"\n  lifecycle {\n    prevent_destroy = true\n  }\n}",
                    "resources": [
                        {"title": "Terraform AWS Tutorial", "url": "https://developer.hashicorp.com/terraform/tutorials/aws"}
                    ]
                }
            ]
        }
    },

    "Phase 8: Streaming & Real-Time Data (Kafka & Flink)": {
        "icon": "🌊",
        "description": "Event-driven architecture, distributed messaging with Apache Kafka, stream processing with Apache Flink and Spark Structured Streaming.",
        "est_time": "3 Weeks",
        "color": "#14B8A6",
        "categories": {
            "Apache Kafka": [
                {
                    "name": "Kafka Core Architecture",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "4 hrs",
                    "summary": "Distributed commit log: Topics, Partitions, Consumer Groups, Offsets, Brokers, Replication factor, and KRaft consensus (Zookeeper-less).",
                    "skills": ["Kafka architecture", "Topic partitioning", "Consumer groups", "KRaft consensus"],
                    "interview_question": "How does Kafka achieve horizontal scalability through topic partitioning, and what determines consumer concurrency?",
                    "code_snippet": "# Kafka Producer in Python (confluent-kafka)\nfrom confluent_kafka import Producer\n\np = Producer({'bootstrap.servers': 'localhost:9092'})\np.produce('user_signups', key='user_42', value='{\"email\":\"user@de.com\"}')\np.flush()",
                    "resources": [
                        {"title": "Apache Kafka Official Documentation", "url": "https://kafka.apache.org/documentation/"},
                        {"title": "Confluent Kafka Fundamentals", "url": "https://developer.confluent.io/courses/apache-kafka-fundamentals/intro/"}
                    ]
                },
                {
                    "name": "Delivery Semantics & Offsets",
                    "importance": "Must Learn",
                    "difficulty": "Advanced",
                    "est_time": "3 hrs",
                    "summary": "Message delivery guarantees: At-most-once, At-least-once (most common), and Exactly-once semantics (EOS via idempotent producers and transactional APIs).",
                    "skills": ["Delivery guarantees", "Offset commit strategies", "Idempotent producer", "Kafka transactions"],
                    "interview_question": "What is the difference between At-least-once and Exactly-once delivery in Kafka pipelines?",
                    "code_snippet": "# Configuration for idempotent producer (prevents duplicate messages)\nproducer_conf = {\n    'bootstrap.servers': 'localhost:9092',\n    'enable.idempotence': True,\n    'acks': 'all',\n    'retries': 5\n}",
                    "resources": [
                        {"title": "Kafka Delivery Guarantees", "url": "https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/"}
                    ]
                },
                {
                    "name": "Kafka Connect & Schema Registry",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "3 hrs",
                    "summary": "Kafka Connect ecosystem: Source connectors (JDBC, Debezium CDC) and Sink connectors (S3, Snowflake, Elasticsearch). Confluent Schema Registry with Avro/Protobuf.",
                    "skills": ["Kafka Connect", "Source/Sink connectors", "Schema Registry", "Avro serialization"],
                    "interview_question": "How does Schema Registry prevent breaking changes between producers and consumers in event-driven systems?",
                    "code_snippet": "# Debezium Postgres CDC -> Kafka Connect -> Snowflake Sink:\n# Zero custom code needed to stream every database change!",
                    "resources": [
                        {"title": "Kafka Connect Overview", "url": "https://docs.confluent.io/platform/current/connect/index.html"}
                    ]
                }
            ],
            "Stream Processing": [
                {
                    "name": "Spark Structured Streaming",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "3.5 hrs",
                    "summary": "Stream processing using the DataFrame API in Spark: Reading from Kafka streams, watermarking for late-arriving data, sliding windows, and writing to Delta Lake.",
                    "skills": ["Structured Streaming", "Watermarking", "Window aggregations", "Trigger(processingTime)"],
                    "interview_question": "What is a Watermark in Spark Structured Streaming and how does it prevent unbounded state storage?",
                    "code_snippet": "stream_df = spark.readStream \\\n    .format(\"kafka\") \\\n    .option(\"kafka.bootstrap.servers\", \"localhost:9092\") \\\n    .option(\"subscribe\", \"orders\") \\\n    .load()\n\nquery = stream_df.writeStream \\\n    .format(\"delta\") \\\n    .outputMode(\"append\") \\\n    .option(\"checkpointLocation\", \"/tmp/checkpoints/\") \\\n    .start(\"/mnt/delta/orders_stream\")",
                    "resources": [
                        {"title": "Spark Structured Streaming Programming Guide", "url": "https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html"}
                    ]
                },
                {
                    "name": "Apache Flink Fundamentals",
                    "importance": "Recommended",
                    "difficulty": "Advanced",
                    "est_time": "4 hrs",
                    "summary": "True event-driven stream processing engine: Stateful computations over streams, Event Time vs Processing Time, Watermarks, and Exactly-Once state checkpoints (Chandy-Lamport).",
                    "skills": ["Apache Flink", "Event Time processing", "Stateful stream checkpoints", "Flink SQL"],
                    "interview_question": "Why is Apache Flink considered 'true streaming' while Spark Streaming was historically 'micro-batching'?",
                    "code_snippet": "-- Flink SQL continuous tumbling window query\nSELECT window_start, window_end, user_id, COUNT(*) AS click_count\nFROM TABLE(TUMBLE(TABLE clicks, DESCRIPTOR(click_time), INTERVAL '5' MINUTE))\nGROUP BY window_start, window_end, user_id;",
                    "resources": [
                        {"title": "Apache Flink Documentation", "url": "https://flink.apache.org/"}
                    ]
                }
            ]
        }
    },

    "Phase 9: Transformation with dbt & Open Formats": {
        "icon": "💎",
        "description": "Analytics Engineering with dbt (Data Build Tool), modular SQL modeling, automated testing, documentation, and Open Table Formats (Apache Iceberg).",
        "est_time": "3 Weeks",
        "color": "#E11D48",
        "categories": {
            "dbt (Data Build Tool)": [
                {
                    "name": "dbt Core Fundamentals",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "3.5 hrs",
                    "summary": "The transformation engine of modern data teams: Project structure (dbt_project.yml), SQL select statements as models, `{{ ref() }}` macro for DAG dependencies, and ephemeral/view/table materializations.",
                    "skills": ["dbt models", "{{ ref() }} macro", "Materialization strategies", "dbt run"],
                    "interview_question": "How does the `{{ ref('model_name') }}` macro in dbt build the lineage DAG automatically?",
                    "code_snippet": "-- models/marts/dim_customers.sql\n{{ config(materialized='table') }}\n\nWITH source_customers AS (\n    SELECT * FROM {{ ref('stg_customers') }}\n)\nSELECT customer_id, name, created_at FROM source_customers;",
                    "resources": [
                        {"title": "dbt Getting Started Tutorial", "url": "https://docs.getdbt.com/docs/build/projects"},
                        {"title": "roadmap.sh dbt Guide", "url": "https://roadmap.sh/data-engineer"}
                    ]
                },
                {
                    "name": "dbt Testing & Documentation",
                    "importance": "Must Learn",
                    "difficulty": "Intermediate",
                    "est_time": "3 hrs",
                    "summary": "Quality assurance and automated documentation: Generic tests (unique, not_null, accepted_values, relationships), singular SQL tests, dbt test command, and `dbt docs generate`.",
                    "skills": ["dbt generic tests", "Singular tests", "Automated data catalog", "dbt docs"],
                    "interview_question": "What is the difference between a generic test and a singular test in dbt?",
                    "code_snippet": "# models/schema.yml\nversion: 2\nmodels:\n  - name: dim_customers\n    columns:\n      - name: customer_id\n        tests:\n          - unique\n          - not_null\n      - name: status\n        tests:\n          - accepted_values:\n              values: ['active', 'inactive', 'churned']",
                    "resources": [
                        {"title": "dbt Tests Guide", "url": "https://docs.getdbt.com/docs/build/tests"}
                    ]
                },
                {
                    "name": "Incremental Models & Jinja",
                    "importance": "Must Learn",
                    "difficulty": "Advanced",
                    "est_time": "3.5 hrs",
                    "summary": "Optimizing warehouse compute costs: Incremental materializations (`is_incremental()` macro), unique_key for upserts, Jinja variables, and custom macros.",
                    "skills": ["Incremental modeling", "is_incremental() macro", "Jinja templating", "Cost optimization"],
                    "interview_question": "Explain how dbt executes an incremental model on its first run vs subsequent runs.",
                    "code_snippet": "{{ config(\n    materialized='incremental',\n    unique_key='order_id'\n) }}\n\nSELECT * FROM {{ ref('stg_orders') }}\n{% if is_incremental() %}\n  WHERE order_timestamp >= (SELECT MAX(order_timestamp) FROM {{ this }})\n{% endif %}",
                    "resources": [
                        {"title": "dbt Incremental Models Guide", "url": "https://docs.getdbt.com/docs/build/incremental-models"}
                    ]
                }
            ],
            "Open Table Formats": [
                {
                    "name": "Apache Iceberg",
                    "importance": "Must Learn",
                    "difficulty": "Advanced",
                    "est_time": "4 hrs",
                    "summary": "High-performance open table format: ACID transactions on S3, hidden partitioning, partition evolution, schema evolution (add/drop/rename columns safely), and snapshot isolation.",
                    "skills": ["Apache Iceberg", "Metadata tree (Snapshots, Manifest Lists, Manifests)", "Hidden partitioning"],
                    "interview_question": "What is Hidden Partitioning in Apache Iceberg and how does it prevent user query errors compared to Hive partitioning?",
                    "code_snippet": "-- Create Iceberg table (Supported across Snowflake, Databricks, Spark, Athena)\nCREATE TABLE iceberg_lake.events (\n    id BIGINT,\n    event_time TIMESTAMP,\n    payload STRING\n)\nUSING ICEBERG\nPARTITIONED BY (days(event_time));",
                    "resources": [
                        {"title": "Apache Iceberg Documentation", "url": "https://iceberg.apache.org/docs/latest/"}
                    ]
                }
            ]
        }
    },

    "Phase 10: Capstone Projects & DE Interview Masterclass": {
        "icon": "🏆",
        "description": "Production-grade portfolio projects, DE System Design patterns, live-coding interview strategies, and behavioral interview preparation.",
        "est_time": "3 Weeks",
        "color": "#10B981",
        "categories": {
            "Portfolio Projects": [
                {
                    "name": "Project 1: Batch ELT Pipeline (dbt + Snowflake + Airflow)",
                    "importance": "Must Learn",
                    "difficulty": "Advanced",
                    "est_time": "8 hrs",
                    "summary": "Build a production ELT pipeline: Ingest API/CSV data into Snowflake with Airflow, build modular Star Schema models with dbt, enforce automated testing, and generate live data docs.",
                    "skills": ["End-to-end ELT", "Airflow orchestration", "dbt modeling", "Snowflake warehouse"],
                    "interview_question": "How did you structure data quality validation and failure alerting in this project?",
                    "code_snippet": "# Architecture:\n# [Data Source API] -> [Python Airflow DAG] -> [Snowflake Raw Tables]\n#       -> [dbt Run + dbt Test] -> [Star Schema Analytics Marts] -> [Power BI]",
                    "resources": [
                        {"title": "dbt + Snowflake Project Blueprint", "url": "https://docs.getdbt.com/guides/snowflake"}
                    ]
                },
                {
                    "name": "Project 2: Real-time Streaming Lakehouse (Kafka + Spark + Delta Lake)",
                    "importance": "Must Learn",
                    "difficulty": "Advanced",
                    "est_time": "10 hrs",
                    "summary": "Build a real-time event pipeline: Produce simulated e-commerce transactions into Kafka, consume and clean with Spark Structured Streaming, write to Delta Lake, and serve analytics.",
                    "skills": ["Kafka producer/consumer", "Spark Structured Streaming", "Delta Lake Medallion", "Real-time SLAs"],
                    "interview_question": "How did your streaming pipeline handle late-arriving events and network disconnects?",
                    "code_snippet": "# Streaming Architecture:\n# [Simulated Orders] -> [Kafka Topic] -> [Spark Structured Streaming]\n#       -> [Delta Lake Bronze -> Silver (Deduped)] -> [Fast BI Reporting]",
                    "resources": [
                        {"title": "Kafka + Spark Streaming Blueprint", "url": "https://github.com/confluentinc"}
                    ]
                }
            ],
            "Interview Masterclass": [
                {
                    "name": "Data Engineering System Design",
                    "importance": "Must Learn",
                    "difficulty": "Advanced",
                    "est_time": "5 hrs",
                    "summary": "How to tackle DE System Design interviews: Clarify requirements (Scale, Throughput, Latency SLA, Cost), Data Flow Diagram, Storage Selection (RDBMS vs NoSQL vs Lakehouse), and Fault Tolerance.",
                    "skills": ["System design framework", "Trade-off analysis", "Throughput/Storage calculations"],
                    "interview_question": "Design a real-time analytics system for a ride-sharing app (like Uber) tracking millions of driver GPS locations per second.",
                    "code_snippet": "# Framework Steps:\n# 1. Requirements: 2M updates/sec, 100ms dashboard latency, 99.99% availability\n# 2. Ingestion: Kafka partitioned by GeoHash\n# 3. Stream Processing: Flink sliding 10-minute window\n# 4. Storage: Redis (real-time driver location) + Iceberg S3 (historical training)",
                    "resources": [
                        {"title": "Data Engineering System Design Interview Guide", "url": "https://github.com/donnemartin/system-design-primer"}
                    ]
                },
                {
                    "name": "SQL & Python Live Coding Prep",
                    "importance": "Must Learn",
                    "difficulty": "Advanced",
                    "est_time": "6 hrs",
                    "summary": "Mastering the live technical screen: Window function patterns (Rank, Lead/Lag, Running totals), JSON parsing in Python, and edge-case handling under time constraints.",
                    "skills": ["Technical interviews", "Live coding", "Think out loud", "Time management"],
                    "interview_question": "Explain your step-by-step thinking aloud before typing the first line of SQL code.",
                    "code_snippet": "# Recommended schedule: Solve 2 SQL problems + 1 Python data script daily for 30 days.",
                    "resources": [
                        {"title": "LeetCode Top SQL 50", "url": "https://leetcode.com/studyplan/top-sql-50/"},
                        {"title": "DataLemur SQL & DE Prep", "url": "https://datalemur.com/"}
                    ]
                }
            ]
        }
    }
}

# -----------------------------------------------------------------------------
# HELPER FUNCTIONS & METRICS
# -----------------------------------------------------------------------------

def get_all_topics() -> List[Dict[str, Any]]:
    """Returns a flat list of all topics across all phases with their metadata."""
    flat = []
    for phase_name, phase_val in ROADMAP_DATA.items():
        for cat_name, topics in phase_val["categories"].items():
            for topic in topics:
                t = topic.copy()
                t["phase"] = phase_name
                t["category"] = cat_name
                # Canonical DB task key: {phase}-{category}-{topic_name}
                t["task_key"] = f"{phase_name}-{cat_name}-{topic['name']}"
                flat.append(t)
    return flat


def get_canonical_key_mapping() -> Dict[str, str]:
    """
    Creates a mapping from any legacy or alias task key to the canonical task key.
    This guarantees 100% backward compatibility for all users.
    """
    mapping = {}
    for phase_name, phase_val in ROADMAP_DATA.items():
        for cat_name, topics in phase_val["categories"].items():
            for topic in topics:
                canon = f"{phase_name}-{cat_name}-{topic['name']}"
                mapping[canon] = canon
                for alias in topic.get("aliases", []):
                    alias_key = f"{phase_name}-{cat_name}-{alias}"
                    mapping[alias_key] = canon
    return mapping


def calculate_progress(completed_dict: Dict[str, bool]) -> Dict[str, Any]:
    """
    Calculates detailed metrics: overall progress, per-phase progress, and rank.
    """
    all_topics = get_all_topics()
    total_count = len(all_topics)
    completed_keys = set()

    canonical_map = get_canonical_key_mapping()

    for k, is_done in completed_dict.items():
        if is_done:
            # Map legacy or canonical key
            canon = canonical_map.get(k, k)
            completed_keys.add(canon)

    completed_count = sum(1 for t in all_topics if t["task_key"] in completed_keys)
    pct = round((completed_count / total_count * 100), 1) if total_count > 0 else 0.0

    # Determine Data Engineer Rank Level
    if pct < 15:
        rank = {"title": "🌱 DE Novice", "color": "#94A3B8", "desc": "Laying the CS and SQL foundation"}
    elif pct < 35:
        rank = {"title": "⚡ Data Apprentice", "color": "#3B82F6", "desc": "Mastering databases & Python scripts"}
    elif pct < 60:
        rank = {"title": "🚀 Pipeline Engineer", "color": "#F59E0B", "desc": "Building robust Spark & Big Data ETL"}
    elif pct < 85:
        rank = {"title": "💎 Senior Data Specialist", "color": "#6366F1", "desc": "Orchestrating Lakehouses, Snowflake & Kafka"}
    else:
        rank = {"title": "👑 Principal Data Architect", "color": "#10B981", "desc": "Mastery of end-to-end Data Engineering"}

    # Calculate Phase breakdown
    phase_metrics = {}
    for phase_name, phase_val in ROADMAP_DATA.items():
        p_topics = [t for t in all_topics if t["phase"] == phase_name]
        p_total = len(p_topics)
        p_done = sum(1 for t in p_topics if t["task_key"] in completed_keys)
        p_pct = round((p_done / p_total * 100), 1) if p_total > 0 else 0.0
        phase_metrics[phase_name] = {
            "total": p_total,
            "completed": p_done,
            "percentage": p_pct,
            "icon": phase_val.get("icon", "📌"),
            "color": phase_val.get("color", "#3B82F6")
        }

    return {
        "total_topics": total_count,
        "completed_count": completed_count,
        "percentage": pct,
        "rank": rank,
        "phase_metrics": phase_metrics,
        "completed_keys_set": completed_keys
    }


def search_topics(
    query: str = "",
    phase_filter: str = "All",
    difficulty_filter: str = "All",
    status_filter: str = "All",
    completed_keys: set = None
) -> List[Dict[str, Any]]:
    """Filters topics based on search keyword and criteria."""
    if completed_keys is None:
        completed_keys = set()

    all_topics = get_all_topics()
    results = []
    q = query.strip().lower()

    for t in all_topics:
        # Phase filter
        if phase_filter != "All" and t["phase"] != phase_filter:
            continue

        # Difficulty filter
        if difficulty_filter != "All" and t["difficulty"] != difficulty_filter:
            continue

        # Status filter
        is_done = t["task_key"] in completed_keys
        if status_filter == "Completed" and not is_done:
            continue
        if status_filter == "Pending" and is_done:
            continue

        # Text search
        if q:
            searchable = f"{t['name']} {t['category']} {t['phase']} {t['summary']} {' '.join(t.get('skills', []))}".lower()
            if q not in searchable:
                continue

        results.append(t)

    return results


PORTFOLIO_PROJECTS = [
    {
        "id": "proj-1",
        "title": "Modern Lakehouse Batch Pipeline (dbt + Snowflake + Airflow)",
        "level": "Intermediate",
        "tech_stack": ["Snowflake", "dbt Core", "Apache Airflow", "Docker", "Great Expectations"],
        "summary": "End-to-end production data pipeline ingesting raw transactional data into Snowflake, orchestrating daily incremental transformations with dbt, enforcing automated data quality tests, and serving dimensional marts for executive dashboards.",
        "architecture_steps": [
            "1. Raw Ingestion: Airflow Python DAG extracts daily transactions and stages into Snowflake.",
            "2. Data Cleansing: dbt staging models clean, cast types, and deduplicate records.",
            "3. Modeling: Star Schema dimensional marts (dim_customer, dim_product, fact_sales) materialized as tables.",
            "4. Testing & Alerts: Generic and singular dbt tests run automatically; Slack notifications sent on failure."
        ],
        "github_template": "https://github.com/topics/data-engineering-project"
    },
    {
        "id": "proj-2",
        "title": "Real-Time Event Streaming Pipeline (Kafka + PySpark + Delta Lake)",
        "level": "Advanced",
        "tech_stack": ["Apache Kafka", "PySpark", "Delta Lake", "Docker", "AWS S3 / MinIO"],
        "summary": "Real-time streaming analytics engine that generates high-throughput simulated clickstream events, publishes to Kafka topics, processes event windows with PySpark Structured Streaming, and writes to a Medallion Delta Lakehouse.",
        "architecture_steps": [
            "1. Event Generator: Python producer writes JSON clickstream events to a partitioned Kafka topic.",
            "2. Stream Processing: PySpark Structured Streaming reads from Kafka, validates schema with StructType, and applies watermarks.",
            "3. Medallion Storage: Micro-batch append to Delta Lake Bronze table, followed by Silver deduplication.",
            "4. Aggregations: 5-minute tumbling windows compute real-time active users and conversion metrics."
        ],
        "github_template": "https://github.com/topics/apache-kafka-project"
    },
    {
        "id": "proj-3",
        "title": "Databricks Delta Lakehouse (PySpark + Medallion + Power BI)",
        "level": "Advanced",
        "tech_stack": ["Databricks", "Delta Lake", "Unity Catalog", "PySpark", "Power BI"],
        "summary": "Production Lakehouse built on Databricks following the Medallion Architecture. Ingests raw multi-gigabyte open dataset with Auto Loader, performs Z-Ordering compaction, and serves a live Power BI dashboard via DirectQuery.",
        "architecture_steps": [
            "1. Ingestion: Databricks Auto Loader (cloudFiles) detects new incoming files in cloud storage.",
            "2. Delta Operations: Bronze -> Silver pipeline using Delta MERGE statements for Change Data Capture.",
            "3. Performance Tuning: OPTIMIZE with Z-ORDER clustering on high-frequency filter keys.",
            "4. BI Serving: Connect Databricks SQL Warehouse to Power BI for sub-second interactive analytics."
        ],
        "github_template": "https://github.com/topics/databricks"
    },
    {
        "id": "proj-4",
        "title": "Change Data Capture (CDC) with Debezium, Postgres & BigQuery",
        "level": "Expert",
        "tech_stack": ["PostgreSQL", "Debezium", "Apache Kafka", "Google BigQuery", "Terraform"],
        "summary": "Enterprise zero-downtime CDC pipeline streaming row-level inserts, updates, and deletes from a transactional PostgreSQL database into an analytical Google BigQuery warehouse in near real-time without querying the primary database.",
        "architecture_steps": [
            "1. Source DB: PostgreSQL WAL (Write-Ahead Logging) configured with logical replication.",
            "2. Capture: Debezium connector streams row-level change events to Kafka.",
            "3. Sink: Kafka Connect BigQuery sink streams changes directly into BigQuery staging tables.",
            "4. Real-time Views: BigQuery views compute latest state using QUALIFY ROW_NUMBER() = 1."
        ],
        "github_template": "https://github.com/topics/change-data-capture"
    }
]

CERTIFICATIONS_GUIDE = [
    {
        "title": "Databricks Certified Data Engineer Associate / Professional",
        "provider": "Databricks",
        "difficulty": "Intermediate - Advanced",
        "topics_covered": ["Databricks Lakehouse", "PySpark", "Delta Lake", "Unity Catalog", "Production Pipelines"],
        "url": "https://www.databricks.com/learn/certification"
    },
    {
        "title": "Snowflake SnowPro Core Certification",
        "provider": "Snowflake",
        "difficulty": "Intermediate",
        "topics_covered": ["Snowflake Architecture", "Virtual Warehouses", "Data Loading (Snowpipe)", "Security & Governance", "Time Travel & Cloning"],
        "url": "https://www.snowflake.com/en/data-cloud/workloads/training-certification/"
    },
    {
        "title": "AWS Certified Data Engineer - Associate (DEA-C01)",
        "provider": "Amazon Web Services",
        "difficulty": "Intermediate",
        "topics_covered": ["AWS S3", "AWS Glue", "Amazon Athena", "Amazon Redshift", "Amazon EMR", "Security & Governance"],
        "url": "https://aws.amazon.com/certification/certified-data-engineer-associate/"
    },
    {
        "title": "Google Cloud Professional Data Engineer",
        "provider": "Google Cloud",
        "difficulty": "Advanced",
        "topics_covered": ["BigQuery", "Dataflow (Apache Beam)", "Cloud Storage", "Cloud Composer (Airflow)", "Pub/Sub", "Data Modeling"],
        "url": "https://cloud.google.com/learn/certification/data-engineer"
    }
]
