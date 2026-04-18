#!/usr/bin/env python3
"""
Task 6: SQL Basics
Design a student database and write queries using SELECT, WHERE, GROUP BY, ORDER BY.
"""

import sqlite3
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StudentDatabase:
    """Student database with basic SQL operations."""
    
    def __init__(self, db_path: str = "data/student_database.db"):
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Connect to the database."""
        import os
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        logger.info(f"Connected to database: {self.db_path}")
    
    def disconnect(self):
        """Disconnect from the database."""
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from database")
    
    def create_schema(self):
        """Create the student database schema."""
        # Students table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS students (
                student_id TEXT PRIMARY KEY,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                date_of_birth DATE,
                enrollment_date DATE DEFAULT CURRENT_DATE,
                major TEXT,
                gpa REAL DEFAULT 0.0,
                credits_earned INTEGER DEFAULT 0,
                status TEXT DEFAULT 'active'
            )
        ''')
        
        # Courses table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS courses (
                course_id TEXT PRIMARY KEY,
                course_name TEXT NOT NULL,
                department TEXT,
                credits INTEGER,
                instructor TEXT,
                max_capacity INTEGER
            )
        ''')
        
        # Enrollments table (junction table)
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS enrollments (
                enrollment_id INTEGER PRIMARY KEY AUTOINCREMENT,
                student_id TEXT,
                course_id TEXT,
                semester TEXT,
                year INTEGER,
                grade TEXT,
                enrollment_date DATE DEFAULT CURRENT_DATE,
                FOREIGN KEY (student_id) REFERENCES students(student_id),
                FOREIGN KEY (course_id) REFERENCES courses(course_id)
            )
        ''')
        
        # Departments table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS departments (
                department_id TEXT PRIMARY KEY,
                department_name TEXT NOT NULL,
                head_instructor TEXT,
                budget REAL
            )
        ''')
        
        self.conn.commit()
        logger.info("Database schema created successfully")
    
    def insert_sample_data(self):
        """Insert sample data into the database."""
        # Sample departments
        departments = [
            ('CS', 'Computer Science', 'Dr. Smith', 500000),
            ('MATH', 'Mathematics', 'Dr. Johnson', 300000),
            ('PHYS', 'Physics', 'Dr. Williams', 400000),
            ('ENG', 'Engineering', 'Dr. Brown', 600000),
            ('BUS', 'Business', 'Dr. Davis', 450000),
        ]
        
        self.cursor.executemany('''
            INSERT OR REPLACE INTO departments (department_id, department_name, head_instructor, budget)
            VALUES (?, ?, ?, ?)
        ''', departments)
        
        # Sample students
        first_names = ['John', 'Jane', 'Michael', 'Emily', 'David', 'Sarah', 'Chris', 'Jessica',
                      'Matthew', 'Amanda', 'Daniel', 'Ashley', 'James', 'Jennifer', 'Robert']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
                     'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson']
        majors = ['Computer Science', 'Mathematics', 'Physics', 'Engineering', 'Business']
        
        students = []
        for i in range(50):
            student_id = f'S{str(i+1).zfill(4)}'
            first = random.choice(first_names)
            last = random.choice(last_names)
            email = f'{first.lower()}.{last.lower()}{i+1}@university.edu'
            dob = (datetime.now() - timedelta(days=random.randint(6570, 9490))).strftime('%Y-%m-%d')
            major = random.choice(majors)
            gpa = round(random.uniform(2.0, 4.0), 2)
            credits = random.randint(0, 120)
            status = random.choice(['active', 'active', 'active', 'inactive', 'graduated'])
            
            students.append((student_id, first, last, email, dob, major, gpa, credits, status))
        
        self.cursor.executemany('''
            INSERT OR REPLACE INTO students 
            (student_id, first_name, last_name, email, date_of_birth, major, gpa, credits_earned, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', students)
        
        # Sample courses
        courses = [
            ('CS101', 'Introduction to Programming', 'CS', 3, 'Prof. Anderson', 50),
            ('CS201', 'Data Structures', 'CS', 3, 'Prof. Baker', 40),
            ('CS301', 'Database Systems', 'CS', 3, 'Prof. Carter', 35),
            ('CS401', 'Machine Learning', 'CS', 4, 'Prof. Davis', 30),
            ('MATH101', 'Calculus I', 'MATH', 4, 'Prof. Evans', 60),
            ('MATH201', 'Linear Algebra', 'MATH', 3, 'Prof. Foster', 45),
            ('PHYS101', 'Physics I', 'PHYS', 4, 'Prof. Green', 55),
            ('PHYS201', 'Quantum Mechanics', 'PHYS', 4, 'Prof. Harris', 25),
            ('ENG101', 'Engineering Principles', 'ENG', 3, 'Prof. Irving', 40),
            ('BUS101', 'Business Fundamentals', 'BUS', 3, 'Prof. Jackson', 50),
        ]
        
        self.cursor.executemany('''
            INSERT OR REPLACE INTO courses 
            (course_id, course_name, department, credits, instructor, max_capacity)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', courses)
        
        # Sample enrollments
        grades = ['A', 'A-', 'B+', 'B', 'B-', 'C+', 'C', 'C-', 'D', 'F', None]
        semesters = ['Fall', 'Spring', 'Summer']
        
        enrollments = []
        for student_id in [s[0] for s in students]:
            num_courses = random.randint(2, 5)
            selected_courses = random.sample([c[0] for c in courses], num_courses)
            
            for course_id in selected_courses:
                semester = random.choice(semesters)
                year = random.randint(2020, 2024)
                grade = random.choice(grades)
                enrollments.append((student_id, course_id, semester, year, grade))
        
        self.cursor.executemany('''
            INSERT INTO enrollments (student_id, course_id, semester, year, grade)
            VALUES (?, ?, ?, ?, ?)
        ''', enrollments)
        
        self.conn.commit()
        logger.info(f"Inserted {len(students)} students, {len(courses)} courses, {len(enrollments)} enrollments")
    
    def execute_query(self, query: str, params: tuple = ()) -> List[Dict]:
        """Execute a SQL query and return results as list of dictionaries."""
        try:
            self.cursor.execute(query, params)
            rows = self.cursor.fetchall()
            return [dict(row) for row in rows]
        except sqlite3.Error as e:
            logger.error(f"Query error: {e}")
            return []
    
    def demonstrate_select(self) -> Dict[str, List[Dict]]:
        """Demonstrate SELECT queries."""
        results = {}
        
        # Basic SELECT
        results['all_students'] = self.execute_query('''
            SELECT student_id, first_name, last_name, major, gpa 
            FROM students 
            LIMIT 10
        ''')
        
        # SELECT with column aliases
        results['student_names'] = self.execute_query('''
            SELECT 
                first_name || ' ' || last_name AS full_name,
                email,
                major AS field_of_study
            FROM students
            LIMIT 5
        ''')
        
        # SELECT DISTINCT
        results['unique_majors'] = self.execute_query('''
            SELECT DISTINCT major FROM students ORDER BY major
        ''')
        
        # SELECT with calculations
        results['student_performance'] = self.execute_query('''
            SELECT 
                student_id,
                first_name,
                gpa,
                credits_earned,
                CASE 
                    WHEN gpa >= 3.5 THEN 'Excellent'
                    WHEN gpa >= 3.0 THEN 'Good'
                    WHEN gpa >= 2.5 THEN 'Average'
                    ELSE 'Below Average'
                END AS performance_level
            FROM students
            LIMIT 10
        ''')
        
        return results
    
    def demonstrate_where(self) -> Dict[str, List[Dict]]:
        """Demonstrate WHERE clause."""
        results = {}
        
        # Simple WHERE
        results['cs_students'] = self.execute_query('''
            SELECT * FROM students WHERE major = 'Computer Science' LIMIT 5
        ''')
        
        # WHERE with comparison operators
        results['high_gpa_students'] = self.execute_query('''
            SELECT first_name, last_name, gpa 
            FROM students 
            WHERE gpa >= 3.5
            ORDER BY gpa DESC
            LIMIT 10
        ''')
        
        # WHERE with AND/OR
        results['cs_high_achievers'] = self.execute_query('''
            SELECT first_name, last_name, major, gpa
            FROM students
            WHERE major = 'Computer Science' AND gpa >= 3.5
            LIMIT 5
        ''')
        
        # WHERE with IN
        results['engineering_business_students'] = self.execute_query('''
            SELECT first_name, last_name, major
            FROM students
            WHERE major IN ('Engineering', 'Business')
            LIMIT 5
        ''')
        
        # WHERE with BETWEEN
        results['mid_gpa_students'] = self.execute_query('''
            SELECT first_name, last_name, gpa
            FROM students
            WHERE gpa BETWEEN 2.5 AND 3.5
            LIMIT 5
        ''')
        
        # WHERE with LIKE
        results['students_with_j'] = self.execute_query('''
            SELECT first_name, last_name
            FROM students
            WHERE first_name LIKE 'J%'
            LIMIT 5
        ''')
        
        return results
    
    def demonstrate_group_by(self) -> Dict[str, List[Dict]]:
        """Demonstrate GROUP BY clause."""
        results = {}
        
        # GROUP BY with COUNT
        results['students_per_major'] = self.execute_query('''
            SELECT 
                major,
                COUNT(*) AS student_count
            FROM students
            GROUP BY major
            ORDER BY student_count DESC
        ''')
        
        # GROUP BY with AVG
        results['avg_gpa_by_major'] = self.execute_query('''
            SELECT 
                major,
                AVG(gpa) AS average_gpa,
                MIN(gpa) AS min_gpa,
                MAX(gpa) AS max_gpa
            FROM students
            GROUP BY major
            ORDER BY average_gpa DESC
        ''')
        
        # GROUP BY with HAVING
        results['popular_majors'] = self.execute_query('''
            SELECT 
                major,
                COUNT(*) AS student_count,
                AVG(gpa) AS avg_gpa
            FROM students
            GROUP BY major
            HAVING COUNT(*) >= 5
            ORDER BY student_count DESC
        ''')
        
        # GROUP BY with multiple aggregations
        results['grade_distribution'] = self.execute_query('''
            SELECT 
                grade,
                COUNT(*) AS count,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM enrollments WHERE grade IS NOT NULL), 2) AS percentage
            FROM enrollments
            WHERE grade IS NOT NULL
            GROUP BY grade
            ORDER BY count DESC
        ''')
        
        return results
    
    def demonstrate_order_by(self) -> Dict[str, List[Dict]]:
        """Demonstrate ORDER BY clause."""
        results = {}
        
        # ORDER BY single column
        results['students_by_gpa'] = self.execute_query('''
            SELECT first_name, last_name, gpa
            FROM students
            ORDER BY gpa DESC
            LIMIT 10
        ''')
        
        # ORDER BY multiple columns
        results['students_by_major_gpa'] = self.execute_query('''
            SELECT major, first_name, last_name, gpa
            FROM students
            ORDER BY major ASC, gpa DESC
            LIMIT 10
        ''')
        
        # ORDER BY with NULLS handling
        results['enrollments_by_grade'] = self.execute_query('''
            SELECT student_id, course_id, grade
            FROM enrollments
            ORDER BY grade IS NULL, grade ASC
            LIMIT 10
        ''')
        
        # ORDER BY with calculated column
        results['students_by_performance'] = self.execute_query('''
            SELECT 
                first_name,
                last_name,
                gpa,
                credits_earned,
                (gpa * credits_earned) AS performance_score
            FROM students
            ORDER BY performance_score DESC
            LIMIT 10
        ''')
        
        return results
    
    def run_all_demos(self) -> Dict:
        """Run all SQL demonstration queries."""
        self.connect()
        self.create_schema()
        self.insert_sample_data()
        
        results = {
            'select_queries': self.demonstrate_select(),
            'where_queries': self.demonstrate_where(),
            'group_by_queries': self.demonstrate_group_by(),
            'order_by_queries': self.demonstrate_order_by()
        }
        
        self.disconnect()
        return results


def main():
    """Main execution for Task 6."""
    db = StudentDatabase()
    results = db.run_all_demos()
    
    logger.info("Task 6 completed successfully!")
    
    print("\n=== SELECT Queries ===")
    print(f"Unique Majors: {[m['major'] for m in results['select_queries']['unique_majors']]}")
    
    print("\n=== WHERE Queries ===")
    print(f"CS Students with GPA >= 3.5: {len(results['where_queries']['cs_high_achievers'])} students")
    
    print("\n=== GROUP BY Queries ===")
    for row in results['group_by_queries']['students_per_major']:
        print(f"  {row['major']}: {row['student_count']} students")
    
    print("\n=== ORDER BY Queries ===")
    print("Top 3 Students by GPA:")
    for i, student in enumerate(results['order_by_queries']['students_by_gpa'][:3], 1):
        print(f"  {i}. {student['first_name']} {student['last_name']}: {student['gpa']}")
    
    return results


if __name__ == "__main__":
    main()
