#!/usr/bin/env python3
"""
Task 7: Advanced SQL
Implement joins, subqueries, window functions for business reporting scenarios.
"""

import sqlite3
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AdvancedSQLDatabase:
    """Advanced SQL operations for resume analysis reporting."""
    
    def __init__(self, db_path: str = "data/advanced_resume_db.db"):
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
        """Create the advanced database schema."""
        # Candidates table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS candidates (
                candidate_id TEXT PRIMARY KEY,
                full_name TEXT NOT NULL,
                email TEXT UNIQUE,
                phone TEXT,
                location TEXT,
                years_experience INTEGER,
                current_salary INTEGER,
                expected_salary INTEGER,
                notice_period_days INTEGER,
                status TEXT DEFAULT 'active'
            )
        ''')
        
        # Skills table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS skills (
                skill_id INTEGER PRIMARY KEY AUTOINCREMENT,
                skill_name TEXT UNIQUE NOT NULL,
                category TEXT,
                demand_level TEXT
            )
        ''')
        
        # Candidate Skills junction
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS candidate_skills (
                candidate_id TEXT,
                skill_id INTEGER,
                proficiency_level INTEGER CHECK (proficiency_level BETWEEN 1 AND 5),
                years_experience INTEGER,
                PRIMARY KEY (candidate_id, skill_id),
                FOREIGN KEY (candidate_id) REFERENCES candidates(candidate_id),
                FOREIGN KEY (skill_id) REFERENCES skills(skill_id)
            )
        ''')
        
        # Job Postings
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS job_postings (
                job_id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                company TEXT,
                department TEXT,
                location TEXT,
                min_salary INTEGER,
                max_salary INTEGER,
                required_experience INTEGER,
                posting_date DATE,
                status TEXT DEFAULT 'open'
            )
        ''')
        
        # Job Requirements
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS job_requirements (
                requirement_id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT,
                skill_id INTEGER,
                required_level INTEGER,
                is_mandatory BOOLEAN,
                FOREIGN KEY (job_id) REFERENCES job_postings(job_id),
                FOREIGN KEY (skill_id) REFERENCES skills(skill_id)
            )
        ''')
        
        # Applications
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS applications (
                application_id INTEGER PRIMARY KEY AUTOINCREMENT,
                candidate_id TEXT,
                job_id TEXT,
                application_date DATE DEFAULT CURRENT_DATE,
                match_score REAL,
                status TEXT DEFAULT 'pending',
                FOREIGN KEY (candidate_id) REFERENCES candidates(candidate_id),
                FOREIGN KEY (job_id) REFERENCES job_postings(job_id)
            )
        ''')
        
        self.conn.commit()
        logger.info("Advanced schema created successfully")
    
    def insert_sample_data(self):
        """Insert sample data for advanced queries."""
        # Skills
        skills = [
            ('Python', 'Programming', 'High'),
            ('SQL', 'Database', 'High'),
            ('Spark', 'Big Data', 'High'),
            ('AWS', 'Cloud', 'High'),
            ('Azure', 'Cloud', 'Medium'),
            ('Docker', 'DevOps', 'Medium'),
            ('Kubernetes', 'DevOps', 'High'),
            ('Machine Learning', 'AI/ML', 'High'),
            ('Deep Learning', 'AI/ML', 'Medium'),
            ('Tableau', 'Visualization', 'Medium'),
            ('Java', 'Programming', 'Medium'),
            ('Scala', 'Programming', 'Low'),
            ('Git', 'Tools', 'High'),
            ('Linux', 'Systems', 'Medium'),
            ('Airflow', 'Orchestration', 'Medium'),
        ]
        
        self.cursor.executemany('''
            INSERT OR REPLACE INTO skills (skill_name, category, demand_level)
            VALUES (?, ?, ?)
        ''', skills)
        
        # Candidates
        locations = ['New York', 'San Francisco', 'Seattle', 'Austin', 'Boston', 'Remote']
        first_names = ['John', 'Jane', 'Michael', 'Emily', 'David', 'Sarah', 'Chris', 'Jessica']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller']
        
        candidates = []
        for i in range(30):
            candidate_id = f'CAND{str(i+1).zfill(4)}'
            full_name = f'{random.choice(first_names)} {random.choice(last_names)}'
            email = f'candidate{i+1}@email.com'
            phone = f'555-{random.randint(1000, 9999)}'
            location = random.choice(locations)
            years_exp = random.randint(1, 15)
            current_sal = random.randint(50000, 150000)
            expected_sal = current_sal + random.randint(10000, 50000)
            notice = random.choice([15, 30, 60, 90])
            
            candidates.append((candidate_id, full_name, email, phone, location, 
                             years_exp, current_sal, expected_sal, notice))
        
        self.cursor.executemany('''
            INSERT OR REPLACE INTO candidates 
            (candidate_id, full_name, email, phone, location, years_experience, 
             current_salary, expected_salary, notice_period_days)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', candidates)
        
        # Candidate Skills
        candidate_skills = []
        for candidate_id in [c[0] for c in candidates]:
            num_skills = random.randint(3, 8)
            skill_ids = random.sample(range(1, len(skills) + 1), num_skills)
            for skill_id in skill_ids:
                proficiency = random.randint(2, 5)
                years = random.randint(1, 10)
                candidate_skills.append((candidate_id, skill_id, proficiency, years))
        
        self.cursor.executemany('''
            INSERT OR REPLACE INTO candidate_skills 
            (candidate_id, skill_id, proficiency_level, years_experience)
            VALUES (?, ?, ?, ?)
        ''', candidate_skills)
        
        # Job Postings
        job_titles = [
            ('Data Engineer', 'Engineering', 3, 80000, 140000),
            ('Senior Data Engineer', 'Engineering', 5, 120000, 180000),
            ('ML Engineer', 'AI/ML', 4, 100000, 160000),
            ('Data Scientist', 'Data Science', 3, 90000, 150000),
            ('Cloud Architect', 'Cloud', 7, 130000, 200000),
            ('DevOps Engineer', 'DevOps', 4, 90000, 140000),
        ]
        
        companies = ['TechCorp', 'DataSystems', 'CloudNative', 'AI Solutions', 'BigData Inc']
        
        jobs = []
        for i, (title, dept, exp, min_sal, max_sal) in enumerate(job_titles):
            job_id = f'JOB{str(i+1).zfill(4)}'
            company = random.choice(companies)
            location = random.choice(locations)
            posting_date = (datetime.now() - timedelta(days=random.randint(1, 60))).strftime('%Y-%m-%d')
            
            jobs.append((job_id, title, company, dept, location, min_sal, max_sal, exp, posting_date))
        
        self.cursor.executemany('''
            INSERT OR REPLACE INTO job_postings 
            (job_id, title, company, department, location, min_salary, max_salary, required_experience, posting_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', jobs)
        
        # Job Requirements
        job_reqs = []
        for job in jobs:
            job_id = job[0]
            required_skills = random.sample(range(1, len(skills) + 1), random.randint(3, 6))
            for skill_id in required_skills:
                req_level = random.randint(3, 5)
                is_mandatory = random.choice([True, False])
                job_reqs.append((job_id, skill_id, req_level, is_mandatory))
        
        self.cursor.executemany('''
            INSERT OR REPLACE INTO job_requirements 
            (job_id, skill_id, required_level, is_mandatory)
            VALUES (?, ?, ?, ?)
        ''', job_reqs)
        
        # Applications
        applications = []
        for candidate_id in [c[0] for c in candidates]:
            num_apps = random.randint(1, 4)
            applied_jobs = random.sample([j[0] for j in jobs], num_apps)
            for job_id in applied_jobs:
                app_date = (datetime.now() - timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d')
                match_score = round(random.uniform(40, 95), 2)
                status = random.choice(['pending', 'screening', 'interview', 'rejected', 'hired'])
                applications.append((candidate_id, job_id, app_date, match_score, status))
        
        self.cursor.executemany('''
            INSERT OR REPLACE INTO applications 
            (candidate_id, job_id, application_date, match_score, status)
            VALUES (?, ?, ?, ?, ?)
        ''', applications)
        
        self.conn.commit()
        logger.info("Sample data inserted successfully")
    
    def execute_query(self, query: str, params: tuple = ()) -> List[Dict]:
        """Execute a SQL query and return results."""
        try:
            self.cursor.execute(query, params)
            rows = self.cursor.fetchall()
            return [dict(row) for row in rows]
        except sqlite3.Error as e:
            logger.error(f"Query error: {e}")
            return []
    
    def demonstrate_joins(self) -> Dict[str, List[Dict]]:
        """Demonstrate various JOIN operations."""
        results = {}
        
        # INNER JOIN - Candidates with their skills
        results['inner_join'] = self.execute_query('''
            SELECT 
                c.candidate_id,
                c.full_name,
                s.skill_name,
                s.category,
                cs.proficiency_level
            FROM candidates c
            INNER JOIN candidate_skills cs ON c.candidate_id = cs.candidate_id
            INNER JOIN skills s ON cs.skill_id = s.skill_id
            ORDER BY c.candidate_id, s.skill_name
            LIMIT 10
        ''')
        
        # LEFT JOIN - All candidates with their skills (including those without skills)
        results['left_join'] = self.execute_query('''
            SELECT 
                c.candidate_id,
                c.full_name,
                COUNT(cs.skill_id) AS skill_count
            FROM candidates c
            LEFT JOIN candidate_skills cs ON c.candidate_id = cs.candidate_id
            GROUP BY c.candidate_id, c.full_name
            ORDER BY skill_count DESC
            LIMIT 10
        ''')
        
        # MULTIPLE JOINS - Complete candidate-job-skill view
        results['multiple_joins'] = self.execute_query('''
            SELECT 
                c.full_name,
                j.title AS job_title,
                j.company,
                a.match_score,
                a.status AS application_status
            FROM applications a
            INNER JOIN candidates c ON a.candidate_id = c.candidate_id
            INNER JOIN job_postings j ON a.job_id = j.job_id
            ORDER BY a.match_score DESC
            LIMIT 10
        ''')
        
        # SELF JOIN - Compare candidates in same location
        results['self_join'] = self.execute_query('''
            SELECT 
                c1.full_name AS candidate1,
                c2.full_name AS candidate2,
                c1.location,
                c1.years_experience AS exp1,
                c2.years_experience AS exp2
            FROM candidates c1
            INNER JOIN candidates c2 ON c1.location = c2.location AND c1.candidate_id < c2.candidate_id
            ORDER BY c1.location
            LIMIT 10
        ''')
        
        # CROSS JOIN - All possible candidate-job combinations
        results['cross_join'] = self.execute_query('''
            SELECT 
                c.full_name,
                j.title,
                j.company,
                CASE 
                    WHEN c.expected_salary BETWEEN j.min_salary AND j.max_salary THEN 'Salary Match'
                    ELSE 'Salary Mismatch'
                END AS salary_compatibility
            FROM candidates c
            CROSS JOIN job_postings j
            WHERE c.years_experience >= j.required_experience
            LIMIT 10
        ''')
        
        return results
    
    def demonstrate_subqueries(self) -> Dict[str, List[Dict]]:
        """Demonstrate subqueries."""
        results = {}
        
        # Subquery in SELECT - Average salary comparison
        results['subquery_select'] = self.execute_query('''
            SELECT 
                full_name,
                expected_salary,
                (SELECT AVG(expected_salary) FROM candidates) AS avg_expected,
                expected_salary - (SELECT AVG(expected_salary) FROM candidates) AS diff_from_avg
            FROM candidates
            ORDER BY diff_from_avg DESC
            LIMIT 10
        ''')
        
        # Subquery in WHERE - Candidates with above-average skills
        results['subquery_where'] = self.execute_query('''
            SELECT 
                c.candidate_id,
                c.full_name,
                COUNT(cs.skill_id) AS skill_count
            FROM candidates c
            INNER JOIN candidate_skills cs ON c.candidate_id = cs.candidate_id
            GROUP BY c.candidate_id, c.full_name
            HAVING skill_count > (
                SELECT AVG(skill_count) 
                FROM (
                    SELECT COUNT(skill_id) AS skill_count 
                    FROM candidate_skills 
                    GROUP BY candidate_id
                )
            )
            ORDER BY skill_count DESC
        ''')
        
        # Subquery in FROM - Skill statistics per candidate
        results['subquery_from'] = self.execute_query('''
            SELECT 
                skill_stats.candidate_id,
                c.full_name,
                skill_stats.total_skills,
                skill_stats.avg_proficiency,
                skill_stats.max_proficiency
            FROM (
                SELECT 
                    candidate_id,
                    COUNT(*) AS total_skills,
                    AVG(proficiency_level) AS avg_proficiency,
                    MAX(proficiency_level) AS max_proficiency
                FROM candidate_skills
                GROUP BY candidate_id
            ) skill_stats
            INNER JOIN candidates c ON skill_stats.candidate_id = c.candidate_id
            ORDER BY skill_stats.avg_proficiency DESC
            LIMIT 10
        ''')
        
        # Correlated subquery - Candidates with matching job requirements
        results['correlated_subquery'] = self.execute_query('''
            SELECT 
                c.full_name,
                j.title,
                j.company,
                (
                    SELECT COUNT(*)
                    FROM candidate_skills cs
                    INNER JOIN job_requirements jr ON cs.skill_id = jr.skill_id
                    WHERE cs.candidate_id = c.candidate_id 
                    AND jr.job_id = j.job_id
                    AND cs.proficiency_level >= jr.required_level
                ) AS matching_skills
            FROM applications a
            INNER JOIN candidates c ON a.candidate_id = c.candidate_id
            INNER JOIN job_postings j ON a.job_id = j.job_id
            ORDER BY matching_skills DESC
            LIMIT 10
        ''')
        
        return results
    
    def demonstrate_window_functions(self) -> Dict[str, List[Dict]]:
        """Demonstrate window functions (SQLite compatible)."""
        results = {}
        
        # ROW_NUMBER - Rank candidates by experience within each location
        results['row_number'] = self.execute_query('''
            SELECT 
                full_name,
                location,
                years_experience,
                ROW_NUMBER() OVER (PARTITION BY location ORDER BY years_experience DESC) AS experience_rank
            FROM candidates
            ORDER BY location, experience_rank
            LIMIT 15
        ''')
        
        # RANK - Rank applications by match score
        results['rank'] = self.execute_query('''
            SELECT 
                c.full_name,
                j.title,
                a.match_score,
                RANK() OVER (ORDER BY a.match_score DESC) AS score_rank
            FROM applications a
            INNER JOIN candidates c ON a.candidate_id = c.candidate_id
            INNER JOIN job_postings j ON a.job_id = j.job_id
            LIMIT 10
        ''')
        
        # Running total - Cumulative applications per job
        results['running_total'] = self.execute_query('''
            SELECT 
                j.title,
                a.application_date,
                COUNT(*) OVER (
                    PARTITION BY j.job_id 
                    ORDER BY a.application_date 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_apps
            FROM applications a
            INNER JOIN job_postings j ON a.job_id = j.job_id
            ORDER BY j.title, a.application_date
            LIMIT 15
        ''')
        
        # Moving average - Average match score over last 5 applications
        results['moving_average'] = self.execute_query('''
            SELECT 
                c.full_name,
                j.title,
                a.match_score,
                AVG(a.match_score) OVER (
                    ORDER BY a.application_date
                    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                ) AS moving_avg_score
            FROM applications a
            INNER JOIN candidates c ON a.candidate_id = c.candidate_id
            INNER JOIN job_postings j ON a.job_id = j.job_id
            ORDER BY a.application_date
            LIMIT 15
        ''')
        
        # LAG/LEAD - Compare with previous application
        results['lag_lead'] = self.execute_query('''
            SELECT 
                c.full_name,
                a.application_date,
                a.match_score,
                LAG(a.match_score, 1) OVER (
                    PARTITION BY a.candidate_id 
                    ORDER BY a.application_date
                ) AS prev_score,
                a.match_score - LAG(a.match_score, 1) OVER (
                    PARTITION BY a.candidate_id 
                    ORDER BY a.application_date
                ) AS score_change
            FROM applications a
            INNER JOIN candidates c ON a.candidate_id = c.candidate_id
            ORDER BY c.full_name, a.application_date
            LIMIT 15
        ''')
        
        # NTILE - Quartile distribution
        results['ntile'] = self.execute_query('''
            SELECT 
                full_name,
                expected_salary,
                NTILE(4) OVER (ORDER BY expected_salary) AS salary_quartile
            FROM candidates
            ORDER BY expected_salary
            LIMIT 15
        ''')
        
        return results
    
    def business_reports(self) -> Dict[str, List[Dict]]:
        """Generate business reports using advanced SQL."""
        results = {}
        
        # Report 1: Top candidates per job
        results['top_candidates_per_job'] = self.execute_query('''
            SELECT 
                j.title,
                j.company,
                c.full_name,
                a.match_score,
                RANK() OVER (PARTITION BY j.job_id ORDER BY a.match_score DESC) AS candidate_rank
            FROM applications a
            INNER JOIN candidates c ON a.candidate_id = c.candidate_id
            INNER JOIN job_postings j ON a.job_id = j.job_id
            QUALIFY candidate_rank <= 3
            ORDER BY j.title, candidate_rank
        ''')
        
        # Report 2: Skill gap analysis
        results['skill_gap_analysis'] = self.execute_query('''
            SELECT 
                j.title,
                s.skill_name,
                jr.required_level,
                COUNT(DISTINCT c.candidate_id) AS qualified_candidates
            FROM job_requirements jr
            INNER JOIN job_postings j ON jr.job_id = j.job_id
            INNER JOIN skills s ON jr.skill_id = s.skill_id
            LEFT JOIN candidate_skills cs ON jr.skill_id = cs.skill_id 
                AND cs.proficiency_level >= jr.required_level
            LEFT JOIN candidates c ON cs.candidate_id = c.candidate_id
            WHERE jr.is_mandatory = 1
            GROUP BY j.title, s.skill_name, jr.required_level
            ORDER BY j.title, qualified_candidates ASC
        ''')
        
        # Report 3: Hiring funnel
        results['hiring_funnel'] = self.execute_query('''
            SELECT 
                status,
                COUNT(*) AS count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage,
                SUM(COUNT(*)) OVER (ORDER BY 
                    CASE status 
                        WHEN 'pending' THEN 1 
                        WHEN 'screening' THEN 2 
                        WHEN 'interview' THEN 3 
                        WHEN 'hired' THEN 4 
                        WHEN 'rejected' THEN 5 
                    END
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative
            FROM applications
            GROUP BY status
            ORDER BY cumulative
        ''')
        
        return results
    
    def run_all_demos(self) -> Dict:
        """Run all advanced SQL demonstrations."""
        self.connect()
        self.create_schema()
        self.insert_sample_data()
        
        results = {
            'joins': self.demonstrate_joins(),
            'subqueries': self.demonstrate_subqueries(),
            'window_functions': self.demonstrate_window_functions(),
            'business_reports': self.business_reports()
        }
        
        self.disconnect()
        return results


def main():
    """Main execution for Task 7."""
    db = AdvancedSQLDatabase()
    results = db.run_all_demos()
    
    logger.info("Task 7 completed successfully!")
    
    print("\n=== JOIN Operations ===")
    print(f"Inner Join results: {len(results['joins']['inner_join'])} rows")
    print(f"Multiple Joins results: {len(results['joins']['multiple_joins'])} rows")
    
    print("\n=== Subqueries ===")
    print(f"Subquery in WHERE: {len(results['subqueries']['subquery_where'])} candidates above average")
    
    print("\n=== Window Functions ===")
    print("Top 3 by experience rank:")
    for row in results['window_functions']['row_number'][:3]:
        print(f"  {row['full_name']} ({row['location']}): {row['years_experience']} years")
    
    print("\n=== Business Reports ===")
    print("Hiring Funnel:")
    for row in results['business_reports']['hiring_funnel']:
        print(f"  {row['status']}: {row['count']} ({row['percentage']}%)")
    
    return results


if __name__ == "__main__":
    main()
