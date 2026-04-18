#!/usr/bin/env python3
"""
Task 8: Database Concepts
Compare OLTP vs OLAP systems by designing schemas and use cases.
"""

import sqlite3
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseConceptsDemo:
    """Demonstrate OLTP vs OLAP database concepts."""
    
    def __init__(self):
        self.oltp_db_path = "data/oltp_system.db"
        self.olap_db_path = "data/olap_system.db"
        
    def create_oltp_schema(self, conn):
        """Create OLTP (Online Transaction Processing) schema - normalized for transactions."""
        cursor = conn.cursor()
        
        # Customers table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS customers (
                customer_id TEXT PRIMARY KEY,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                phone TEXT,
                address TEXT,
                city TEXT,
                country TEXT,
                registration_date DATE DEFAULT CURRENT_DATE,
                is_active BOOLEAN DEFAULT 1
            )
        ''')
        
        # Products table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS products (
                product_id TEXT PRIMARY KEY,
                product_name TEXT NOT NULL,
                description TEXT,
                category_id TEXT,
                supplier_id TEXT,
                unit_price REAL NOT NULL,
                stock_quantity INTEGER DEFAULT 0,
                reorder_level INTEGER DEFAULT 10,
                is_active BOOLEAN DEFAULT 1
            )
        ''')
        
        # Categories table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS categories (
                category_id TEXT PRIMARY KEY,
                category_name TEXT NOT NULL,
                description TEXT
            )
        ''')
        
        # Orders table (header)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS orders (
                order_id TEXT PRIMARY KEY,
                customer_id TEXT,
                order_date DATE DEFAULT CURRENT_DATE,
                ship_date DATE,
                ship_address TEXT,
                ship_city TEXT,
                ship_country TEXT,
                shipping_cost REAL DEFAULT 0,
                status TEXT DEFAULT 'pending',
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
            )
        ''')
        
        # Order Details table (line items)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS order_details (
                detail_id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT,
                product_id TEXT,
                quantity INTEGER NOT NULL,
                unit_price REAL NOT NULL,
                discount REAL DEFAULT 0,
                FOREIGN KEY (order_id) REFERENCES orders(order_id),
                FOREIGN KEY (product_id) REFERENCES products(product_id)
            )
        ''')
        
        # Inventory Transactions
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS inventory_transactions (
                transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id TEXT,
                transaction_type TEXT,  -- 'in', 'out', 'adjustment'
                quantity INTEGER,
                transaction_date DATE DEFAULT CURRENT_DATE,
                notes TEXT,
                FOREIGN KEY (product_id) REFERENCES products(product_id)
            )
        ''')
        
        conn.commit()
        logger.info("OLTP schema created (normalized for transactions)")
    
    def create_olap_schema(self, conn):
        """Create OLAP (Online Analytical Processing) schema - denormalized for analytics."""
        cursor = conn.cursor()
        
        # Date Dimension
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dim_date (
                date_key INTEGER PRIMARY KEY,
                full_date DATE,
                day_of_week INTEGER,
                day_name TEXT,
                day_of_month INTEGER,
                day_of_year INTEGER,
                week_of_year INTEGER,
                month_number INTEGER,
                month_name TEXT,
                quarter INTEGER,
                year INTEGER,
                fiscal_quarter INTEGER,
                is_weekend BOOLEAN,
                is_holiday BOOLEAN
            )
        ''')
        
        # Customer Dimension
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dim_customer (
                customer_key INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id TEXT,
                first_name TEXT,
                last_name TEXT,
                email TEXT,
                city TEXT,
                country TEXT,
                customer_segment TEXT,
                registration_date DATE,
                is_current BOOLEAN DEFAULT 1,
                effective_date DATE,
                expiration_date DATE
            )
        ''')
        
        # Product Dimension
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dim_product (
                product_key INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id TEXT,
                product_name TEXT,
                category_name TEXT,
                supplier_name TEXT,
                unit_cost REAL,
                is_current BOOLEAN DEFAULT 1,
                effective_date DATE,
                expiration_date DATE
            )
        ''')
        
        # Sales Fact Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fact_sales (
                sale_key INTEGER PRIMARY KEY AUTOINCREMENT,
                date_key INTEGER,
                customer_key INTEGER,
                product_key INTEGER,
                order_id TEXT,
                quantity INTEGER,
                unit_price REAL,
                discount_amount REAL,
                sales_amount REAL,
                cost_amount REAL,
                profit_amount REAL,
                FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
                FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
                FOREIGN KEY (product_key) REFERENCES dim_product(product_key)
            )
        ''')
        
        # Inventory Fact Table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fact_inventory (
                inventory_key INTEGER PRIMARY KEY AUTOINCREMENT,
                date_key INTEGER,
                product_key INTEGER,
                quantity_on_hand INTEGER,
                quantity_reserved INTEGER,
                quantity_available INTEGER,
                inventory_value REAL,
                FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
                FOREIGN KEY (product_key) REFERENCES dim_product(product_key)
            )
        ''')
        
        # Aggregated Sales Summary (for faster queries)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS agg_sales_monthly (
                year INTEGER,
                month INTEGER,
                category_name TEXT,
                country TEXT,
                total_sales REAL,
                total_quantity INTEGER,
                total_profit REAL,
                order_count INTEGER,
                PRIMARY KEY (year, month, category_name, country)
            )
        ''')
        
        conn.commit()
        logger.info("OLAP schema created (star schema for analytics)")
    
    def insert_oltp_data(self, conn):
        """Insert sample data into OLTP database."""
        cursor = conn.cursor()
        
        # Categories
        categories = [
            ('CAT001', 'Electronics', 'Electronic devices and accessories'),
            ('CAT002', 'Clothing', 'Apparel and fashion items'),
            ('CAT003', 'Books', 'Physical and digital books'),
            ('CAT004', 'Home & Garden', 'Home improvement and garden supplies'),
            ('CAT005', 'Sports', 'Sports equipment and accessories'),
        ]
        cursor.executemany('INSERT OR REPLACE INTO categories VALUES (?, ?, ?)', categories)
        
        # Products
        products = []
        product_names = {
            'CAT001': ['Laptop', 'Smartphone', 'Headphones', 'Tablet', 'Smartwatch'],
            'CAT002': ['T-Shirt', 'Jeans', 'Jacket', 'Shoes', 'Hat'],
            'CAT003': ['Novel', 'Textbook', 'Magazine', 'E-Book Reader', 'Cookbook'],
            'CAT004': ['Furniture', 'Decor', 'Kitchen', 'Garden Tools', 'Lighting'],
            'CAT005': ['Fitness Equipment', 'Outdoor Gear', 'Team Sports', 'Water Sports', 'Cycling'],
        }
        
        product_id = 1
        for cat_id, cat_name, _ in categories:
            for prod_name in product_names[cat_id]:
                products.append((
                    f'PROD{str(product_id).zfill(4)}',
                    prod_name,
                    f'{prod_name} description',
                    cat_id,
                    f'SUP{random.randint(1, 10)}',
                    round(random.uniform(10, 1000), 2),
                    random.randint(50, 500),
                    random.randint(10, 50)
                ))
                product_id += 1
        
        cursor.executemany('''
            INSERT OR REPLACE INTO products 
            (product_id, product_name, description, category_id, supplier_id, unit_price, stock_quantity, reorder_level)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', products)
        
        # Customers
        first_names = ['John', 'Jane', 'Michael', 'Emily', 'David', 'Sarah', 'Chris', 'Jessica']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller']
        cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia']
        countries = ['USA', 'Canada', 'UK', 'Germany', 'France']
        
        customers = []
        for i in range(100):
            customers.append((
                f'CUST{str(i+1).zfill(4)}',
                random.choice(first_names),
                random.choice(last_names),
                f'customer{i+1}@email.com',
                f'555-{random.randint(1000, 9999)}',
                f'{random.randint(100, 9999)} Main St',
                random.choice(cities),
                random.choice(countries),
                (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d'),
                random.choice([True, True, True, False])  # 75% active
            ))
        
        cursor.executemany('''
            INSERT OR REPLACE INTO customers 
            (customer_id, first_name, last_name, email, phone, address, city, country, registration_date, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', customers)
        
        # Orders and Order Details
        order_statuses = ['pending', 'processing', 'shipped', 'delivered', 'cancelled']
        
        for i in range(500):
            order_id = f'ORD{str(i+1).zfill(6)}'
            customer_id = f'CUST{random.randint(1, 100):04d}'
            order_date = datetime.now() - timedelta(days=random.randint(1, 180))
            ship_date = order_date + timedelta(days=random.randint(1, 7))
            status = random.choice(order_statuses)
            
            cursor.execute('''
                INSERT OR REPLACE INTO orders 
                (order_id, customer_id, order_date, ship_date, ship_address, ship_city, ship_country, shipping_cost, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                order_id, customer_id, order_date.strftime('%Y-%m-%d'), 
                ship_date.strftime('%Y-%m-%d'), 'Shipping Address', 
                random.choice(cities), random.choice(countries),
                round(random.uniform(5, 50), 2), status
            ))
            
            # Order details
            num_items = random.randint(1, 5)
            for _ in range(num_items):
                product = random.choice(products)
                quantity = random.randint(1, 10)
                unit_price = product[5]
                discount = random.choice([0, 0, 0, 0.1, 0.2])
                
                cursor.execute('''
                    INSERT OR REPLACE INTO order_details 
                    (order_id, product_id, quantity, unit_price, discount)
                    VALUES (?, ?, ?, ?, ?)
                ''', (order_id, product[0], quantity, unit_price, discount))
        
        conn.commit()
        logger.info(f"OLTP data inserted: {len(customers)} customers, 500 orders")
    
    def insert_olap_data(self, conn):
        """Insert sample data into OLAP database (star schema)."""
        cursor = conn.cursor()
        
        # Date dimension
        dates = []
        start_date = datetime(2023, 1, 1)
        for i in range(730):  # 2 years
            current_date = start_date + timedelta(days=i)
            dates.append((
                int(current_date.strftime('%Y%m%d')),  # date_key
                current_date.strftime('%Y-%m-%d'),
                current_date.weekday(),
                current_date.strftime('%A'),
                current_date.day,
                current_date.timetuple().tm_yday,
                current_date.isocalendar()[1],
                current_date.month,
                current_date.strftime('%B'),
                (current_date.month - 1) // 3 + 1,
                current_date.year,
                (current_date.month - 1) // 3 + 1,
                current_date.weekday() >= 5,
                0  # is_holiday
            ))
        
        cursor.executemany('''
            INSERT OR REPLACE INTO dim_date 
            (date_key, full_date, day_of_week, day_name, day_of_month, day_of_year, 
             week_of_year, month_number, month_name, quarter, year, fiscal_quarter, is_weekend, is_holiday)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', dates)
        
        # Customer dimension
        customer_segments = ['Premium', 'Standard', 'Basic', 'New']
        cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']
        countries = ['USA', 'Canada', 'UK', 'Germany', 'France']
        
        for i in range(100):
            cursor.execute('''
                INSERT OR REPLACE INTO dim_customer 
                (customer_id, first_name, last_name, email, city, country, customer_segment, 
                 registration_date, is_current, effective_date, expiration_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                f'CUST{str(i+1).zfill(4)}',
                f'First{i+1}',
                f'Last{i+1}',
                f'customer{i+1}@email.com',
                random.choice(cities),
                random.choice(countries),
                random.choice(customer_segments),
                (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d'),
                1, '2023-01-01', '9999-12-31'
            ))
        
        # Product dimension
        categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports']
        for i in range(25):
            cursor.execute('''
                INSERT OR REPLACE INTO dim_product 
                (product_id, product_name, category_name, supplier_name, unit_cost, 
                 is_current, effective_date, expiration_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                f'PROD{str(i+1).zfill(4)}',
                f'Product {i+1}',
                random.choice(categories),
                f'Supplier {random.randint(1, 10)}',
                round(random.uniform(5, 500), 2),
                1, '2023-01-01', '9999-12-31'
            ))
        
        # Sales fact table
        for _ in range(10000):
            date_key = random.choice(dates)[0]
            customer_key = random.randint(1, 100)
            product_key = random.randint(1, 25)
            quantity = random.randint(1, 10)
            unit_price = round(random.uniform(10, 1000), 2)
            discount = random.choice([0, 0, 0, 0.1, 0.2])
            cost = unit_price * 0.6
            
            sales_amount = quantity * unit_price * (1 - discount)
            cost_amount = quantity * cost
            profit_amount = sales_amount - cost_amount
            
            cursor.execute('''
                INSERT INTO fact_sales 
                (date_key, customer_key, product_key, order_id, quantity, unit_price, 
                 discount_amount, sales_amount, cost_amount, profit_amount)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                date_key, customer_key, product_key,
                f'ORD{random.randint(1, 100000):06d}',
                quantity, unit_price, discount * quantity * unit_price,
                sales_amount, cost_amount, profit_amount
            ))
        
        # Monthly aggregation
        cursor.execute('''
            INSERT INTO agg_sales_monthly 
            SELECT 
                d.year,
                d.month_number,
                p.category_name,
                c.country,
                SUM(f.sales_amount) AS total_sales,
                SUM(f.quantity) AS total_quantity,
                SUM(f.profit_amount) AS total_profit,
                COUNT(DISTINCT f.order_id) AS order_count
            FROM fact_sales f
            JOIN dim_date d ON f.date_key = d.date_key
            JOIN dim_product p ON f.product_key = p.product_key
            JOIN dim_customer c ON f.customer_key = c.customer_key
            GROUP BY d.year, d.month_number, p.category_name, c.country
        ''')
        
        conn.commit()
        logger.info("OLAP data inserted (star schema with facts and dimensions)")
    
    def compare_systems(self) -> Dict:
        """Compare OLTP vs OLAP characteristics."""
        return {
            'oltp_characteristics': {
                'full_name': 'Online Transaction Processing',
                'purpose': 'Day-to-day business operations',
                'data_structure': 'Highly normalized (3NF)',
                'schema_design': 'Entity-Relationship model',
                'optimization': 'Optimized for INSERT, UPDATE, DELETE',
                'query_type': 'Simple, short transactions',
                'data_volume': 'GB range',
                'users': 'Thousands of concurrent users',
                'response_time': 'Milliseconds',
                'backup_frequency': 'Continuous',
                'use_cases': [
                    'Order processing',
                    'Inventory management',
                    'Customer registration',
                    'Payment processing',
                    'Real-time transactions'
                ]
            },
            'olap_characteristics': {
                'full_name': 'Online Analytical Processing',
                'purpose': 'Business intelligence and analytics',
                'data_structure': 'Denormalized (Star/Snowflake schema)',
                'schema_design': 'Dimensional model',
                'optimization': 'Optimized for SELECT, aggregations',
                'query_type': 'Complex, long-running queries',
                'data_volume': 'TB to PB range',
                'users': 'Hundreds of analysts',
                'response_time': 'Seconds to minutes',
                'backup_frequency': 'Periodic (daily/weekly)',
                'use_cases': [
                    'Sales trend analysis',
                    'Customer segmentation',
                    'Financial reporting',
                    'Performance dashboards',
                    'Predictive analytics'
                ]
            },
            'key_differences': {
                'data_modification': {
                    'oltp': 'Frequent INSERT/UPDATE/DELETE',
                    'olap': 'Primarily read-only, batch loads'
                },
                'data_redundancy': {
                    'oltp': 'Minimal (normalized)',
                    'olap': 'Acceptable (denormalized for performance)'
                },
                'historical_data': {
                    'oltp': 'Current data only (3-6 months)',
                    'olap': 'Years of historical data'
                }
            }
        }
    
    def run_demo(self) -> Dict:
        """Run the complete OLTP vs OLAP demonstration."""
        import os
        os.makedirs('data', exist_ok=True)
        
        # OLTP Database
        oltp_conn = sqlite3.connect(self.oltp_db_path)
        self.create_oltp_schema(oltp_conn)
        self.insert_oltp_data(oltp_conn)
        oltp_conn.close()
        
        # OLAP Database
        olap_conn = sqlite3.connect(self.olap_db_path)
        self.create_olap_schema(olap_conn)
        self.insert_olap_data(olap_conn)
        olap_conn.close()
        
        comparison = self.compare_systems()
        
        logger.info("Task 8 completed successfully!")
        
        return {
            'oltp_schema': 'Normalized: customers, products, categories, orders, order_details, inventory_transactions',
            'olap_schema': 'Star Schema: dim_date, dim_customer, dim_product, fact_sales, fact_inventory, agg_sales_monthly',
            'comparison': comparison
        }


def main():
    """Main execution for Task 8."""
    demo = DatabaseConceptsDemo()
    results = demo.run_demo()
    
    print("\n=== OLTP vs OLAP Comparison ===")
    print("\nOLTP System:")
    print(f"  Purpose: {results['comparison']['oltp_characteristics']['purpose']}")
    print(f"  Data Structure: {results['comparison']['oltp_characteristics']['data_structure']}")
    print(f"  Query Type: {results['comparison']['oltp_characteristics']['query_type']}")
    
    print("\nOLAP System:")
    print(f"  Purpose: {results['comparison']['olap_characteristics']['purpose']}")
    print(f"  Data Structure: {results['comparison']['olap_characteristics']['data_structure']}")
    print(f"  Query Type: {results['comparison']['olap_characteristics']['query_type']}")
    
    return results


if __name__ == "__main__":
    main()
