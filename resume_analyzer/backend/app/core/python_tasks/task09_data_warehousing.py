#!/usr/bin/env python3
"""
Task 9: Data Warehousing
Design a star schema for e-commerce analytics including fact and dimension tables.
"""

import sqlite3
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ECommerceDataWarehouse:
    """Design and implement a star schema for e-commerce analytics."""
    
    def __init__(self, db_path: str = "data/ecommerce_warehouse.db"):
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Connect to the data warehouse."""
        import os
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()
        logger.info(f"Connected to data warehouse: {self.db_path}")
    
    def disconnect(self):
        """Disconnect from the data warehouse."""
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from data warehouse")
    
    def create_star_schema(self):
        """Create star schema with fact and dimension tables."""
        
        # ==================== DIMENSION TABLES ====================
        
        # Date Dimension - Conformed dimension (used by multiple fact tables)
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS dim_date (
                date_key INTEGER PRIMARY KEY,
                full_date DATE NOT NULL,
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
                is_holiday BOOLEAN,
                holiday_name TEXT
            )
        ''')
        
        # Customer Dimension - Slowly Changing Dimension Type 2
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS dim_customer (
                customer_key INTEGER PRIMARY KEY AUTOINCREMENT,
                customer_id TEXT NOT NULL,
                first_name TEXT,
                last_name TEXT,
                email TEXT,
                phone TEXT,
                date_of_birth DATE,
                gender TEXT,
                city TEXT,
                state TEXT,
                country TEXT,
                postal_code TEXT,
                customer_segment TEXT,
                registration_date DATE,
                lifetime_value REAL DEFAULT 0,
                is_current BOOLEAN DEFAULT 1,
                effective_date DATE,
                expiration_date DATE
            )
        ''')
        
        # Product Dimension - Slowly Changing Dimension Type 2
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS dim_product (
                product_key INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id TEXT NOT NULL,
                product_name TEXT,
                product_description TEXT,
                brand TEXT,
                category_name TEXT,
                subcategory_name TEXT,
                supplier_name TEXT,
                unit_cost REAL,
                unit_weight REAL,
                is_current BOOLEAN DEFAULT 1,
                effective_date DATE,
                expiration_date DATE
            )
        ''')
        
        # Promotion Dimension
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS dim_promotion (
                promotion_key INTEGER PRIMARY KEY AUTOINCREMENT,
                promotion_id TEXT NOT NULL,
                promotion_name TEXT,
                promotion_type TEXT,  -- 'discount', 'bundle', 'free_shipping', etc.
                discount_percentage REAL,
                start_date DATE,
                end_date DATE,
                is_active BOOLEAN
            )
        ''')
        
        # Store Dimension (for omnichannel retail)
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS dim_store (
                store_key INTEGER PRIMARY KEY AUTOINCREMENT,
                store_id TEXT NOT NULL,
                store_name TEXT,
                store_type TEXT,  -- 'online', 'physical', 'mobile_app'
                city TEXT,
                state TEXT,
                country TEXT,
                region TEXT,
                square_footage INTEGER,
                opening_date DATE,
                is_active BOOLEAN
            )
        ''')
        
        # ==================== FACT TABLES ====================
        
        # Sales Fact Table - Grain: One row per order line item
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS fact_sales (
                sale_key INTEGER PRIMARY KEY AUTOINCREMENT,
                date_key INTEGER,
                customer_key INTEGER,
                product_key INTEGER,
                promotion_key INTEGER,
                store_key INTEGER,
                order_id TEXT,
                order_line_number INTEGER,
                quantity INTEGER,
                unit_price REAL,
                unit_cost REAL,
                discount_amount REAL,
                sales_amount REAL,
                cost_amount REAL,
                profit_amount REAL,
                tax_amount REAL,
                shipping_amount REAL,
                FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
                FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
                FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
                FOREIGN KEY (promotion_key) REFERENCES dim_promotion(promotion_key),
                FOREIGN KEY (store_key) REFERENCES dim_store(store_key)
            )
        ''')
        
        # Inventory Fact Table - Grain: One row per product per day
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS fact_inventory (
                inventory_key INTEGER PRIMARY KEY AUTOINCREMENT,
                date_key INTEGER,
                product_key INTEGER,
                store_key INTEGER,
                quantity_on_hand INTEGER,
                quantity_reserved INTEGER,
                quantity_available INTEGER,
                inventory_value REAL,
                days_of_supply INTEGER,
                reorder_flag BOOLEAN,
                FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
                FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
                FOREIGN KEY (store_key) REFERENCES dim_store(store_key)
            )
        ''')
        
        # Customer Activity Fact Table - Grain: One row per customer action
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS fact_customer_activity (
                activity_key INTEGER PRIMARY KEY AUTOINCREMENT,
                date_key INTEGER,
                customer_key INTEGER,
                store_key INTEGER,
                activity_type TEXT,  -- 'page_view', 'add_to_cart', 'purchase', 'review'
                session_id TEXT,
                product_key INTEGER,
                quantity INTEGER,
                duration_seconds INTEGER,
                FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
                FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
                FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
                FOREIGN KEY (store_key) REFERENCES dim_store(store_key)
            )
        ''')
        
        # ==================== AGGREGATE TABLES ====================
        
        # Monthly Sales Summary
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS agg_sales_monthly (
                year INTEGER,
                month INTEGER,
                category_name TEXT,
                customer_segment TEXT,
                region TEXT,
                total_sales REAL,
                total_quantity INTEGER,
                total_profit REAL,
                order_count INTEGER,
                customer_count INTEGER,
                avg_order_value REAL,
                PRIMARY KEY (year, month, category_name, customer_segment, region)
            )
        ''')
        
        # Daily Store Performance
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS agg_store_daily (
                date_key INTEGER,
                store_key INTEGER,
                total_sales REAL,
                transaction_count INTEGER,
                unique_customers INTEGER,
                avg_transaction_value REAL,
                total_items_sold INTEGER,
                PRIMARY KEY (date_key, store_key)
            )
        ''')
        
        self.conn.commit()
        logger.info("Star schema created successfully")
    
    def populate_dimensions(self):
        """Populate dimension tables with sample data."""
        
        # Populate Date Dimension
        dates = []
        start_date = datetime(2023, 1, 1)
        holidays = {
            '2023-01-01': 'New Year',
            '2023-07-04': 'Independence Day',
            '2023-12-25': 'Christmas',
            '2024-01-01': 'New Year',
        }
        
        for i in range(730):  # 2 years
            current_date = start_date + timedelta(days=i)
            date_str = current_date.strftime('%Y-%m-%d')
            
            dates.append((
                int(current_date.strftime('%Y%m%d')),  # date_key
                date_str,
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
                date_str in holidays,
                holidays.get(date_str, None)
            ))
        
        self.cursor.executemany('''
            INSERT OR REPLACE INTO dim_date 
            (date_key, full_date, day_of_week, day_name, day_of_month, day_of_year, 
             week_of_year, month_number, month_name, quarter, year, fiscal_quarter, 
             is_weekend, is_holiday, holiday_name)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', dates)
        
        # Populate Customer Dimension
        first_names = ['John', 'Jane', 'Michael', 'Emily', 'David', 'Sarah', 'Chris', 'Jessica',
                      'Matthew', 'Amanda', 'Daniel', 'Ashley', 'James', 'Jennifer']
        last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis']
        cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio']
        states = ['NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'TX']
        segments = ['Premium', 'Standard', 'Basic', 'New']
        
        for i in range(200):
            self.cursor.execute('''
                INSERT OR REPLACE INTO dim_customer 
                (customer_id, first_name, last_name, email, phone, date_of_birth, gender,
                 city, state, country, postal_code, customer_segment, registration_date,
                 lifetime_value, is_current, effective_date, expiration_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                f'CUST{str(i+1).zfill(5)}',
                random.choice(first_names),
                random.choice(last_names),
                f'customer{i+1}@email.com',
                f'555-{random.randint(1000, 9999)}',
                (datetime.now() - timedelta(days=random.randint(6570, 25550))).strftime('%Y-%m-%d'),
                random.choice(['M', 'F']),
                random.choice(cities),
                random.choice(states),
                'USA',
                f'{random.randint(10000, 99999)}',
                random.choice(segments),
                (datetime.now() - timedelta(days=random.randint(1, 730))).strftime('%Y-%m-%d'),
                round(random.uniform(0, 10000), 2),
                1,
                '2023-01-01',
                '9999-12-31'
            ))
        
        # Populate Product Dimension
        categories = {
            'Electronics': ['Smartphones', 'Laptops', 'Tablets', 'Headphones', 'Cameras'],
            'Clothing': ['Men\'s Wear', 'Women\'s Wear', 'Kids\' Wear', 'Shoes', 'Accessories'],
            'Home': ['Furniture', 'Decor', 'Kitchen', 'Bedding', 'Lighting'],
            'Sports': ['Fitness', 'Outdoor', 'Team Sports', 'Water Sports', 'Cycling'],
            'Books': ['Fiction', 'Non-Fiction', 'Educational', 'Comics', 'Magazines']
        }
        
        brands = ['BrandA', 'BrandB', 'BrandC', 'BrandD', 'BrandE']
        suppliers = ['Supplier1', 'Supplier2', 'Supplier3', 'Supplier4', 'Supplier5']
        
        product_id = 1
        for category, subcategories in categories.items():
            for subcategory in subcategories:
                for _ in range(5):  # 5 products per subcategory
                    self.cursor.execute('''
                        INSERT OR REPLACE INTO dim_product 
                        (product_id, product_name, product_description, brand, category_name,
                         subcategory_name, supplier_name, unit_cost, unit_weight, is_current,
                         effective_date, expiration_date)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        f'PROD{str(product_id).zfill(5)}',
                        f'{subcategory} Product {product_id}',
                        f'Description for {subcategory} product {product_id}',
                        random.choice(brands),
                        category,
                        subcategory,
                        random.choice(suppliers),
                        round(random.uniform(5, 500), 2),
                        round(random.uniform(0.1, 10), 2),
                        1,
                        '2023-01-01',
                        '9999-12-31'
                    ))
                    product_id += 1
        
        # Populate Promotion Dimension
        promotion_types = ['percentage_discount', 'fixed_amount', 'buy_one_get_one', 'free_shipping']
        
        for i in range(20):
            start = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 600))
            end = start + timedelta(days=random.randint(7, 30))
            
            self.cursor.execute('''
                INSERT OR REPLACE INTO dim_promotion 
                (promotion_id, promotion_name, promotion_type, discount_percentage, start_date, end_date, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                f'PROMO{str(i+1).zfill(4)}',
                f'Promotion {i+1}',
                random.choice(promotion_types),
                round(random.uniform(5, 50), 2),
                start.strftime('%Y-%m-%d'),
                end.strftime('%Y-%m-%d'),
                end > datetime.now()
            ))
        
        # Populate Store Dimension
        store_types = ['online', 'physical', 'mobile_app']
        regions = ['North', 'South', 'East', 'West', 'Central']
        
        for i in range(10):
            self.cursor.execute('''
                INSERT OR REPLACE INTO dim_store 
                (store_id, store_name, store_type, city, state, country, region, square_footage, opening_date, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                f'STORE{str(i+1).zfill(3)}',
                f'Store {i+1}',
                random.choice(store_types),
                random.choice(cities),
                random.choice(states),
                'USA',
                random.choice(regions),
                random.randint(1000, 50000) if random.choice(store_types) != 'online' else 0,
                (datetime.now() - timedelta(days=random.randint(365, 1825))).strftime('%Y-%m-%d'),
                1
            ))
        
        self.conn.commit()
        logger.info("Dimension tables populated")
    
    def populate_fact_tables(self):
        """Populate fact tables with sample data."""
        
        # Populate Sales Fact Table
        date_keys = [r[0] for r in self.cursor.execute('SELECT date_key FROM dim_date').fetchall()]
        customer_keys = [r[0] for r in self.cursor.execute('SELECT customer_key FROM dim_customer').fetchall()]
        product_keys = [r[0] for r in self.cursor.execute('SELECT product_key FROM dim_product').fetchall()]
        promotion_keys = [r[0] for r in self.cursor.execute('SELECT promotion_key FROM dim_promotion').fetchall()]
        store_keys = [r[0] for r in self.cursor.execute('SELECT store_key FROM dim_store').fetchall()]
        
        # Get product costs
        product_costs = {r[0]: r[1] for r in self.cursor.execute('SELECT product_key, unit_cost FROM dim_product').fetchall()}
        
        for _ in range(50000):
            date_key = random.choice(date_keys)
            customer_key = random.choice(customer_keys)
            product_key = random.choice(product_keys)
            promotion_key = random.choice([None] + promotion_keys[:5])  # 50% no promotion
            store_key = random.choice(store_keys)
            
            quantity = random.randint(1, 5)
            unit_price = round(random.uniform(10, 1000), 2)
            unit_cost = product_costs.get(product_key, unit_price * 0.6)
            discount = round(random.uniform(0, 0.3), 2) if promotion_key else 0
            
            sales_amount = quantity * unit_price * (1 - discount)
            cost_amount = quantity * unit_cost
            profit_amount = sales_amount - cost_amount
            tax_amount = sales_amount * 0.08
            shipping = sales_amount * 0.05 if sales_amount < 100 else 0
            
            self.cursor.execute('''
                INSERT INTO fact_sales 
                (date_key, customer_key, product_key, promotion_key, store_key, order_id, 
                 order_line_number, quantity, unit_price, unit_cost, discount_amount,
                 sales_amount, cost_amount, profit_amount, tax_amount, shipping_amount)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                date_key, customer_key, product_key, promotion_key, store_key,
                f'ORD{random.randint(1, 1000000):07d}',
                random.randint(1, 5),
                quantity, unit_price, unit_cost, discount * quantity * unit_price,
                sales_amount, cost_amount, profit_amount, tax_amount, shipping
            ))
        
        # Populate Inventory Fact Table
        for _ in range(10000):
            self.cursor.execute('''
                INSERT INTO fact_inventory 
                (date_key, product_key, store_key, quantity_on_hand, quantity_reserved,
                 quantity_available, inventory_value, days_of_supply, reorder_flag)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                random.choice(date_keys),
                random.choice(product_keys),
                random.choice(store_keys),
                random.randint(0, 500),
                random.randint(0, 50),
                random.randint(0, 450),
                round(random.uniform(1000, 50000), 2),
                random.randint(1, 60),
                random.choice([True, False])
            ))
        
        # Populate Customer Activity Fact Table
        activity_types = ['page_view', 'add_to_cart', 'purchase', 'review', 'wishlist_add']
        
        for _ in range(20000):
            self.cursor.execute('''
                INSERT INTO fact_customer_activity 
                (date_key, customer_key, store_key, activity_type, session_id, product_key, quantity, duration_seconds)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                random.choice(date_keys),
                random.choice(customer_keys),
                random.choice(store_keys),
                random.choice(activity_types),
                f'SESS{random.randint(1, 1000000):07d}',
                random.choice(product_keys),
                random.randint(1, 3),
                random.randint(10, 600)
            ))
        
        self.conn.commit()
        logger.info("Fact tables populated")
    
    def create_aggregates(self):
        """Create pre-computed aggregate tables."""
        
        # Monthly Sales Summary
        self.cursor.execute('''
            INSERT OR REPLACE INTO agg_sales_monthly 
            SELECT 
                d.year,
                d.month_number,
                p.category_name,
                c.customer_segment,
                st.region,
                SUM(f.sales_amount) AS total_sales,
                SUM(f.quantity) AS total_quantity,
                SUM(f.profit_amount) AS total_profit,
                COUNT(DISTINCT f.order_id) AS order_count,
                COUNT(DISTINCT f.customer_key) AS customer_count,
                AVG(f.sales_amount) AS avg_order_value
            FROM fact_sales f
            JOIN dim_date d ON f.date_key = d.date_key
            JOIN dim_product p ON f.product_key = p.product_key
            JOIN dim_customer c ON f.customer_key = c.customer_key
            JOIN dim_store st ON f.store_key = st.store_key
            GROUP BY d.year, d.month_number, p.category_name, c.customer_segment, st.region
        ''')
        
        # Daily Store Performance
        self.cursor.execute('''
            INSERT OR REPLACE INTO agg_store_daily 
            SELECT 
                f.date_key,
                f.store_key,
                SUM(f.sales_amount) AS total_sales,
                COUNT(DISTINCT f.order_id) AS transaction_count,
                COUNT(DISTINCT f.customer_key) AS unique_customers,
                AVG(f.sales_amount) AS avg_transaction_value,
                SUM(f.quantity) AS total_items_sold
            FROM fact_sales f
            GROUP BY f.date_key, f.store_key
        ''')
        
        self.conn.commit()
        logger.info("Aggregate tables created")
    
    def get_schema_info(self) -> Dict:
        """Get information about the star schema."""
        return {
            'fact_tables': {
                'fact_sales': {
                    'grain': 'One row per order line item',
                    'measures': ['quantity', 'unit_price', 'sales_amount', 'profit_amount', 'tax_amount', 'shipping_amount'],
                    'dimensions': ['date', 'customer', 'product', 'promotion', 'store']
                },
                'fact_inventory': {
                    'grain': 'One row per product per day',
                    'measures': ['quantity_on_hand', 'quantity_reserved', 'inventory_value', 'days_of_supply'],
                    'dimensions': ['date', 'product', 'store']
                },
                'fact_customer_activity': {
                    'grain': 'One row per customer action',
                    'measures': ['quantity', 'duration_seconds'],
                    'dimensions': ['date', 'customer', 'product', 'store']
                }
            },
            'dimension_tables': {
                'dim_date': {
                    'type': 'Conformed dimension',
                    'attributes': ['day', 'week', 'month', 'quarter', 'year', 'holiday flags']
                },
                'dim_customer': {
                    'type': 'SCD Type 2',
                    'attributes': ['demographics', 'geography', 'segment', 'lifetime_value']
                },
                'dim_product': {
                    'type': 'SCD Type 2',
                    'attributes': ['name', 'brand', 'category', 'subcategory', 'cost']
                },
                'dim_promotion': {
                    'type': 'Standard dimension',
                    'attributes': ['name', 'type', 'discount', 'date range']
                },
                'dim_store': {
                    'type': 'Standard dimension',
                    'attributes': ['name', 'type', 'location', 'region']
                }
            },
            'aggregate_tables': {
                'agg_sales_monthly': 'Pre-aggregated sales by month, category, segment, region',
                'agg_store_daily': 'Pre-aggregated store performance by day'
            }
        }
    
    def run_demo(self) -> Dict:
        """Run the complete data warehousing demonstration."""
        self.connect()
        self.create_star_schema()
        self.populate_dimensions()
        self.populate_fact_tables()
        self.create_aggregates()
        
        schema_info = self.get_schema_info()
        
        # Get row counts
        counts = {}
        for table in ['dim_date', 'dim_customer', 'dim_product', 'dim_promotion', 
                      'dim_store', 'fact_sales', 'fact_inventory', 'fact_customer_activity']:
            count = self.cursor.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
            counts[table] = count
        
        self.disconnect()
        
        logger.info("Task 9 completed successfully!")
        
        return {
            'schema_info': schema_info,
            'row_counts': counts
        }


def main():
    """Main execution for Task 9."""
    warehouse = ECommerceDataWarehouse()
    results = warehouse.run_demo()
    
    print("\n=== E-Commerce Data Warehouse (Star Schema) ===")
    print("\nFact Tables:")
    for fact, info in results['schema_info']['fact_tables'].items():
        print(f"  {fact}: {info['grain']}")
    
    print("\nDimension Tables:")
    for dim, info in results['schema_info']['dimension_tables'].items():
        print(f"  {dim}: {info['type']}")
    
    print("\nRow Counts:")
    for table, count in results['row_counts'].items():
        print(f"  {table}: {count:,} rows")
    
    return results


if __name__ == "__main__":
    main()
