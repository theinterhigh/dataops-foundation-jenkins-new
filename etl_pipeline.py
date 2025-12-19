#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ETL Pipeline - Simple CI/CD Demo
ใช้ functions ทั้ง 3 ในการประมวลผลข้อมูล Loan Data
"""

import pandas as pd
import numpy as np
import sys
import os
from datetime import datetime
from sqlalchemy import create_engine, text
import warnings
warnings.filterwarnings('ignore')

# Import functions
from functions.guess_column_types import guess_column_types
from functions.filter_issue_date_range import filter_issue_date_range
from functions.clean_missing_values import clean_missing_values


def create_star_schema(df):
    """
    สร้าง Star Schema จาก DataFrame ที่ประมวลผลแล้ว
    
    Args:
        df: DataFrame ที่ทำความสะอาดแล้ว
        
    Returns:
        tuple: (fact_table, dim_tables_dict)
    """
    print("\n🌟 Creating Star Schema...")
    
    dim_tables = {}
    fact_data = df.copy()
    
    # 1. Home Ownership Dimension
    if 'home_ownership' in df.columns:
        home_ownership_dim = df[['home_ownership']].drop_duplicates().reset_index(drop=True)
        home_ownership_dim['home_ownership_id'] = home_ownership_dim.index + 1
        dim_tables['home_ownership_dim_yourname'] = home_ownership_dim
        
        # Map to fact table
        home_ownership_map = home_ownership_dim.set_index('home_ownership')['home_ownership_id'].to_dict()
        fact_data['home_ownership_id'] = fact_data['home_ownership'].map(home_ownership_map)
        print(f"   ✅ Home Ownership Dimension: {len(home_ownership_dim)} records")
    
    # 2. Loan Status Dimension
    if 'loan_status' in df.columns:
        loan_status_dim = df[['loan_status']].drop_duplicates().reset_index(drop=True)
        loan_status_dim['loan_status_id'] = loan_status_dim.index + 1
        dim_tables['loan_status_dim_yourname'] = loan_status_dim
        
        # Map to fact table
        loan_status_map = loan_status_dim.set_index('loan_status')['loan_status_id'].to_dict()
        fact_data['loan_status_id'] = fact_data['loan_status'].map(loan_status_map)
        print(f"   ✅ Loan Status Dimension: {len(loan_status_dim)} records")
    
    # 3. Issue Date Dimension
    if 'issue_d' in df.columns:
        issue_d_dim = df[['issue_d']].drop_duplicates().reset_index(drop=True)
        issue_d_dim['issue_d_id'] = issue_d_dim.index + 1
        issue_d_dim['month'] = issue_d_dim['issue_d'].dt.month
        issue_d_dim['year'] = issue_d_dim['issue_d'].dt.year
        issue_d_dim['quarter'] = issue_d_dim['issue_d'].dt.quarter
        dim_tables['issue_d_dim_yourname'] = issue_d_dim
        
        # Map to fact table
        issue_d_map = issue_d_dim.set_index('issue_d')['issue_d_id'].to_dict()
        fact_data['issue_d_id'] = fact_data['issue_d'].map(issue_d_map)
        print(f"   ✅ Issue Date Dimension: {len(issue_d_dim)} records")
    
    # 4. Create Fact Table
    fact_columns = [
        'loan_amnt', 'funded_amnt', 'term', 'int_rate', 'installment',
        'home_ownership_id', 'loan_status_id', 'issue_d_id'
    ]
    
    # เลือกเฉพาะคอลัมน์ที่มีอยู่
    available_columns = [col for col in fact_columns if col in fact_data.columns]
    fact_table = fact_data[available_columns].reset_index(drop=True)
    fact_table['fact_id'] = fact_table.index + 1
    
    print(f"   ✅ Fact Table: {len(fact_table)} records, {len(available_columns)} measures")
    
    return fact_table, dim_tables


def show_etl_results(fact_table, dim_tables):
    """
    แสดงผลลัพธ์ ETL แบบสวยงาม
    
    Args:
        fact_table: DataFrame ของ fact table
        dim_tables: dict ของ dimension tables
    """
    print("\n" + "="*80)
    print("🎯 ETL PIPELINE RESULTS")
    print("="*80)
    
    # แสดง Dimension Tables
    print("\n📊 Dimension Tables:")
    for table_name, dim_df in dim_tables.items():
        print(f"   {table_name}:")
        print(f"     - Records: {len(dim_df):,}")
        print(f"     - Columns: {list(dim_df.columns)}")
        if len(dim_df) > 0:
            print(f"     - Sample: {dim_df.iloc[0].to_dict()}")
    
    # แสดง Fact Table
    print(f"\n📈 Fact Table:")
    print(f"   - Records: {len(fact_table):,}")
    print(f"   - Columns: {list(fact_table.columns)}")
    
    if len(fact_table) > 0:
        print(f"\n💰 Sample Fact Records (Top 5):")
        sample_cols = ['fact_id', 'loan_amnt', 'funded_amnt', 'int_rate']
        available_sample_cols = [col for col in sample_cols if col in fact_table.columns]
        print(fact_table[available_sample_cols].head())
        
        # Statistics
        if 'loan_amnt' in fact_table.columns:
            print(f"\n📊 Loan Amount Statistics:")
            print(f"   - Min: ${fact_table['loan_amnt'].min():,.2f}")
            print(f"   - Max: ${fact_table['loan_amnt'].max():,.2f}")
            print(f"   - Average: ${fact_table['loan_amnt'].mean():,.2f}")
            print(f"   - Total: ${fact_table['loan_amnt'].sum():,.2f}")


def deploy_to_database(fact_table, dim_tables):
    """
    Deploy ข้อมูลไปยัง MSSQL Database
    
    Args:
        fact_table: DataFrame ของ fact table
        dim_tables: dict ของ dimension tables
    """
    print("\n🚀 Deploying to Database...")
    
    # Database configuration
    server = "mssql.minddatatech.com"
    database = 'TestDB'
    username = 'SA'
    password = os.getenv('DB_PASSWORD', 'Passw0rd123456')
    
    try:
        # Create database engine
        connection_string = f'mssql+pymssql://{username}:{password}@{server}/{database}'
        engine = create_engine(connection_string)
        
        print(f"   📡 Connecting to {server}/{database}...")
        
        # Test connection
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1 as test"))
            if result.fetchone()[0] == 1:
                print("   ✅ Database connection successful")
        
        # Deploy dimension tables
        print("\n   📤 Deploying dimension tables...")
        for table_name, dim_df in dim_tables.items():
            dim_df.to_sql(table_name, con=engine, if_exists='replace', index=False)
            print(f"     ✅ {table_name}: {len(dim_df)} records")
        
        # Deploy fact table
        print("\n   📤 Deploying fact table...")
        fact_table.to_sql('loans_fact_yourname', con=engine, if_exists='replace', index=False)
        print(f"     ✅ loans_fact: {len(fact_table)} records")
        
        print("\n🎉 Database deployment completed successfully!")
        
        # Verify deployment
        print("\n🔍 Verifying deployment...")
        with engine.connect() as connection:
            for table_name in list(dim_tables.keys()) + ['loans_fact']:
                count_result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                count = count_result.fetchone()[0]
                print(f"   📊 {table_name}: {count:,} records in database")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Database deployment failed: {str(e)}")
        print("   Note: This might be expected if database is not accessible")
        return False


def main():
    """
    ETL Pipeline หลัก
    """
    print("🚀 Starting ETL Pipeline with Custom Functions")
    print("="*80)
    
    # Configuration
    data_file = 'data/LoanStats_web_small.csv'  # Path in Jenkins workspace
    
    # Check if running in deployment mode
    deploy_mode = '--deploy' in sys.argv
    
    try:
        # Step 1: Analyze column types
        print("\n📋 Step 1: Analyzing Column Types...")
        success, column_types = guess_column_types(data_file)
        
        if not success:
            print(f"❌ Column type analysis failed: {column_types}")
            return False
        
        print(f"✅ Analyzed {len(column_types)} columns")
        print("   Sample column types:")
        for i, (col, dtype) in enumerate(list(column_types.items())[:5]):
            print(f"     - {col}: {dtype}")
        if len(column_types) > 5:
            print(f"     ... and {len(column_types)-5} more")
        
        # Step 2: Load raw data
        print(f"\n📂 Step 2: Loading Data from {data_file}...")
        df = pd.read_csv(data_file, low_memory=False)
        print(f"✅ Loaded: {len(df):,} rows, {len(df.columns)} columns")
        
        # Step 3: Clean missing values
        print(f"\n🧹 Step 3: Cleaning Missing Values...")
        df_clean = clean_missing_values(df, max_null_percentage=30)
        print(f"✅ After cleaning: {len(df_clean):,} rows, {len(df_clean.columns)} columns")
        
        # Step 4: Filter date range (if issue_d exists)
        print(f"\n📅 Step 4: Filtering Date Range...")
        if 'issue_d' in df_clean.columns:
            df_filtered = filter_issue_date_range(df_clean)
            print(f"✅ After date filtering: {len(df_filtered):,} rows")
        else:
            df_filtered = df_clean
            print("⚠️  No 'issue_d' column found, skipping date filtering")
        
        # Step 5: Remove rows with any null values (for clean fact table)
        print(f"\n🔧 Step 5: Final Data Cleanup...")
        df_final = df_filtered.dropna()
        print(f"✅ Final dataset: {len(df_final):,} rows, {len(df_final.columns)} columns")
        
        # Step 6: Create star schema
        fact_table, dim_tables = create_star_schema(df_final)
        
        # Step 7: Show results
        show_etl_results(fact_table, dim_tables)
        
        # Step 8: Deploy to database (if in deploy mode)
        if deploy_mode:
            success = deploy_to_database(fact_table, dim_tables)
            if not success:
                return False
        else:
            print(f"\n💡 Tip: Run with '--deploy' flag to deploy to database")
        
        print(f"\n🎉 ETL Pipeline completed successfully!")
        print(f"   - Original data: {len(df):,} rows")
        print(f"   - Final data: {len(df_final):,} rows")
        print(f"   - Dimension tables: {len(dim_tables)}")
        print(f"   - Fact table records: {len(fact_table):,}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ ETL Pipeline failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    # รัน ETL Pipeline
    success = main()
    
    print(f"\n{'='*80}")
    print("🔚 ETL Pipeline Execution Complete")
    print(f"{'='*80}")
    
    # Exit with appropriate code for Jenkins
    sys.exit(0 if success else 1)
