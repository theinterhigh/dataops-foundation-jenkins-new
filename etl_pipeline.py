#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
    """ สร้าง Star Schema และเปลี่ยนชื่อ Table เป็นชื่อมาตรฐาน """
    print("\n🌟 Creating Star Schema...")
    dim_tables = {}
    fact_data = df.copy()
    
    # 1. Home Ownership Dimension
    if 'home_ownership' in df.columns:
        home_ownership_dim = df[['home_ownership']].drop_duplicates().reset_index(drop=True)
        home_ownership_dim['home_ownership_id'] = home_ownership_dim.index + 1
        dim_tables['dim_home_ownership'] = home_ownership_dim # เปลี่ยนชื่อที่นี่
        
        home_ownership_map = home_ownership_dim.set_index('home_ownership')['home_ownership_id'].to_dict()
        fact_data['home_ownership_id'] = fact_data['home_ownership'].map(home_ownership_map)
        print(f"   ✅ dim_home_ownership: {len(home_ownership_dim)} records")
    
    # 2. Loan Status Dimension
    if 'loan_status' in df.columns:
        loan_status_dim = df[['loan_status']].drop_duplicates().reset_index(drop=True)
        loan_status_dim['loan_status_id'] = loan_status_dim.index + 1
        dim_tables['dim_loan_status'] = loan_status_dim # เปลี่ยนชื่อที่นี่
        
        loan_status_map = loan_status_dim.set_index('loan_status')['loan_status_id'].to_dict()
        fact_data['loan_status_id'] = fact_data['loan_status'].map(loan_status_map)
        print(f"   ✅ dim_loan_status: {len(loan_status_dim)} records")
    
    # 3. Issue Date Dimension
    if 'issue_d' in df.columns:
        issue_d_dim = df[['issue_d']].drop_duplicates().reset_index(drop=True)
        issue_d_dim['issue_d_id'] = issue_d_dim.index + 1
        issue_d_dim['month'] = issue_d_dim['issue_d'].dt.month
        issue_d_dim['year'] = issue_d_dim['issue_d'].dt.year
        issue_d_dim['quarter'] = issue_d_dim['issue_d'].dt.quarter
        dim_tables['dim_issue_date'] = issue_d_dim # เปลี่ยนชื่อที่นี่
        
        issue_d_map = issue_d_dim.set_index('issue_d')['issue_d_id'].to_dict()
        fact_data['issue_d_id'] = fact_data['issue_d'].map(issue_d_map)
        print(f"   ✅ dim_issue_date: {len(issue_d_dim)} records")
    
    # 4. Create Fact Table
    fact_columns = [
        'loan_amnt', 'funded_amnt', 'term', 'int_rate', 'installment',
        'home_ownership_id', 'loan_status_id', 'issue_d_id'
    ]
    available_columns = [col for col in fact_columns if col in fact_data.columns]
    fact_table = fact_data[available_columns].reset_index(drop=True)
    fact_table['fact_id'] = fact_table.index + 1
    
    print(f"   ✅ fact_loans: {len(fact_table)} records")
    return fact_table, dim_tables

def deploy_to_database(fact_table, dim_tables):
    """ Deploy ไปยัง Database โดยใช้ค่าจาก Environment Variables """
    print("\n🚀 Deploying to Database...")
    
    # ดึงค่าจาก Jenkins Environment (ถ้าไม่มีให้ใช้ Default ตามที่คุณตั้งใน CB)
    server = os.getenv('DB_SERVER', '35.193.69.27')
    database = os.getenv('DB_NAME', 'TestDB')
    username = os.getenv('DB_USERNAME', 'SA')
    password = os.getenv('DB_PASSWORD', 'Passw0rd123456') 
    
    try:
        connection_string = f'mssql+pymssql://{username}:{password}@{server}/{database}'
        engine = create_engine(connection_string)
        
        print(f"   📡 Connecting to {server}/{database}...")
        
        # Deploy Dimensions
        for table_name, dim_df in dim_tables.items():
            dim_df.to_sql(table_name, con=engine, if_exists='replace', index=False)
            print(f"     ✅ Uploaded: {table_name}")
        
        # Deploy Fact
        fact_table.to_sql('fact_loans', con=engine, if_exists='replace', index=False)
        print(f"     ✅ Uploaded: fact_loans")
        
        return True
    except Exception as e:
        print(f"   ❌ Database deployment failed: {str(e)}")
        return False

def main():
    print("🚀 Starting ETL Pipeline")
    print("="*50)
    data_file = 'data/LoanStats_web_small.csv'
    deploy_mode = '--deploy' in sys.argv
    
    try:
        # 1. Analyze & Load
        success, column_types = guess_column_types(data_file)
        df = pd.read_csv(data_file, low_memory=False)
        
        # 2. Clean & Filter
        df_clean = clean_missing_values(df, max_null_percentage=30)
        df_filtered = filter_issue_date_range(df_clean) if 'issue_d' in df_clean.columns else df_clean
        df_final = df_filtered.dropna()
        
        # 3. Transform to Star Schema
        fact_table, dim_tables = create_star_schema(df_final)
        
        # 4. Deploy
        if deploy_mode:
            if deploy_to_database(fact_table, dim_tables):
                print("\n🎉 ETL & Deployment Successful!")
                return True
            return False
        
        print("\n💡 Run with --deploy to upload data.")
        return True
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
