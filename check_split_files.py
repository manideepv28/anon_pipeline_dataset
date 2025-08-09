#!/usr/bin/env python3
"""
Split Files Directory Checker
=============================
This script checks what files are available in your split_files directory
and provides information about them.
"""

import os
import glob
import pandas as pd

def check_split_files():
    """Check what split files are available"""
    split_files_dir = r"c:\Users\vaddi\OneDrive\Desktop\dbeaver\anon_app_platform_dataset\split_files"
    
    print("🔍 Checking Split Files Directory")
    print("=" * 60)
    print(f"Directory: {split_files_dir}")
    
    if not os.path.exists(split_files_dir):
        print("❌ Directory does not exist!")
        return
    
    # Find all CSV files
    csv_files = glob.glob(os.path.join(split_files_dir, "*.csv"))
    
    if not csv_files:
        print("❌ No CSV files found in directory!")
        return
    
    print(f"\n📁 Found {len(csv_files)} CSV files:")
    
    # Group files by table type
    file_groups = {
        'ANON_VIEWS': [],
        'ANON_USER_DAY_FACT': [],
        'ANON_COMPANY_DAY_FACT': [],
        'OTHER': []
    }
    
    for file_path in csv_files:
        filename = os.path.basename(file_path).lower()
        
        if 'anon_views' in filename:
            file_groups['ANON_VIEWS'].append(file_path)
        elif 'user_day_fact' in filename:
            file_groups['ANON_USER_DAY_FACT'].append(file_path)
        elif 'company_day_fact' in filename:
            file_groups['ANON_COMPANY_DAY_FACT'].append(file_path)
        else:
            file_groups['OTHER'].append(file_path)
    
    # Display information for each group
    total_rows = 0
    total_size = 0
    
    for table_name, files in file_groups.items():
        if not files:
            continue
            
        print(f"\n📊 {table_name} Files:")
        table_rows = 0
        table_size = 0
        
        for file_path in sorted(files):
            filename = os.path.basename(file_path)
            file_size = os.path.getsize(file_path)
            file_size_mb = file_size / (1024 * 1024)
            
            try:
                # Quick row count (just read first few lines to get structure)
                df_sample = pd.read_csv(file_path, nrows=0)  # Just headers
                with open(file_path, 'r') as f:
                    row_count = sum(1 for line in f) - 1  # Subtract header
                
                table_rows += row_count
                table_size += file_size
                
                print(f"  📄 {filename}")
                print(f"     Size: {file_size_mb:.1f} MB")
                print(f"     Rows: {row_count:,}")
                print(f"     Columns: {len(df_sample.columns)} ({', '.join(df_sample.columns[:5])}{'...' if len(df_sample.columns) > 5 else ''})")
                
            except Exception as e:
                print(f"  📄 {filename}")
                print(f"     Size: {file_size_mb:.1f} MB")
                print(f"     Error reading file: {e}")
        
        if table_rows > 0:
            print(f"  📈 Total for {table_name}: {table_rows:,} rows, {table_size/(1024*1024):.1f} MB")
            total_rows += table_rows
            total_size += table_size
    
    print(f"\n🎯 SUMMARY:")
    print(f"  Total files: {len(csv_files)}")
    print(f"  Total estimated rows: {total_rows:,}")
    print(f"  Total size: {total_size/(1024*1024):.1f} MB")
    
    # Check if files look ready for loading
    print(f"\n✅ READINESS CHECK:")
    ready_tables = 0
    for table_name in ['ANON_VIEWS', 'ANON_USER_DAY_FACT', 'ANON_COMPANY_DAY_FACT']:
        if file_groups[table_name]:
            print(f"  ✅ {table_name}: {len(file_groups[table_name])} files ready")
            ready_tables += 1
        else:
            print(f"  ❌ {table_name}: No files found")
    
    if ready_tables == 3:
        print(f"\n🎉 All table files are ready for loading!")
        print(f"   You can now run: python load_split_files.py")
    else:
        print(f"\n⚠️  Only {ready_tables}/3 table types have files ready")
    
    print("=" * 60)

if __name__ == "__main__":
    check_split_files()