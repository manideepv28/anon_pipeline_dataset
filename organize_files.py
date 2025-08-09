#!/usr/bin/env python3
"""
File Organization Script
========================
This script organizes the pipeline files into the proper structure
for easy deployment and distribution.
"""

import os
import shutil

def organize_files():
    """Organize files into proper directory structure"""
    
    print("📁 Organizing pipeline files...")
    
    # Create scripts directory if it doesn't exist
    if not os.path.exists('scripts'):
        os.makedirs('scripts')
        print("✅ Created scripts/ directory")
    
    # Files to move to scripts directory
    script_files = [
        'load_split_files.py',
        'verify_data_integrity.py', 
        'create_semantic_model_from_yaml.py',
        'check_split_files.py'
    ]
    
    # Copy files to scripts directory
    for file in script_files:
        if os.path.exists(file):
            shutil.copy2(file, f'scripts/{file}')
            print(f"✅ Copied {file} to scripts/")
        else:
            print(f"⚠️  {file} not found")
    
    # Update imports in scripts to use config from parent directory
    for file in script_files:
        script_path = f'scripts/{file}'
        if os.path.exists(script_path):
            update_imports_in_file(script_path)
    
    print("\n📋 File organization complete!")
    print("\nYour project structure should now be:")
    print("├── config.py")
    print("├── setup_pipeline.py")
    print("├── requirements.txt")
    print("├── README.md")
    print("├── DEPLOYMENT_GUIDE.md")
    print("├── semantic_model.yaml")
    print("├── split_files/")
    print("└── scripts/")
    print("    ├── load_split_files.py")
    print("    ├── verify_data_integrity.py")
    print("    ├── create_semantic_model_from_yaml.py")
    print("    └── check_split_files.py")

def update_imports_in_file(file_path):
    """Update imports in script files to use config from parent directory"""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Add sys.path.append to access parent directory
        if 'sys.path.append' not in content:
            # Find the imports section and add path modification
            lines = content.split('\n')
            import_index = -1
            
            for i, line in enumerate(lines):
                if line.startswith('import ') or line.startswith('from '):
                    import_index = i
                    break
            
            if import_index >= 0:
                lines.insert(import_index, 'import sys')
                lines.insert(import_index + 1, 'sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))')
                lines.insert(import_index + 2, '')
                
                with open(file_path, 'w') as f:
                    f.write('\n'.join(lines))
                
                print(f"✅ Updated imports in {file_path}")
    
    except Exception as e:
        print(f"⚠️  Could not update imports in {file_path}: {e}")

if __name__ == "__main__":
    organize_files()