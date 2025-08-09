@echo off
echo Adding complete dataset to GitHub repository with Git LFS...

REM Navigate to your repository directory
cd /d "c:\Users\vaddi\OneDrive\Desktop\dbeaver\anon_app_platform_dataset"

REM Install and configure Git LFS
echo Installing Git LFS...
git lfs install

REM Track CSV files with LFS
echo Configuring LFS to track CSV files...
git lfs track "split_files/*.csv"
git lfs track "*.csv"

REM Copy your CSV files from the source directory
echo Copying CSV files...
copy "c:\Users\vaddi\OneDrive\Desktop\dbeaver\split_files\*.csv" "split_files\"

REM Add .gitattributes file
git add .gitattributes

REM Add all files including CSV files
echo Adding files to git...
git add .
git add split_files/

REM Commit the changes
echo Committing changes...
git commit -m "Add complete dataset with CSV files using Git LFS

- Added split CSV files using Git LFS for efficient storage
- anon_views_part_01.csv and anon_views_part_02.csv (~2.5M rows total)
- anon_user_day_fact.csv (~722K rows)
- anon_company_day_fact.csv (~331K rows)
- Updated .gitattributes to track CSV files with LFS
- Complete dataset now available for one-command deployment"

REM Push to GitHub
echo Pushing to GitHub...
git push origin main

echo.
echo ✅ Dataset successfully added to repository with Git LFS!
echo Repository URL: https://github.com/manideepv28/anon_pipeline_dataset.git
pause