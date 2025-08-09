@echo off
echo Adding complete dataset to GitHub repository...

REM Navigate to your repository directory
cd /d "c:\Users\vaddi\OneDrive\Desktop\dbeaver\anon_app_platform_dataset"

REM Create backup of current .gitignore
copy .gitignore .gitignore.backup

REM Temporarily modify .gitignore to allow CSV files in split_files
echo Modifying .gitignore to allow CSV files...
powershell -Command "(Get-Content .gitignore) -replace '^split_files/\*\.csv', '# split_files/*.csv' | Set-Content .gitignore"

REM Copy your CSV files from the source directory
echo Copying CSV files...
copy "c:\Users\vaddi\OneDrive\Desktop\dbeaver\split_files\*.csv" "split_files\"

REM Add all files including CSV files
echo Adding files to git...
git add .
git add split_files/

REM Commit the changes
echo Committing changes...
git commit -m "Add complete dataset with CSV files

- Added split CSV files for complete pipeline deployment
- anon_views_part_01.csv and anon_views_part_02.csv (~2.5M rows total)
- anon_user_day_fact.csv (~722K rows)  
- anon_company_day_fact.csv (~331K rows)
- Modified .gitignore to allow CSV files in split_files directory
- Users can now clone and run pipeline without additional data setup"

REM Push to GitHub
echo Pushing to GitHub...
git push origin main

echo.
echo ✅ Dataset successfully added to repository!
echo Repository URL: https://github.com/manideepv28/anon_pipeline_dataset.git
echo.
echo Note: CSV files are now included directly in the repository.
echo Consider using Git LFS if file sizes cause issues.
pause