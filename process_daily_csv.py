import pandas as pd
import os
import datetime

# -----------------------------
# OneDrive synced folders
# -----------------------------
input_file = r"C:\Users\E027978\Shreyank\OneDrive - Sansera Engineering Limited\Data_ERMS\Daily_Data.csv"
output_folder = r"C:\Users\E027978\Shreyank\OneDrive - Sansera Engineering Limited\Data_ERMS_Cleaned"

# Make sure output folder exists
os.makedirs(output_folder, exist_ok=True)

print(f"Processing file: {input_file}")

# -----------------------------
# Load CSV
# -----------------------------
df = pd.read_csv(input_file)

# Convert Date column
df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

records = []

# -----------------------------
# Process activity blocks 1-10
# -----------------------------
for i in range(1, 11):
    start_col = f"Start Time_{i}"
    end_col = f"End Time_{i}"
    activity_col = f"Activity_{i}"
    project_col = f"Project_Entry_{i}"
    desc_col = f"Description_{i}"

    if start_col not in df.columns:
        continue

    temp = df[['Employee_ID', 'Created By', 'Department', 'Date',
               start_col, end_col, activity_col,
               project_col, desc_col]].copy()

    temp = temp.dropna(subset=[activity_col, start_col, end_col])

    temp[start_col] = pd.to_datetime(temp[start_col], errors='coerce')
    temp[end_col] = pd.to_datetime(temp[end_col], errors='coerce')

    temp['Hours'] = (temp[end_col] - temp[start_col]).dt.total_seconds() / 3600
    temp = temp[temp['Hours'] > 0]

    temp = temp[['Employee_ID','Created By','Department','Date',
                 activity_col, project_col, desc_col, 'Hours']]

    temp.columns = ['Employee_ID','Created By','Department','Date',
                    'Activity', 'Project_Entry', 'Description', 'Hours']

    records.append(temp)

# -----------------------------
# Combine all activities
# -----------------------------
final_df = pd.concat(records, ignore_index=True)

# -----------------------------
# Aggregate by Employee, Date, Activity, Project, Description
# -----------------------------
summary = (
    final_df
    .groupby(['Employee_ID','Created By','Department','Date',
              'Activity', 'Project_Entry', 'Description'],
             as_index=False, dropna=False)['Hours']
    .sum()
)

# Add total hours per Employee per Date
summary['Date_Total_Hours'] = (
    summary
    .groupby(['Employee_ID','Date'])['Hours']
    .transform('sum')
)

# Round hours
summary['Hours'] = summary['Hours'].round(2)
summary['Date_Total_Hours'] = summary['Date_Total_Hours'].round(2)

# Format date
summary['Date'] = summary['Date'].dt.strftime('%Y-%m-%d')

# -----------------------------
# Save cleaned CSV with date in filename
# -----------------------------
date_str = datetime.datetime.now().strftime("%Y-%m-%d")
output_file = os.path.join(output_folder, f"employee_date_activity_summary_{date_str}.csv")
summary.to_csv(output_file, index=False)

print(f"CSV created successfully: {output_file}")
