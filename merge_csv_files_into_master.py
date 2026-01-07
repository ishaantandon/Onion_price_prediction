import pandas as pd
import glob
import os

# Folder with cleaned files
input_folder = r"C:\Users\PESU-RF\Downloads\Onions\Onions\raw_data_cleaned"
output_folder = r"C:\Users\PESU-RF\Downloads\Onions\Onions\processed_only_onion"
os.makedirs(output_folder, exist_ok=True)
output_file = os.path.join(output_folder, "master_dataset.parquet")

# Get all cleaned Excel files
files = glob.glob(os.path.join(input_folder, "*.xlsx"))

all_dfs = []

for file in files:
    try:
        df = pd.read_excel(file)
        all_dfs.append(df)
        print(f"✅ Loaded: {file}")
    except Exception as e:
        print(f"❌ Error loading {file}: {e}")

# Merge all DataFrames
merged_df = pd.concat(all_dfs, ignore_index=True)

# Convert date column to datetime if it exists
if "date" in merged_df.columns:
    merged_df['date'] = pd.to_datetime(merged_df['date'], errors='coerce')

# Sort by date and mandi
sort_cols = [c for c in ["date", "mandi"] if c in merged_df.columns]
merged_df.sort_values(by=sort_cols, inplace=True)
merged_df.reset_index(drop=True, inplace=True)

# Save as Parquet
merged_df.to_parquet(output_file, index=False)
print(f"\n🎉 Master dataset saved to: {output_file}")
