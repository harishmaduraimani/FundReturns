import pandas as pd
import glob
import os

folder_path = r"C:\Users\Harish Maduraimani S\Downloads\ABSCL2023NAV"  
os.chdir(folder_path)

files = glob.glob("*.txt")
print(" Files found:", files)

all_data = []

for file in files:
    try:
        df = pd.read_csv(
            file,
            sep=";",
            engine="python",
            encoding="latin1",
            on_bad_lines='skip'
        )

        df.columns = df.columns.str.strip()

        expected_cols = ["Scheme Code", "Scheme Name", "Net Asset Value", "Date"]
        df = df[[col for col in expected_cols if col in df.columns]]

        df["Net Asset Value"] = pd.to_numeric(df["Net Asset Value"], errors="coerce")
        df = df[df["Net Asset Value"].notna()]

        df["Date"] = pd.to_datetime(df["Date"], format="%d-%b-%Y", errors="coerce")
        df = df[df["Date"].notna()]

        df = df[df["Scheme Name"].str.contains("Growth") & ~df["Scheme Name"].str.contains("IDCW")]

        if df.empty:
            print(f"⚠ No valid NAV in {file}, skipping...")
            continue

        all_data.append(df)
        print(f" Processed {file} → {len(df)} rows")

    except Exception as e:
        print(f" Failed to process {file}: {e}")

#  Combine all files
if not all_data:
    raise ValueError(" No valid NAV data found in any file!")

final_df = pd.concat(all_data, ignore_index=True)
final_df = final_df.sort_values(["Scheme Code", "Date"])
final_df = final_df.drop_duplicates(subset=["Scheme Code", "Date"])

# SAVE
final_df.to_csv("ABSL_NAV_Clean.csv", index=False)
final_df.to_excel("ABSL_NAV_Clean.xlsx", index=False)
print(" SUCCESS: Clean CSV & Excel created!")
