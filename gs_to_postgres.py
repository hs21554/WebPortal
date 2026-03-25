# gs_to_postgres.py
import os
import pandas as pd
import gspread
from sqlalchemy import create_engine, text
from oauth2client.service_account import ServiceAccountCredentials

# ---------- GOOGLE SHEET AUTH ----------
SERVICE_ACCOUNT_FILE = r"D:\Flask Project\cbs_portal\cbs-project-475204-ee784259d047.json"
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]
creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
client = gspread.authorize(creds)

# ---------- POSTGRES CONNECTION ----------
PG_CONN = "postgresql+psycopg2://postgres:12345@localhost:5432/cbs_data"
engine = create_engine(PG_CONN)

# ---------- GOOGLE SHEET TRACKERS ----------
sheet_trackers = [
    {
        "url": "https://docs.google.com/spreadsheets/d/1DO1YgXylB6ezJiKYuXl4OSxrxj3w81XoRmUfelIs7PA/edit",
        "sheet_name": "North Links",
        "table_name": "cbs_database_links",
        "header_rows": 1
    },
    {
        "url": "https://docs.google.com/spreadsheets/d/1fcizT8G2lucdKPe7X0th3tyDgNKMd2_fsgyRLDnFa4c/edit",
        "sheet_name": "Presales",
        "table_name": "presales_data",
        "header_rows": 2
    }
]

# ---------- DAILY ACTIVITY TRACKER FILE ----------
DAILY_FILE = r"D:\Daily Activity\Daily Activty Tracker.xlsx"
DAILY_SHEET = "Data"
DAILY_TABLE = "survey"

# ---------- NEW FILES ----------
NORTH_DB_FILE = r"D:\Daily Activity\North DB.xlsb"
NORTH_DB_SHEET = "Mastersheet"
NORTH_DB_TABLE = "north_db"

RESPONSE_FILE = r"D:\Daily Activity\Response.xlsx"
RESPONSE_SHEET = "Response_Time_Qry"
RESPONSE_TABLE = "response_data"


# ---------- CLEAN DATA ----------
def clean_data_safe(df):
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace(r'^\s*$', 'Unknown', regex=True)
    return df


# ---------- GOOGLE SHEET MULTI HEADER ----------
def get_multiheader_sheet(sheet, sheet_name, header_rows=1):
    worksheet = sheet.worksheet(sheet_name)
    all_values = worksheet.get_all_values()
    headers = all_values[:header_rows]
    data_rows = all_values[header_rows:]

    expected_cols = len(headers[0])
    data_rows = [row + [""]*(expected_cols - len(row)) for row in data_rows]

    parents = [h.strip() if h.strip() else None for h in headers[0]]
    for i in range(len(parents)):
        if not parents[i] and i > 0:
            parents[i] = parents[i - 1]
    parents = [p if p else "Unnamed" for p in parents]

    if header_rows > 1:
        children = [h.strip() if h.strip() else "" for h in headers[1]]
    else:
        children = [""] * len(parents)

    combined_headers = [f"{p} - {c}" if c else p for p, c in zip(parents, children)]
    df = pd.DataFrame(data_rows, columns=combined_headers)
    df = df.loc[:, ~df.columns.duplicated()]
    return df


# ---------- AUTO CREATE TABLE ----------
def ensure_table_exists(table_name, df):
    cols = ", ".join([f'"{c}" TEXT' for c in df.columns])
    create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({cols});'
    with engine.begin() as conn:
        conn.execute(text(create_sql))


# ---------- GENERIC LOADER ----------
def load_excel(table_name, file_path, sheet_name, xlsb=False):
    try:
        print(f"\nLoading Excel → {file_path} | Sheet: {sheet_name}")

        if xlsb:
            df = pd.read_excel(file_path, sheet_name=sheet_name, engine="pyxlsb")
        else:
            df = pd.read_excel(file_path, sheet_name=sheet_name)

        df.columns = [str(c).strip() for c in df.columns]
        df = clean_data_safe(df)

        ensure_table_exists(table_name, df)

        with engine.begin() as conn:
            conn.execute(text(f'DELETE FROM "{table_name}"'))

        df.to_sql(table_name, engine, if_exists="append", index=False)
        print(f"✔ Inserted {len(df)} rows → {table_name}")

    except Exception as e:
        print(f"Error loading {table_name}:", e)


# ---------- DAILY FILE LOADER ----------
def load_daily_tracker():
    try:
        print(f"\nLoading DAILY ACTIVITY TRACKER → {DAILY_FILE}")
        df = pd.read_excel(DAILY_FILE, sheet_name=DAILY_SHEET)
        df.columns = [str(c).strip() for c in df.columns]
        df = clean_data_safe(df)

        ensure_table_exists(DAILY_TABLE, df)

        with engine.begin() as conn:
            conn.execute(text(f'DELETE FROM "{DAILY_TABLE}"'))

        df.to_sql(DAILY_TABLE, engine, if_exists="append", index=False)
        print(f"✔ Inserted {len(df)} rows → {DAILY_TABLE}")

    except Exception as e:
        print("Daily Tracker Error:", e)


# -------------------------------------------------------
# PROCESS GOOGLE SHEETS
# -------------------------------------------------------
# -------------------------------------------------------
# PROCESS GOOGLE SHEETS
# -------------------------------------------------------
for tracker in sheet_trackers:
    print(f"\nFetching Google Sheet → {tracker['sheet_name']}")
    try:
        sheet = client.open_by_url(tracker["url"])
        df = get_multiheader_sheet(sheet, tracker["sheet_name"], tracker["header_rows"])
        df = clean_data_safe(df)

        # -----------------------------
        # ENSURE TABLE EXISTS + ADD MISSING COLUMNS
        # -----------------------------
        with engine.begin() as conn:
            table_exists = conn.execute(
                text(f"SELECT to_regclass('{tracker['table_name']}');")
            ).scalar()

            if not table_exists:
                # Create table if not exists
                cols = ", ".join([f'"{c}" TEXT' for c in df.columns])
                create_sql = f'CREATE TABLE "{tracker["table_name"]}" ({cols});'
                conn.execute(text(create_sql))
            else:
                # Add any missing columns
                existing_cols = [row[0] for row in conn.execute(
                    text(f"SELECT column_name FROM information_schema.columns WHERE table_name='{tracker['table_name']}'")
                ).fetchall()]

                for c in df.columns:
                    if c not in existing_cols:
                        conn.execute(text(f'ALTER TABLE "{tracker["table_name"]}" ADD COLUMN "{c}" TEXT;'))

        # -----------------------------
        # DELETE OLD DATA
        # -----------------------------
        with engine.begin() as conn:
            conn.execute(text(f'DELETE FROM "{tracker["table_name"]}"'))

        # -----------------------------
        # INSERT NEW DATA
        # -----------------------------
        df.to_sql(tracker["table_name"], engine, if_exists="append", index=False)
        print(f"✔ Inserted {len(df)} rows → {tracker['table_name']}")

    except Exception as e:
        print("Sheet Error:", e)



def load_response_file():
    df = None     # <-- FIX: define df before try block

    try:
        print(f"\nLoading RESPONSE FILE → {RESPONSE_FILE}")

        # --- Read sheet with REAL headers ---
        df = pd.read_excel(RESPONSE_FILE, sheet_name=RESPONSE_SHEET, header=0)

        df = df.fillna("Unknown")

        # Drop table
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{RESPONSE_TABLE}" CASCADE;'))

        # Create new structure
        ensure_table_exists(RESPONSE_TABLE, df)

        # Fast load
        df.to_sql(RESPONSE_TABLE, engine, if_exists="append", index=False)


        print(f"✔ Inserted {len(df)} rows → {RESPONSE_TABLE}")

    except Exception as e:
        print("Response File Error:", e)
        return   # <-- prevent code running further if df not loaded


# ---------- LOAD EXCEL FILES ----------
load_excel(NORTH_DB_TABLE, NORTH_DB_FILE, NORTH_DB_SHEET, xlsb=True)
load_response_file()


# ---------- LOAD DAILY LAST ----------
load_daily_tracker()

print("\n✔ ALL SHEETS + EXCEL + DAILY TRACKER LOADED SUCCESSFULLY!")
