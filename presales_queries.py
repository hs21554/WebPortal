from flask import Flask, jsonify, request
import pandas as pd
from sqlalchemy import create_engine

app = Flask(__name__)
engine = create_engine("postgresql://postgres:12345@localhost/cbs_data")

def presales_get_distinct_values(column_name):
    valid_columns = [
        "Unique ID", "Link Type", "Customer Name", "Account Name",
        "Service Type", "Year", "Month", "Quarter",
    ]
    if column_name not in valid_columns:
        return []

    try:
        # -------- STATIC VALUES --------
        if column_name == "Year":
            return ["2021", "2022", "2023", "2024", "2025"]

        if column_name == "Month":
            return [
                "January","February","March","April","May","June",
                "July","August","September","October","November","December"
            ]

        if column_name == "Quarter":
            return ["Q-1", "Q-2", "Q-3", "Q-4"]

        # -------- LINK NAME (CBS → Customer Name) --------
        if column_name == "Customer Name":
            query = '''
                SELECT DISTINCT "Customer Name" AS "Customer Name"
                FROM cbs_database_links
                ORDER BY "Customer Name";
            '''
            df = pd.read_sql_query(query, con=engine)
            return df["Customer Name"].dropna().tolist()

        # -------- UNIQUE ID (CBS) --------
        if column_name == "Unique ID":
            query = '''
                SELECT DISTINCT unique_id
                FROM cbs_database_links
                ORDER BY unique_id;
            '''
            df = pd.read_sql_query(query, con=engine)
            return df["unique_id"].dropna().tolist()

        # -------- CUSTOMER NAME (CBS → Account Name) --------
        if column_name == "Account Name":
            query = '''
                SELECT DISTINCT "Account Name" AS "Account Name"
                FROM cbs_database_links
                ORDER BY "Account Name";
            '''
            df = pd.read_sql_query(query, con=engine)
            return df["Account Name"].dropna().tolist()

        # 🔥 SERVICE TYPE → NOW FROM CBS (Product Type)
        if column_name == "Service Type":
            query = '''
                SELECT DISTINCT "Product Type" AS "Service Type"
                FROM cbs_database_links
                ORDER BY "Product Type";
            '''
            df = pd.read_sql_query(query, con=engine)
            return df["Service Type"].dropna().tolist()

        # -------- REMAINING FROM PRESALES --------
        query = f'''
            SELECT DISTINCT "{column_name}"
            FROM presales_data
            ORDER BY "{column_name}";
        '''
        df = pd.read_sql_query(query, con=engine)
        return df[column_name].dropna().tolist()

    except Exception as e:
        print("Database Error:", e)
        return []

def active_links_modification_filtered(filters_dict):
    try:
        query = '''
            SELECT 
                c."unique_id" AS "Unique ID",
                c."Account Name",
                c."Customer Name",
                c."Product Type",
                p."Link Type",
                p."Existing BW" AS "Existing BW",
                p."BW Upgraded/Downgraded/Terminated" AS "BW Modification",
                p."Current BW" AS "BW After Modification",
                p."Deployment Date" AS "Modification Date",
                c."Deployment Date" AS "Deployment Date"
            FROM cbs_database_links c
            LEFT JOIN presales_data p 
                ON TRIM(UPPER(c."unique_id")) = TRIM(UPPER(p."Unique ID"))
            WHERE 1=1
        '''

        filters = []
        params = {}

        # -------- UNIQUE ID --------
        if filters_dict.get("Unique ID"):
            filters.append('c."unique_id" = %(unique_id)s')
            params["unique_id"] = filters_dict["Unique ID"]

        # -------- CUSTOMER NAME --------
        if filters_dict.get("Customer Name"):
            filters.append('LOWER(c."Customer Name") LIKE %(customer_name)s')
            params["customer_name"] = f"%{filters_dict['Customer Name'].lower()}%"

        # -------- ACCOUNT NAME --------
        if filters_dict.get("Account Name"):
            filters.append('LOWER(c."Account Name") LIKE %(account_name)s')
            params["account_name"] = f"%{filters_dict['Account Name'].lower()}%"

        # -------- LINK TYPE --------
        if filters_dict.get("Link Type"):
            filters.append('LOWER(p."Link Type") LIKE %(link_type)s')
            params["link_type"] = f"%{filters_dict['Link Type'].lower()}%"

        # -------- SERVICE TYPE --------
        if filters_dict.get("Service Type"):
            filters.append('LOWER(c."Product Type") LIKE %(service_type)s')
            params["service_type"] = f"%{filters_dict['Service Type'].lower()}%"

        # -------- APPLY FILTERS --------
        if filters:
            query += " AND " + " AND ".join(filters)

        # -------- FINAL ORDER (Modification Date ASC) --------
        query += '''
    ORDER BY 
        CAST(NULLIF(p."Deployment Date", 'Unknown') AS DATE) ASC NULLS LAST,
        c."unique_id"
    LIMIT 12000
'''


        print("ACTIVE LINKS QUERY:", query)
        print("PARAMS:", params)

        # -------- FETCH DATA --------
        df = pd.read_sql_query(query, con=engine, params=params)

        # -------- DATE CONVERSION --------
        for col in ['Deployment Date', 'Modification Date']:
            df[col] = pd.to_datetime(
                df[col].replace(['Unknown', '', None], pd.NaT),
                errors='coerce'
            )

        # -------- DATE FILTERS (POST SQL) --------
        if filters_dict.get("Year"):
            df = df[df['Deployment Date'].dt.year == int(filters_dict["Year"])]

        if filters_dict.get("Months"):
            df = df[df['Deployment Date'].dt.month.isin(
                list(map(int, filters_dict["Months"]))
            )]

        if filters_dict.get("Quarters"):
            df = df[df['Deployment Date'].dt.quarter.isin(
                list(map(int, filters_dict["Quarters"]))
            )]

        # -------- FORMAT DATES --------
        for col in ['Deployment Date', 'Modification Date']:
            df[col] = df[col].dt.strftime('%Y-%m-%d')
            df[col] = df[col].fillna("")

        if df.empty:
            return []

        return df.to_dict(orient="records")

    except Exception as e:
        print("Active Links DB Error:", e)
        return []


def get_presales_filtered_df(filters):
    df = pd.read_sql('''
        SELECT "Deployment Date", "Actual BW Taken By Customer", "Link Type", "Service Type", "Unique ID"
        FROM presales_data
    ''', engine)

    # ---------------- CLEAN ----------------
    df['Deployment Date'] = pd.to_datetime(
        df['Deployment Date'].replace(['Unknown', '', None], pd.NaT),
        errors='coerce'
    )
    df = df.dropna(subset=['Deployment Date'])

    df['Actual BW Taken By Customer'] = (
        df['Actual BW Taken By Customer']
        .astype(str)
        .str.replace(',', '')
        .str.strip()
    )
    df['Actual BW Taken By Customer'] = pd.to_numeric(
        df['Actual BW Taken By Customer'].replace('Unknown', '0'),
        errors='coerce'
    ).fillna(0)

    df['Link Type'] = df['Link Type'].astype(str).str.lower().str.strip()
    df['Service Type'] = df['Service Type'].astype(str).str.lower().str.strip()
    df['Unique ID'] = df['Unique ID'].astype(str).str.lower().str.strip()

    # ---------------- DATE FILTER ----------------
    if filters.get("Year"):
        df = df[df['Deployment Date'].dt.year == int(filters["Year"])]
    if filters.get("Months"):
        df = df[df['Deployment Date'].dt.month.isin(filters["Months"])]

    # ---------------- CBS FILTER ----------------
    customer = filters.get("Customer Name")
    account = filters.get("Account Name")
    service = filters.get("Service Type")

    if customer or account or service:
        q = 'SELECT DISTINCT LOWER(unique_id) AS uid FROM cbs_database_links WHERE 1=1'
        params = {}
        if customer:
            q += ' AND LOWER("Customer Name") LIKE %(customer)s'
            params['customer'] = f"%{customer.lower().strip()}%"
        if account:
            q += ' AND LOWER("Account Name") LIKE %(account)s'
            params['account'] = f"%{account.lower().strip()}%"
        if service:
            q += ' AND LOWER("Product Type") LIKE %(service)s'
            params['service'] = f"%{service.lower().strip()}%"
        cbs_df = pd.read_sql_query(q, engine, params=params)
        if cbs_df.empty:
            return pd.DataFrame()
        df = df[df['Unique ID'].isin(cbs_df['uid'])]

    # 🔥 IMPORTANT FIX
    if service:
        df = df[df['Service Type'] == service.lower().strip()]

    # ---------------- DIRECT FILTERS ----------------
    if filters.get("Unique ID"):
        df = df[df['Unique ID'] == filters['Unique ID'].lower().strip()]
    if filters.get("Link Type"):
        df = df[df['Link Type'] == filters['Link Type'].lower().strip()]

    return df


def get_presales_cards(filters):
    try:
        # -------------------------------
        # 1️⃣ Get filtered dataframe
        # -------------------------------
        df = get_presales_filtered_df(filters)

        if df.empty:
            return {
                "deployed_bw": "0 Mbps",
                "terminated_bw": "0 Mbps",
                "cumulative_bw": "0 Mbps"
            }

        # -------------------------------
        # 2️⃣ CLEAN DATA (MOST IMPORTANT)
        # -------------------------------

        # Clean Link Type (case + spaces)
        df['Link Type'] = (
            df['Link Type']
            .astype(str)
            .str.strip()
            .str.lower()
        )

        # Clean BW column (remove Mbps, spaces, text)
        df['Actual BW Taken By Customer'] = (
            df['Actual BW Taken By Customer']
            .astype(str)
            .str.replace("Mbps", "", regex=False)
            .str.replace("mbps", "", regex=False)
            .str.strip()
        )

        # Convert to numeric
        df['Actual BW Taken By Customer'] = pd.to_numeric(
            df['Actual BW Taken By Customer'],
            errors='coerce'
        )

        # -------------------------------
        # 3️⃣ DEFINE LINK TYPES
        # -------------------------------
        DEPLOYED_TYPES = [
            'new link activation',
            'link upgradation'
        ]

        TERMINATED_TYPES = [
            'link termination',
            'link downgradation'
        ]

        # -------------------------------
        # 4️⃣ FILTER ROWS
        # -------------------------------
        deployed_df = df[df['Link Type'].isin(DEPLOYED_TYPES)]
        terminated_df = df[df['Link Type'].isin(TERMINATED_TYPES)]

        # -------------------------------
        # 5️⃣ DEBUG INFO (VERY IMPORTANT)
        # -------------------------------
        print("🔍 TOTAL ROWS AFTER FILTER:", len(df))
        print("🔍 DEPLOYED ROWS:", len(deployed_df))
        print("🔍 TERMINATED ROWS:", len(terminated_df))

        # Missing BW rows (these cause mismatch)
        missing_bw_rows = deployed_df[
            deployed_df['Actual BW Taken By Customer'].isna()
        ]

        if not missing_bw_rows.empty:
            print("❌ ROWS WITH MISSING BW (DEPLOYED):")
            print(
                missing_bw_rows[
                    ['Unique ID', 'Deployment Date', 'Link Type', 'Actual BW Taken By Customer']
                ]
            )

        # -------------------------------
        # 6️⃣ CALCULATIONS
        # -------------------------------
        deployed = deployed_df['Actual BW Taken By Customer'].sum()
        terminated = terminated_df['Actual BW Taken By Customer'].sum()
        cumulative = deployed - terminated

        # -------------------------------
        # 7️⃣ FINAL RESPONSE
        # -------------------------------
        return {
            "deployed_bw": f"{int(deployed)} Mbps",
            "terminated_bw": f"{int(terminated)} Mbps",
            "cumulative_bw": f"{int(cumulative)} Mbps"
        }

    except Exception as e:
        print("❌ Cards Error:", e)
        return {
            "deployed_bw": "-",
            "terminated_bw": "-",
            "cumulative_bw": "-"
        }
