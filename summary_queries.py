import pandas as pd
from database import get_engine

def fetch_summary_cards(year=None, month=None):
    engine = get_engine()

    # ---------------------------
    # Load CBS Data
    # ---------------------------
    df_cbs = pd.read_sql('SELECT * FROM cbs_database_links;', engine)

    # Convert columns
    df_cbs['Mbps'] = pd.to_numeric(df_cbs.get('Mbps', 0), errors='coerce').fillna(0)
    df_cbs['Deployment Date'] = pd.to_datetime(df_cbs.get('Deployment Date'), errors='coerce')

    # Apply year/month filter on deployment date
    if year:
        df_cbs = df_cbs[df_cbs['Deployment Date'].dt.year == year]
    if month:
        df_cbs = df_cbs[df_cbs['Deployment Date'].dt.month == month]

    # Only operational links
    df_operational = df_cbs[df_cbs["Link Status"].str.lower() == "operational"]

    # =========================
    # ACTIVE CUSTOMER (Unique) - Original Names Count
    # =========================
    active_customers_count = (
        df_operational["Customer Name"]
        .dropna()                        # remove NaNs
        .astype(str)                     # ensure string type
        .str.strip()                      # remove leading/trailing spaces
        .nunique()                        # count unique
    )

    # Active links and total BW
    active_links = len(df_operational)
    active_bw = df_operational["Mbps"].sum()

    # =========================
    # PRESALES DATA
    # =========================
    year_filter = f"AND EXTRACT(YEAR FROM {{}}::date) = {year}" if year else ""
    month_filter = f"AND EXTRACT(MONTH FROM {{}}::date) = {month}" if month else ""

    # Feasibility Feedback Count
    feas_res = pd.read_sql(f'''
        SELECT COUNT(*) AS count
        FROM presales_data
        WHERE "Feasibility Feedback Date" NOT IN ('Unknown','', 'None','null','NULL')
        {year_filter.format('"Feasibility Feedback Date"')}
        {month_filter.format('"Feasibility Feedback Date"')}
    ''', engine)
    feas_count = int(feas_res.iloc[0]["count"])

    # SD Submission Count
    df_sd = pd.read_sql('''
        SELECT "SD Submission Date"
        FROM presales_data
        WHERE TRIM("SD Submission Date") <> ''
          AND LOWER(TRIM("SD Submission Date")) NOT IN ('unknown','none','null','-')
    ''', engine)
    df_sd['SD Submission Date'] = pd.to_datetime(df_sd['SD Submission Date'], errors='coerce')
    if year:
        df_sd = df_sd[df_sd['SD Submission Date'].dt.year == year]
    if month:
        df_sd = df_sd[df_sd['SD Submission Date'].dt.month == month]
    sd_count = len(df_sd)

    total_presales = feas_count + sd_count

    # In-Sales Count
    insales_res = pd.read_sql(f'''
        SELECT COUNT(*) AS count
        FROM presales_data
        WHERE "Deployment Date" NOT IN ('Unknown','', 'None','null','NULL')
        {year_filter.format('"Deployment Date"')}
        {month_filter.format('"Deployment Date"')}
    ''', engine)
    in_sales_count = int(insales_res.iloc[0]["count"])

    # =========================
    # DEPLOYED BW
    # =========================
    df_bw = pd.read_sql(f'''
        SELECT "Deployment Date", "Link Type", "Actual BW Taken By Customer"
        FROM presales_data
        WHERE "Link Type" IN ('New Link Activation','Link Upgradation')
          AND TRIM("Deployment Date") NOT IN ('Unknown','', 'None','null','NULL')
    ''', engine)
    df_bw['Deployment Date'] = pd.to_datetime(df_bw['Deployment Date'], errors='coerce')
    df_bw['Actual BW Taken By Customer'] = pd.to_numeric(df_bw['Actual BW Taken By Customer'], errors='coerce').fillna(0)

    if year:
        df_bw = df_bw[df_bw['Deployment Date'].dt.year == year]
    if month:
        df_bw = df_bw[df_bw['Deployment Date'].dt.month == month]

    deployed_bw = df_bw['Actual BW Taken By Customer'].sum()

    # =========================
    # FINAL RESPONSE
    # =========================
    return {
        "summary-active-customers": active_customers_count,  # ✅ 175
        "summary-active-links": int(active_links),
        "summary-active-bw": int(active_bw),
        "summary-deployed-bw": int(deployed_bw),
        "summary-feasibility": feas_count,
        "summary-sd": sd_count,
        "summary-pre-sales": total_presales,
        "summary-in-sales": in_sales_count,
    }
