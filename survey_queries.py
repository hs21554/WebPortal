# survey_queries_fast.py
import pandas as pd
from database import get_engine

# -----------------------------
# Global Cache
# -----------------------------
survey_cache = None

def load_survey_table():
    """
    FAST VERSION:
    ✔ Only required columns loaded (NOT SELECT *)
    ✔ No backticks → Full Postgres support
    ✔ Cached for ultra-fast filtering
    """
    global survey_cache

    if survey_cache is None:
        engine = get_engine()

        query = """
            SELECT
                "Request Date",
                "Feedback Date",
                "Month",
                "Activity type",
                "Sub Activity type",
                "Status",
                "DIA/DPLC",
                "Account Name",
                "Last Mile CMPAK Site",
                "Last Mile Connectivity",
                "Aggregation",
                "KAM",
                "Vender",
                "City",
                "Bandwidth Required (Mbps)"
            FROM survey;
        """

        df = pd.read_sql(query, engine)

        # Preprocess once
        df.fillna('', inplace=True)

        # Lowercase normalize once
        lowercase_cols = [
            'Activity type', 'Sub Activity type', 'Status',
            'DIA/DPLC', 'Account Name'
        ]
        for col in lowercase_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.lower()

        # Convert dates once
        if 'Request Date' in df.columns:
            df['Request Date'] = pd.to_datetime(df['Request Date'], errors='coerce')

        if 'Month' in df.columns:
            df['Month'] = pd.to_datetime(df['Month'], errors='coerce')

        # Cache final df
        survey_cache = df

    return survey_cache.copy()

# -----------------------------
# FAST FILTER
# -----------------------------
def filter_survey(df, year=None, month=None, type_filter=None, Status=None):

    # Year-month filter
    if year:
        df = df[df['Request Date'].dt.year == int(year)]

    if month:
        df = df[df['Request Date'].dt.month == int(month)]

    # Product Type filter (DIA/DPLC)
    if type_filter:
        df = df[df['DIA/DPLC'].str.contains(type_filter.lower(), na=False)]

    # Status filter
    if Status:
        Status = Status.lower().strip()

        if Status == "completed":
            df = df[df['Status'] == "complete"]

        elif Status == "in progress" or Status == "pending":
            df = df[df['Status'] == "in progress"]

    return df

# -----------------------------
# Survey Cards
# -----------------------------
def get_survey_cards(year=None, month=None, type_filter=None, Status=None):

    df = load_survey_table()
    df = filter_survey(df, year, month, type_filter, Status)

    cards = {
        'desktop_survey'     : len(df[(df['Activity type']=='survey') & (df['Sub Activity type']=='desktop survey')]),
        'desktop_completed'  : len(df[(df['Activity type']=='survey') & (df['Sub Activity type']=='desktop survey') & (df['Status']=='complete')]),
        'desktop_pending'    : len(df[(df['Activity type']=='survey') & (df['Sub Activity type']=='desktop survey') & (df['Status']=='in progress')]),

        'troubleshooting'    : len(df[df['Activity type']=='troubleshooting']),

        'physical_survey'    : len(df[(df['Activity type']=='survey') & (df['Sub Activity type']=='physical survey')]),
        'physical_completed' : len(df[(df['Activity type']=='survey') & (df['Sub Activity type']=='physical survey') & (df['Status']=='complete')]),
        'physical_pending'   : len(df[(df['Activity type']=='survey') & (df['Sub Activity type']=='physical survey') & (df['Status']=='in progress')]),

        'service_type'       : len(df[df['DIA/DPLC'].notna() & (df['DIA/DPLC']!='')])
    }

    return cards

# -----------------------------
# Survey Table Data
# -----------------------------
def get_survey(year=None, month=None, type_filter=None, Status=None):

    df = load_survey_table()
    df = filter_survey(df, year, month, type_filter, Status)

    df = df.rename(columns={
        'Request Date': 'Date Assign',
        'DIA/DPLC': 'Product Type',
        'Bandwidth Required (Mbps)': 'BW (Mbps)'
    })

    if 'Month' in df.columns:
        df['Month'] = df['Month'].dt.strftime('%b-%y').fillna('')

    for col in ['Date Assign', 'Feedback Date']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%d-%b-%Y').fillna('')

    return df.fillna('').to_dict(orient='records')

# -----------------------------
# PM Activity
# -----------------------------
def get_pmactivity(year=None, month=None, type_filter=None, Status=None):

    df = load_survey_table()
    df = filter_survey(df, year, month, type_filter, Status)

    df = df.rename(columns={
        'Account Name': 'Link Name',
        'Last Mile CMPAK Site': 'Node ID',
        'Vender': 'Vendor',
        'Request Date': 'Date'
    })

    if 'Month' in df.columns:
        df['Month'] = df['Month'].dt.strftime('%b-%y').fillna('')

    if 'Date' in df.columns:
        df['Date'] = df['Date'].dt.strftime('%d-%b-%Y').fillna('')

    return df.to_dict(orient='records')

# -----------------------------
# Survey Details
# -----------------------------
def get_details(year=None, month=None, type_filter=None, Status=None):

    df = load_survey_table()
    df = filter_survey(df, year, month, type_filter, Status)

    df = df.rename(columns={
        'Last Mile CMPAK Site': 'Node ID',
        'Vender': 'Vendor'
    })

    cols = ['Node ID', 'Last Mile Connectivity', 'Aggregation', 'KAM', 'Vendor', 'City']
    for c in cols:
        if c not in df.columns:
            df[c] = ''

    return df[cols].to_dict(orient='records')
