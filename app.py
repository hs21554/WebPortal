from flask import Flask, render_template, request, redirect, url_for, session, jsonify
from werkzeug.security import generate_password_hash, check_password_hash
from apscheduler.schedulers.background import BackgroundScheduler
from overview_queries import get_overview_cards, get_overview_tables, get_services_chart, get_bw_availability_chart, get_active_links_chart
from presales_queries import presales_get_distinct_values, active_links_modification_filtered, get_presales_cards, get_presales_filtered_df
from survey_queries import get_survey_cards, get_details, get_pmactivity,get_survey
from queries import get_dashboard_data, get_table_data
from summary_queries import fetch_summary_cards
from post_queries import get_post_sales_data  
from googleapiclient.discovery import build
from google.oauth2 import service_account
from urllib.parse import unquote_plus
from sqlalchemy import create_engine
import pandas as pd
import subprocess
import psycopg2 
import base64


app = Flask(__name__)
app.secret_key = 'supersecretkey'

SERVICE_ACCOUNT_FILE = 'cbs-project-475204-ee784259d047.json'
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)

drive_service = build('drive', 'v3', credentials=credentials)

# Database Connection
def get_connection():
    try:
        return psycopg2.connect(
            host="localhost",
            database="cbs_data",
            user="postgres",
            password="12345",
        )
    except Exception as e:
        print("PostgreSQL Error:", e)
        return None

# Home
@app.route('/')
def home():
    return redirect(url_for('login'))

# LOGIN
@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    message = None

    if request.method == 'POST':
        username = request.form['username'].strip()
        password = request.form['password'].strip()

        conn = get_connection()
        if conn is None:
            return render_template('login.html', error="Database connection error")

        cur = conn.cursor()
        cur.execute("SELECT password, active FROM users WHERE username=%s", (username,))
        user = cur.fetchone()

        if user:
            db_password, active = user
            if not active:
                message = "Your account is pending approval."
            elif check_password_hash(db_password, password):
                session['username'] = username
                return redirect(url_for('dashboard'))
            else:
                error = "Invalid username or password."
        else:
            hashed = generate_password_hash(password)
            cur.execute(
                "INSERT INTO users (username, password, active) VALUES (%s, %s, %s)",
                (username, hashed, False)
            )
            conn.commit()
            message = "Request submitted! Wait for admin approval."

        cur.close()
        conn.close()

    return render_template('login.html', error=error, message=message)

# RESET PASSWORD
@app.route('/reset_password', methods=['POST'])
def reset_password():
    data = request.get_json()
    username = data.get("username")
    new_password = data.get("new_password")
    confirm = data.get("confirm_password")

    if new_password != confirm:
        return jsonify({"status": "error", "message": "Passwords do not match!"})

    conn = get_connection()
    if conn is None:
        return jsonify({"status": "error", "message": "DB connection error"})

    cur = conn.cursor()
    cur.execute("SELECT id FROM users WHERE username=%s", (username,))
    user = cur.fetchone()

    if not user:
        return jsonify({"status": "error", "message": "User not found"})

    hashed = generate_password_hash(new_password)
    cur.execute("UPDATE users SET password=%s WHERE username=%s", (hashed, username))
    conn.commit()

    cur.close()
    conn.close()

    return jsonify({"status": "success", "message": "Password updated!"})

# DASHBOARD 
@app.route('/dashboard')
def dashboard():
    if 'username' not in session:
        return redirect(url_for('login'))
    

    dashboard_data = get_dashboard_data()
    table_data = get_table_data()
    merged = {**dashboard_data, **table_data}

    return render_template(
        'dashboard.html',
        username=session['username'],
        data=dashboard_data,
        table_data=merged,
        chart_image=table_data.get("service_type_chart")
    )

# Database Tab Cards API (Queries.PY)
@app.route("/api/get_dashboard_data")
def api_get_dashboard_data():
    data = get_dashboard_data()   # No filters → default '-' data return karega
    return jsonify(data)

# Database Tab Tables Data API (Queries.PY)
@app.route('/get_table_data')
def get_table_data_route():
    filters = {
        'site_id': request.args.get('site_id'),
        'city': request.args.get('city'),
        'unique_id': request.args.get('unique_id'),
        'customer_name': request.args.get('customer_name'),
        'product_type': request.args.get('product_type'),
        'isp': request.args.get('isp'),
        'bgp': request.args.get('bgp'),
        'region': request.args.get('region'),
        'account_name': request.args.get('account_name'),
        'deployment_date': request.args.get('deployment_date'),
        'deployment_month': request.args.get('deployment_month')
    }

    data = get_table_data(**filters)
    return jsonify(data)

# Database Tab Search Button Data Fetch API (Queries.PY)
@app.route('/search', methods=['GET'])
def search():
    filters = [
        'site_id', 'city', 'unique_id', 'customer_name', 'product_type', 
        'isp', 'bgp', 'region', 'vendor_name', 
        'account_name', 'deployment_date', 'deployment_month'
    ]
    search_params = {f: request.args.get(f) or None for f in filters}

    table_data_dict = get_table_data(**search_params)
    dashboard_data = get_dashboard_data(**search_params)

    response = {**dashboard_data, **table_data_dict}
    if "service_type_chart" in table_data_dict:
        response["chart_image"] = table_data_dict["service_type_chart"]

    return jsonify(response)

# Database Search Bar POPUP COLUMN VALUES API 
@app.route("/get_column_values")
def get_column_values():
    column = request.args.get("column")

    allowed = [
        "CMPak Site Id", "City", "unique_id", "Customer Name",
        "Product Type", "ISP", "BGP", "Region", "Vendor Name",
        "Account Name", "Deployment Date", "Deployment Month"
    ]

    if column not in allowed:
        return {"values": []}

    conn = get_connection()
    df = pd.read_sql(
        f'SELECT DISTINCT "{column}" FROM cbs_database_links WHERE "{column}" IS NOT NULL AND "{column}" != \'\' ORDER BY "{column}"',
        conn
    )

    return {"values": df[column].astype(str).tolist()}

# Database SD Folder API 
def get_drive_service():
    return build('drive', 'v3', credentials=credentials)

# Database SD Folder API 
def find_folders_under_parent_by_name(parent_folder_id, customer_name):
    q = f"'{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    page_token = None
    all_folders = []

    while True:
        resp = drive_service.files().list(
            q=q,
            fields="nextPageToken, files(id, name)",
            pageToken=page_token,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True
        ).execute()

        all_folders.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    # case-insensitive filter
    customer_lower = customer_name.lower()
    matched = [{"id": f["id"], "name": f["name"]} for f in matched]
    return jsonify({"status":"success","folders":matched})

# Database SD Folder API 
@app.route("/list_drive_folders_by_customer")
def list_drive_folders_by_customer():
    try:
        customer_name = request.args.get("customer_name", "").strip()
        if not customer_name:
            return jsonify({"status":"error","message":"Customer Name required"}), 400

        parent_folder = "18kfxe1wygpUA4pajER3IJw7ksOPVfPb1"
        folders = find_folders_under_parent_by_name(parent_folder, customer_name)

        if not folders:
            return jsonify({"status":"empty","message":"No folder found!"})

        return jsonify({"status":"success","folders":folders})
    except Exception as e:
        return jsonify({"status":"error","message":str(e)}), 500

# Database SD Folder API 
@app.route("/list_customer_files")
def list_customer_files():
    folder_name = request.args.get("folder_name")  # name se fetch
    if not folder_name:
        return jsonify({"status": "error", "message": "Folder Name missing!"}), 400

    try:
        parent_folder = "18kfxe1wygpUA4pajER3IJw7ksOPVfPb1"  # main parent folder

        # 1️⃣ Pehle folder find karo by name
        folder_query = (
            f"'{parent_folder}' in parents and "
            f"mimeType='application/vnd.google-apps.folder' and "
            f"name contains '{folder_name}' and trashed=false"
        )
        folder_res = drive_service.files().list(
            q=folder_query, fields="files(id,name)"
        ).execute()
        folders = folder_res.get("files", [])
        if not folders:
            return jsonify({"status":"empty","message":"Folder not found!"})

        # assume first match (agar multiple same name)
        folder_id = folders[0]["id"]

        # 2️⃣ Files fetch inside this folder
        files_res = drive_service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="files(id,name,mimeType,webViewLink,exportLinks)",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            pageSize=200
        ).execute()

        files = files_res.get("files", [])
        if not files:
            return jsonify({"status":"empty","message":"No files found!"})

        files_data = [{"id": f["id"], "name": f["name"], "mimeType": f.get("mimeType")} for f in files]
        return jsonify({"status":"success","files":files_data})

    except Exception as e:
        return jsonify({"status":"error","message": str(e)}), 500

# Database SD Folder API 
@app.route("/view_drive_file")
def view_drive_file():
    file_id = request.args.get("file_id")
    if not file_id:
        return jsonify({"status":"error","message":"File ID missing!"}), 400

    try:
        # get file metadata (links)
        file = drive_service.files().get(
            fileId=file_id,
            fields="id,name,mimeType,webViewLink,exportLinks"
        ).execute()

        mime = file.get("mimeType", "")
        # If Google Docs/Sheets/Slides — try to export as PDF if available
        if mime.startswith("application/vnd.google-apps"):
            export_links = file.get("exportLinks", {})
            # prefer pdf
            pdf_link = export_links.get("application/pdf")
            if pdf_link:
                return jsonify({"status":"success","file_url":pdf_link,"name":file["name"]})
            # otherwise return first export link
            if export_links:
                first = next(iter(export_links.values()))
                return jsonify({"status":"success","file_url":first,"name":file["name"]})

        # Non-Google file (pdf, docx, images) — use webViewLink
        web_view = file.get("webViewLink")
        if web_view:
            return jsonify({"status":"success","file_url":web_view,"name":file["name"]})

        # As fallback: stream content and return base64 (works but heavier)
        media = drive_service.files().get_media(fileId=file_id).execute()
        encoded = base64.b64encode(media).decode("utf-8")
        # caller can do: src="data:<mime>;base64,<encoded>"
        return jsonify({"status":"success","file_name":file["name"], "content_base64": encoded, "mime": mime})

    except Exception as e:
        return jsonify({"status":"error","message":str(e)}), 500

# Overview Cards API 
@app.route("/overview/cards")
def overview_cards():
    year = request.args.get("year")
    month = request.args.get("month")

    year = int(year) if year else None
    month = int(month) if month else None

    cards = get_overview_cards(year, month)
    return jsonify(cards)

# Overview ISP Table API
@app.route("/overview/isp")
def overview_isp():
    return jsonify(get_overview_tables()["ovr-isp"])

# Overview VRF Table API
@app.route("/overview/vrf")
def overview_vrf():
    return jsonify(get_overview_tables()["ovr-vrf"])

# Overview BGP/Non-BGP Table API
@app.route("/overview/bgp")
def overview_bgp():
    return jsonify(get_overview_tables()["ovr-bgp"])

# Overview Top Customers w.r.t BW (Mbps) Table API
@app.route("/overview/customers")
def overview_customers():
    return jsonify(get_overview_tables()["overview-table-customers"])

# Overview Last Mile Vendors Table API
@app.route("/overview/vendors")
def overview_vendors():
    return jsonify(get_overview_tables()["overview-table-vendors"])

# Overview Last Mile Connectivity Table API
@app.route("/overview/connectivity")
def overview_connectivity():
    return jsonify(get_overview_tables()["overview-table-connectivity"])

# Overview Region Table API
@app.route("/overview/region")
def overview_region():
    return jsonify(get_overview_tables()["overview-table-regions"])

# Overview Service Type of Active Links Table API
@app.route("/overview/packages")
def overview_packages():
    return jsonify(get_overview_tables()["overview-table-packages"])

# Overview Service Link and BW Sum Chart API
@app.route("/overview/services_chart")
def services_chart():
    year = request.args.get("year") or None
    month = request.args.get("month") or None

    service_chart = get_services_chart(year, month)       # returns {"chart": ...}
    bw_chart = get_bw_availability_chart(year, month)     # returns {"chart": ...}

    return jsonify({
    "service_chart": service_chart.get("chart", ""),
    "bw_chart": bw_chart.get("chart", "")
    })

# Overview Service Type of Active Links Chart API
@app.route("/overview/active_links_chart")
def active_links_chart():
    chart_data = get_active_links_chart()   # returns {"chart": base64string}

    return jsonify({
        "chart": chart_data.get("chart", "")
    })

# Survey Tab API
@app.route('/survey')
def survey_page():
    return render_template('survey.html')

# Survey Cards API
@app.route('/survey/cards')
def survey_cards_api():
    year = request.args.get('year') or None
    month = request.args.get('month') or None
    type_filter = request.args.get('type') or None
    Status = request.args.get('Status') or None
    cards = get_survey_cards(year, month, type_filter, Status)
    return jsonify(cards)

# Survey Table API
@app.route("/survey/data")
def survey_api():
    year = request.args.get('year') or None
    month = request.args.get('month') or None
    type_filter = request.args.get('type') or None
    Status = request.args.get('Status') or None
    data = get_survey(year, month, type_filter, Status)
    return jsonify(data)

# Survey PM-Activity Table API
@app.route("/survey/pmactivity")
def pmactivity_api():
    year = request.args.get('year')
    month = request.args.get('month')
    type_filter = request.args.get('type')
    Status = request.args.get('Status') or None
    data = get_pmactivity(year, month, type_filter, Status)
    return jsonify(data)

# Survey Detail Table API
@app.route("/survey/details")
def details_api():
    year = request.args.get('year')
    month = request.args.get('month')
    type_filter = request.args.get('type')
    Status = request.args.get('Status') or None
    data = get_details(year, month, type_filter, Status)
    return jsonify(data)

# Post-Sales Cards API
@app.route("/api/post_sales_cards")
def post_sales_cards():
    try:
        # Fetch query params
        year = request.args.get('year')
        month_param = request.args.get('month')
        link_name = request.args.get('link_name')
        
        if link_name:
            link_name = unquote_plus(link_name)  # Convert + to space, decode special chars

        # Convert year to int
        year = int(year) if year and year.isdigit() else None

        # Convert month_param to list of ints
        months = parse_months_param(month_param)

        # DEBUG: show filters being applied
        print("Filters applied:", {"year": year, "months": months, "link_name": link_name})

        # Call main data function and pass months list
        data = get_post_sales_data(year, months, link_name)

        # DEBUG: show returned data
        print("Returned data:", data)

        return jsonify(data)
    except Exception as e:
        print("Error in API:", e)
        return jsonify({
            "total_complaints": 0,
            "service_degradation_count": 0,
            "service_degradation_avg": 0,
            "service_outage_count": 0,
            "service_outage_avg": 0,
            "sla": "-"
        })

# connection
conn = psycopg2.connect(
    host="localhost",
    database="cbs_data",
    user="postgres",
    password="12345"
)
# Post-Sales Link Name Search Bar Filter API
@app.route("/api/get_link_name_values")
def get_link_name_values():
    try:
        cur = conn.cursor()
        # Filter by Account Region = 'NORTH' AND Reported Issue Category = 'SERVICE OUTAGE'
        cur.execute('''
    SELECT DISTINCT "NMS User Label"
    FROM response_data
    WHERE TRIM("Account Region") ILIKE 'north'
    AND UPPER(TRIM("Reported Issue Category")) = 'SERVICE OUTAGE'
    ORDER BY "NMS User Label"
    ''')

        data = [row[0] for row in cur.fetchall()]
        cur.close()
        return jsonify(data)
    except Exception as e:
        print("Error fetching link names:", e)
        return jsonify([])

# Post-Sales Troubleshooting Table API
@app.route("/api/troubleshooting_data")
def troubleshooting_data():
    year = request.args.get("year", type=int)
    month_param = request.args.get("month")
    months = parse_months_param(month_param)
    link_name = request.args.get("link_name")

    query = """
    SELECT "Client",
           "NMS User Label",
           "Service Type",
           "Reported Issue Category",
           "Complaint Date/Time",
           "Resolution Time (minutes)" AS "Resolution Time",
           "Status"
    FROM "response_data"
    WHERE "Account Region" ILIKE 'North'
    """

    params = {}
    if year:
        query += " AND EXTRACT(YEAR FROM TO_TIMESTAMP(\"Complaint Date/Time\", 'YYYY-MM-DD HH24:MI:SS')) = :year"
        params["year"] = year
    if months:
        query += f" AND EXTRACT(MONTH FROM TO_TIMESTAMP(\"Complaint Date/Time\", 'YYYY-MM-DD HH24:MI:SS')) IN ({','.join(map(str, months))})"
    if link_name:
        query += " AND \"NMS User Label\" ILIKE :link_name"
        params["link_name"] = f"%{link_name}%"

    with engine.connect() as conn:
        result = conn.execute(text(query), params).mappings().all()
        return jsonify([dict(row) for row in result])

# Post-Sales RCA Table API
@app.route("/api/rca_data")
def rca_data():
    year = request.args.get("year", type=int)
    month_param = request.args.get("month")
    months = parse_months_param(month_param)
    link_name = request.args.get("link_name")

    query = """
    SELECT "NMS User Label" AS "Account Name",
           "Reported Issue Category" AS "Reported Issue",
           "RCA",
           "Complaint Date/Time"
    FROM "response_data"
    WHERE "Account Region" ILIKE 'North'
    """

    params = {}
    if year:
        query += " AND EXTRACT(YEAR FROM TO_TIMESTAMP(\"Complaint Date/Time\", 'YYYY-MM-DD HH24:MI:SS')) = :year"
        params["year"] = year
    if months:
        query += f" AND EXTRACT(MONTH FROM TO_TIMESTAMP(\"Complaint Date/Time\", 'YYYY-MM-DD HH24:MI:SS')) IN ({','.join(map(str, months))})"
    if link_name:
        query += " AND \"NMS User Label\" ILIKE :link_name"
        params["link_name"] = f"%{link_name}%"

    with engine.connect() as conn:
        result = conn.execute(text(query), params).mappings().all()
        return jsonify([dict(row) for row in result])

# Site ID for Site Avaialbility Pop Up
engine = create_engine("postgresql://postgres:12345@localhost/cbs_data")
@app.route("/api/get_site_values")
def get_site_values():
    column_map = {
        "site-avb-id-input": "CMPak Site Id",
    }

    input_id = request.args.get("column")
    if input_id not in column_map:
        return jsonify([])

    col_name = column_map[input_id]
    query = f'''
        SELECT DISTINCT "{col_name}" AS val
        FROM cbs_database_links
        WHERE "{col_name}" IS NOT NULL
        ORDER BY "{col_name}" ASC;
    '''
    print("Running query:", query)

    try:
        df = pd.read_sql(query, con=engine)
        print("Fetched rows:", len(df))
        if df.empty:
            print("No data returned for column:", col_name)
        values = df['val'].dropna().astype(str).tolist()
        return jsonify(values)
    except Exception as e:
        print(f"Error fetching values for {col_name}: {e}")
        return jsonify([])

# Get Site Availability from north_db (PMO Database)
from sqlalchemy import text
@app.route("/api/get_site_availability", methods=["GET"])
def get_site_availability():
    site_id = request.args.get("site_id")

    if not site_id:
        return jsonify({"status": "error", "message": "Missing site_id"}), 400

    try:
        query = text("""
        SELECT 
            "NAME",
            "Jan1","Feb1","Mar1","Apr1","May1","Jun1",
            "Jul1","Aug1","Sep1","Oct1","Nov1","Dec1"
        FROM public.north_db
        WHERE "NAME" = :site_id
        LIMIT 1;
        """)

        df = pd.read_sql(query, con=engine, params={"site_id": site_id})

        if df.empty:
            return jsonify({"status": "not_found"}), 200

        row = df.iloc[0].to_dict()

        def clean_number(val):
            if val is None:
                return 0
            try:
                return float(str(val).replace("%","").replace(",","").strip())
            except:
                return 0

        months = [clean_number(row.get(m)) for m in ["Jan1","Feb1","Mar1","Apr1","May1","Jun1",
                                                      "Jul1","Aug1","Sep1","Oct1","Nov1","Dec1"]]

        # Ensure all values are valid numbers (replace NaN with 0)
        months = [0 if not isinstance(x, (int,float)) or x != x else x for x in months]

        return jsonify({
            "status": "success",
            "site_id": row["NAME"],
            "months": months
        })

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)})

# Fetch distinct Account Names for Site Link Name (Match site id according to link Name from CBS)
from sqlalchemy import text
@app.route("/api/get_site_id_by_link")
def get_site_id_by_link():
    link_name = request.args.get("link_name")
    if not link_name:
        return jsonify({"status":"error", "message":"Link Name missing"})

    try:
        with engine.connect() as conn:
            query = text("""
                SELECT "CMPak Site Id"
                FROM cbs_database_links
                WHERE "Account Name" ILIKE :link_name
                LIMIT 1
            """)
            result = conn.execute(query, {"link_name": link_name.strip()})
            site_id = result.scalar()  # Directly get first value

        if site_id:
            return jsonify({"status":"success", "site_id": site_id})
        else:
            return jsonify({"status":"success", "site_id": ""})  # no site found

    except Exception as e:
        print("DB Error:", e)
        return jsonify({"status":"error", "message": str(e)})

# Site Link Name Distinct Values
@app.route("/api/get_site_link_name_values")
def get_site_link_name_values():
    try:
        query = text("""
            SELECT DISTINCT "Account Name" AS name
            FROM cbs_database_links
            WHERE "Account Name" IS NOT NULL
            ORDER BY "Account Name" ASC
        """)
        df = pd.read_sql(query, con=engine)
        values = df['name'].dropna().astype(str).tolist()
        return jsonify(values)
    except Exception as e:
        print("Error fetching site link names:", e)
        return jsonify([])

# Post Sales Site Link Detail Table API
@app.route("/api/get_link_details")
def get_link_details():
    site_id = request.args.get("site_id")
    link_name = request.args.get("link_name")

    query = """
    SELECT
        "CMPak Site Id",
        "Account Name",
        "Customer Name",
        "Region",
        "Deployment Date",
        "Deployment Month",
        "Product Type",
        "Mbps"
    FROM cbs_database_links
    WHERE 1=1
    """
    params = {}

    if site_id:
        query += ' AND "CMPak Site Id" = :site_id'
        params["site_id"] = site_id

    if link_name:
        query += ' AND "Account Name" ILIKE :link_name'
        params["link_name"] = f"%{link_name}%"

    with engine.connect() as conn:
        rows = conn.execute(text(query), params).mappings().all()
        data = [dict(row) for row in rows]  # <-- FIX

    return jsonify(data)  # ALWAYS SAFE

# Post-Sales Quarter Filter API
def parse_months_param(month_param):
    """
    Convert month parameter (string or int or comma-separated string) to list of ints.
    Example: "1,2,3" -> [1,2,3]
             "4"     -> [4]
    """
    if not month_param:
        return []
    
    # Agar integer string aa raha hai
    if isinstance(month_param, str):
        return [int(m.strip()) for m in month_param.split(",") if m.strip().isdigit()]
    
    # Agar int aa gaya
    if isinstance(month_param, int):
        return [month_param]
    
    return []

# In-Sales not presales ok Pop Up Column Values API
@app.route("/api/presales_filter_column_values")
def presales_filter_column_values():
    column = request.args.get("column")
    values = presales_get_distinct_values(column)
    return jsonify(values)

# In-Sales Active Link Table API
@app.route("/api/active_links_modification", methods=["POST"])
def api_active_links_modification():
    filters = request.json or {}
    print("ACTIVE LINKS FILTERS:", filters)
    data = active_links_modification_filtered(filters)
    return jsonify(data)

# In-Sales 3 Cards API
@app.route("/api/presales_cards", methods=["POST"])
def presales_cards_route():
    filters = request.json or {}
    result = get_presales_cards(filters)
    return jsonify(result)

# In-Sales Line Chart API and Query
@app.route("/api/presales_line_chart_data", methods=["POST"])
def presales_line_chart_data():
    filters = request.json or {}
    try:
        # 🔹 Load filtered data
        df = get_presales_filtered_df(filters)
        if df.empty:
            return jsonify([])

        # -------------------------------
        # Ensure Deployment Date is datetime
        # -------------------------------
        df['Deployment Date'] = pd.to_datetime(df['Deployment Date'], errors='coerce')
        df = df.dropna(subset=['Deployment Date'])

        # -------------------------------
        # BW ADJUSTMENT
        # -------------------------------
        def adjust_bw(row):
            if row["Link Type"] in ["link termination", "link downgradation"]:
                return -row["Actual BW Taken By Customer"]
            return row["Actual BW Taken By Customer"]

        df["bw_adjusted"] = df.apply(adjust_bw, axis=1)

        # -------------------------------
        # TOOLTIP DATA
        # -------------------------------
        tooltip = {}
        for _, r in df.iterrows():
            date_key = r["Deployment Date"].strftime("%Y-%m-%d")
            tooltip.setdefault(
                date_key,
                {
                    "added_bw": 0,
                    "removed_bw": 0,
                    "link_types_add": set(),
                    "service_types_add": set(),
                    "link_types_remove": set(),
                    "service_types_remove": set(),
                },
            )
            bw = r["Actual BW Taken By Customer"]
            bw_adj = r["bw_adjusted"]
            if bw_adj >= 0:
                tooltip[date_key]["added_bw"] += bw
                tooltip[date_key]["link_types_add"].add(r["Link Type"])
                tooltip[date_key]["service_types_add"].add(r["Service Type"])
            else:
                tooltip[date_key]["removed_bw"] += bw
                tooltip[date_key]["link_types_remove"].add(r["Link Type"])
                tooltip[date_key]["service_types_remove"].add(r["Service Type"])

        # -------------------------------
        # BUILD CHART DATA
        # -------------------------------
        chart_df = (
            df.groupby("Deployment Date")["bw_adjusted"]
            .sum()
            .reset_index()
            .sort_values("Deployment Date")  # chronological order guaranteed
        )

        result = []
        cumulative = 0
        for _, r in chart_df.iterrows():
            date = r["Deployment Date"].strftime("%Y-%m-%d")
            bw = float(r["bw_adjusted"])
            cumulative += bw
            t = tooltip.get(date, {})
            result.append(
                {
                    "date": date,
                    "bw": bw,
                    "cumulative_bw": cumulative,
                    "added_bw": t.get("added_bw", 0),
                    "removed_bw": t.get("removed_bw", 0),
                    "link_types_add": list(t.get("link_types_add", [])),
                    "service_types_add": list(t.get("service_types_add", [])),
                    "link_types_remove": list(t.get("link_types_remove", [])),
                    "service_types_remove": list(t.get("service_types_remove", [])),
                    "is_addition": bw >= 0,
                }
            )

        return jsonify(result)

    except Exception as e:
        print("Line Chart Error:", e)
        return jsonify([])

# Summary Cards API
@app.route("/summary/cards")
def summary_cards():
    year = request.args.get("year")
    month = request.args.get("month")

    year = int(year) if year else None
    month = int(month) if month else None

    data = fetch_summary_cards(year, month)
    return jsonify(data)

# Summary Pie Charts API and Query
from flask import jsonify, request
import pandas as pd

@app.route("/summary/pie_data")
def summary_pie_data():
    year=request.args.get("year",type=int)
    month=request.args.get("month",type=int)

    df=pd.read_sql(
        'SELECT "Product Type","Account Name","Mbps","Deployment Date","Link Status" FROM cbs_database_links;',
        engine
    )

    df["Deployment Date"]=pd.to_datetime(df["Deployment Date"],errors="coerce")
    df["Mbps"]=pd.to_numeric(df["Mbps"],errors="coerce").fillna(0)

    df=df[df["Link Status"].str.upper()=="OPERATIONAL"]

    if year:
        df=df[df["Deployment Date"].dt.year==year]
    if month:
        df=df[df["Deployment Date"].dt.month==month]

    product_types=["DIA","Turbo","DPLC","MPLS","FTTH","SIP","DPLC M2M","PRI"]

    agg=df.groupby("Product Type").agg(
        links=("Account Name","count"),
        bw=("Mbps","sum")
    ).reindex(product_types,fill_value=0).reset_index()

    agg.rename(columns={"Product Type":"product_type"},inplace=True)
    return jsonify(agg.to_dict(orient="records"))

# Summary Top 5 Customer API and Query
@app.route("/summary/top5_customers")
def top5_customers():
    year = request.args.get("year", type=int)
    month = request.args.get("month", type=int)

    df = pd.read_sql(
        'SELECT "Customer Name","Mbps","Deployment Date","Link Status" FROM cbs_database_links;',
        engine
    )

    df["Deployment Date"] = pd.to_datetime(df["Deployment Date"], errors="coerce")
    df["Mbps"] = pd.to_numeric(df["Mbps"], errors="coerce").fillna(0)
    df = df[df["Link Status"].str.upper() == "OPERATIONAL"]

    if year:
        df = df[df["Deployment Date"].dt.year == year]
    if month:
        df = df[df["Deployment Date"].dt.month == month]

    # Sort by total BW descending
    agg = (
        df.groupby("Customer Name")
        .agg(
            no_of_links=("Customer Name","count"),
            total_bw=("Mbps","sum")
        )
        .sort_values("total_bw", ascending=False)
        .head(5)
        .reset_index()
    )

    data = []
    for _, row in agg.iterrows():
        data.append({
            "Customer_Name": row["Customer Name"],
            "no_of_links": int(row["no_of_links"]),
            "total_bw": float(row["total_bw"])
        })

    return jsonify(data)

# Summary Region Chart API and Query
@app.route("/summary/region_stacked")
def region_stacked():
    year = request.args.get("year", type=int)
    month = request.args.get("month", type=int)

    df = pd.read_sql(
        'SELECT "Region","Mbps","Deployment Date","Link Status" FROM cbs_database_links;',
        engine
    )

    # Cleaning
    df["Deployment Date"] = pd.to_datetime(df["Deployment Date"], errors="coerce")
    df["Mbps"] = pd.to_numeric(df["Mbps"], errors="coerce").fillna(0)

    # Only OPERATIONAL
    df = df[df["Link Status"].str.upper() == "OPERATIONAL"]

    # Filters
    if year:
        df = df[df["Deployment Date"].dt.year == year]
    if month:
        df = df[df["Deployment Date"].dt.month == month]

    # Group by Region
    agg = (
        df.groupby("Region")
        .agg(
            no_of_links=("Region", "count"),
            total_bw=("Mbps", "sum")
        )
        .reset_index()
        .sort_values("Region")
    )

    data = []
    for _, row in agg.iterrows():
        data.append({
            "region": row["Region"],
            "no_of_links": int(row["no_of_links"]),
            "total_bw": float(row["total_bw"])
        })

    return jsonify(data)

# Summary Last Mile Connectivity Chart API and Query
@app.route("/summary/last_mile_connectivity")
def last_mile_connectivity():
    year = request.args.get("year", type=int)
    month = request.args.get("month", type=int)

    # ================= READ TABLE =================
    df = pd.read_sql(
        '''
        SELECT 
            "Last Mile Connectivity",
            "Vendor Name",
            "Mbps",
            "Link Status",
            "Deployment Date"
        FROM cbs_database_links;
        ''',
        engine
    )

    # ================= CLEANING =================
    df["Deployment Date"] = pd.to_datetime(df["Deployment Date"], errors="coerce")
    df["Mbps"] = pd.to_numeric(df["Mbps"], errors="coerce").fillna(0)

    # Normalize strings (🔥 IMPORTANT)
    df["LMC_CLEAN"] = (
        df["Last Mile Connectivity"]
        .astype(str)
        .str.upper()
        .str.replace(" ", "", regex=False)
    )

    df["VENDOR_CLEAN"] = (
        df["Vendor Name"]
        .astype(str)
        .str.upper()
        .str.strip()
    )

    df["STATUS_CLEAN"] = (
        df["Link Status"]
        .astype(str)
        .str.upper()
        .str.strip()
    )

    # ================= ONLY OPERATIONAL =================
    df = df[df["STATUS_CLEAN"] == "OPERATIONAL"]

    # ================= DATE FILTERS =================
    if year:
        df = df[df["Deployment Date"].dt.year == year]

    if month:
        df = df[df["Deployment Date"].dt.month == month]

    # ================= BAR DEFINITIONS =================
    bars = {
        # 1️⃣ Wired (Customer's Own)
        "Wired (Customer's Own)": df[
            (df["LMC_CLEAN"] == "WIRED") &
            (df["VENDOR_CLEAN"] == "CUSTOMER'S OWN LAST MILE")
        ],

        # 2️⃣ Wired (Other Vendors)
        "Wired (Other Vendors)": df[
            (df["LMC_CLEAN"] == "WIRED") &
            (df["VENDOR_CLEAN"] != "CUSTOMER'S OWN LAST MILE")
        ],

        # 3️⃣ Wireless
        "Wireless": df[df["LMC_CLEAN"] == "WIRELESS"],

        # 4️⃣ Wireless + Wired  🔥 FIXED
        "Wireless + Wired": df[
            df["LMC_CLEAN"].str.contains("WIRELESS") &
            df["LMC_CLEAN"].str.contains("WIRED")
        ]
    }

    # ================= AGGREGATION =================
    data = []
    for bar_name, df_bar in bars.items():
        data.append({
            "name": bar_name,
            "total_bw": float(df_bar["Mbps"].sum()),
            "no_of_links": int(len(df_bar))
        })

    return jsonify(data)

# Summary Top 5 Last Mile Vendor Chart API and Query
@app.route("/summary/top5_lastmile")
def top5_lastmile():
    year = request.args.get("year", type=int)
    month = request.args.get("month", type=int)

    df = pd.read_sql(
        'SELECT "Vendor Name","Mbps","Deployment Date","Link Status" FROM cbs_database_links;',
        engine
    )

    df["Deployment Date"] = pd.to_datetime(df["Deployment Date"], errors="coerce")
    df["Mbps"] = pd.to_numeric(df["Mbps"], errors="coerce").fillna(0)
    df = df[df["Link Status"].str.upper() == "OPERATIONAL"]

    if year:
        df = df[df["Deployment Date"].dt.year == year]
    if month:
        df = df[df["Deployment Date"].dt.month == month]

    # Aggregate by Vendor Name
    agg = (
        df.groupby("Vendor Name")
        .agg(
            no_of_links=("Vendor Name","count"),
            total_bw=("Mbps","sum")
        )
        .sort_values("total_bw", ascending=False)
        .head(5)
        .reset_index()
    )

    data = []
    for _, row in agg.iterrows():
        data.append({
            "vendor_name": row["Vendor Name"],
            "no_of_links": int(row["no_of_links"]),
            "total_bw": float(row["total_bw"])
        })

    return jsonify(data)

# gs_to_postgres.py Refresh API
def sync_google_sheets():
    print("⏳ Running GS → PostgreSQL sync...")
    subprocess.call(["python", "gs_to_postgres.py"])
    print("✔ Sync Complete")

scheduler = BackgroundScheduler()
scheduler.add_job(sync_google_sheets, 'interval', minutes=30)
scheduler.start()

# Logout API
@app.route('/logout')
def logout():
    session.pop('username', None)
    return redirect(url_for('login'))

#Server Run
if __name__ == "__main__":
    app.run(host="172.25.35.210",port=5000,debug=True,use_reloader=False)
