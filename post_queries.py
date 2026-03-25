from flask import Flask, jsonify, request
import pandas as pd
from sqlalchemy import create_engine

app = Flask(__name__)
engine = create_engine("postgresql://postgres:12345@localhost/cbs_data")

def fetch_single_numeric(query, col):
    try:
        df = pd.read_sql_query(query, con=engine)
        if df.empty or col not in df.columns or df[col].isnull().all():
            return 0
        return float(df[col].iloc[0])
    except Exception as e:
        print(f"Error fetching {col}: {e}")
        return 0

def get_post_sales_data(year=None, month=None, link_name=None):
    filter_cond = 'WHERE UPPER(TRIM("Account Region")) = \'NORTH\''

    if year:
        filter_cond += f" AND EXTRACT(YEAR FROM (\"Complaint Date/Time\")::timestamp) = {year}"
    if month:
        filter_cond += f" AND EXTRACT(MONTH FROM (\"Complaint Date/Time\")::timestamp) IN ({','.join(map(str, month))})"
    if link_name:
        filter_cond += f" AND TRIM(UPPER(\"NMS User Label\")) = '{link_name.strip().upper()}'"

    queries = {
        "total_complaints": f"""
            SELECT COUNT(*) AS total_complaints
            FROM response_data
            {filter_cond};
        """,
        "service_degradation_count": f"""
            SELECT COUNT(*) AS service_degradation_count
            FROM response_data
            {filter_cond}
            AND UPPER(TRIM("Reported Issue Category")) = 'SERVICE DEGRADATION';
        """,
        "service_degradation_avg": f"""
            SELECT AVG(val) AS service_degradation_avg
            FROM (
                SELECT NULLIF(REGEXP_REPLACE("Resolution Time (minutes)", '[^0-9.]', '', 'g'), '')::numeric AS val
                FROM response_data
                {filter_cond}
                AND UPPER(TRIM("Reported Issue Category")) = 'SERVICE DEGRADATION'
            ) AS sub;
        """,
        "service_outage_count": f"""
            SELECT COUNT(*) AS service_outage_count
            FROM response_data
            {filter_cond}
            AND UPPER(TRIM("Reported Issue Category")) = 'SERVICE OUTAGE';
        """,
        "service_outage_avg": f"""
            SELECT AVG(val) AS service_outage_avg
            FROM (
                SELECT NULLIF(REGEXP_REPLACE("Resolution Time (minutes)", '[^0-9.]', '', 'g'), '')::numeric AS val
                FROM response_data
                {filter_cond}
                AND UPPER(TRIM("Reported Issue Category")) = 'SERVICE OUTAGE'
            ) AS sub;
        """
    }

    data = {}
    for key, q in queries.items():
        val = fetch_single_numeric(q, key)
        data[key] = round(val, 2) if 'avg' in key else int(val)

    # SLA logic: only calculate if link_name exists, otherwise show "-"
    if link_name:
        data['sla'] = get_sla(year, month, link_name)
    else:
        data['sla'] = "-"

    return data


def get_sla(year=None, month=None, link_name=None):
    try:
        if not link_name:
            return "-"

        import calendar

        # Clean link name
        link_clean = link_name.strip().upper()

        # Base SQL filter for the link
        base_filter = f"""
            WHERE UPPER(TRIM("Account Region")) = 'NORTH'
              AND UPPER(TRIM("Reported Issue Category")) = 'SERVICE OUTAGE'
              AND TRIM(UPPER("NMS User Label")) = '{link_clean}'
        """

        if year:
            base_filter += f" AND EXTRACT(YEAR FROM (\"Complaint Date/Time\")::timestamp) = {year}"

        # Month logic
        selected_months = month if month else list(range(1,13))
        if isinstance(selected_months, int):
            selected_months = [selected_months]

        total_minutes = 0
        total_downtime = 0

        for mn in selected_months:
            # Total minutes in this month
            if year:
                days_in_month = calendar.monthrange(year, mn)[1]
            else:
                days_in_month = 30  # default if year not provided
            total_minutes += days_in_month * 1440

            # Downtime for this month
            month_filter = f"{base_filter} AND EXTRACT(MONTH FROM (\"Complaint Date/Time\")::timestamp) = {mn}"
            query_downtime = f"""
                SELECT SUM(
                    COALESCE(NULLIF(REGEXP_REPLACE("Resolution Time (minutes)", '[^0-9.]', '', 'g'), '')::numeric, 0)
                ) AS downtime
                FROM response_data
                {month_filter};
            """
            month_downtime = fetch_single_numeric(query_downtime, 'downtime') or 0
            total_downtime += month_downtime

        if total_minutes == 0:
            return "100%"

        sla_percent = ((total_minutes - total_downtime) / total_minutes) * 100
        return f"{round(sla_percent, 2)}%"

    except Exception as e:
        print("SLA calculation error:", e)
        return "-"


