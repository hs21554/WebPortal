# queries.py
import pandas as pd
import matplotlib.pyplot as plt
import io
import base64
import random
from database import get_engine  # PostgreSQL engine

# -----------------------------
# Dashboard Data
# -----------------------------
def get_dashboard_data(site_id=None, city=None, unique_id=None, customer_name=None,
                       vendor_name=None, product_type=None, isp=None, bgp=None, region=None,
                       deployment_date=None, deployment_month=None, account_name=None):

    # --- Show '-' by default if no search is applied ---
    if not any([site_id, city, unique_id, customer_name, vendor_name, product_type,
                isp, bgp, region, deployment_date, deployment_month, account_name]):
        return {
            "customer_count": "-", "active_links": "-", "total_bw": "-", "last_mile_site_id": "-",
            "product_type": "-", "customer_name": "-", "bgp_status": "-", "region": "-",
            "vendor": "-", "port_type": "-", "ess_poc": "-", "ess_poc_num": "-", "vendor_poc_num": "-",
            "last_mile_port": "-", "last_mile_port_detail": "-", "agg_port": "-", "vlan": "-", "ip_pool": "-",
            "top_customers": []
        }

    # --- Original logic ---
    engine = get_engine()
    filters, params = [], []
    if site_id:
        filters.append('"CMPak Site Id" = %s'); params.append(site_id)
    if city:
        filters.append('LOWER(TRIM("City")) LIKE %s'); params.append(f"%{city.lower()}%")
    if unique_id:
        filters.append('"unique_id" = %s'); params.append(unique_id)
    if customer_name:
        filters.append('LOWER(TRIM("Customer Name")) LIKE %s'); params.append(f"%{customer_name.lower()}%")
    if vendor_name:
        filters.append('LOWER(TRIM("Vendor Name")) LIKE %s'); params.append(f"%{vendor_name.lower()}%")
    if product_type:
        filters.append('LOWER(TRIM("Product Type")) LIKE %s'); params.append(f"%{product_type.lower()}%")
    if isp:
        filters.append('LOWER(TRIM("ISP")) LIKE %s'); params.append(f"%{isp.lower()}%")
    if bgp:
        filters.append('LOWER(TRIM("BGP")) LIKE %s'); params.append(f"%{bgp.lower()}%")
    if region:
        filters.append('LOWER(TRIM("Region")) LIKE %s'); params.append(f"%{region.lower()}%")
    if deployment_date:
        filters.append('"Deployment Date" = %s'); params.append(deployment_date)
    if deployment_month:
        filters.append('"Deployment Month" = %s'); params.append(deployment_month)
    if account_name:
        filters.append('LOWER(TRIM("Account Name")) LIKE %s'); params.append(f"%{account_name.lower()}%")

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    query = f'SELECT * FROM cbs_database_links {where_clause};'
    df = pd.read_sql(query, engine, params=tuple(params))

    df.fillna('-', inplace=True)
    df.replace({'': '-', 'NA': '-', 'N/A': '-'}, inplace=True)
    df['Mbps'] = pd.to_numeric(df.get('Mbps', 0), errors='coerce').fillna(0)

    data = {}

    if not df.empty:
        data["total_bw"] = int(df["Mbps"].sum())
        data["customer_count"] = df["Customer Name"].nunique() if "Customer Name" in df else 0
        df_operational = df[df["Link Status"].str.lower() == "operational"] if "Link Status" in df else df
        data["active_links"] = len(df_operational)
        data["vendor"] = df["Vendor Name"].nunique() if "Vendor Name" in df else 0

        def first(col):
            if col in df.columns and any(df[col] != '-'):
                return df.loc[df[col] != '-', col].iloc[0]
            return '-'

        data.update({
            "last_mile_site_id": first("CMPak Site Id"),
            "product_type": first("Product Type"),
            "customer_name": first("Customer Name"),
            "bgp_status": first("BGP"),
            "region": first("Region"),
            "vendor": first("Vendor Name"),
            "port_type": first("Last Mile Connectivity"),
            "ess_poc": first("ESS POC"),
            "ess_poc_num": first("ESS POC Contact Number"),
            "vendor_poc_num": first("Vendor POC"),
            "last_mile_port": first("Port Type"),
            "last_mile_port_detail": first("Last Mile Port"),
            "agg_port": first("Aggeration Port (B End port for DPLC case)"),
            "vlan": first("Vlan"),
            "ip_pool": first("Public IP Pool & Customer Own IP Pool")
        })

        # Top customers summary
        if "Customer Name" in df.columns and "Account Name" in df.columns:
            top_customers_df = df.groupby("Customer Name", as_index=False).agg(
                links=("Account Name", "count"),
                bw=("Mbps", "sum")
            ).sort_values(by="links", ascending=False)

            data["top_customers"] = top_customers_df.to_dict(orient="records")
        else:
            data["top_customers"] = []
    else:
        data = {k: "-" for k in [
            "customer_count", "active_links", "total_bw", "last_mile_site_id",
            "product_type", "customer_name", "bgp_status", "region",
            "vendor", "port_type"
        ]}
        data["top_customers"] = []

    return data


# -----------------------------
# Service Type Chart
# -----------------------------
def generate_service_type_chart(service_type_links):
    # --- If no data, return a blank chart with '-' text ---
    if not service_type_links or all(x.get("bw", 0) == 0 for x in service_type_links):
        plt.figure(figsize=(6, 5))
        plt.text(0.5, 0.5, '-', fontsize=30, ha='center', va='center', alpha=0.5)
        plt.axis('off')
        buffer = io.BytesIO()
        plt.savefig(buffer, format='png')
        buffer.seek(0)
        img_base64 = base64.b64encode(buffer.read()).decode('utf-8')
        plt.close()
        return img_base64

    # --- Original logic ---
    filtered = [x for x in service_type_links if x["Product Type"].lower() != "total"]
    filtered = sorted(filtered, key=lambda x: x["bw"], reverse=True)

    product_types = [x["Product Type"] for x in filtered]
    bw_values = [x["bw"] for x in filtered]

    # Extra check in case filtered is empty
    if not product_types:
        plt.figure(figsize=(6, 5))
        plt.text(0.5, 0.5, '-', fontsize=30, ha='center', va='center', alpha=0.5)
        plt.axis('off')
        buffer = io.BytesIO()
        plt.savefig(buffer, format='png')
        buffer.seek(0)
        img_base64 = base64.b64encode(buffer.read()).decode('utf-8')
        plt.close()
        return img_base64

    plt.figure(figsize=(6, 5))
    colors = [f"#{random.randint(0, 0xFFFFFF):06x}" for _ in range(len(product_types))]
    bars = plt.bar(product_types, bw_values, color=colors)
    plt.xlabel("Product Type")
    plt.ylabel("Total BW (Mbps)")
    plt.xticks(rotation=20, ha='right')

    for bar in bars:
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 2,
            f"{int(bar.get_height())}",
            ha='center', va='bottom', fontsize=10, fontweight='bold'
        )

    plt.tight_layout()
    buffer = io.BytesIO()
    plt.savefig(buffer, format='png')
    buffer.seek(0)
    img_base64 = base64.b64encode(buffer.read()).decode('utf-8')
    plt.close()

    return img_base64



# -----------------------------
# Table Data
# -----------------------------
def get_table_data(site_id=None, city=None, unique_id=None, customer_name=None,
                   vendor_name=None, product_type=None, isp=None, bgp=None, region=None,
                   deployment_date=None, deployment_month=None, account_name=None):

    # --- Show '-' by default if no search is applied ---
    if not any([site_id, city, unique_id, customer_name, vendor_name, product_type,
                isp, bgp, region, deployment_date, deployment_month, account_name]):
        return {
            "isp_summary": [], "bgp_summary": [], "vrf_summary": [], "region_summary": [],
            "product_type_summary": [], "north_customers": [], "last_mile_vendors": [],
            "service_type_links": [], "service_type_chart": None, "top_customers": [],
            "last_mile_connectivity_summary": []
        }

    engine = get_engine()
    filters, params = [], []

    # ----------------------------
    # ADD FIXED FILTER #1
    # Always only OPERATIONAL
    # ----------------------------
    filters.append('LOWER(TRIM("Link Status")) = %s')
    params.append("operational")

    if site_id:
        filters.append('"CMPak Site Id" = %s')
        params.append(site_id)
    if city:
        filters.append('LOWER(TRIM("City")) LIKE %s')
        params.append(f"%{city.lower()}%")
    if unique_id:
        filters.append('"unique_id" = %s')
        params.append(unique_id)
    if customer_name:
        filters.append('LOWER(TRIM("Customer Name")) LIKE %s')
        params.append(f"%{customer_name.lower()}%")
    if product_type:
        filters.append('LOWER(TRIM("Product Type")) LIKE %s')
        params.append(f"%{product_type.lower()}%")
    if isp:
        filters.append('LOWER(TRIM("ISP")) LIKE %s')
        params.append(f"%{isp.lower()}%")
    if bgp:
        filters.append('LOWER(TRIM("BGP")) LIKE %s')
        params.append(f"%{bgp.lower()}%")
    if region:
        filters.append('LOWER(TRIM("Region")) LIKE %s')
        params.append(f"%{region.lower()}%")
    if vendor_name:
        filters.append('LOWER(TRIM("Vendor Name")) LIKE %s')
        params.append(f"%{vendor_name.lower()}%")
    if deployment_date:
        filters.append('"Deployment Date" = %s')
        params.append(deployment_date)
    if deployment_month:
        filters.append('"Deployment Month" = %s')
        params.append(deployment_month)
    if account_name:
        filters.append('LOWER(TRIM("Account Name")) LIKE %s')
        params.append(f"%{account_name.lower()}%")

    # Build query
    where_clause = f"WHERE {' AND '.join(filters)}"
    query = f'SELECT * FROM cbs_database_links {where_clause};'

    df = pd.read_sql(query, engine, params=tuple(params))

    df.fillna('-', inplace=True)
    df.replace({'': '-', 'NA': '-', 'N/A': '-'}, inplace=True)
    df['Mbps'] = pd.to_numeric(df.get('Mbps', 0), errors='coerce').fillna(0)

    def summarize(col):
        if col not in df.columns:
            return []

        # Unknown ko ignore NAHI karna (Your requirement)
        g = df.groupby(col, as_index=False).agg(
            links=("Account Name", "count"),
            bw=("Mbps", "sum")
        ).sort_values(by="links", ascending=False)

        return [
            {col: r[col], "links": int(r.links), "bw": int(r.bw)}
            for _, r in g.iterrows()
        ]

    table_data = {
        "isp_summary": summarize("ISP"),
        "bgp_summary": summarize("BGP"),
        "vrf_summary": summarize("VRF"),
        "region_summary": summarize("Region"),
        "product_type_summary": summarize("Product Type"),
        "north_customers": df.to_dict(orient="records"),
        "last_mile_vendors": summarize("Vendor Name") if "Vendor Name" in df.columns else [],
        "service_type_links": summarize("Product Type") if "Product Type" in df.columns else [],
        "service_type_chart": generate_service_type_chart(summarize("Product Type")),
        "top_customers": summarize("Customer Name"),
        "last_mile_connectivity_summary": summarize("Last Mile Connectivity") if "Last Mile Connectivity" in df.columns else []
    }

    return table_data

