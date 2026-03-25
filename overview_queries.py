import pandas as pd
from database import get_engine
import matplotlib.pyplot as plt
import io
import base64

def get_overview_cards(year=None, month=None):
    engine = get_engine()

    # ---------------------------
    # CBS DATA
    # ---------------------------
    df_cbs = pd.read_sql('SELECT * FROM cbs_database_links;', engine)
    df_cbs['Mbps'] = pd.to_numeric(df_cbs.get('Mbps', 0), errors='coerce').fillna(0)
    df_operational = df_cbs[df_cbs["Link Status"].str.lower() == "operational"]

    # Unique active customers
    active_customers_count = len(df_operational["Customer Name"].dropna().str.strip().unique())

    # ---------------------------
    # SQL FILTER STRINGS
    # ---------------------------
    year_filter = f"AND EXTRACT(YEAR FROM {{}}::date) = {year}" if year else ""
    month_filter = f"AND EXTRACT(MONTH FROM {{}}::date) = {month}" if month else ""

    # Feasibility
    feas_res = pd.read_sql(
        f'''
        SELECT COUNT(*) AS count
        FROM presales_data
        WHERE "Feasibility Feedback Date" NOT IN ('Unknown','', 'None','null','NULL')
        {year_filter.format('"Feasibility Feedback Date"')}
        {month_filter.format('"Feasibility Feedback Date"')}
        ''', engine
    )
    feas_count = int(feas_res.iloc[0]["count"])

    # SD Submission
    df_sd = pd.read_sql(
        '''
        SELECT "SD Submission Date"
        FROM presales_data
        WHERE TRIM("SD Submission Date") <> ''
          AND LOWER(TRIM("SD Submission Date")) NOT IN ('unknown','none','null','-')
        ''', engine
    )
    df_sd['SD Submission Date'] = pd.to_datetime(df_sd['SD Submission Date'], errors='coerce')
    if year:
        df_sd = df_sd[df_sd['SD Submission Date'].dt.year == int(year)]
    if month:
        df_sd = df_sd[df_sd['SD Submission Date'].dt.month == int(month)]
    sd_count = len(df_sd)
    total_presales = feas_count + sd_count

    # In-Sales Count
    insales_res = pd.read_sql(
        f'''
        SELECT COUNT(*) AS count
        FROM presales_data
        WHERE "Deployment Date" NOT IN ('Unknown','', 'None','null','NULL')
        {year_filter.format('"Deployment Date"')}
        {month_filter.format('"Deployment Date"')}
        ''', engine
    )
    in_sales_count = int(insales_res.iloc[0]["count"])

    # Deployed BW
    df_bw = pd.read_sql(
        '''
        SELECT "Deployment Date", "Actual BW Taken By Customer"
        FROM presales_data
        WHERE "Link Type" IN ('New Link Activation','Link Upgradation')
          AND TRIM("Deployment Date") <> ''
          AND LOWER(TRIM("Deployment Date")) NOT IN ('unknown','none','null','-','n/a')
        ''', engine
    )
    df_bw['Deployment Date'] = pd.to_datetime(df_bw['Deployment Date'], errors='coerce')
    df_bw['Actual BW Taken By Customer'] = pd.to_numeric(
        df_bw['Actual BW Taken By Customer'], errors='coerce'
    ).fillna(0)
    df_bw = df_bw.dropna(subset=['Deployment Date'])

    if year:
        df_bw = df_bw[df_bw['Deployment Date'].dt.year == int(year)]
    if month:
        df_bw = df_bw[df_bw['Deployment Date'].dt.month == int(month)]

    deployed_bw = df_bw['Actual BW Taken By Customer'].sum()

    # ---------------------------
    # FINAL CARDS RETURN
    # ---------------------------
    return {
        "overview-active-customer": active_customers_count,
        "overview-active-links": int(len(df_operational)),
        "overview-bw": int(df_operational["Mbps"].sum()),
        "overview-deployed-bw": int(deployed_bw),
        "overview-feasibility": feas_count,
        "overview-sd": sd_count,
        "overview-pre-sales": total_presales,
        "overview-in-sales": in_sales_count,
    }


def get_services_chart(year=None, month=None):
    import pandas as pd, matplotlib.pyplot as plt, io, base64
    from database import get_engine

    engine = get_engine()
    
    query = '''
        SELECT "Current Status", "Feasibility Feedback Date", "SD Submission Date", "Link Type", "Deployment Date"
        FROM presales_data
    '''
    df = pd.read_sql(query, engine)

    # Convert dates
    df['Feasibility Feedback Date'] = pd.to_datetime(df['Feasibility Feedback Date'], errors='coerce')
    df['SD Submission Date'] = pd.to_datetime(df['SD Submission Date'], errors='coerce')
    df['Deployment Date'] = pd.to_datetime(df['Deployment Date'], errors='coerce')

    apply_filter = True if (year or month) else False

    # --- Feasibility ---
    df_feas = df[df['Feasibility Feedback Date'].notna()]
    if apply_filter:
        if year: df_feas = df_feas[df_feas['Feasibility Feedback Date'].dt.year == int(year)]
        if month: df_feas = df_feas[df_feas['Feasibility Feedback Date'].dt.month == int(month)]

    feasible_count = len(df_feas[df_feas['Current Status'].isin(['Feasible', 'Revalidation', 'Solution Design'])])
    not_feasible_count = len(df_feas[df_feas['Current Status'] == 'Not Feasible'])
    partially_count = len(df_feas[df_feas['Current Status'] == 'Partially Feasible'])

    # --- SD Submission ---
    df_sd = df[df['SD Submission Date'].notna()]
    if apply_filter:
        if year: df_sd = df_sd[df_sd['SD Submission Date'].dt.year == int(year)]
        if month: df_sd = df_sd[df_sd['SD Submission Date'].dt.month == int(month)]

    solution_design_count = len(df_sd[df_sd['Current Status'] == 'Solution Design'])
    revalidation_count = len(df_sd[df_sd['Current Status'] == 'Revalidation'])

    # --- Deployment ---
    df_link = df[df['Deployment Date'].notna()]
    if apply_filter:
        if year: df_link = df_link[df_link['Deployment Date'].dt.year == int(year)]
        if month: df_link = df_link[df_link['Deployment Date'].dt.month == int(month)]

    link_downgrade_count = len(df_link[df_link['Link Type'] == 'Link Downgradation'])
    link_termination_count = len(df_link[df_link['Link Type'] == 'Link Termination'])
    link_upgradation_count = len(df_link[df_link['Link Type'] == 'Link Upgradation'])
    new_link_count = len(df_link[df_link['Link Type'] == 'New Link Activation'])

    # --- Chart ---
    categories = [
        'Feasible', 'Not Feasible', 'Partially Feasible',
        'Solution Design', 'Revalidation',
        'Link Downgradation', 'Link Termination', 'Link Upgradation', 'New Link Activation'
    ]
    counts = [
        feasible_count, not_feasible_count, partially_count,
        solution_design_count, revalidation_count,
        link_downgrade_count, link_termination_count, link_upgradation_count, new_link_count
    ]
    colors = [
        '#ffa500', '#ff1493', "#00bfff",
        '#28a745', '#6f42c1',
        '#ff5733', "#3de8cb", '#33ff57', '#ff33f6'
    ]

    fig, ax = plt.subplots(figsize=(12,6))
    bars = ax.bar(categories, counts, color=colors, width=0.5)

    # Add counts above bars
    for bar, count in zip(bars, counts):
        if count > 0:
            ax.text(
                bar.get_x() + bar.get_width()/2,
                count + 0.5,
                str(count),
                ha='center',
                va='bottom',
                fontweight='bold',
                color='black'
            )

    ax.set_xticks(range(len(categories)))
    ax.set_xticklabels(categories, rotation=45)

# Loop through ticks and change color individually
    for tick_label in ax.get_xticklabels():
        if tick_label.get_text() == 'Link Termination':
            tick_label.set_color('red')
    else:
        tick_label.set_color('black')

    # Optional: vertical lines
    ax.axvline(x=2.5, color='black', linestyle='--')
    ax.axvline(x=4.5, color='black', linestyle='--')

    ax.set_ylabel("Counts")
    plt.tight_layout()

    # Save to buffer
    img = io.BytesIO()
    plt.savefig(img, format='png')
    plt.close(fig)
    img.seek(0)

    return {"chart": base64.b64encode(img.getvalue()).decode()}

def get_bw_availability_chart(year=None, month=None):
    import pandas as pd
    import matplotlib.pyplot as plt
    import io
    import base64
    from database import get_engine

    engine = get_engine()

    # -------------------------
    # 1 — Load full data
    # -------------------------
    query = '''
        SELECT 
            "Feasibility Feedback Date",
            "Available BW",
            "Required BW",
            "BW Unavailable reasons"
        FROM presales_data
    '''
    df = pd.read_sql(query, engine)

    # -------------------------
    # 2 — Filter Feasibility Feedback Date
    # -------------------------
    df['Feasibility Feedback Date'] = pd.to_datetime(df['Feasibility Feedback Date'], errors='coerce')
    df = df[df['Feasibility Feedback Date'].notna()]

    if year:
        df = df[df['Feasibility Feedback Date'].dt.year == int(year)]
    if month:
        df = df[df['Feasibility Feedback Date'].dt.month == int(month)]

    if df.empty:
        return {"chart": ""}

    # -------------------------
    # 3 — Add Month column
    # -------------------------
    df['Month'] = df['Feasibility Feedback Date'].dt.strftime('%b')

    # -------------------------
    # 4 — Calculate Available BW per month
    # -------------------------
    df_avail = df.copy()
    df_avail = df_avail[~df_avail['Available BW'].isin(["Unknown", "NA", "Check"])]
    df_avail['Available BW'] = pd.to_numeric(df_avail['Available BW'], errors='coerce')
    df_avail = df_avail.dropna(subset=['Available BW'])
    available_per_month = df_avail.groupby('Month')['Available BW'].sum()

    # -------------------------
    # 5 — Calculate Unavailable BW per month (selected reasons)
    # -------------------------
    valid_reasons = [
        "Unknown",
        "BW unavailable due to high MW utilization",
        "BW unavailable due to high utilization of ring",
        "Multiple BW already reserved"
    ]
    df_unavail = df[df['BW Unavailable reasons'].isin(valid_reasons)].copy()
    df_unavail['Available BW'] = pd.to_numeric(df_unavail['Available BW'], errors='coerce')
    df_unavail['Required BW'] = pd.to_numeric(df_unavail['Required BW'], errors='coerce')
    df_unavail = df_unavail.dropna(subset=['Available BW', 'Required BW'])
    df_unavail['Month'] = df_unavail['Feasibility Feedback Date'].dt.strftime('%b')
    unavailable_per_month = df_unavail.groupby('Month').apply(
        lambda x: x['Required BW'].sum() - x['Available BW'].sum()
    )
    unavailable_per_month[unavailable_per_month < 0] = 0

    # -------------------------
    # 6 — Ensure months order (Jan → Dec)
    # -------------------------
    months_order = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
    available_per_month = available_per_month.reindex(months_order, fill_value=0)
    unavailable_per_month = unavailable_per_month.reindex(months_order, fill_value=0)

    # -------------------------
    # 7 — Plot horizontal stacked bar
    # -------------------------
    fig, ax = plt.subplots(figsize=(12, 7))
    y = available_per_month.index

# Reverse the y-axis for Jan → Dec top-to-bottom
    ax.invert_yaxis()

    ax.barh(y, available_per_month, color="#28a745", label='Available BW')
    ax.barh(y, unavailable_per_month, left=available_per_month, color="#ff1493", label='Unavailable BW')

# Add values on bars
    for i, (a, u) in enumerate(zip(available_per_month, unavailable_per_month)):
        if a > 0:
            ax.text(a/2, i, f"{int(a):,}", va='center', ha='center', color='white', fontweight='bold')
        if u > 0:
            ax.text(a + u/2, i, f"{int(u):,}", va='center', ha='center', color='white', fontweight='bold')

    ax.set_ylabel("Month")
    ax.set_xlabel("Bandwidth (BW)")
    ax.set_title("Sum of Available BW vs Sum of Un-Available BW")
    ax.legend()
    plt.tight_layout()

    # -------------------------
    # 8 — Return Base64 PNG
    # -------------------------
    img = io.BytesIO()
    plt.savefig(img, format='png', dpi=200)
    plt.close(fig)
    img.seek(0)

    return {"chart": base64.b64encode(img.getvalue()).decode()}

def get_active_links_chart():
    import pandas as pd
    import matplotlib.pyplot as plt
    import io, base64
    from database import get_engine
    import numpy as np

    engine = get_engine()

    # -----------------------------
    # Load only OPERATIONAL Links
    # -----------------------------
    df = pd.read_sql('''
        SELECT "Product Type", "Account Name", "Mbps"
        FROM cbs_database_links
        WHERE "Link Status" = 'Operational'
    ''', engine)

    if df.empty:
        return {"chart": ""}

    df['Mbps'] = pd.to_numeric(df['Mbps'], errors='coerce').fillna(0)

    grouped = df.groupby("Product Type").agg(
        total_bw=("Mbps", "sum"),
        total_links=("Account Name", "count")
    ).reset_index()

    grouped = grouped.sort_values("total_bw", ascending=False)

    product_types = grouped["Product Type"]
    bw = grouped["total_bw"].values
    links = grouped["total_links"].values

    min_bar_width = max(bw.max() * 0.05, 1)
    bw_display = np.where(bw < min_bar_width, min_bar_width, bw)

    for i, pt in enumerate(product_types):
        if pt.upper() == "DPLC":
            bw_display[i] = bw_display[i] * 1
        elif pt.upper() == "DPLC M2M":
            bw_display[i] = bw_display[i] * 1.8
        elif pt.upper() == "MPLS":
            bw_display[i] = bw_display[i] * 1.8

    min_link_width = max(links.max() * 0.05, 1)
    links_display = np.where(links < min_link_width, min_link_width, links)

    for i, pt in enumerate(product_types):
        if pt.upper() == "DPLC":
            links_display[i] = links_display[i] * 1
        elif pt.upper() == "DPLC M2M":
            links_display[i] = links_display[i] * 1
        elif pt.upper() == "MPLS":
            links_display[i] = links_display[i] * 2

    fig, axes = plt.subplots(1, 2, figsize=(16, 7), sharey=True)

    # Left (BW)
    axes[0].barh(product_types, bw_display, color="#28a745")
    for i, (actual, disp) in enumerate(zip(bw, bw_display)):
        axes[0].text(disp/2, i, str(int(actual)), va='center', ha='center',
                     color='white', fontweight='bold')

    axes[0].set_xticks([])

    # Right (Links)
    axes[1].barh(product_types, links_display, color="#ff1493")
    for i, (actual, disp) in enumerate(zip(links, links_display)):
        axes[1].text(disp/2, i, str(int(actual)), va='center', ha='center',
                     color='white', fontweight='bold')

    axes[1].set_xticks([])

    axes[0].invert_yaxis()
    plt.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=200)
    plt.close(fig)
    buf.seek(0)
    chart_base64 = base64.b64encode(buf.read()).decode("utf-8")
    buf.close()
    return {"chart": chart_base64}


def get_overview_tables():
    engine = get_engine()
    df = pd.read_sql('SELECT * FROM cbs_database_links;', engine)
    df.fillna('-', inplace=True)

    df['Mbps'] = pd.to_numeric(df.get('Mbps', 0), errors='coerce').fillna(0)

    # ---------------------------------------------------
    # Filter only OPERATIONAL rows
    # ---------------------------------------------------
    df = df[df['Link Status'].astype(str).str.lower() == 'operational']

    # ---------------------------------------------------
    # Summary function (NO removal of Unknown)
    # ---------------------------------------------------
    def summarize(col):
        if col not in df.columns:
            return []

        g = df.groupby(col, as_index=False).agg(
            links=("Account Name", "count"),
            bw=("Mbps", "sum")
        ).sort_values(by="links", ascending=False)

        return [
            {col: r[col], "links": int(r.links), "bw": int(r.bw)}
            for _, r in g.iterrows()
        ]

    tables = {
        "ovr-isp": summarize("ISP"),
        "ovr-vrf": summarize("VRF"),
        "ovr-bgp": summarize("BGP"),
        "overview-table-regions": summarize("Region"),
        "overview-table-packages": summarize("Product Type"),
        "ovr-north-customers": df.to_dict(orient="records"),
        "overview-table-customers": summarize("Customer Name"),
        "overview-table-vendors": summarize("Vendor Name"),
        "overview-table-connectivity": summarize("Last Mile Connectivity")
    }

    return tables
