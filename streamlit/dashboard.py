import streamlit as st
import pandas as pd
import altair as alt
import plotly.express as px
from google.oauth2 import service_account
from google.cloud import bigquery
import os, sys
import json
from dotenv import load_dotenv

current_dir = os.path.dirname(os.path.abspath(__file__))

# Append the 'project_root' directory to the Python path
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.append(project_root)
from dags.config_data.gcp_config_parameters import *

load_dotenv(f"{project_root}/.env")

page_title = "US Retail dashboard"
alt.themes.enable("dark")

st.set_page_config(
    page_title=page_title,
    page_icon="🏂",
    layout="wide",
    initial_sidebar_state="expanded")

dow = {
    "Monday": 0,
    "Tuesday": 1,
    "Wednesday": 2,
    "Thursday": 3,
    "Friday": 4,
    "Saturday": 5,
    "Sunday": 6
}

mm_name = {
    "January": 1,
    "February": 2,
    "March": 3,
    "April": 4,
    "May": 5,
    "June": 6,
    "July": 7,
    "August": 8,
    "September": 9,
    "October": 10,
    "November": 11,
    "December": 12
}

try:
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
)
except:
    credentials = service_account.Credentials.from_service_account_info(
    json.load(open(os.getenv('HOST_GOOGLE_APPLICATION_CREDENTIALS')))
)


client = bigquery.Client(credentials=credentials)

@st.cache_data(ttl=600)
def prepare_txn_query(query):
    print(query)
    query_job = client.query(query).to_dataframe()
   #  rows = [dict(row) for row in recs]
    return query_job

# products = prepare_txn_query("""
#                  SELECT 
#                     product_id, 
#                     name,
#                     category
#                  FROM `{project_number}.{bq_dataset}.{table}`
#                 """.format(project_number=project_number, bq_dataset=DATASET, table=PRODUCT_TABLE)
# )

# stores = prepare_txn_query("""
#                  SELECT 
#                     store_id, 
#                     location,
#                     size
#                  FROM `{project_number}.{bq_dataset}.{table}`
#                 """.format(project_number=project_number, bq_dataset=DATASET, table=STORE_TABLE)
# )


# txn_rows = prepare_txn_query("""
#                  SELECT 
#                     transaction_id, 
#                     product_id,
#                     timestamp,
#                     quantity,
#                     unit_price,
#                     store_id
#                  FROM `{project_number}.{bq_dataset}.{table}`
#                 """.format(project_number=project_number, bq_dataset=DATASET, table="transactions")
# )

# inv_rows = prepare_txn_query("""
#                  SELECT 
#                     inventory_id, 
#                     product_id,
#                     timestamp,
#                     quantity_change,
#                     store_id
#                  FROM `{project_number}.{bq_dataset}.{table}`
#                 """.format(project_number=project_number, bq_dataset=DATASET, table="inventories")
# )

mv_txn_prod = prepare_txn_query("""
                  SELECT
                      transaction_id,
                      quantity,
                      unit_price,
                      timestamp,
                      name,
                      category,
                      location,
                      size,
                      manager,
                      extract(date from timestamp) as txn_date,
                      extract(year from timestamp) as year,
                      extract(month from timestamp) as month,
                      format_date('%B', timestamp) as month_name,
                      case extract(dayofweek from timestamp)
                              when 1 then 'Sunday'
                              when 2 then 'Monday'
                              when 3 then 'Tuesday'
                              when 4 then 'Wednesday'
                              when 5 then 'Thursday'
                              when 6 then 'Friday'
                     else 'Saturday' end as dow,
                     quantity * unit_price as revenue,
                     trim(split(location, ',')[offset(1)]) state_code,
                     -- lag(quantity, 1) over(order by timestamp, transaction_id) prev_day_qty 
                  FROM
                     `{project_number}.{bq_dataset}.{mv}`
                  ORDER BY
                     timestamp, transaction_id
                  """.format(project_number=credentials.project_id, bq_dataset=DBT_DATASET, mv="mv_retail_transactions")
)

# prod_df = pd.DataFrame(products)
# store_df = pd.DataFrame(stores)
# txn_df = pd.DataFrame(txn_rows)
# inv_df = pd.DataFrame(inv_rows)
def transforms (df):
    """Create some new fields"""
    mv_txn_prod["year"] = mv_txn_prod["timestamp"].dt.year
    mv_txn_prod["month"] = mv_txn_prod["timestamp"].dt.month
    mv_txn_prod["day"] = mv_txn_prod["timestamp"].dt.day
    mv_txn_prod["dow"] = mv_txn_prod["timestamp"].dt.day_name()
    mv_txn_prod["revenue"] = mv_txn_prod["quantity"] * mv_txn_prod["unit_price"]

# print(mv_txn_prod.head())


with st.sidebar:
    st.title(f'🏂 {page_title}')
   #  transforms(mv_txn_prod)
    year_list = sorted(mv_txn_prod["year"].unique())
    month_list = sorted(mv_txn_prod["month_name"].unique())
   #  day_list = sorted(mv_txn_prod["dow"].unique(), key=lambda x: dow[x])
    selected_year = st.selectbox('Select a year', year_list)
    selected_month = st.selectbox('Select a month', sorted(month_list, key=lambda x: mm_name[x]))
   #  selected_day = st.selectbox('Select a day of week', list(dow.keys()))
    # selected_date = st.date_input("Select a date")
    color_theme_list = ['blues', 'cividis', 'greens', 'inferno', 'magma', 'plasma', 'reds', 'rainbow', 'turbo', 'viridis']
    selected_color_theme = st.selectbox('Select a color theme', color_theme_list)
    mv_txn_prod["txn_date"] = pd.to_datetime(mv_txn_prod["txn_date"], format="%Y-%m-%d")
    aggregated_revenue = mv_txn_prod.groupby(["year", "month", "location", "category"], as_index=False)\
                                 .agg({"revenue": 'mean'})\
                                 .round(0)\
                                 .sort_values(["location", "revenue"], ascending=[True, False])
    daily_pct_change = mv_txn_prod.groupby(["year", "month", "location"], as_index=False).agg({"quantity": 'sum'})
    daily_pct_change["prev_day_qty"] = daily_pct_change.groupby(["location"], as_index=False)["quantity"].shift(1)
    daily_pct_change["pct_change"] = (daily_pct_change["quantity"] - daily_pct_change["prev_day_qty"]) / daily_pct_change["prev_day_qty"]
   #  daily_pct_change["year"] = daily_pct_change["txn_date"].dt.year
   #  daily_pct_change["month"] = daily_pct_change["txn_date"].dt.month
   #  daily_pct_change["dow"] = daily_pct_change["txn_date"].dt.day_name()


def make_heatmap(input_df, input_x, input_y, input_z, input_color, input_color_theme):
    # input_df["txn_date"] = input_df["txn_date"].astype(str)
    subset = input_df.query(f"({input_x} == @selected_year)").reset_index(drop=True)
    base = alt.Chart(subset).transform_aggregate(
    mean_revenue=f'mean({input_color})',
    groupby=[f"{input_y}", f"{input_z}"]
         ).encode(
            alt.Y(f"{input_y}:O", axis=alt.Axis(title="", titleFontSize=18, titlePadding=15, titleFontWeight=900, labelAngle=0), sort=list(mm_name.keys())),
            alt.X(f"{input_z}:O", axis=alt.Axis(title="", titleFontSize=18, titlePadding=15, titleFontWeight=900, labelAngle=-90)),
            tooltip=[input_y, input_z, alt.Tooltip("mean_revenue:Q", format="$,.0f")],
         )
    heatmap = base.mark_rect().encode(
            # alt.Y(f"{input_y}").axis(format="%Y-%m-%d").title("txn date"),
            color=alt.Color(f'mean_revenue:Q',
                             legend=None,
                             scale=alt.Scale(scheme=input_color_theme)),
            stroke=alt.value('black'),
            strokeWidth=alt.value(0.25),
        ).properties(width=900)
   #  # Configure text
   #  text = base.mark_text().encode(
   #       alt.Text(f'mean_revenue:Q', format="$,.0f")
   #  )
    
    alt.layer(heatmap).configure_axis(
        labelFontSize=12,
        titleFontSize=12,
        labelLimit=12
        ) 
    
    return heatmap

# Choropleth map
def make_choropleth(input_df, grp_1, grp_2, input_id, input_column, input_color_theme):
    aggregate = input_df.groupby([grp_1, grp_2, input_id], as_index=False).agg({'revenue': 'mean'})
    avg_revenue = aggregate['revenue']
    choropleth = px.choropleth(aggregate, locations=input_id, color=avg_revenue, locationmode="USA-states",
                               color_continuous_scale=input_color_theme,
                               color_continuous_midpoint=aggregate["revenue"].median(),
                               range_color=(aggregate["revenue"].min(), aggregate["revenue"].max()),
                               scope="usa",
                              #  labels={'revenue':'Revenue'},
                               hover_data={'revenue': ':$,.0f'}
                              )
   #  choropleth.update_traces(hovertemplate=f'revenue: {avg_revenue:,.0f}')
    choropleth.update_layout(
        template='plotly_dark',
        plot_bgcolor='rgba(0, 0, 0, 0)',
        paper_bgcolor='rgba(0, 0, 0, 0)',
        margin=dict(l=0, r=0, t=0, b=0),
        height=350
    )
    return choropleth

def _format_arrow(val):
    return f"{'↑' if val > 0 else '↓'} {abs(val):.2f}%" if val != 0 and val != 999 else '-'

def _color_arrow(val):
    return "color: green" if val > 0 and val != 999 else "color: red" if val < 0 else "color: white"

# Dashboard Main Panel
col = st.columns((1.5, 4.5, 2), gap='medium')

with col[0]:
    st.markdown('#### Monthly quantity change by location')

    mm = mm_name[selected_month] 
    subset = daily_pct_change.query("(year == @selected_year) & (month == @mm)").reset_index(drop=True)
    # subset = daily_pct_change.query("txn_date == @selected_date").reset_index(drop=True)

    if subset.shape[0] == 0:
        st.text("No data for the period")
    else:
      subset["pct_change"].fillna(999, inplace=True)
      st.dataframe(
         subset[["location", "quantity", "pct_change"]].style.format(_format_arrow, subset=["pct_change"]).applymap(_color_arrow, subset=["pct_change"]),
         hide_index=True,
         width=None
      )



with col[1]:
   st.markdown('#### Average revenue')
   choropleth = make_choropleth(mv_txn_prod, 'year', 'month', 'state_code', 'revenue', selected_color_theme)
   st.plotly_chart(choropleth, use_container_width=True)
   heatmap = make_heatmap(mv_txn_prod, 'year', 'month_name', 'location', 'revenue', selected_color_theme)
   st.altair_chart(heatmap, use_container_width=True)

with col[2]:
    st.markdown('#### Top location by category')

    subset = aggregated_revenue.query("(year == @selected_year) & (month == @mm)").reset_index(drop=True)
    # subset = aggregated_revenue.query("txn_date == @selected_date").reset_index(drop=True)

    if subset.shape[0] == 0:
        st.write("No data for the period")
    else: 

      st.dataframe(subset,
                  column_order=("location", "category", "revenue"),
                  hide_index=True,
                  width=None,
                  column_config={
                     "location": st.column_config.TextColumn(
                           "States",
                     ),
                     "category": st.column_config.TextColumn(
                           "Category",
                     ),
                     "revenue": st.column_config.ProgressColumn(
                           "Avg revenue",
                           format="$%f",
                           min_value=0,
                           max_value=max(subset.revenue),
                        )}
                  )
    
    with st.expander('About', expanded=True):
        st.write('''
            - Data: Dummy data generated using a python script.
            ''')


