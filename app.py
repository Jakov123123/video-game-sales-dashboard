import streamlit as st
import duckdb
import plotly.express as px

st.set_page_config(page_title="Video Game Sales Dashboard", layout="wide")
st.title("Video Game Sales Analytics")

@st.cache_resource
def get_connection():
    return duckdb.connect("video_games.db", read_only=True)

conn = get_connection()

# Sidebar Filters
st.sidebar.header("Filters")

# Platform filter
platforms = conn.execute("SELECT DISTINCT Platform FROM dim_platforms ORDER BY Platform").fetchdf()
selected_platforms = st.sidebar.multiselect("Select Platforms", platforms['Platform'].tolist(), default=platforms['Platform'].tolist())

# Region filter
region_options = ["Global", "North America", "Europe", "Japan", "Other"]
selected_region = st.sidebar.selectbox("Select Region", region_options)

# Map region to column
region_mapping = {
    "Global": "Global_Sales",
    "North America": "NA_Sales",
    "Europe": "EU_Sales",
    "Japan": "JP_Sales",
    "Other": "Other_Sales"
}
sales_column = region_mapping[selected_region]

# Build platform filter
platform_filter = ""
if selected_platforms:
    platform_list = "','".join(selected_platforms)
    platform_filter = f"AND pl.Platform IN ('{platform_list}')"

# Top Games by Sales
st.header(f"Top 5 Games by {selected_region} Sales")
top_games_query = f"""
SELECT 
    f.Name,
    ROUND(SUM(f.{sales_column}), 2) as Total_Sales
FROM fact_sales f
LEFT JOIN dim_platforms pl ON f.platform_id = pl.platform_id
WHERE 1=1 {platform_filter}
GROUP BY f.Name
ORDER BY Total_Sales DESC
LIMIT 5
"""
top_games = conn.execute(top_games_query).fetchdf()
st.dataframe(top_games, use_container_width=True)

# Top 5 Games by User Score
st.header("Top 5 Games by User Score")
top_user_query = f"""
SELECT 
    f.Name,
    pl.Platform,
    f.User_Score,
    f.User_Count
FROM fact_sales f
LEFT JOIN dim_platforms pl ON f.platform_id = pl.platform_id
WHERE f.User_Score IS NOT NULL 
AND f.User_Count > 25
{platform_filter}
ORDER BY f.User_Score DESC, f.User_Count DESC
LIMIT 5
"""
top_user = conn.execute(top_user_query).fetchdf()
st.dataframe(top_user, use_container_width=True)

# Top 5 Games by Critic Score
st.header("Top 5 Games by Critic Score")
top_critic_query = f"""
SELECT 
    f.Name,
    pl.Platform,
    f.Critic_Score,
    f.Critic_Count
FROM fact_sales f
LEFT JOIN dim_platforms pl ON f.platform_id = pl.platform_id
WHERE f.Critic_Score IS NOT NULL 
AND f.Critic_Count > 15
{platform_filter}
ORDER BY f.Critic_Score DESC, f.Critic_Count DESC
LIMIT 5
"""
top_critic = conn.execute(top_critic_query).fetchdf()
st.dataframe(top_critic, use_container_width=True)

# Sales by Platform
st.header(f"Sales by Platform ({selected_region})")
platform_sales_query = f"""
SELECT 
    pl.Platform,
    ROUND(SUM(f.{sales_column}), 2) as Total_Sales
FROM fact_sales f
LEFT JOIN dim_platforms pl ON f.platform_id = pl.platform_id
WHERE 1=1 {platform_filter}
GROUP BY pl.Platform
ORDER BY Total_Sales DESC
"""
platform_sales = conn.execute(platform_sales_query).fetchdf()
fig = px.bar(platform_sales, x='Platform', y='Total_Sales', 
             title=f'Sales by Platform - {selected_region}',
             labels={'Total_Sales': 'Sales (Millions)'})
st.plotly_chart(fig, use_container_width=True)



