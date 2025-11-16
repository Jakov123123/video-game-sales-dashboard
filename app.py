import streamlit as st
import duckdb
import plotly.express as px

st.set_page_config(page_title="Video Game Sales Dashboard", layout="wide")
st.title("🎮 Video Game Sales Analytics")

@st.cache_resource
def get_connection():
    return duckdb.connect("C:/Users/jakov/OneDrive/Desktop/video_games.db", read_only=True)

conn = get_connection()

# Total Sales
total_query = "SELECT ROUND(SUM(Global_Sales), 2) as total_sales FROM fact_sales"
total = conn.execute(total_query).fetchdf()

st.header("Total Global Sales")
st.metric("Total Sales", f"${total['total_sales'][0]:,.1f}M")