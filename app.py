# app.py
import os
import streamlit as st

# <<<< LỆNH STREAMLIT ĐẦU TIÊN PHẢI LÀ set_page_config >>>>
st.set_page_config(
    page_title="Fruit Tea ERP v5",
    page_icon="🍵",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Sau đó mới import các thứ khác (an toàn nếu các module KHÔNG gọi st.* ở global)
from core import get_conn, require_login, header_top, store_selector
from catalog import page_catalog
from inventory import page_inventory
from production import page_production
from finance import page_finance


def router(conn, user):
    st.sidebar.markdown("## 📌 Chức năng")
    menu = st.sidebar.radio("", ["Danh mục", "Kho", "Sản xuất", "Tài chính"], index=0, label_visibility="collapsed")
    store_selector(conn, user)
    if menu == "Danh mục":
        page_catalog(conn, user)
    elif menu == "Kho":
        page_inventory(conn, user)
    elif menu == "Sản xuất":
        page_production(conn, user)
    elif menu == "Tài chính":
        page_finance(conn, user)

if __name__ == "__main__":
    if not os.getenv("DATABASE_URL", "").strip():
        st.error("❌ Thiếu DATABASE_URL (Postgres).")
        st.stop()
    conn = get_conn()
    user = require_login(conn)
    header_top(conn, user)
    router(conn, user)
