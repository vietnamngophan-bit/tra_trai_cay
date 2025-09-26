# app.py — chạy core + catalog (Module 2)

import streamlit as st
from core import get_conn, require_login, header_top, store_selector
from catalog import page_catalog
from production import page_production

def router(conn, user):
    st.sidebar.markdown("## 📌 Chức năng")
    menu = st.sidebar.radio("", ["Danh mục"], index=0, label_visibility="collapsed")
    if menu == "Danh mục":
        page_catalog(conn, user)
    elif menu=="Sản xuất":
        page_production(conn, user)
if __name__ == "__main__":
    conn = get_conn()
    user = require_login(conn)
    header_top(conn, user)
    store_selector(conn, user)  # để sẵn ở sidebar
    router(conn, user)

