# app.py — chạy core + catalog (Module 2) + production (Module 3)

import streamlit as st
from core import get_conn, require_login, header_top, store_selector, fetch_df
from catalog import page_catalog
from production import page_production   # thêm module 3

def router(conn, user):
    st.sidebar.markdown("## 📌 Chức năng")
    menu = st.sidebar.radio(
        "",
        ["Danh mục", "Sản xuất", "Nhật ký"],
        index=0,
        label_visibility="collapsed"
    )

    if menu == "Danh mục":
        page_catalog(conn, user)

    elif menu == "Sản xuất":
        page_production(conn, user)

    elif menu == "Nhật ký":
        st.markdown("## 🗒️ Nhật ký hệ thống")
        try:
            df = fetch_df(conn,
                "SELECT ts,actor,action,detail FROM syslog ORDER BY ts DESC LIMIT 200")
            st.dataframe(df, use_container_width=True)
        except Exception as e:
            st.error(f"Lỗi tải nhật ký: {e}")

if __name__ == "__main__":
    conn = get_conn()
    user = require_login(conn)
    header_top(conn, user)
    store_selector(conn, user)  # chọn cửa hàng ở sidebar
    router(conn, user)
