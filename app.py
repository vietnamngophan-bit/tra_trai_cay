# app.py — Entry & Router cho Module 1
import streamlit as st
from core import get_conn, require_login, header_top, page_dashboard, page_syslog

st.set_page_config(page_title="Fruit Tea ERP v5", page_icon="🍵", layout="wide", initial_sidebar_state="expanded")

def router(conn, user):
    st.sidebar.markdown("### 📌 Chức năng")
    choice = st.sidebar.radio("", ["Dashboard", "Nhật ký"], index=0, label_visibility="collapsed")
    if choice == "Dashboard":
        page_dashboard(conn, user)
    elif choice == "Nhật ký":
        page_syslog(conn, user)

def main():
    conn = get_conn()
    user = require_login(conn)
    header_top(conn, user)
    router(conn, user)

if __name__ == "__main__":
    main()
