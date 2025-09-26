import streamlit as st
from core import get_conn, require_login, header_top, store_selector

def router(conn, user):
    st.sidebar.markdown("## 📌 Chức năng")
    st.write("Chỉ là demo khung. Thêm các trang sau.")

if __name__ == "__main__":
    conn = get_conn()
    user = require_login(conn)
    header_top(conn, user)           # header + đổi mật khẩu + logout
    current_store = store_selector(conn, user)  # <-- CHỌN CỬA HÀNG Ở SIDEBAR
    router(conn, user)
