# app.py — Entry của hệ thống (Postgres only)
# Gọi 4 module chính: Danh mục, Kho, Sản xuất, Tài chính

import os
import streamlit as st

# ====== Core (bắt buộc) ======
from core import get_conn, require_login, header_top, store_selector

# ====== Các page module (đã viết ở các file riêng) ======
# LƯU Ý: các file này phải tồn tại cùng thư mục với app.py
from catalog import page_catalog
from inventory import page_inventory
from production import page_production
from finance   import page_finance


# ------------------- Cấu hình trang -------------------
st.set_page_config(
    page_title="Fruit Tea ERP v5",
    page_icon="🍵",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ------------------- Router duy nhất -------------------
def router(conn, user):
    st.sidebar.markdown("## 📌 Chức năng")
    menu = st.sidebar.radio(
        label="",
        options=["Danh mục", "Kho", "Sản xuất", "Tài chính"],
        index=0,
        label_visibility="collapsed"
    )

    # Chọn cửa hàng (xuất hiện ở sidebar cho mọi trang)
    store_selector(conn, user)

    # Gọi đúng trang
    if menu == "Danh mục":
        page_catalog(conn, user)
    elif menu == "Kho":
        page_inventory(conn, user)
    elif menu == "Sản xuất":
        page_production(conn, user)
    elif menu == "Tài chính":
        page_finance(conn, user)


# ------------------- Entry point -------------------
if __name__ == "__main__":
    # Bắt buộc có DATABASE_URL (Postgres/Supabase)
    if not os.getenv("DATABASE_URL", "").strip():
        st.error("❌ Thiếu biến môi trường DATABASE_URL (Postgres).")
        st.stop()

    conn = get_conn()
    user = require_login(conn)
    header_top(conn, user)   # khối user (đổi mật khẩu/đăng xuất)

    # Vào router
    router(conn, user)
