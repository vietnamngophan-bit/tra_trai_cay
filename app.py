# app.py — Khung & Router gọi 3 module (Core, Catalog, Production)
import streamlit as st

# === Import từ các module bạn đã có ===
from core import get_conn, require_login, header_top, write_audit
from catalog import page_catalog
from production import page_production
# (nếu có dashboard/report sau này thì import thêm, còn bây giờ 3 phần như yêu cầu)

# ===================== ROUTER DUY NHẤT =====================
def router(conn, user):
    st.sidebar.markdown("## 📌 Chức năng")
    menu = st.sidebar.radio(
        "Chọn trang",
        ["Danh mục", "Sản xuất", "Nhật ký"],  # 3 mục chính theo yêu cầu hiện tại
        index=0,
        label_visibility="collapsed"
    )

    # Gọi trang tương ứng
    if menu == "Danh mục":
        page_catalog(conn, user)

    elif menu == "Sản xuất":
        page_production(conn, user)

    elif menu == "Nhật ký":
        st.markdown("## 🗒️ Nhật ký hệ thống")
        if st.button("Tải 200 dòng mới nhất"):
            df = None
            try:
                from core import fetch_df
                df = fetch_df(conn, "SELECT ts,actor,action,detail FROM syslog ORDER BY ts DESC LIMIT 200")
            except Exception as e:
                st.error(f"Lỗi tải nhật ký: {e}")
            if df is not None:
                st.dataframe(df, use_container_width=True)

    # Footer nhỏ
    st.sidebar.divider()
    st.sidebar.caption("DB: Postgres (Supabase)")
    st.sidebar.caption("Fruit Tea ERP v5")

# ===================== ENTRY POINT =====================
def main():
    st.set_page_config(page_title="Fruit Tea ERP", page_icon="🍵", layout="wide")
    conn = get_conn()                 # từ core.py
    user = require_login(conn)        # từ core.py
    header_top(user)                  # từ core.py (hiển thị tên + logout)
    router(conn, user)                # gọi router duy nhất

if __name__ == "__main__":
    main()
