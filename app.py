# app.py
import os, socket, urllib.parse
import streamlit as st

# 1) LỆNH STREAMLIT ĐẦU TIÊN
st.set_page_config(
    page_title="Fruit Tea ERP v5",
    page_icon="🍵",
    layout="wide",
    initial_sidebar_state="expanded",
)

# 2) Import sau khi set_page_config
from core import get_conn, require_login, header_top, store_selector
from catalog import page_catalog
from inventory import page_inventory
from production import page_production
from finance import page_finance

def _mask_url(url: str) -> str:
    """Ẩn mật khẩu trong connection string khi debug"""
    try:
        if not url: return ""
        p = urllib.parse.urlsplit(url)
        # p.netloc = user:pass@host:port
        userinfo, _, hostport = p.netloc.rpartition("@")
        if ":" in userinfo:
            user, _ = userinfo.split(":", 1)
            masked_userinfo = f"{user}:********"
        else:
            masked_userinfo = userinfo or ""
        netloc = f"{masked_userinfo}@{hostport}" if hostport else masked_userinfo
        return urllib.parse.urlunsplit((p.scheme, netloc, p.path, p.query, p.fragment))
    except Exception:
        return "<cannot mask>"

def _debug_db_url():
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        st.error("❌ Thiếu biến môi trường DATABASE_URL (Postgres). Vào Streamlit → Settings → Advanced → Secrets để thêm.")
        st.stop()

    st.caption("🔗 DATABASE_URL (đã mask):")
    st.code(_mask_url(url))

    # Hiển thị host/port để người dùng đối chiếu nhanh
    try:
        p = urllib.parse.urlsplit(url)
        hostport = (p.netloc.split("@", 1)[-1])  # phần sau @
        host = hostport.split(":", 1)[0]
        port = int(hostport.split(":")[1]) if ":" in hostport else None
        st.write(f"🖥️ Host: `{host}`  •  🔌 Port: `{port}`")
        # Thử resolve DNS
        ip = socket.gethostbyname(host)
        st.success(f"DNS OK → {host} → {ip}")
    except Exception as e:
        st.error(f"❌ DNS lỗi hoặc host sai. Kiểm tra lại host trong Supabase (dạng `db.<project-ref>.supabase.co`). Chi tiết: {e}")
        st.stop()

def router(conn, user):
    st.sidebar.markdown("## 📌 Chức năng")
    menu = st.sidebar.radio(
        "",
        ["Danh mục", "Kho", "Sản xuất", "Tài chính"],
        index=0,
        label_visibility="collapsed",
    )
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
    # 3) Kiểm tra URL + DNS trước khi kết nối
    _debug_db_url()

    # 4) Kết nối DB
    try:
        conn = get_conn()
    except Exception as e:
        st.error(f"❌ Không kết nối được Postgres. Kiểm tra lại `DATABASE_URL`, port (6543 cho pooler), và password URL-encode. Chi tiết: {e}")
        st.stop()

    # 5) Auth + UI
    user = require_login(conn)
    header_top(conn, user)
    router(conn, user)
