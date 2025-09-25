# ============================================================
# app.py — PHẦN 1/5: Hạ tầng & Giao diện khung (Postgres only)
# ============================================================
# LƯU Ý:
# - Đặt file này là duy nhất chạy app (không có router cũ ở cuối).
# - Các trang nghiệp vụ sẽ được thêm ở Phần 2–5 thông qua các hàm route_*.
# - Không dùng SQLite. Chỉ Postgres qua biến môi trường DATABASE_URL.
# ============================================================

import os, re, json, hashlib
from datetime import datetime
from typing import Dict, Any

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

# ------------------- CẤU HÌNH TRANG (PHẢI Ở TRÊN CÙNG) -------------------
st.set_page_config(
    page_title="Fruit Tea ERP v5",
    page_icon="🍵",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ------------------- KẾT NỐI POSTGRES -------------------
_ENGINE = None  # SQLAlchemy Engine (global duy nhất)

def _normalize_pg_url(url: str) -> str:
    """Chuẩn hoá URL Postgres → driver psycopg2 + ép sslmode=require nếu thiếu."""
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif url.startswith("postgresql://") and "+psycopg2" not in url:
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    if "sslmode=" not in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}sslmode=require"
    return url

def get_conn() -> Connection:
    """Tạo 1 kết nối (connection) từ Engine. Chỉ dùng Postgres."""
    global _ENGINE
    pg_url = os.getenv("DATABASE_URL", "").strip()
    if not pg_url:
        st.error("❌ Thiếu biến môi trường **DATABASE_URL** (Postgres).")
        st.stop()
    if _ENGINE is None:
        _ENGINE = create_engine(_normalize_pg_url(pg_url), pool_pre_ping=True, future=True)
    return _ENGINE.connect()

# ------------------- TIỆN ÍCH SQL -------------------
def _qmark_to_named(sql: str, params):
    """Đổi ? → :p1, :p2... để dùng với sqlalchemy.text()."""
    if not isinstance(params, (list, tuple)):
        return sql, (params or {})
    idx = 1
    def repl(_):
        nonlocal idx
        s = f":p{idx}"
        idx += 1
        return s
    sql_named = re.sub(r"\?", repl, sql)
    named_params = {f"p{i+1}": v for i, v in enumerate(params)}
    return sql_named, named_params

def run_sql(conn: Connection, sql: str, params=None):
    """Thực thi SQL (INSERT/UPDATE/DELETE). Tự commit."""
    if isinstance(params, (list, tuple)):
        sql, params = _qmark_to_named(sql, params)
    res = conn.execute(text(sql), params or {})
    try:
        conn.commit()
    except Exception:
        pass
    return res

def fetch_df(conn: Connection, sql: str, params=None) -> pd.DataFrame:
    """SELECT trả DataFrame (hỗ trợ ? params)."""
    if isinstance(params, (list, tuple)):
        sql, params = _qmark_to_named(sql, params)
    return pd.read_sql_query(text(sql), conn, params=params or {})

# ------------------- AUTH, PHÂN QUYỀN, NHẬT KÝ -------------------
PERM_ALL = [
    # Danh mục / Sản phẩm / Công thức / Người dùng / Cửa hàng
    "CAT_VIEW","CAT_EDIT",
    "SKU_VIEW","SKU_EDIT",
    "CT_VIEW","CT_EDIT",
    "USER_VIEW","USER_EDIT",
    "STORE_VIEW","STORE_EDIT",

    # Kho
    "INV_VIEW","INV_IN","INV_OUT","INV_ADJUST",

    # Sản xuất
    "MFG_VIEW","MFG_EXEC","MFG_CLOSE","MFG_WIP_VIEW",

    # Doanh thu
    "REV_VIEW","REV_EDIT",

    # Báo cáo
    "RPT_INV","RPT_FIN",

    # Tài sản cố định
    "FA_VIEW","FA_EDIT",

    # Nhật ký
    "AUDIT_VIEW",
]

def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def has_perm(user: Dict[str, Any], perm: str) -> bool:
    if not user:
        return False
    if user.get("role") == "SuperAdmin":
        return True
    perms = (user.get("perms") or "").split(",")
    return perm in perms

def write_audit(conn: Connection, action: str, detail: str = ""):
    """Ghi nhật ký hệ thống. Không chặn nếu lỗi để không làm gián đoạn nghiệp vụ."""
    try:
        run_sql(
            conn,
            "INSERT INTO syslog(ts,actor,action,detail) VALUES (NOW(), :u, :a, :d)",
            {
                "u": st.session_state.get("user", {}).get("email", "anonymous"),
                "a": action,
                "d": (detail or "")[:1000],
            },
        )
    except Exception:
        pass

# ------------------- SESSION DEFAULTS -------------------
def _ensure_session_defaults():
    ss = st.session_state
    ss.setdefault("user", None)
    ss.setdefault("store", "")
    ss.setdefault("menu", "Dashboard")

# ------------------- FORM ĐĂNG NHẬP -------------------
def login_form(conn: Connection):
    st.markdown("### 🔐 Đăng nhập hệ thống")
    email = st.text_input("Email", key="login_email")
    pw    = st.text_input("Mật khẩu", type="password", key="login_pw")

    if st.button("Đăng nhập", type="primary", use_container_width=True):
        df = fetch_df(conn,
                      "SELECT email, display, password, role, store_code, perms "
                      "FROM users WHERE email=:e",
                      {"e": email})
        if df.empty:
            st.error("Sai tài khoản hoặc mật khẩu.")
            return

        row = df.iloc[0]
        if row["password"] != sha256(pw):
            st.error("Sai tài khoản hoặc mật khẩu.")
            return

        user = {
            "email": row["email"],
            "display": row["display"] or row["email"],
            "role": row["role"] or "User",
            "perms": row["perms"] or "",
            "store": row["store_code"] or "",
        }
        st.session_state["user"] = user
        st.session_state["store"] = user["store"]
        write_audit(conn, "LOGIN", user["email"])
        st.success("Đăng nhập thành công.")
        st.rerun()

def require_login(conn: Connection) -> Dict[str, Any]:
    if not st.session_state.get("user"):
        login_form(conn)
        st.stop()
    return st.session_state["user"]

def logout(conn: Connection):
    u = st.session_state.get("user", {})
    write_audit(conn, "LOGOUT", u.get("email", ""))
    st.session_state.clear()
    st.rerun()

# ------------------- TIỆU ĐỀ & MENU PHẢI (POPOVER TÀI KHOẢN) -------------------
def header_top(conn: Connection, user: Dict[str, Any]):
    left, right = st.columns([0.8, 0.2])
    with left:
        st.markdown("## 🍵 Fruit Tea ERP v5")
        st.caption("Kết nối: **Postgres (Supabase)**")
    with right:
        with st.popover(f"👤 {user.get('display','')}", use_container_width=True):
            st.caption(user.get("email", ""))
            st.markdown("---")
            st.markdown("**Đổi mật khẩu**")
            with st.form("form_change_pw", clear_on_submit=True):
                old = st.text_input("Mật khẩu cũ", type="password")
                new1 = st.text_input("Mật khẩu mới", type="password")
                new2 = st.text_input("Xác nhận mật khẩu", type="password")
                ok = st.form_submit_button("Cập nhật")
            if ok:
                df = fetch_df(conn, "SELECT password FROM users WHERE email=:e", {"e": user["email"]})
                if df.empty or df.iloc[0]["password"] != sha256(old):
                    st.error("Mật khẩu cũ không đúng.")
                elif not new1 or new1 != new2:
                    st.error("Xác nhận mật khẩu chưa khớp.")
                else:
                    run_sql(conn, "UPDATE users SET password=:p WHERE email=:e",
                            {"p": sha256(new1), "e": user["email"]})
                    write_audit(conn, "CHANGE_PASSWORD", user["email"])
                    st.success("Đã đổi mật khẩu. Vui lòng đăng nhập lại.")
                    logout(conn)

            st.markdown("---")
            if st.button("Đăng xuất", use_container_width=True):
                logout(conn)

# ------------------- SIDEBAR: CỬA HÀNG + MENU CHÍNH -------------------
def sidebar_menu(conn: Connection, user: Dict[str, Any]) -> str:
    st.sidebar.markdown("### 🏬 Cửa hàng")
    stores = fetch_df(conn, "SELECT code, name FROM stores ORDER BY name")
    store_map = {r["name"] if r["name"] else r["code"]: r["code"] for _, r in stores.iterrows()}
    disp_list = list(store_map.keys()) or ["(chưa có cửa hàng)"]

    # Nếu user có store mặc định thì chọn sẵn
    default_label = None
    if st.session_state.get("store"):
        for k, v in store_map.items():
            if v == st.session_state["store"]:
                default_label = k
                break

    chosen = st.sidebar.selectbox("Đang thao tác tại", disp_list, index=(
        disp_list.index(default_label) if default_label in disp_list else 0
    ), key="sidebar_store_select")

    # Lưu code cửa hàng vào session
    st.session_state["store"] = store_map.get(chosen, "")

    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📌 Chức năng")

    menu = st.sidebar.radio(
        "Điều hướng",
        [
            "Dashboard",
            "Danh mục",
            "Kho",
            "Sản xuất",
            "Doanh thu",
            "Báo cáo",
            "TSCD",
            "Nhật ký",
            "Cửa hàng",
            "Người dùng",
        ],
        index=0,
        label_visibility="collapsed",
        key="main_menu_radio",
    )

    st.sidebar.markdown("---")
    st.sidebar.caption("DB: Postgres (Supabase)")

    return menu

# ------------------- PLACEHOLDER ROUTES (sẽ viết ở Phần 2–5) -------------------
def route_part2_placeholder(menu: str):
    if menu == "Dashboard":
        st.info("Dashboard sẽ được hoàn thiện ở **Phần 2**.")
    elif menu == "Danh mục":
        st.info("Danh mục (Sản phẩm, Danh mục, Công thức) sẽ có ở **Phần 2**.")
    elif menu == "Cửa hàng":
        st.info("Quản lý cửa hàng (CRUD) sẽ nằm ở **Phần 2**.")
    elif menu == "Người dùng":
        st.info("Quản lý người dùng (CRUD + phân quyền) sẽ nằm ở **Phần 2**.")

def route_part3_placeholder(menu: str):
    if menu == "Kho":
        st.info("Kho (Nhập/Xuất/Kiểm kê) + Tồn số **cốc** → ở **Phần 3**.")
    elif menu == "Sản xuất":
        st.info("Sản xuất **CỐT** (1 bước) & **MỨT** (2 bước) → ở **Phần 3**.")

def route_part4_placeholder(menu: str):
    if menu == "Báo cáo":
        st.info("Báo cáo Tồn kho/Trị giá, Tài chính (BCKQKD, CĐKT, LCTT) → ở **Phần 4**.")
    elif menu == "TSCD":
        st.info("Tài sản cố định (thêm/sửa/xóa, khấu hao, báo cáo) → ở **Phần 4**.")

def route_part5_placeholder(menu: str):
    if menu == "Doanh thu":
        st.info("Doanh thu (CASH/BANK), xuất Excel/PDF, tra cứu… → ở **Phần 5**.")

def route_audit(conn: Connection, user: Dict[str, Any], menu: str):
    if menu == "Nhật ký":
        if has_perm(user, "AUDIT_VIEW"):
            df = fetch_df(conn,
                          "SELECT ts, actor, action, detail "
                          "FROM syslog ORDER BY ts DESC LIMIT 300")
            st.markdown("### 🗒️ Nhật ký hệ thống (mới nhất)")
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.warning("Bạn không có quyền xem nhật ký.")

# ------------------- ROUTER DUY NHẤT -------------------
def router():
    _ensure_session_defaults()
    conn = get_conn()

    # Nếu chưa đăng nhập → dừng tại form login
    user = require_login(conn)

    # Tiêu đề + popover tài khoản (đổi mật khẩu/đăng xuất)
    header_top(conn, user)

    # Sidebar: chọn cửa hàng & menu
    menu = sidebar_menu(conn, user)

    # Điều hướng (Phần 2–5 sẽ override các placeholder này)
    route_part2_placeholder(menu)
    route_part3_placeholder(menu)
    route_part4_placeholder(menu)
    route_part5_placeholder(menu)
    route_audit(conn, user, menu)

# ------------------- ENTRY -------------------
if __name__ == "__main__":
    router()
