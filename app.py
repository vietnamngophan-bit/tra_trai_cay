# =========================
# app.py — Phần 1/5
# =========================
import os, re, json, hashlib
from datetime import datetime, date, timedelta
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

# --------- PAGE CONFIG ----------
st.set_page_config(
    page_title="Fruit Tea ERP v5",
    page_icon="🍵",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --------- POSTGRES ONLY ----------
_ENGINE = None
def _normalize_pg_url(url: str) -> str:
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif url.startswith("postgresql://") and "+psycopg2" not in url:
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    if "sslmode=" not in url:
        url += ("&" if "?" in url else "?") + "sslmode=require"
    return url

def get_conn() -> Connection:
    global _ENGINE
    pg_url = os.getenv("DATABASE_URL", "").strip()
    if not pg_url:
        st.error("❌ Chưa cấu hình DATABASE_URL (Supabase).")
        st.stop()
    if _ENGINE is None:
        _ENGINE = create_engine(_normalize_pg_url(pg_url), pool_pre_ping=True, future=True)
    return _ENGINE.connect()

# --------- SQL HELPERS ----------
def _qmark_to_named(sql: str, params):
    idx = 1
    def repl(_):
        nonlocal idx
        s = f":p{idx}"
        idx += 1
        return s
    sql2 = re.sub(r"\?", repl, sql)
    params2 = {f"p{i+1}": v for i, v in enumerate(params)}
    return sql2, params2

def run_sql(conn: Connection, sql: str, params=None):
    if isinstance(params, (list, tuple)):
        sql, params = _qmark_to_named(sql, params)
        res = conn.execute(text(sql), params)
    else:
        res = conn.execute(text(sql), params or {})
    try: conn.commit()
    except Exception: pass
    return res

def fetch_df(conn: Connection, sql: str, params=None) -> pd.DataFrame:
    if isinstance(params, (list, tuple)):
        sql, params = _qmark_to_named(sql, params)
        return pd.read_sql_query(text(sql), conn, params=params)
    return pd.read_sql_query(text(sql), conn, params=params or {})

# --------- AUTH / PERMS / AUDIT ----------
def sha256(s: str) -> str: return hashlib.sha256(s.encode("utf-8")).hexdigest()
def has_perm(user: dict, perm: str) -> bool:
    if not user: return False
    if user.get("role") == "SuperAdmin": return True
    return perm in (user.get("perms") or "").split(",")

def write_audit(conn: Connection, action: str, detail: str = ""):
    try:
        run_sql(conn,
            "INSERT INTO syslog(ts,actor,action,detail) VALUES (NOW(),:u,:a,:d)",
            {"u": st.session_state.get("user",{}).get("email","anonymous"),
             "a": action, "d": (detail or "")[:1000]})
    except: pass

def login_form(conn: Connection):
    st.markdown("### 🔐 Đăng nhập hệ thống")
    email = st.text_input("Email", key="login_email")
    pw = st.text_input("Mật khẩu", type="password", key="login_pw")
    if st.button("Đăng nhập", type="primary", use_container_width=True):
        df = fetch_df(conn,
            "SELECT email,display,password,role,store_code,perms FROM users WHERE email=:e",
            {"e": email})
        if df.empty: st.error("Sai tài khoản hoặc mật khẩu."); return
        row = df.iloc[0]
        if row["password"] != sha256(pw):
            st.error("Sai tài khoản hoặc mật khẩu."); return
        st.session_state["user"] = {
            "email": row["email"],
            "display": row["display"] or row["email"],
            "role": row["role"] or "User",
            "perms": row["perms"] or "",
            "store": row["store_code"] or ""
        }
        if row["store_code"]:
            st.session_state["store"] = row["store_code"]
        write_audit(conn, "LOGIN", email)
        st.rerun()

def require_login(conn: Connection) -> dict:
    if "user" not in st.session_state:
        login_form(conn); st.stop()
    return st.session_state["user"]

def logout(conn: Connection):
    u = st.session_state.get("user",{})
    write_audit(conn, "LOGOUT", u.get("email",""))
    st.session_state.clear()
    st.rerun()

# --------- HEADER (avatar góc phải) ----------
def header_top(conn: Connection, user: dict):
    col1, col2 = st.columns([0.8,0.2])
    with col1:
        st.markdown("## 🍵 Fruit Tea ERP v5")
        st.caption("Kết nối: Postgres (Supabase)")
    with col2:
        with st.popover(f"👤 {user.get('display','')}"):
            st.caption(user.get("email",""))
            st.markdown("---")
            st.markdown("**Đổi mật khẩu**")
            with st.form("form_pwd", clear_on_submit=True):
                old = st.text_input("Mật khẩu cũ", type="password", key="oldpw")
                new1 = st.text_input("Mật khẩu mới", type="password", key="newpw1")
                new2 = st.text_input("Xác nhận", type="password", key="newpw2")
                ok = st.form_submit_button("Cập nhật")
            if ok:
                df = fetch_df(conn,"SELECT password FROM users WHERE email=:e",{"e":user["email"]})
                if df.empty or df.iloc[0]["password"]!=sha256(old):
                    st.error("Mật khẩu cũ không đúng.")
                elif not new1 or new1!=new2:
                    st.error("Xác nhận chưa khớp.")
                else:
                    run_sql(conn,"UPDATE users SET password=:p WHERE email=:e",
                        {"p":sha256(new1),"e":user["email"]})
                    write_audit(conn,"CHANGE_PW",user["email"])
                    st.success("Đã đổi mật khẩu, đăng nhập lại.")
                    logout(conn)
            st.markdown("---")
            if st.button("Đăng xuất", use_container_width=True):
                logout(conn)

# --------- NHẬT KÝ (đã dùng ngay) ----------
def page_nhatky(conn: Connection, user: dict):
    if not has_perm(user,"AUDIT_VIEW") and user.get("role")!="SuperAdmin":
        st.warning("Bạn không có quyền xem nhật ký."); return
    st.markdown("### 🧾 Nhật ký")
    df = fetch_df(conn,"SELECT ts,actor,action,detail FROM syslog ORDER BY ts DESC LIMIT 300")
    st.dataframe(df, use_container_width=True)

# --------- ROUTER (chỉ gọi phần đã có: P2 sẽ bổ sung) ----------
def router():
    conn = get_conn()
    user = require_login(conn)
    header_top(conn, user)

    st.sidebar.markdown("### 🏪 Cửa hàng")
    # danh sách cửa hàng để chọn
    df_st = fetch_df(conn, "SELECT code,name FROM stores ORDER BY name")
    options = ["(Tất cả)"] + df_st["code"].tolist()
    curr = st.sidebar.selectbox("Đang thao tác tại", options,
                                index=(options.index(user.get("store")) if user.get("store") in options else 0),
                                key="store")
    st.sidebar.divider()

    st.sidebar.markdown("### 📌 Chức năng")
    menu = st.sidebar.radio("",
        ["Dashboard","Danh mục","Cửa hàng","Người dùng","Nhật ký"],
        index=0, label_visibility="collapsed")

    if menu == "Nhật ký":
        page_nhatky(conn, user)
    elif menu == "Dashboard":
        # phần 2 sẽ ghi đè
        st.info("Dashboard (KPIs / biểu đồ) — xem Phần 2.")
    elif menu == "Danh mục":
        st.info("Quản lý danh mục & sản phẩm — xem Phần 2.")
    elif menu == "Cửa hàng":
        st.info("Quản lý cửa hàng — xem Phần 2.")
    elif menu == "Người dùng":
        st.info("Quản lý người dùng — xem Phần 2.")

if __name__ == "__main__":
    router()
