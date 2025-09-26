# ============================================================
# app.py — Fruit Tea ERP v5 (Postgres Only)
# Phần 1/4: Hạ tầng, Kết nối, Đăng nhập, Header, Router
# ============================================================

import os, re, json, hashlib
from datetime import datetime, date, timedelta
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

# ------------------- CẤU HÌNH TRANG -------------------
st.set_page_config(
    page_title="Fruit Tea ERP v5",
    page_icon="🍵",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ------------------- KẾT NỐI POSTGRES -------------------
_ENGINE = None
def _normalize_pg_url(url: str) -> str:
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif url.startswith("postgresql://") and "+psycopg2" not in url:
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    if "sslmode=" not in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}sslmode=require"
    return url

def get_conn() -> Connection:
    global _ENGINE
    pg_url = os.getenv("DATABASE_URL", "").strip()
    if not pg_url:
        st.error("❌ DATABASE_URL chưa được cấu hình.")
        st.stop()
    if _ENGINE is None:
        _ENGINE = create_engine(_normalize_pg_url(pg_url), pool_pre_ping=True, future=True)
    return _ENGINE.connect()

# ------------------- HELPER SQL -------------------
def run_sql(conn: Connection, sql: str, params=None):
    if isinstance(params, (list, tuple)):
        idx = 1
        def repl(_):
            nonlocal idx
            s = f":p{idx}"
            idx += 1
            return s
        sql_named = re.sub(r"\?", repl, sql)
        params = {f"p{i+1}": v for i,v in enumerate(params)}
        res = conn.execute(text(sql_named), params)
    else:
        res = conn.execute(text(sql), params or {})
    try: conn.commit()
    except Exception: pass
    return res

def fetch_df(conn: Connection, sql: str, params=None) -> pd.DataFrame:
    if isinstance(params, (list, tuple)):
        idx = 1
        def repl(_):
            nonlocal idx
            s = f":p{idx}"
            idx += 1
            return s
        sql_named = re.sub(r"\?", repl, sql)
        params = {f"p{i+1}": v for i,v in enumerate(params)}
        return pd.read_sql_query(text(sql_named), conn, params=params)
    return pd.read_sql_query(text(sql), conn, params=params or {})

# ------------------- AUTH & PHÂN QUYỀN -------------------
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
             "a": action, "d": detail[:1000]})
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
        user = {
            "email": row["email"],
            "display": row["display"] or row["email"],
            "role": row["role"] or "User",
            "perms": row["perms"] or "",
            "store": row["store_code"] or ""
        }
        st.session_state["user"] = user
        if user["store"]: st.session_state["store"] = user["store"]
        write_audit(conn, "LOGIN", email)
        st.rerun()

def require_login(conn: Connection) -> dict:
    if "user" not in st.session_state: login_form(conn); st.stop()
    return st.session_state["user"]

def logout(conn: Connection):
    u = st.session_state.get("user",{})
    write_audit(conn, "LOGOUT", u.get("email",""))
    st.session_state.clear()
    st.rerun()

# ------------------- HEADER TRÊN CÙNG -------------------
def header_top(conn: Connection, user: dict):
    col1, col2 = st.columns([0.8,0.2])
    with col1:
        st.markdown("## 🍵 Quản Trị Trà Trái Cây Anh Gầy")
        st.caption("Kết nối: Postgres (Supabase)")
    with col2:
        with st.popover(f"👤 {user.get('display','')}"):
            st.caption(user.get("email",""))
            st.markdown("---")
            st.markdown("**Đổi mật khẩu**")
            with st.form("form_pwd", clear_on_submit=True):
                old = st.text_input("Mật khẩu cũ", type="password")
                new1 = st.text_input("Mật khẩu mới", type="password")
                new2 = st.text_input("Xác nhận", type="password")
                ok = st.form_submit_button("Cập nhật")
            if ok:
                df = fetch_df(conn,"SELECT password FROM users WHERE email=:e",{"e":user["email"]})
                if df.empty or df.iloc[0]["password"]!=sha256(old):
                    st.error("Mật khẩu cũ không đúng.")
                elif new1!=new2 or not new1:
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

# ------------------- ROUTER (DUY NHẤT) -------------------
def router():
    conn = get_conn()
    user = require_login(conn)
    header_top(conn, user)

    st.sidebar.markdown("## 📌 Menu")
    menu = st.sidebar.radio(
        "Chọn chức năng",
        ["Dashboard","Danh mục","Cửa hàng","Người dùng","Kho","Sản xuất",
         "Doanh thu","Báo cáo","TSCD","Nhật ký"],
        index=0
    )
    st.sidebar.divider()

    if menu=="Dashboard": page_dashboard(conn,user)
    elif menu=="Danh mục": page_catalog(conn,user)
    elif menu=="Cửa hàng": page_stores(conn,user)
    elif menu=="Người dùng": page_users(conn,user)
    elif menu=="Kho": page_kho(conn,user)
    elif menu=="Sản xuất": page_sanxuat(conn,user)
    elif menu=="Doanh thu": page_doanhthu(conn,user)
    elif menu=="Báo cáo": page_baocao(conn,user)
    elif menu=="TSCD": page_tscd(conn,user)
    elif menu=="Nhật ký": page_audit(conn,user)

# ------------------- ENTRY -------------------
if __name__=="__main__":
    router()
