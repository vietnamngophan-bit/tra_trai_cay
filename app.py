# ============================================================
# app.py — Phần 1/4: Cấu hình, DB, Auth, Header, Audit
# ============================================================
import os, re, json, hashlib
from datetime import date, datetime, timedelta
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

st.set_page_config(page_title="Fruit Tea ERP v5", page_icon="🍵", layout="wide")

# ------------------- DB CONNECT -------------------
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

# ------------------- SQL HELPERS -------------------
def run_sql(conn: Connection, sql: str, params=None):
    if isinstance(params, (list, tuple)):
        idx = 1
        def repl(_): nonlocal idx; s=f":p{idx}"; idx+=1; return s
        sql_named = re.sub(r"\?", repl, sql)
        params = {f"p{i+1}": v for i,v in enumerate(params)}
        res = conn.execute(text(sql_named), params)
    else:
        res = conn.execute(text(sql), params or {})
    try: conn.commit()
    except: pass
    return res

def fetch_df(conn: Connection, sql: str, params=None) -> pd.DataFrame:
    if isinstance(params, (list, tuple)):
        idx = 1
        def repl(_): nonlocal idx; s=f":p{idx}"; idx+=1; return s
        sql_named = re.sub(r"\?", repl, sql)
        params = {f"p{i+1}": v for i,v in enumerate(params)}
        return pd.read_sql_query(text(sql_named), conn, params=params)
    return pd.read_sql_query(text(sql), conn, params=params or {})

# ------------------- AUTH -------------------
def sha256(s: str) -> str: return hashlib.sha256(s.encode("utf-8")).hexdigest()

def write_audit(conn: Connection, action: str, detail: str = ""):
    try:
        run_sql(conn,
            "INSERT INTO syslog(ts,actor,action,detail) VALUES (NOW(),:u,:a,:d)",
            {"u": st.session_state.get("user",{}).get("email","anonymous"),
             "a": action, "d": detail[:500]})
    except: pass

def login_form(conn: Connection):
    st.markdown("### 🔐 Đăng nhập")
    email = st.text_input("Email", key="login_email")
    pw    = st.text_input("Mật khẩu", type="password", key="login_pw")
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

# ------------------- HEADER -------------------
def header_top(conn: Connection, user: dict):
    col1, col2 = st.columns([0.8,0.2])
    with col1: st.markdown("## 🍵 Fruit Tea ERP v5")
    with col2:
        with st.popover(f"👤 {user.get('display','')}"):
            st.caption(user.get("email",""))
            st.markdown("---")
            with st.form("form_pwd", clear_on_submit=True):
                old = st.text_input("Mật khẩu cũ", type="password")
                new1= st.text_input("Mật khẩu mới", type="password")
                new2= st.text_input("Xác nhận", type="password")
                ok  = st.form_submit_button("Đổi")
            if ok:
                df = fetch_df(conn,"SELECT password FROM users WHERE email=:e",{"e":user["email"]})
                if df.empty or df.iloc[0]["password"]!=sha256(old):
                    st.error("Sai mật khẩu cũ.")
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

# ------------------- MAIN FRAME -------------------
def main_frame():
    conn = get_conn()
    user = require_login(conn)
    header_top(conn, user)

    # Chỉ hiện Nhật ký (nếu có quyền), KHÔNG gọi trang khác
    if user.get("role")=="SuperAdmin" or "AUDIT_VIEW" in (user.get("perms") or ""):
        st.markdown("### 🗒️ Nhật ký")
        df = fetch_df(conn,"SELECT ts,actor,action,detail FROM syslog ORDER BY ts DESC LIMIT 200")
        st.dataframe(df, use_container_width=True)

# ------------------- ENTRY -------------------
if __name__=="__main__":
    main_frame()
