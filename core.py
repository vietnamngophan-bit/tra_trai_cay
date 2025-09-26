# core.py — DB Postgres-only + Auth + Header + Syslog (rút gọn)
import os, re, hashlib
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

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
    # Ưu tiên secrets của Streamlit Cloud, fallback ENV local
    dsn = st.secrets.get("DATABASE_URL", os.getenv("DATABASE_URL","")).strip()
    if not dsn:
        st.error("❌ Thiếu DATABASE_URL (đặt trong Secrets hoặc biến môi trường)."); st.stop()
    if _ENGINE is None:
        _ENGINE = create_engine(_normalize_pg_url(dsn), pool_pre_ping=True, future=True)
    return _ENGINE.connect()

def _qmark_to_named(sql: str, params):
    if not isinstance(params, (list, tuple)): return sql, (params or {})
    idx = 1
    def repl(_):
        nonlocal idx
        s = f":p{idx}"; idx += 1; return s
    sql2 = re.sub(r"\?", repl, sql)
    params2 = {f"p{i+1}": v for i, v in enumerate(params)}
    return sql2, params2

def run_sql(conn: Connection, sql: str, params=None):
    if isinstance(params, (list, tuple)):
        sql, params = _qmark_to_named(sql, params)
    res = conn.execute(text(sql), params or {})
    try: conn.commit()
    except: pass
    return res

def fetch_df(conn: Connection, sql: str, params=None) -> pd.DataFrame:
    if isinstance(params, (list, tuple)):
        sql, params = _qmark_to_named(sql, params)
    return pd.read_sql_query(text(sql), conn, params=params or {})

def sha256(s: str) -> str: return hashlib.sha256(s.encode("utf-8")).hexdigest()

def write_audit(conn: Connection, action: str, detail: str = ""):
    try:
        run_sql(conn, "INSERT INTO syslog(ts,actor,action,detail) VALUES (NOW(),:u,:a,:d)", {
            "u": st.session_state.get("user",{}).get("email","anonymous"),
            "a": action, "d": (detail or "")[:1000]
        })
    except: pass

def has_perm(user: dict, perm: str) -> bool:
    if not user: return False
    if user.get("role") == "SuperAdmin": return True
    return perm in (user.get("perms") or "").split(",")

def login_form(conn: Connection):
    st.markdown("### 🔐 Đăng nhập")
    email = st.text_input("Email")
    pw    = st.text_input("Mật khẩu", type="password")
    if st.button("Đăng nhập", type="primary", use_container_width=True):
        df = fetch_df(conn, "SELECT email,display,password,role,store_code,perms FROM users WHERE email=:e", {"e": email})
        if df.empty or df.iloc[0]["password"] != sha256(pw):
            st.error("Sai tài khoản hoặc mật khẩu."); return
        row = df.iloc[0]
        st.session_state["user"] = {
            "email": row["email"], "display": row["display"] or row["email"],
            "role": row["role"] or "User", "perms": row["perms"] or "",
            "store": row["store_code"] or ""
        }
        if row["store_code"]: st.session_state["store"] = row["store_code"]
        write_audit(conn,"LOGIN",row["email"]); st.rerun()

def require_login(conn: Connection) -> dict:
    if "user" not in st.session_state: login_form(conn); st.stop()
    return st.session_state["user"]

def logout(conn: Connection):
    u = st.session_state.get("user",{})
    write_audit(conn,"LOGOUT",u.get("email",""))
    st.session_state.clear(); st.rerun()

def header_top(conn: Connection, user: dict):
    left, right = st.columns([0.75, 0.25])
    with left:
        st.markdown("## 🍵 Fruit Tea ERP v5 — Module 2 (Catalog)")
        st.caption("Kết nối: Postgres (Supabase)")
    with right:
        with st.popover(f"👤 {user.get('display','')}", use_container_width=True):
            st.caption(user.get("email",""))
            st.markdown("---")
            st.markdown("**Đổi mật khẩu**")
            with st.form("fm_pw", clear_on_submit=True):
                old = st.text_input("Mật khẩu cũ", type="password")
                new1= st.text_input("Mật khẩu mới", type="password")
                new2= st.text_input("Xác nhận", type="password")
                ok  = st.form_submit_button("Cập nhật")
            if ok:
                dfp = fetch_df(conn,"SELECT password FROM users WHERE email=:e",{"e":user["email"]})
                if dfp.empty or dfp.iloc[0]["password"]!=sha256(old): st.error("Mật khẩu cũ không đúng.")
                elif not new1 or new1!=new2: st.error("Xác nhận chưa khớp.")
                else:
                    run_sql(conn,"UPDATE users SET password=:p WHERE email=:e",{"p":sha256(new1),"e":user["email"]})
                    write_audit(conn,"CHANGE_PW",user["email"])
                    st.success("Đã đổi MK. Đăng nhập lại."); logout(conn)
            st.markdown("---")
            if st.button("Đăng xuất", use_container_width=True): logout(conn)

def page_dashboard(conn, user):
    st.markdown("### 📊 Dashboard (Module 2)")
    st.info("Sẽ hoàn thiện ở Module 3/4.")

def page_syslog(conn, user):
    st.markdown("### 📜 Nhật ký")
    df = fetch_df(conn,"SELECT ts,actor,action,detail FROM syslog ORDER BY ts DESC LIMIT 300")
    st.dataframe(df, use_container_width=True)
