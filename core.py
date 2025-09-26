# core.py — Module 1: DB, SQL helpers, Auth, Header, Syslog
import os, re, hashlib
from datetime import datetime
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

# ============== DB CONNECTION ==============
_ENGINE = None

def _normalize_pg_url(url: str) -> str:
    # cho phép cả postgres:// và postgresql://
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif url.startswith("postgresql://") and "+psycopg2" not in url:
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    # luôn yêu cầu SSL (Supabase)
    if "sslmode=" not in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}sslmode=require"
    return url

def get_conn() -> Connection:
    """Lấy connection (engine cache) – gọi ở đầu mỗi request."""
    global _ENGINE
    pg_url = os.getenv("DATABASE_URL", "").strip()
    if not pg_url:
        st.error("❌ Thiếu biến môi trường DATABASE_URL.")
        st.stop()
    if _ENGINE is None:
        _ENGINE = create_engine(_normalize_pg_url(pg_url), pool_pre_ping=True, future=True)
    return _ENGINE.connect()

# ============== SQL HELPERS ==============
def run_sql(conn: Connection, sql: str, params=None):
    """
    Chạy INSERT/UPDATE/DELETE… Hỗ trợ dấu '?' hoặc ':name'.
    """
    if isinstance(params, (list, tuple)):
        # chuyển ? -> :p1, :p2…
        idx = 1
        def repl(_):
            nonlocal idx
            s = f":p{idx}"
            idx += 1
            return s
        sql_named = re.sub(r"\?", repl, sql)
        params = {f"p{i+1}": v for i, v in enumerate(params)}
        res = conn.execute(text(sql_named), params)
    else:
        res = conn.execute(text(sql), params or {})
    try:
        conn.commit()
    except Exception:
        pass
    return res

def fetch_df(conn: Connection, sql: str, params=None) -> pd.DataFrame:
    """Trả về DataFrame, hỗ trợ '?' giống run_sql."""
    if isinstance(params, (list, tuple)):
        idx = 1
        def repl(_):
            nonlocal idx
            s = f":p{idx}"
            idx += 1
            return s
        sql_named = re.sub(r"\?", repl, sql)
        params = {f"p{i+1}": v for i, v in enumerate(params)}
        return pd.read_sql_query(text(sql_named), conn, params=params)
    return pd.read_sql_query(text(sql), conn, params=params or {})

# ============== AUTH, AUDIT, HEADER ==============
def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def write_audit(conn: Connection, action: str, detail: str = ""):
    """Ghi syslog (bỏ qua lỗi nếu bảng chưa có)."""
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

def login_form(conn: Connection):
    st.markdown("### 🔐 Đăng nhập")
    email = st.text_input("Email", key="login_email")
    pw = st.text_input("Mật khẩu", type="password", key="login_pw")
    if st.button("Đăng nhập", type="primary", use_container_width=True, key="btn_login"):
        df = fetch_df(conn,
                      "SELECT email, display, password, role, store_code, perms FROM users WHERE email=:e",
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
        if user["store"]:
            st.session_state["store"] = user["store"]
        write_audit(conn, "LOGIN", user["email"])
        st.rerun()

def require_login(conn: Connection) -> dict:
    if "user" not in st.session_state:
        login_form(conn)
        st.stop()
    return st.session_state["user"]

def logout(conn: Connection):
    u = st.session_state.get("user", {})
    write_audit(conn, "LOGOUT", u.get("email", ""))
    st.session_state.clear()
    st.rerun()

def header_top(conn: Connection, user: dict):
    left, right = st.columns([0.8, 0.2])
    with left:
        st.markdown("## 🍵 Quản Trị Trà Trái Cây Anh Gầy")
        st.caption("Kết nối: Postgres (Supabase)")
    with right:
        with st.popover(f"👤 {user.get('display','')}", use_container_width=True):
            st.caption(user.get("email", ""))
            st.markdown("---")
            st.markdown("**Đổi mật khẩu**")
            with st.form("fm_change_pw", clear_on_submit=True):
                old = st.text_input("Mật khẩu cũ", type="password", key="old_pw")
                new1 = st.text_input("Mật khẩu mới", type="password", key="new_pw1")
                new2 = st.text_input("Xác nhận", type="password", key="new_pw2")
                ok = st.form_submit_button("Cập nhật", use_container_width=True)
            if ok:
                df = fetch_df(conn, "SELECT password FROM users WHERE email=:e", {"e": user["email"]})
                if df.empty or df.iloc[0]["password"] != sha256(old):
                    st.error("Mật khẩu cũ không đúng.")
                elif not new1 or new1 != new2:
                    st.error("Xác nhận mật khẩu chưa khớp.")
                else:
                    run_sql(conn, "UPDATE users SET password=:p WHERE email=:e",
                            {"p": sha256(new1), "e": user["email"]})
                    write_audit(conn, "CHANGE_PW", user["email"])
                    st.success("Đã đổi mật khẩu, đăng nhập lại.")
                    logout(conn)

            st.markdown("---")
            if st.button("Đăng xuất", use_container_width=True, key="btn_logout"):
                logout(conn)

# ============== PAGES (Module 1) ==============
def page_dashboard(conn: Connection, user: dict):
    st.markdown("### 📊 Dashboard (Module 1 demo)")
    st.info("Dashboard chi tiết sẽ bổ sung ở Module 2/3/4.")

def page_syslog(conn: Connection, user: dict):
    st.markdown("### 📜 Nhật ký")
    df = fetch_df(conn, "SELECT ts, actor, action, detail FROM syslog ORDER BY ts DESC LIMIT 300")
    st.dataframe(df, use_container_width=True)

# tiện ích quyền (dùng từ Module 2 trở đi)
def has_perm(user: dict, perm: str) -> bool:
    if not user:
        return False
    if user.get("role") == "SuperAdmin":
        return True
    return perm in (user.get("perms") or "").split(",")
