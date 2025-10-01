# core.py
import os, re, hashlib
from datetime import datetime
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

st.set_page_config(page_title="Quản Trị Trà Trái Cây", page_icon="🍵", layout="wide")

# ---------- Kết nối Postgres ----------
_ENGINE = None
def _normalize(url: str) -> str:
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif url.startswith("postgresql://") and "+psycopg2" not in url:
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    if "sslmode=" not in url:
        url += ("&" if "?" in url else "?") + "sslmode=require"
    return url

def get_conn() -> Connection:
    global _ENGINE
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        st.error("Thiếu biến môi trường DATABASE_URL"); st.stop()
    if _ENGINE is None:
        _ENGINE = create_engine(_normalize(url), pool_pre_ping=True, future=True)
    return _ENGINE.connect()

# ---------- SQL helpers ----------
def _qmark_to_named(sql: str, params):
    if not isinstance(params, (list, tuple)): return sql, params
    i = 1
    def repl(_):
        nonlocal i
        tag = f":p{i}"; i += 1; return tag
    sql = re.sub(r"\?", repl, sql)
    return sql, {f"p{k+1}": v for k, v in enumerate(params)}

def run_sql(conn: Connection, sql: str, params=None):
    sql, params = _qmark_to_named(sql, params)
    res = conn.execute(text(sql), params or {})
    try: conn.commit()
    except Exception: pass
    return res

def fetch_df(conn: Connection, sql: str, params=None) -> pd.DataFrame:
    sql, params = _qmark_to_named(sql, params)
    return pd.read_sql_query(text(sql), conn, params=params or {})

# ---------- Auth & Audit ----------
def sha256(s: str) -> str: return hashlib.sha256(s.encode("utf-8")).hexdigest()

def write_audit(conn: Connection, action: str, detail: str = ""):
    try:
        run_sql(conn, "INSERT INTO syslog(ts,actor,action,detail) VALUES (NOW(),:u,:a,:d)",
                {"u": st.session_state.get("user", {}).get("email","anonymous"),
                 "a": action, "d": detail[:1000]})
    except Exception:
        pass

def _login(conn: Connection):
    st.markdown("### 🔐 Đăng nhập")
    e = st.text_input("Email")
    p = st.text_input("Mật khẩu", type="password")
    if st.button("Đăng nhập", type="primary"):
        df = fetch_df(conn, "SELECT email,display,password,role,store_code FROM users WHERE email=:e", {"e": e})
        if df.empty or df.iloc[0]["password"] != sha256(p):
            st.error("Sai tài khoản hoặc mật khẩu."); return
        row = df.iloc[0].to_dict()
        st.session_state["user"] = {
            "email": row["email"], "display": row.get("display") or row["email"],
            "role": row.get("role") or "User", "store": row.get("store_code") or ""
        }
        write_audit(conn, "LOGIN", row["email"]); st.rerun()

def require_login(conn: Connection) -> dict:
    if "user" not in st.session_state:
        _login(conn); st.stop()
    return st.session_state["user"]

def logout(conn: Connection):
    write_audit(conn, "LOGOUT", st.session_state.get("user", {}).get("email",""))
    st.session_state.clear(); st.rerun()

# ---------- Header & chọn cửa hàng ----------
def header_top(conn: Connection, user: dict):
    c1, c2 = st.columns([0.75, 0.25])
    with c1: st.markdown("## 🍵 Quản Trị Trà Trái Cây")
    with c2:
        with st.popover(f"👤 {user.get('display','')}"):
            st.caption(user.get("email",""))
            with st.form("fm_pw", clear_on_submit=True):
                old = st.text_input("Mật khẩu cũ", type="password")
                new1 = st.text_input("Mật khẩu mới", type="password")
                new2 = st.text_input("Xác nhận", type="password")
                ok = st.form_submit_button("Đổi mật khẩu")
            if ok:
                df = fetch_df(conn, "SELECT password FROM users WHERE email=:e", {"e": user["email"]})
                if df.empty or df.iloc[0]["password"] != sha256(old):
                    st.error("Mật khẩu cũ không đúng.")
                elif not new1 or new1 != new2:
                    st.error("Xác nhận chưa khớp.")
                else:
                    run_sql(conn, "UPDATE users SET password=:p WHERE email=:e",
                            {"p": sha256(new1), "e": user["email"]})
                    write_audit(conn, "CHANGE_PW", user["email"])
                    st.success("Đã đổi. Đăng nhập lại."); logout(conn)
            st.divider()
            if st.button("Đăng xuất", use_container_width=True): logout(conn)

def store_selector(conn: Connection, user: dict):
    st.sidebar.caption("Kết nối: Postgres (Supabase)")
    df = fetch_df(conn, "SELECT code,name FROM stores ORDER BY name")
    labels = ["— Tất cả —"] + [f"{r['code']} — {r['name']}" for _, r in df.iterrows()]
    pick = st.sidebar.selectbox("Cửa hàng", labels, index=(labels.index(f"{user.get('store','')} — {df[df['code']==user.get('store','')]['name'].iloc[0]}") if user.get('store') and not df.empty and user['store'] in df['code'].values else 0))
    if pick != "— Tất cả —":
        st.session_state["store"] = pick.split(" — ",1)[0]
    elif "store" in st.session_state:
        del st.session_state["store"]

# ---------- Router duy nhất (chỉ hiện module đã có) ----------
def router(conn: Connection, user: dict):
    # (label, handler_name)
    candidates = [
        ("Danh mục",   "page_catalog"),
        ("Sản xuất",   "page_production"),
        ("Kho",        "page_inventory"),
        ("Tài chính",  "page_finance"),
        ("TSCD",       "page_tscd"),
        ("Nhật ký",    "page_audit"),
        ("Người dùng", "page_users"),
        ("Cửa hàng",   "page_stores"),
    ]
    visible = [(lbl, fn) for (lbl, fn) in candidates if fn in globals() or fn in st.session_state.get("_externals_", set())]
    st.sidebar.markdown("## 📌 Chức năng")
    labels = [lbl for lbl, _ in visible]
    choice = st.sidebar.radio("", labels, index=0, label_visibility="collapsed")

    # gọi hàm nếu đã import từ module khác
    for lbl, fn in visible:
        if lbl == choice:
            # ưu tiên hàm đã import vào global qua from <module> import page_xxx
            if fn in globals() and callable(globals()[fn]):
                globals()[fn](conn, user)
            else:
                st.warning("Module chưa nạp.")
            break
