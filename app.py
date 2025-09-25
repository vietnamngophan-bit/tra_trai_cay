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
# ============================================================
# app.py — Phần 2/4: Dashboard + Danh mục + Cửa hàng + Người dùng
# ============================================================

# ------------------- DASHBOARD -------------------
def page_dashboard(conn, user):
    st.markdown("### 📊 Dashboard")
    col1, col2, col3 = st.columns(3)
    with col1:
        df = fetch_df(conn,"SELECT COUNT(*) AS n FROM products")
        st.metric("Sản phẩm", int(df.iloc[0]["n"]))
    with col2:
        df = fetch_df(conn,"SELECT COUNT(*) AS n FROM stores")
        st.metric("Cửa hàng", int(df.iloc[0]["n"]))
    with col3:
        df = fetch_df(conn,"SELECT COUNT(*) AS n FROM users")
        st.metric("Người dùng", int(df.iloc[0]["n"]))

# ------------------- DANH MỤC -------------------
def page_danhmuc(conn, user):
    st.markdown("### 📂 Danh mục sản phẩm")
    df = fetch_df(conn,"SELECT code,name FROM categories ORDER BY code")
    st.dataframe(df, use_container_width=True)
    mode = st.radio("Chế độ", ["Tạo mới","Sửa/Xóa"], horizontal=True)

    if mode=="Tạo mới":
        code = st.text_input("Mã DM")
        name = st.text_input("Tên danh mục")
        if st.button("💾 Lưu"):
            if code and name:
                run_sql(conn,"INSERT INTO categories(code,name) VALUES(:c,:n) ON CONFLICT(code) DO UPDATE SET name=:n",
                        {"c":code,"n":name})
                st.success("Đã lưu danh mục")
                st.rerun()
    else:
        if df.empty:
            st.info("Chưa có danh mục.")
            return
        sel = st.selectbox("Chọn", df["code"])
        row = df[df["code"]==sel].iloc[0]
        name = st.text_input("Tên danh mục", row["name"])
        c1,c2 = st.columns(2)
        with c1:
            if st.button("💾 Cập nhật"):
                run_sql(conn,"UPDATE categories SET name=:n WHERE code=:c",{"n":name,"c":sel})
                st.success("Đã cập nhật"); st.rerun()
        with c2:
            if st.button("🗑️ Xóa"):
                run_sql(conn,"DELETE FROM categories WHERE code=:c",{"c":sel})
                st.success("Đã xóa"); st.rerun()

# ------------------- CỬA HÀNG -------------------
def page_cuahang(conn, user):
    st.markdown("### 🏬 Quản lý cửa hàng")
    df = fetch_df(conn,"SELECT code,name,addr,note FROM stores ORDER BY code")
    st.dataframe(df,use_container_width=True)
    mode = st.radio("Chế độ",["Tạo mới","Sửa/Xóa"],horizontal=True)

    if mode=="Tạo mới":
        code = st.text_input("Mã cửa hàng")
        name = st.text_input("Tên cửa hàng")
        addr = st.text_input("Địa chỉ")
        note = st.text_area("Ghi chú")
        if st.button("💾 Lưu"):
            run_sql(conn,"INSERT INTO stores(code,name,addr,note) VALUES(:c,:n,:a,:t) ON CONFLICT(code) DO UPDATE SET name=:n,addr=:a,note=:t",
                    {"c":code,"n":name,"a":addr,"t":note})
            st.success("Đã lưu"); st.rerun()
    else:
        if df.empty:
            st.info("Chưa có cửa hàng."); return
        sel = st.selectbox("Chọn", df["code"])
        row = df[df["code"]==sel].iloc[0]
        name = st.text_input("Tên", row["name"])
        addr = st.text_input("Địa chỉ", row["addr"] or "")
        note = st.text_area("Ghi chú", row["note"] or "")
        c1,c2 = st.columns(2)
        with c1:
            if st.button("💾 Cập nhật"):
                run_sql(conn,"UPDATE stores SET name=:n,addr=:a,note=:t WHERE code=:c",
                        {"n":name,"a":addr,"t":note,"c":sel})
                st.success("Đã cập nhật"); st.rerun()
        with c2:
            if st.button("🗑️ Xóa"):
                run_sql(conn,"DELETE FROM stores WHERE code=:c",{"c":sel})
                st.success("Đã xóa"); st.rerun()

# ------------------- NGƯỜI DÙNG -------------------
def page_nguoidung(conn, user):
    st.markdown("### 👥 Quản lý người dùng")
    df = fetch_df(conn,"SELECT email,display,role,store_code FROM users ORDER BY email")
    st.dataframe(df,use_container_width=True)
    mode = st.radio("Chế độ",["Tạo mới","Sửa/Xóa"],horizontal=True)

    if mode=="Tạo mới":
        email = st.text_input("Email")
        display = st.text_input("Tên hiển thị")
        pw = st.text_input("Mật khẩu", type="password")
        role = st.selectbox("Vai trò",["User","Admin","SuperAdmin"])
        store = st.text_input("Mã cửa hàng (bỏ trống nếu SuperAdmin)")
        if st.button("💾 Lưu"):
            if email and pw:
                run_sql(conn,"INSERT INTO users(email,display,password,role,store_code) VALUES(:e,:d,:p,:r,:s) ON CONFLICT(email) DO UPDATE SET display=:d,role=:r,store_code=:s",
                        {"e":email,"d":display,"p":sha256(pw),"r":role,"s":(store if role!="SuperAdmin" else None)})
                st.success("Đã lưu user"); st.rerun()
    else:
        if df.empty:
            st.info("Chưa có user."); return
        sel = st.selectbox("Chọn user", df["email"])
        row = df[df["email"]==sel].iloc[0]
        display = st.text_input("Tên hiển thị", row["display"] or "")
        role = st.selectbox("Vai trò",["User","Admin","SuperAdmin"], index=["User","Admin","SuperAdmin"].index(row["role"]))
        store = st.text_input("Mã cửa hàng", row["store_code"] or "")
        newpw = st.text_input("Mật khẩu mới (nếu đổi)", type="password")
        c1,c2 = st.columns(2)
        with c1:
            if st.button("💾 Cập nhật"):
                if newpw:
                    run_sql(conn,"UPDATE users SET display=:d,role=:r,store_code=:s,password=:p WHERE email=:e",
                            {"d":display,"r":role,"s":(store if role!="SuperAdmin" else None),"p":sha256(newpw),"e":sel})
                else:
                    run_sql(conn,"UPDATE users SET display=:d,role=:r,store_code=:s WHERE email=:e",
                            {"d":display,"r":role,"s":(store if role!="SuperAdmin" else None),"e":sel})
                st.success("Đã cập nhật"); st.rerun()
        with c2:
            if st.button("🗑️ Xóa"):
                run_sql(conn,"DELETE FROM users WHERE email=:e",{"e":sel})
                st.success("Đã xóa"); st.rerun()

# ------------------- CẬP NHẬT MAIN FRAME -------------------
def main_frame():
    conn = get_conn()
    user = require_login(conn)
    header_top(conn, user)

    menu = st.sidebar.radio("Chọn chức năng",
        ["Dashboard","Danh mục","Cửa hàng","Người dùng","Nhật ký"],
        index=0)
    st.sidebar.caption("DB: Postgres (Supabase)")

    if menu=="Dashboard": page_dashboard(conn,user)
    elif menu=="Danh mục": page_danhmuc(conn,user)
    elif menu=="Cửa hàng": page_cuahang(conn,user)
    elif menu=="Người dùng": page_nguoidung(conn,user)
    elif menu=="Nhật ký":
        if has_perm(user,"AUDIT_VIEW") or user.get("role")=="SuperAdmin":
            df = fetch_df(conn,"SELECT ts,actor,action,detail FROM syslog ORDER BY ts DESC LIMIT 200")
            st.dataframe(df,use_container_width=True)
        else:
            st.warning("Không có quyền xem nhật ký.")
