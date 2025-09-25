# ============================================================
# app.py — Phần 1+2: Hạ tầng + CRUD Danh mục, Sản phẩm, Cửa hàng, Người dùng
# ============================================================
import os, re, hashlib
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

def has_perm(user: dict, perm: str) -> bool:
    if not user: return False
    if user.get("role")=="SuperAdmin": return True
    return perm in (user.get("perms") or "").split(",")

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

# ------------------- DASHBOARD -------------------
def page_dashboard(conn, user):
    st.subheader("📊 Dashboard")
    col1,col2,col3 = st.columns(3)
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
def page_danhmuc(conn,user):
    st.subheader("📂 Quản lý Danh mục")
    if not has_perm(user,"CAT_EDIT"):
        st.warning("Không có quyền."); return
    df = fetch_df(conn,"SELECT code,name FROM categories ORDER BY code")
    st.dataframe(df,use_container_width=True)
    with st.form("form_cat", clear_on_submit=True):
        code = st.text_input("Mã DM").upper()
        name = st.text_input("Tên DM")
        act  = st.radio("Hành động",["Thêm","Sửa","Xóa"])
        ok = st.form_submit_button("Thực hiện")
    if ok and code:
        if act=="Thêm":
            run_sql(conn,"INSERT INTO categories(code,name) VALUES(:c,:n) "
                         "ON CONFLICT(code) DO UPDATE SET name=:n",{"c":code,"n":name})
        elif act=="Sửa":
            run_sql(conn,"UPDATE categories SET name=:n WHERE code=:c",{"c":code,"n":name})
        elif act=="Xóa":
            run_sql(conn,"DELETE FROM categories WHERE code=:c",{"c":code})
        st.rerun()

# ------------------- SẢN PHẨM -------------------
def page_sanpham(conn,user):
    st.subheader("📦 Quản lý Sản phẩm")
    if not has_perm(user,"PROD_EDIT"):
        st.warning("Không có quyền."); return
    df = fetch_df(conn,"SELECT code,name,cat_code,uom,cups_per_kg,price_ref FROM products ORDER BY code")
    st.dataframe(df,use_container_width=True)
    with st.form("form_prod", clear_on_submit=True):
        code = st.text_input("Mã SP").upper()
        name = st.text_input("Tên SP")
        cat  = st.text_input("Danh mục").upper()
        uom  = st.text_input("ĐVT")
        cups = st.number_input("Số cốc/kg",0.0,100.0,0.0,1.0)
        price= st.number_input("Giá tham chiếu",0.0,1e9,0.0,1000.0)
        act  = st.radio("Hành động",["Thêm","Sửa","Xóa"])
        ok = st.form_submit_button("Thực hiện")
    if ok and code:
        if act=="Thêm":
            run_sql(conn,"INSERT INTO products(code,name,cat_code,uom,cups_per_kg,price_ref) "
                         "VALUES(:c,:n,:cat,:u,:cups,:p) "
                         "ON CONFLICT(code) DO UPDATE SET name=:n,cat_code=:cat,uom=:u,cups_per_kg=:cups,price_ref=:p",
                         {"c":code,"n":name,"cat":cat,"u":uom,"cups":cups,"p":price})
        elif act=="Sửa":
            run_sql(conn,"UPDATE products SET name=:n,cat_code=:cat,uom=:u,cups_per_kg=:cups,price_ref=:p WHERE code=:c",
                    {"c":code,"n":name,"cat":cat,"u":uom,"cups":cups,"p":price})
        elif act=="Xóa":
            run_sql(conn,"DELETE FROM products WHERE code=:c",{"c":code})
        st.rerun()

# ------------------- CỬA HÀNG -------------------
def page_cuahang(conn,user):
    st.subheader("🏬 Quản lý Cửa hàng")
    if not has_perm(user,"STORE_EDIT"):
        st.warning("Không có quyền."); return
    df = fetch_df(conn,"SELECT code,name,addr,note FROM stores ORDER BY code")
    st.dataframe(df,use_container_width=True)
    with st.form("form_store", clear_on_submit=True):
        code= st.text_input("Mã cửa hàng").upper()
        name= st.text_input("Tên cửa hàng")
        addr= st.text_input("Địa chỉ")
        note= st.text_area("Ghi chú")
        act = st.radio("Hành động",["Thêm","Sửa","Xóa"])
        ok= st.form_submit_button("Thực hiện")
    if ok and code:
        if act=="Thêm":
            run_sql(conn,"INSERT INTO stores(code,name,addr,note) VALUES(:c,:n,:a,:t) "
                         "ON CONFLICT(code) DO UPDATE SET name=:n,addr=:a,note=:t",
                         {"c":code,"n":name,"a":addr,"t":note})
        elif act=="Sửa":
            run_sql(conn,"UPDATE stores SET name=:n,addr=:a,note=:t WHERE code=:c",
                    {"c":code,"n":name,"a":addr,"t":note})
        elif act=="Xóa":
            run_sql(conn,"DELETE FROM stores WHERE code=:c",{"c":code})
        st.rerun()

# ------------------- NGƯỜI DÙNG -------------------
def page_nguoidung(conn,user):
    st.subheader("👥 Quản lý Người dùng")
    if not has_perm(user,"USER_EDIT"):
        st.warning("Không có quyền."); return
    df = fetch_df(conn,"SELECT email,display,role,store_code,perms FROM users ORDER BY email")
    st.dataframe(df,use_container_width=True)
    with st.form("form_user", clear_on_submit=True):
        email= st.text_input("Email").lower()
        display= st.text_input("Tên hiển thị")
        pw= st.text_input("Mật khẩu (bỏ trống nếu không đổi)", type="password")
        role= st.selectbox("Vai trò",["User","Admin","SuperAdmin"])
        store= st.text_input("Mã cửa hàng").upper()
        perms= st.text_area("Quyền (phân cách bằng dấu phẩy)")
        act= st.radio("Hành động",["Thêm","Sửa","Xóa"])
        ok= st.form_submit_button("Thực hiện")
    if ok and email:
        if act=="Thêm":
            run_sql(conn,"INSERT INTO users(email,display,password,role,store_code,perms) "
                         "VALUES(:e,:d,:p,:r,:s,:m)",
                         {"e":email,"d":display,"p":sha256(pw) if pw else "","r":role,"s":store,"m":perms})
        elif act=="Sửa":
            if pw:
                run_sql(conn,"UPDATE users SET display=:d,password=:p,role=:r,store_code=:s,perms=:m WHERE email=:e",
                        {"d":display,"p":sha256(pw),"r":role,"s":store,"m":perms,"e":email})
            else:
                run_sql(conn,"UPDATE users SET display=:d,role=:r,store_code=:s,perms=:m WHERE email=:e",
                        {"d":display,"r":role,"s":store,"m":perms,"e":email})
        elif act=="Xóa":
            run_sql(conn,"DELETE FROM users WHERE email=:e",{"e":email})
        st.rerun()

# ------------------- ROUTER -------------------
def router():
    conn = get_conn()
    user = require_login(conn)
    header_top(conn,user)

    menu = st.sidebar.radio("Chọn chức năng",
        ["Dashboard","Danh mục","Sản phẩm","Cửa hàng","Người dùng","Nhật ký"], index=0)

    if menu=="Dashboard":
        page_dashboard(conn,user)
    elif menu=="Danh mục":
        page_danhmuc(conn,user)
    elif menu=="Sản phẩm":
        page_sanpham(conn,user)
    elif menu=="Cửa hàng":
        page_cuahang(conn,user)
    elif menu=="Người dùng":
        page_nguoidung(conn,user)
    elif menu=="Nhật ký":
        if has_perm(user,"AUDIT_VIEW") or user.get("role")=="SuperAdmin":
            df = fetch_df(conn,"SELECT ts,actor,action,detail FROM syslog ORDER BY ts DESC LIMIT 200")
            st.dataframe(df,use_container_width=True)
        else:
            st.warning("Không có quyền xem nhật ký.")

# ------------------- ENTRY -------------------
if __name__=="__main__":
    router()
