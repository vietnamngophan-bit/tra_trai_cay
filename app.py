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
    page_title="Quản Trị Trà Trái Cây Anh Gầy",
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

if __name__ == "__main__":
    router()
# =========================
# app.py — Phần 2/5 (bổ sung vào file hiện tại)
# =========================
import math

# ------ TIỆN ÍCH GIAO DIỆN NHỎ ------
def _select_row(df: pd.DataFrame, label: str, key: str, show_col="name", val_col="code"):
    opts = ["-- Chọn --"] + [f"{r[val_col]} — {r[show_col]}" for _, r in df.iterrows()]
    val = st.selectbox(label, opts, key=key)
    if val == "-- Chọn --": return None
    code = val.split(" — ",1)[0]
    return code

# ========== DASHBOARD ==========
def page_dashboard(conn: Connection, user: dict):
    st.markdown("### 📊 Dashboard")

    colA, colB, colC, colD = st.columns(4)
    # Tổng số sản phẩm / danh mục / người dùng / cửa hàng
    c_prod = fetch_df(conn, "SELECT COUNT(*) n FROM products").iloc[0]["n"]
    c_cat  = fetch_df(conn, "SELECT COUNT(*) n FROM categories").iloc[0]["n"]
    c_user = fetch_df(conn, "SELECT COUNT(*) n FROM users").iloc[0]["n"]
    c_store= fetch_df(conn, "SELECT COUNT(*) n FROM stores").iloc[0]["n"]
    colA.metric("Sản phẩm", int(c_prod))
    colB.metric("Danh mục", int(c_cat))
    colC.metric("Người dùng", int(c_user))
    colD.metric("Cửa hàng", int(c_store))

    st.markdown("#### Nhật ký gần đây")
    df_log = fetch_df(conn, "SELECT ts,actor,action,detail FROM syslog ORDER BY ts DESC LIMIT 20")
    st.dataframe(df_log, use_container_width=True, height=300)

# ========== DANH MỤC ==========
def page_danhmuc(conn: Connection, user: dict):
    st.markdown("### 📚 Danh mục")
    tab_cat, tab_prod = st.tabs(["Nhóm hàng (Category)", "Sản phẩm (Product)"])

    # ---- Category CRUD ----
    with tab_cat:
        st.markdown("#### Nhóm hàng")
        df = fetch_df(conn, "SELECT code,name FROM categories ORDER BY name")
        st.dataframe(df, use_container_width=True, height=240)

        st.markdown("##### Thêm nhóm")
        with st.form("cat_add", clear_on_submit=True):
            c = st.text_input("Mã nhóm", key="cat_code_add")
            n = st.text_input("Tên nhóm", key="cat_name_add")
            ok = st.form_submit_button("➕ Thêm")
        if ok:
            if not c or not n:
                st.warning("Nhập đủ mã & tên.")
            else:
                run_sql(conn, "INSERT INTO categories(code,name) VALUES(:c,:n) ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name",
                        {"c": c.strip(), "n": n.strip()})
                write_audit(conn, "CAT_UPSERT", c)
                st.success("Đã lưu."); st.rerun()

        st.markdown("##### Sửa/Xóa nhóm")
        code_sel = _select_row(df, "Chọn nhóm", "cat_pick")
        if code_sel:
            row = fetch_df(conn, "SELECT code,name FROM categories WHERE code=:c", {"c": code_sel}).iloc[0]
            col1, col2 = st.columns(2)
            with col1:
                new_name = st.text_input("Tên nhóm", value=row["name"], key="cat_name_edit")
                if st.button("💾 Cập nhật", key="cat_update"):
                    run_sql(conn, "UPDATE categories SET name=:n WHERE code=:c", {"n": new_name, "c": code_sel})
                    write_audit(conn,"CAT_UPDATE", code_sel)
                    st.success("Đã cập nhật."); st.rerun()
            with col2:
                if st.button("🗑️ Xóa nhóm", key="cat_delete"):
                    run_sql(conn, "DELETE FROM categories WHERE code=:c", {"c": code_sel})
                    write_audit(conn,"CAT_DELETE", code_sel)
                    st.success("Đã xóa."); st.rerun()

    # ---- Product CRUD ----
    with tab_prod:
        st.markdown("#### Sản phẩm")
        dfp = fetch_df(conn, "SELECT code,name,cat_code,uom,cups_per_kg,price_ref FROM products ORDER BY cat_code,name")
        st.dataframe(dfp, use_container_width=True, height=320)

        df_cat = fetch_df(conn, "SELECT code,name FROM categories ORDER BY name")
        st.markdown("##### Thêm sản phẩm")
        with st.form("prod_add", clear_on_submit=True):
            pcode = st.text_input("Mã SP", key="p_code_add")
            pname = st.text_input("Tên SP", key="p_name_add")
            ccode = _select_row(df_cat, "Nhóm", "p_cat_add")
            uom   = st.text_input("ĐVT", value="kg", key="p_uom_add")
            cups  = st.number_input("Cốc/kg (nếu là CỐT/MỨT)", value=0.0, step=0.1, min_value=0.0, key="p_cups_add")
            pref  = st.number_input("Giá tham chiếu", value=0.0, step=1000.0, min_value=0.0, key="p_pref_add")
            ok = st.form_submit_button("➕ Thêm")
        if ok:
            if not pcode or not pname or not ccode:
                st.warning("Nhập đủ Mã/Tên/Nhóm.")
            else:
                run_sql(conn, """
                    INSERT INTO products(code,name,cat_code,uom,cups_per_kg,price_ref)
                    VALUES(:c,:n,:cg,:u,:cups,:pr)
                    ON CONFLICT(code) DO UPDATE SET
                      name=EXCLUDED.name, cat_code=EXCLUDED.cat_code, uom=EXCLUDED.uom,
                      cups_per_kg=EXCLUDED.cups_per_kg, price_ref=EXCLUDED.price_ref
                """, {"c":pcode.strip(),"n":pname.strip(),"cg":ccode,"u":uom.strip(),"cups":cups,"pr":pref})
                write_audit(conn,"PROD_UPSERT", pcode)
                st.success("Đã lưu."); st.rerun()

        st.markdown("##### Sửa/Xóa sản phẩm")
        p_sel = _select_row(dfp.assign(show=lambda d: d["name"]), "Chọn SP", "p_pick")
        if p_sel:
            row = fetch_df(conn, "SELECT * FROM products WHERE code=:c", {"c": p_sel}).iloc[0]
            colA, colB = st.columns(2)
            with colA:
                new_name = st.text_input("Tên SP", value=row["name"], key="p_name_edit")
                ccode2   = _select_row(df_cat, "Nhóm", "p_cat_edit")
                if not ccode2: ccode2 = row["cat_code"]
                uom2   = st.text_input("ĐVT", value=row["uom"], key="p_uom_edit")
                cups2  = st.number_input("Cốc/kg", value=float(row["cups_per_kg"] or 0.0), step=0.1, min_value=0.0, key="p_cups_edit")
                pref2  = st.number_input("Giá tham chiếu", value=float(row["price_ref"] or 0.0), step=1000.0, min_value=0.0, key="p_pref_edit")
                if st.button("💾 Cập nhật", key="p_update"):
                    run_sql(conn, """
                        UPDATE products SET name=:n, cat_code=:cg, uom=:u, cups_per_kg=:cp, price_ref=:pr
                        WHERE code=:c
                    """, {"n":new_name,"cg":ccode2,"u":uom2,"cp":cups2,"pr":pref2,"c":p_sel})
                    write_audit(conn,"PROD_UPDATE", p_sel); st.success("Đã cập nhật."); st.rerun()
            with colB:
                if st.button("🗑️ Xóa sản phẩm", key="p_delete"):
                    run_sql(conn, "DELETE FROM products WHERE code=:c", {"c": p_sel})
                    write_audit(conn,"PROD_DELETE", p_sel); st.success("Đã xóa."); st.rerun()

# ========== CỬA HÀNG ==========
def page_cuahang(conn: Connection, user: dict):
    if not has_perm(user,"STORE_EDIT") and user.get("role")!="SuperAdmin":
        st.warning("Bạn không có quyền quản lý cửa hàng."); return
    st.markdown("### 🏬 Cửa hàng")
    df = fetch_df(conn, "SELECT code,name,addr,note FROM stores ORDER BY name")
    st.dataframe(df, use_container_width=True, height=280)

    st.markdown("#### Thêm cửa hàng")
    with st.form("store_add", clear_on_submit=True):
        c = st.text_input("Mã", key="st_code_add")
        n = st.text_input("Tên", key="st_name_add")
        a = st.text_input("Địa chỉ", key="st_addr_add")
        note = st.text_input("Ghi chú", key="st_note_add")
        ok = st.form_submit_button("➕ Thêm")
    if ok:
        if not c or not n:
            st.warning("Nhập đủ Mã & Tên.")
        else:
            run_sql(conn, """
                INSERT INTO stores(code,name,addr,note)
                VALUES(:c,:n,:a,:t)
                ON CONFLICT(code) DO UPDATE SET name=EXCLUDED.name, addr=EXCLUDED.addr, note=EXCLUDED.note
            """, {"c":c.strip(),"n":n.strip(),"a":a.strip(),"t":note.strip()})
            write_audit(conn,"STORE_UPSERT", c); st.success("Đã lưu."); st.rerun()

    st.markdown("#### Sửa/Xóa")
    s_sel = _select_row(df, "Chọn cửa hàng", "st_pick")
    if s_sel:
        row = fetch_df(conn, "SELECT * FROM stores WHERE code=:c", {"c": s_sel}).iloc[0]
        col1, col2 = st.columns(2)
        with col1:
            n2 = st.text_input("Tên", value=row["name"], key="st_name_edit")
            a2 = st.text_input("Địa chỉ", value=row["addr"] or "", key="st_addr_edit")
            note2 = st.text_input("Ghi chú", value=row["note"] or "", key="st_note_edit")
            if st.button("💾 Cập nhật", key="st_update"):
                run_sql(conn, "UPDATE stores SET name=:n, addr=:a, note=:t WHERE code=:c",
                       {"n":n2,"a":a2,"t":note2,"c":s_sel})
                write_audit(conn,"STORE_UPDATE", s_sel); st.success("Đã cập nhật."); st.rerun()
        with col2:
            if st.button("🗑️ Xóa cửa hàng", key="st_delete"):
                run_sql(conn, "DELETE FROM stores WHERE code=:c", {"c": s_sel})
                write_audit(conn,"STORE_DELETE", s_sel); st.success("Đã xóa."); st.rerun()

# ========== NGƯỜI DÙNG ==========
def page_users(conn: Connection, user: dict):
    if not has_perm(user,"USER_EDIT") and user.get("role")!="SuperAdmin":
        st.warning("Bạn không có quyền quản lý người dùng."); return
    st.markdown("### 👥 Người dùng")

    df = fetch_df(conn, "SELECT email,display,role,store_code,perms,created_at FROM users ORDER BY created_at DESC")
    st.dataframe(df, use_container_width=True, height=320)

    df_store = fetch_df(conn, "SELECT code,name FROM stores ORDER BY name")

    st.markdown("#### Thêm người dùng")
    with st.form("u_add", clear_on_submit=True):
        email = st.text_input("Email", key="u_email_add")
        disp  = st.text_input("Tên hiển thị", key="u_disp_add")
        role  = st.selectbox("Vai trò", ["User","Admin","SuperAdmin"], index=0, key="u_role_add")
        store = _select_row(df_store, "Cửa hàng (tùy chọn)", "u_store_add")
        perms = st.multiselect("Quyền", ["CAT_EDIT","PROD_EDIT","INV_EDIT","USER_EDIT","STORE_EDIT","REPORT_VIEW","AUDIT_VIEW"], key="u_perms_add")
        pw    = st.text_input("Mật khẩu", type="password", key="u_pw_add")
        ok = st.form_submit_button("➕ Tạo tài khoản")
    if ok:
        if not email or not pw:
            st.warning("Cần Email & Mật khẩu.")
        else:
            run_sql(conn, """
                INSERT INTO users(email,display,password,role,store_code,perms)
                VALUES(:e,:d,:p,:r,:s,:m)
                ON CONFLICT(email) DO UPDATE SET
                  display=EXCLUDED.display, role=EXCLUDED.role, store_code=EXCLUDED.store_code, perms=EXCLUDED.perms
            """, {"e":email.strip(),"d":disp.strip() or email.strip(),"p":sha256(pw),
                  "r":role,"s":store,"m":",".join(perms)})
            write_audit(conn,"USER_UPSERT", email); st.success("Đã lưu."); st.rerun()

    st.markdown("#### Sửa/Xóa/Đặt lại mật khẩu")
    u_sel = _select_row(df.assign(show=lambda d: d["display"]), "Chọn người dùng", "u_pick")
    if u_sel:
        row = fetch_df(conn, "SELECT * FROM users WHERE email=:e", {"e": u_sel}).iloc[0]
        colA, colB = st.columns(2)
        with colA:
            disp2 = st.text_input("Tên hiển thị", value=row["display"] or "", key="u_disp_edit")
            role2 = st.selectbox("Vai trò", ["User","Admin","SuperAdmin"],
                                 index=["User","Admin","SuperAdmin"].index(row["role"] or "User"),
                                 key="u_role_edit")
            store2 = _select_row(df_store, "Cửa hàng (tùy chọn)", "u_store_edit")
            if not store2: store2 = row["store_code"]
            perms2 = st.multiselect("Quyền", ["CAT_EDIT","PROD_EDIT","INV_EDIT","USER_EDIT","STORE_EDIT","REPORT_VIEW","AUDIT_VIEW"],
                                    default=(row["perms"] or "").split(",") if row["perms"] else [], key="u_perms_edit")
            if st.button("💾 Cập nhật", key="u_update"):
                run_sql(conn, """
                    UPDATE users SET display=:d, role=:r, store_code=:s, perms=:m WHERE email=:e
                """, {"d":disp2,"r":role2,"s":store2,"m":",".join(perms2),"e":u_sel})
                write_audit(conn,"USER_UPDATE", u_sel); st.success("Đã cập nhật."); st.rerun()
        with colB:
            newpw = st.text_input("Mật khẩu mới", type="password", key="u_pw_reset")
            if st.button("🔑 Đặt lại mật khẩu", key="u_reset"):
                if not newpw: st.warning("Nhập mật khẩu mới.")
                else:
                    run_sql(conn, "UPDATE users SET password=:p WHERE email=:e", {"p":sha256(newpw), "e":u_sel})
                    write_audit(conn,"USER_RESET_PW", u_sel); st.success("Đã đặt lại mật khẩu.")
            st.markdown("")
            if st.button("🗑️ Xóa tài khoản", key="u_delete"):
                run_sql(conn, "DELETE FROM users WHERE email=:e", {"e": u_sel})
                write_audit(conn,"USER_DELETE", u_sel); st.success("Đã xóa."); st.rerun()

# ---------- GHÉP VÀO ROUTER (ghi đè đoạn placeholder) ----------
def router():
    conn = get_conn()
    user = require_login(conn)
    header_top(conn, user)

    st.sidebar.markdown("### 🏪 Cửa hàng")
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

    if menu == "Dashboard":
        page_dashboard(conn, user)
    elif menu == "Danh mục":
        page_danhmuc(conn, user)
    elif menu == "Cửa hàng":
        page_cuahang(conn, user)
    elif menu == "Người dùng":
        page_users(conn, user)
    elif menu == "Nhật ký":
        page_nhatky(conn, user)

if __name__ == "__main__":
    router()
