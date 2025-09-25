# =========================
# Fruit Tea ERP v5  (Postgres only) — P1+P2
# =========================
import os, re, json, hashlib
from datetime import datetime, date, timedelta
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

# ---------- PAGE CONFIG ----------
st.set_page_config(page_title="Hệ Thống Quản Lý Sản Xuất", page_icon="🍹",
                   layout="wide", initial_sidebar_state="expanded")

# ---------- POSTGRES ----------
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
        st.error("❌ Chưa cấu hình DATABASE_URL."); st.stop()
    if _ENGINE is None:
        _ENGINE = create_engine(_normalize_pg_url(pg_url),
                                pool_pre_ping=True, future=True)
    return _ENGINE.connect()

# ---------- SQL HELPERS ----------
def _qmark_to_named(sql: str, params):
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

# ---------- AUTH / AUDIT ----------
def sha256(s: str) -> str: return hashlib.sha256(s.encode("utf-8")).hexdigest()
def has_perm(user: dict, perm: str) -> bool:
    if not user: return False
    if user.get("role") == "SuperAdmin": return True
    return perm in (user.get("perms") or "").split(",")

def write_audit(conn: Connection, action: str, detail: str = ""):
    try:
        run_sql(conn, "INSERT INTO syslog(ts,actor,action,detail) VALUES(NOW(),:u,:a,:d)",
                {"u": st.session_state.get("user",{}).get("email","anonymous"),
                 "a": action, "d": (detail or "")[:1000]})
    except: pass

def login_form(conn: Connection):
    st.markdown("### 🔐 Đăng nhập hệ thống")
    email = st.text_input("Email", key="login_email")
    pw    = st.text_input("Mật khẩu", type="password", key="login_pw")
    if st.button("Đăng nhập", type="primary", use_container_width=True):
        df = fetch_df(conn, "SELECT email,display,password,role,store_code,perms FROM users WHERE email=:e",
                      {"e": email})
        if df.empty or df.iloc[0]["password"] != sha256(pw):
            st.error("Sai tài khoản hoặc mật khẩu."); return
        row = df.iloc[0]
        st.session_state["user"] = {
            "email": row["email"],
            "display": row["display"] or row["email"],
            "role": row["role"] or "User",
            "perms": row["perms"] or "",
            "store": row["store_code"] or ""
        }
        if row["store_code"]: st.session_state["store"] = row["store_code"]
        write_audit(conn, "LOGIN", row["email"])
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

# ---------- HEADER ----------
def header_top(conn: Connection, user: dict):
    c1, c2 = st.columns([0.8,0.2])
    with c1:
        st.markdown("## 🍹 Quản Trị Trà Trái Cây Anh Gầy")
        st.caption("Kết nối: Postgres (Supabase)")
    with c2:
        with st.popover(f"👤 {user.get('display','')}"):
            st.caption(user.get("email",""))
            st.markdown("---")
            st.markdown("**Đổi mật khẩu**")
            with st.form("form_pwd", clear_on_submit=True):
                old = st.text_input("Mật khẩu cũ", type="password", key="oldpw")
                new1= st.text_input("Mật khẩu mới", type="password", key="newpw1")
                new2= st.text_input("Xác nhận", type="password", key="newpw2")
                ok  = st.form_submit_button("Cập nhật")
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
                    st.success("Đã đổi mật khẩu, đăng nhập lại."); logout(conn)
            st.markdown("---")
            if st.button("Đăng xuất", use_container_width=True): logout(conn)

# ---------- TIỆN ÍCH UI ----------
def _select_row(df: pd.DataFrame, label: str, key: str, show_col="name", val_col="code"):
    opts = ["-- Chọn --"] + [f"{r[val_col]} — {r[show_col]}" for _, r in df.iterrows()]
    val = st.selectbox(label, opts, key=key)
    if val == "-- Chọn --": return None
    return val.split(" — ",1)[0]

# ---------- PAGES ----------
def page_nhatky(conn: Connection, user: dict):
    if not has_perm(user, "AUDIT_VIEW") and user.get("role")!="SuperAdmin":
        st.warning("Bạn không có quyền xem nhật ký."); return
    st.markdown("### 🧾 Nhật ký")
    df = fetch_df(conn, "SELECT ts,actor,action,detail FROM syslog ORDER BY ts DESC LIMIT 300")
    st.dataframe(df, use_container_width=True)

def page_dashboard(conn: Connection, user: dict):
    st.markdown("### 📊 Dashboard")
    c1,c2,c3,c4 = st.columns(4)
    c_prod = fetch_df(conn,"SELECT COUNT(*) n FROM products").iloc[0]["n"]
    c_cat  = fetch_df(conn,"SELECT COUNT(*) n FROM categories").iloc[0]["n"]
    c_user = fetch_df(conn,"SELECT COUNT(*) n FROM users").iloc[0]["n"]
    c_store= fetch_df(conn,"SELECT COUNT(*) n FROM stores").iloc[0]["n"]
    c1.metric("Sản phẩm", int(c_prod)); c2.metric("Danh mục", int(c_cat))
    c3.metric("Người dùng", int(c_user)); c4.metric("Cửa hàng", int(c_store))
    st.markdown("#### Nhật ký gần đây")
    df_log = fetch_df(conn,"SELECT ts,actor,action,detail FROM syslog ORDER BY ts DESC LIMIT 20")
    st.dataframe(df_log, use_container_width=True, height=300)

def page_danhmuc(conn: Connection, user: dict):
    st.markdown("### 📚 Danh mục")
    tab_cat, tab_prod = st.tabs(["Nhóm hàng", "Sản phẩm"])

    # Category
    with tab_cat:
        df = fetch_df(conn,"SELECT code,name FROM categories ORDER BY name")
        st.dataframe(df, use_container_width=True, height=240)
        st.markdown("##### Thêm nhóm")
        with st.form("cat_add", clear_on_submit=True):
            c = st.text_input("Mã nhóm", key="cat_code_add")
            n = st.text_input("Tên nhóm", key="cat_name_add")
            ok = st.form_submit_button("➕ Thêm")
        if ok and c and n:
            run_sql(conn, """
                INSERT INTO categories(code,name) VALUES(:c,:n)
                ON CONFLICT(code) DO UPDATE SET name=EXCLUDED.name
            """, {"c":c.strip(),"n":n.strip()})
            write_audit(conn,"CAT_UPSERT",c); st.success("Đã lưu."); st.rerun()

        st.markdown("##### Sửa/Xóa nhóm")
        pick = _select_row(df,"Chọn nhóm","cat_pick")
        if pick:
            row = fetch_df(conn,"SELECT code,name FROM categories WHERE code=:c",{"c":pick}).iloc[0]
            col1,col2 = st.columns(2)
            with col1:
                n2 = st.text_input("Tên nhóm", value=row["name"], key="cat_name_edit")
                if st.button("💾 Cập nhật", key="cat_update"):
                    run_sql(conn,"UPDATE categories SET name=:n WHERE code=:c",{"n":n2,"c":pick})
                    write_audit(conn,"CAT_UPDATE",pick); st.success("Đã cập nhật."); st.rerun()
            with col2:
                if st.button("🗑️ Xóa nhóm", key="cat_delete"):
                    run_sql(conn,"DELETE FROM categories WHERE code=:c",{"c":pick})
                    write_audit(conn,"CAT_DELETE",pick); st.success("Đã xóa."); st.rerun()

    # Product
    with tab_prod:
        dfp = fetch_df(conn,"SELECT code,name,cat_code,uom,cups_per_kg,price_ref FROM products ORDER BY cat_code,name")
        st.dataframe(dfp, use_container_width=True, height=320)
        df_cat = fetch_df(conn,"SELECT code,name FROM categories ORDER BY name")

        st.markdown("##### Thêm sản phẩm")
        with st.form("prod_add", clear_on_submit=True):
            pcode = st.text_input("Mã SP", key="p_code_add")
            pname = st.text_input("Tên SP", key="p_name_add")
            ccode = _select_row(df_cat, "Nhóm", "p_cat_add")
            uom   = st.text_input("ĐVT", value="kg", key="p_uom_add")
            cups  = st.number_input("Cốc/kg (CỐT/MỨT)", value=0.0, step=0.1, min_value=0.0, key="p_cups_add")
            pref  = st.number_input("Giá tham chiếu", value=0.0, step=1000.0, min_value=0.0, key="p_pref_add")
            ok = st.form_submit_button("➕ Thêm")
        if ok and pcode and pname and ccode:
            run_sql(conn, """
                INSERT INTO products(code,name,cat_code,uom,cups_per_kg,price_ref)
                VALUES(:c,:n,:cg,:u,:cups,:pr)
                ON CONFLICT(code) DO UPDATE SET
                  name=EXCLUDED.name, cat_code=EXCLUDED.cat_code, uom=EXCLUDED.uom,
                  cups_per_kg=EXCLUDED.cups_per_kg, price_ref=EXCLUDED.price_ref
            """, {"c":pcode.strip(),"n":pname.strip(),"cg":ccode,"u":uom.strip(),"cups":cups,"pr":pref})
            write_audit(conn,"PROD_UPSERT",pcode); st.success("Đã lưu."); st.rerun()

        st.markdown("##### Sửa/Xóa sản phẩm")
        p_sel = _select_row(dfp.assign(show=lambda d:d["name"]), "Chọn SP", "p_pick")
        if p_sel:
            row = fetch_df(conn,"SELECT * FROM products WHERE code=:c",{"c":p_sel}).iloc[0]
            colA,colB = st.columns(2)
            with colA:
                new_name = st.text_input("Tên SP", value=row["name"], key="p_name_edit")
                ccode2   = _select_row(df_cat,"Nhóm","p_cat_edit") or row["cat_code"]
                uom2 = st.text_input("ĐVT", value=row["uom"], key="p_uom_edit")
                cups2= st.number_input("Cốc/kg", value=float(row["cups_per_kg"] or 0.0), step=0.1, min_value=0.0, key="p_cups_edit")
                pref2= st.number_input("Giá tham chiếu", value=float(row["price_ref"] or 0.0), step=1000.0, min_value=0.0, key="p_pref_edit")
                if st.button("💾 Cập nhật", key="p_update"):
                    run_sql(conn, """
                        UPDATE products SET name=:n, cat_code=:cg, uom=:u,
                          cups_per_kg=:cp, price_ref=:pr WHERE code=:c
                    """, {"n":new_name,"cg":ccode2,"u":uom2,"cp":cups2,"pr":pref2,"c":p_sel})
                    write_audit(conn,"PROD_UPDATE",p_sel); st.success("Đã cập nhật."); st.rerun()
            with colB:
                if st.button("🗑️ Xóa sản phẩm", key="p_delete"):
                    run_sql(conn,"DELETE FROM products WHERE code=:c",{"c":p_sel})
                    write_audit(conn,"PROD_DELETE",p_sel); st.success("Đã xóa."); st.rerun()

def page_cuahang(conn: Connection, user: dict):
    if not has_perm(user,"STORE_EDIT") and user.get("role")!="SuperAdmin":
        st.warning("Bạn không có quyền quản lý cửa hàng."); return
    st.markdown("### 🏬 Cửa hàng")
    df = fetch_df(conn,"SELECT code,name,addr,note FROM stores ORDER BY name")
    st.dataframe(df, use_container_width=True, height=280)

    st.markdown("#### Thêm cửa hàng")
    with st.form("store_add", clear_on_submit=True):
        c = st.text_input("Mã", key="st_code_add")
        n = st.text_input("Tên", key="st_name_add")
        a = st.text_input("Địa chỉ", key="st_addr_add")
        t = st.text_input("Ghi chú", key="st_note_add")
        ok = st.form_submit_button("➕ Thêm")
    if ok and c and n:
        run_sql(conn, """
            INSERT INTO stores(code,name,addr,note)
            VALUES(:c,:n,:a,:t)
            ON CONFLICT(code) DO UPDATE SET name=EXCLUDED.name, addr=EXCLUDED.addr, note=EXCLUDED.note
        """, {"c":c.strip(),"n":n.strip(),"a":a.strip(),"t":t.strip()})
        write_audit(conn,"STORE_UPSERT",c); st.success("Đã lưu."); st.rerun()

    st.markdown("#### Sửa/Xóa")
    pick = _select_row(df,"Chọn cửa hàng","st_pick")
    if pick:
        row = fetch_df(conn,"SELECT * FROM stores WHERE code=:c",{"c":pick}).iloc[0]
        col1,col2 = st.columns(2)
        with col1:
            n2 = st.text_input("Tên", value=row["name"], key="st_name_edit")
            a2 = st.text_input("Địa chỉ", value=row["addr"] or "", key="st_addr_edit")
            t2 = st.text_input("Ghi chú", value=row["note"] or "", key="st_note_edit")
            if st.button("💾 Cập nhật", key="st_update"):
                run_sql(conn,"UPDATE stores SET name=:n, addr=:a, note=:t WHERE code=:c",
                       {"n":n2,"a":a2,"t":t2,"c":pick})
                write_audit(conn,"STORE_UPDATE",pick); st.success("Đã cập nhật."); st.rerun()
        with col2:
            if st.button("🗑️ Xóa cửa hàng", key="st_delete"):
                run_sql(conn,"DELETE FROM stores WHERE code=:c",{"c":pick})
                write_audit(conn,"STORE_DELETE",pick); st.success("Đã xóa."); st.rerun()

def page_users(conn: Connection, user: dict):
    if not has_perm(user,"USER_EDIT") and user.get("role")!="SuperAdmin":
        st.warning("Bạn không có quyền quản lý người dùng."); return
    st.markdown("### 👥 Người dùng")
    df = fetch_df(conn,"SELECT email,display,role,store_code,perms,created_at FROM users ORDER BY created_at DESC")
    st.dataframe(df, use_container_width=True, height=320)
    df_store = fetch_df(conn,"SELECT code,name FROM stores ORDER BY name")

    st.markdown("#### Thêm người dùng")
    with st.form("u_add", clear_on_submit=True):
        email = st.text_input("Email", key="u_email_add")
        disp  = st.text_input("Tên hiển thị", key="u_disp_add")
        role  = st.selectbox("Vai trò", ["User","Admin","SuperAdmin"], index=0, key="u_role_add")
        store = _select_row(df_store, "Cửa hàng (tùy chọn)", "u_store_add")
        perms = st.multiselect("Quyền",
            ["CAT_EDIT","PROD_EDIT","INV_EDIT","USER_EDIT","STORE_EDIT","REPORT_VIEW","AUDIT_VIEW"],
            key="u_perms_add")
        pw    = st.text_input("Mật khẩu", type="password", key="u_pw_add")
        ok = st.form_submit_button("➕ Tạo tài khoản")
    if ok and email and pw:
        run_sql(conn, """
            INSERT INTO users(email,display,password,role,store_code,perms)
            VALUES(:e,:d,:p,:r,:s,:m)
            ON CONFLICT(email) DO UPDATE SET
              display=EXCLUDED.display, role=EXCLUDED.role, store_code=EXCLUDED.store_code, perms=EXCLUDED.perms
        """, {"e":email.strip(),"d":disp.strip() or email.strip(),"p":sha256(pw),
              "r":role,"s":store,"m":",".join(perms)})
        write_audit(conn,"USER_UPSERT",email); st.success("Đã lưu."); st.rerun()

    st.markdown("#### Sửa/Xóa/Đổi mật khẩu")
    pick = _select_row(df, "Chọn người dùng", "u_pick", show_col="display", val_col="email")
    if pick:
        row = fetch_df(conn,"SELECT * FROM users WHERE email=:e",{"e":pick}).iloc[0]
        colA,colB = st.columns(2)
        with colA:
            disp2 = st.text_input("Tên hiển thị", value=row["display"] or "", key="u_disp_edit")
            role2 = st.selectbox("Vai trò", ["User","Admin","SuperAdmin"],
                                 index=["User","Admin","SuperAdmin"].index(row["role"] or "User"),
                                 key="u_role_edit")
            store2= _select_row(df_store,"Cửa hàng (tùy chọn)","u_store_edit") or row["store_code"]
            perms2= st.multiselect("Quyền",
                    ["CAT_EDIT","PROD_EDIT","INV_EDIT","USER_EDIT","STORE_EDIT","REPORT_VIEW","AUDIT_VIEW"],
                    default=(row["perms"] or "").split(",") if row["perms"] else [], key="u_perms_edit")
            if st.button("💾 Cập nhật", key="u_update"):
                run_sql(conn,"UPDATE users SET display=:d, role=:r, store_code=:s, perms=:m WHERE email=:e",
                       {"d":disp2,"r":role2,"s":store2,"m":",".join(perms2),"e":pick})
                write_audit(conn,"USER_UPDATE",pick); st.success("Đã cập nhật."); st.rerun()
        with colB:
            newpw = st.text_input("Mật khẩu mới", type="password", key="u_pw_reset")
            if st.button("🔑 Đặt lại mật khẩu", key="u_reset"):
                if not newpw: st.warning("Nhập mật khẩu mới.")
                else:
                    run_sql(conn,"UPDATE users SET password=:p WHERE email=:e",
                           {"p":sha256(newpw),"e":pick})
                    write_audit(conn,"USER_RESET_PW",pick); st.success("Đã đặt lại mật khẩu.")
            if st.button("🗑️ Xóa tài khoản", key="u_delete"):
                run_sql(conn,"DELETE FROM users WHERE email=:e",{"e":pick})
                write_audit(conn,"USER_DELETE",pick); st.success("Đã xóa."); st.rerun()

# ---------- ROUTER DUY NHẤT ----------
def router():
    conn = get_conn()
    user = require_login(conn)
    header_top(conn, user)

    st.sidebar.markdown("### 🏪 Cửa hàng")
    df_st = fetch_df(conn,"SELECT code,name FROM stores ORDER BY name")
    opts = ["(Tất cả)"] + df_st["code"].tolist()
    st.sidebar.selectbox("Đang thao tác tại", opts,
                         index=(opts.index(user.get("store")) if user.get("store") in opts else 0),
                         key="store")
    st.sidebar.divider()

    st.sidebar.markdown("### 📌 Chức năng")
    menu = st.sidebar.radio("", ["Dashboard","Danh mục","Cửa hàng","Người dùng","Nhật ký"],
                            index=0, label_visibility="collapsed")

    if menu == "Dashboard":   page_dashboard(conn, user)
    elif menu == "Danh mục":  page_danhmuc(conn, user)
    elif menu == "Cửa hàng":  page_cuahang(conn, user)
    elif menu == "Người dùng":page_users(conn, user)
    elif menu == "Nhật ký":   page_nhatky(conn, user)

if __name__ == "__main__":
    router()
# =========================
# PHẦN 3/5 — KHO & SẢN XUẤT
# =========================

# ---------- helpers tồn kho ----------
def _stock_of(conn, store: str, pcode: str, to_dt: date | None = None) -> float:
    """
    Tồn & cốc đến ngày to_dt (None => tới hiện tại).
    """
    if to_dt is None:
        to_dt = date.today()
    sql = """
        SELECT COALESCE(SUM(CASE WHEN kind='IN' THEN qty ELSE -qty END),0) AS qty
        FROM inventory_ledger
        WHERE store=:s AND pcode=:p AND ts < (:d::date + INTERVAL '1 day')
    """
    x = fetch_df(conn, sql, {"s": store, "p": pcode, "d": to_dt.isoformat()})
    return float(x.iloc[0]["qty"] if not x.empty else 0.0)

def _avg_cost(conn, store: str, pcode: str, to_dt: date | None = None) -> float:
    """
    Giá vốn bình quân di động tới ngày to_dt (None => hiện tại).
    """
    if to_dt is None:
        to_dt = date.today()
    sql = """
      SELECT ts, kind, qty, COALESCE(price_in,0) AS price_in
      FROM inventory_ledger
      WHERE store=:s AND pcode=:p AND ts < (:d::date + INTERVAL '1 day')
      ORDER BY ts
    """
    df = fetch_df(conn, sql, {"s": store, "p": pcode, "d": to_dt.isoformat()})
    stock = 0.0
    cost  = 0.0
    for _, r in df.iterrows():
        q = float(r["qty"] or 0)
        if r["kind"] == "IN" and q > 0:
            p = float(r["price_in"] or 0)
            total = stock * cost + q * p
            stock += q
            cost  = (total/stock) if stock>0 else 0.0
        else:
            stock -= q
            if stock < 0: stock = 0
    return round(cost, 2)

def _cups_for_row(row) -> float:
    # Tính số cốc nếu là CỐT hoặc MỨT
    cat = (row.get("cat_code") or "").upper()
    if cat in ("COT", "MUT"):
        return float(row.get("qty", 0)) * float(row.get("cups_per_kg", 0))
    return 0.0

def _product_lookup(conn) -> pd.DataFrame:
    return fetch_df(conn, "SELECT code,name,cat_code,uom,cups_per_kg FROM products ORDER BY code")

def _lot_new_id(conn, prefix="BATCH") -> str:
    d = datetime.now().strftime("%y%m%d")
    base = f"{prefix}{d}-"
    sql = "SELECT COUNT(*) AS n FROM production_batches WHERE batch_id LIKE :b"
    n = int(fetch_df(conn, sql, {"b": base+"%"}).iloc[0]["n"])
    return f"{base}{n+1:03d}"

# ---------- trang KHO ----------
def page_kho(conn: Connection, user: dict):
    st.markdown("## 📦 Kho (Nhập/Xuất/Kiểm kê) + Tồn số cốc")

    if not has_perm(user, "INV_EDIT"):
        st.warning("Bạn không có quyền thao tác kho.")
        return

    store = st.session_state.get("store") or (fetch_df(conn, "SELECT code FROM stores LIMIT 1").iloc[0]["code"])
    st.caption(f"Cửa hàng: **{store}**")

    tab_in, tab_out, tab_stock, tab_count = st.tabs(["Nhập", "Xuất", "Tồn kho", "Kiểm kê nâng cao"])

    # ====== NHẬP ======
    with tab_in:
        st.subheader("Nhập kho")
        dfp = _product_lookup(conn)
        # dropdown có filter
        opt = st.selectbox(
            "Chọn sản phẩm nhập",
            dfp["code"] + " — " + dfp["name"],
            index=None,
            placeholder="Gõ để tìm…",
            key="in_pick"
        )
        c1, c2, c3 = st.columns([0.25,0.25,0.5])
        with c1:
            qty = st.number_input("Số lượng", value=0.0, step=0.1, min_value=0.0, key="in_qty")
        with c2:
            price = st.number_input("Đơn giá nhập (VND/DVT)", value=0.0, step=100.0, min_value=0.0, key="in_price")
        with c3:
            reason = st.text_input("Lý do/ghi chú", key="in_note")

        if st.button("➕ Xác nhận nhập", type="primary", disabled=(not opt or qty<=0)):
            pcode = opt.split(" — ",1)[0]
            run_sql(conn, """
                INSERT INTO inventory_ledger(store,ts,kind,reason,pcode,qty,price_in,lot_id,note)
                VALUES (:s, NOW(),'IN','NHAP_TAY', :p, :q, :pr, NULL, :r)
            """, {"s":store,"p":pcode,"q":qty,"pr":price,"r":reason})
            write_audit(conn,"INV_IN", f"{store}:{pcode}:{qty}")
            st.success("Đã nhập.")
            st.rerun()

    # ====== XUẤT ======
    with tab_out:
        st.subheader("Xuất kho")
        dfp = _product_lookup(conn)
        opt = st.selectbox(
            "Chọn sản phẩm xuất",
            dfp["code"] + " — " + dfp["name"],
            index=None, placeholder="Gõ để tìm…", key="out_pick"
        )
        c1, c2 = st.columns([0.25,0.75])
        with c1:
            qty = st.number_input("Số lượng", value=0.0, step=0.1, min_value=0.0, key="out_qty")
        with c2:
            reason = st.selectbox("Lý do xuất",
                ["BÁN_HÀNG","HỦY_HAO_HỤT","ĐIỀU_CHUYỂN","SẢN_XUẤT"], index=0)

        # chặn xuất âm
        cur_stock = 0.0
        if opt:
            pcode = opt.split(" — ",1)[0]
            cur_stock = _stock_of(conn, store, pcode)
            st.caption(f"Tồn hiện tại: **{cur_stock:.2f}** {dfp.loc[dfp.code==pcode,'uom'].iloc[0]}")

        if st.button("➖ Xác nhận xuất", type="primary", disabled=(not opt or qty<=0)):
            pcode = opt.split(" — ",1)[0]
            if qty > cur_stock + 1e-9:
                st.error("Không cho phép xuất âm tồn.")
            else:
                run_sql(conn, """
                    INSERT INTO inventory_ledger(store,ts,kind,reason,pcode,qty,price_in,lot_id,note)
                    VALUES (:s, NOW(),'OUT', :rs, :p, :q, NULL, NULL, NULL)
                """, {"s":store,"rs":reason,"p":pcode,"q":qty})
                write_audit(conn,"INV_OUT", f"{store}:{pcode}:{qty}:{reason}")
                st.success("Đã xuất.")
                st.rerun()

    # ====== TỒN KHO (có cốc) ======
    with tab_stock:
        st.subheader("Báo cáo tồn kho")
        with st.expander("🔎 Bộ lọc (chỉ áp khi bấm nút)"):
            to_dt = st.date_input("Chốt đến ngày", value=date.today(), key="stock_to")
            catf  = st.selectbox("Nhóm", ["TẤT CẢ","TRAI_CAY","COT","MUT","PHU_GIA","TP_KHAC"], index=0)
            do_filter = st.button("Áp dụng lọc", key="btn_stock_filter")

        # lấy toàn bộ rồi mới lọc khi nhấn
        dfp = _product_lookup(conn)
        rows = []
        for _, r in dfp.iterrows():
            if (catf!="TẤT CẢ") and (r["cat_code"]!=catf): 
                if do_filter: 
                    continue
            qty = _stock_of(conn, store, r["code"], to_dt if do_filter else None)
            if abs(qty) < 1e-9:  # ẩn zero để nhẹ; muốn hiện hết thì bỏ if này
                if do_filter: 
                    pass
            rows.append({
                "pcode": r["code"],
                "name": r["name"],
                "cat_code": r["cat_code"],
                "uom": r["uom"],
                "qty": qty,
                "cups_per_kg": float(r["cups_per_kg"] or 0)
            })
        df_ton = pd.DataFrame(rows)
        if not df_ton.empty:
            # cốc chỉ cho COT/MUT
            df_ton["cups"] = df_ton.apply(_cups_for_row, axis=1)
            # tổng tiền tồn (theo giá vốn bq)
            prices = {r["code"]: _avg_cost(conn, store, r["code"]) for _, r in dfp.iterrows()}
            df_ton["avg_cost"] = df_ton["pcode"].map(prices).astype(float)
            df_ton["value"] = (df_ton["qty"] * df_ton["avg_cost"]).round(0)
            st.dataframe(df_ton[["pcode","name","cat_code","uom","qty","cups","avg_cost","value"]],
                         use_container_width=True, hide_index=True)
            c1,c2,c3 = st.columns(3)
            with c1: st.metric("Tổng SL", f"{df_ton['qty'].sum():,.2f}")
            with c2: st.metric("Tổng số cốc (COT+MỨT)", f"{df_ton['cups'].sum():,.0f}")
            with c3: st.metric("Tổng trị giá", f"{df_ton['value'].sum():,.0f} đ")
        else:
            st.info("Không có dữ liệu.")

    # ====== KIỂM KÊ ======
    with tab_count:
        st.subheader("Kiểm kê kho (tạo bút toán chênh lệch)")
        dfp = _product_lookup(conn)
        pick_cat = st.selectbox("Lọc nhóm để nhập nhanh", ["TẤT CẢ","TRAI_CAY","COT","MUT","PHU_GIA","TP_KHAC"], index=0)
        show = dfp if pick_cat=="TẤT CẢ" else dfp[dfp["cat_code"]==pick_cat]
        data = []
        for _, r in show.iterrows():
            cur = _stock_of(conn, store, r["code"])
            data.append([r["code"], r["name"], r["uom"], cur, 0.0])
        df_inp = pd.DataFrame(data, columns=["pcode","name","uom","hiện_tại","thực_đếm"])
        st.dataframe(df_inp, use_container_width=True, hide_index=True)
        st.caption("Nhập số **thực_đếm** trực tiếp trong form phía trên (dạng editable DataFrame nếu bạn dùng AgGrid/Component).")
        st.warning("Bản đơn giản: hãy xuất excel báo cáo tồn, đi kiểm đếm, rồi quay lại nhập/điều chỉnh thủ công ở tab Nhập/Xuất.")

# ---------- trang SẢN XUẤT ----------
def _formulas(conn, typ: str, src: str | None = None) -> pd.DataFrame:
    df = fetch_df(conn, "SELECT * FROM formulas WHERE type=:t ORDER BY code", {"t": typ})
    if typ=="MUT" and src:
        df = df[(df["note"] or "").fillna("").str.contains(f"SRC={src}")]
    return df

def _parse_adds(js: str) -> dict:
    try:
        return json.loads(js or "{}")
    except Exception:
        return {}

def _preview_inputs_for_cot(conn, fm_row, qty_out: float) -> tuple[list[tuple], float]:
    # recovery: HS thu hồi = kg TP / kg sau sơ (ngược lại nguyên liệu = qty_out / recovery)
    rec = float(fm_row["recovery"] or 1.0)
    kg_after_pre = qty_out / rec if rec>0 else qty_out
    fruits = [(c, kg_after_pre) for c in (fm_row["fruits_csv"] or "").split(",") if c]
    adds   = _parse_adds(fm_row["additives_json"])
    add_list = [(k, kg_after_pre*float(v)) for k,v in adds.items()]
    # giá thành = tổng giá NVL / qty_out
    total_cost = 0.0
    for p,q in fruits + add_list:
        total_cost += _avg_cost(conn, st.session_state.get("store"), p) * q
    unit_cost = (total_cost/qty_out) if qty_out>0 else 0.0
    return fruits + add_list, unit_cost

def _preview_inputs_for_mut_from(conn, fm_row, qty_out: float, src: str) -> tuple[list[tuple], float]:
    rec = float(fm_row["recovery"] or 1.0)
    kg_after_pre = qty_out / rec if rec>0 else qty_out
    fruits = [(c, kg_after_pre) for c in (fm_row["fruits_csv"] or "").split(",") if c]
    adds   = _parse_adds(fm_row["additives_json"])
    add_list = [(k, kg_after_pre*float(v)) for k,v in adds.items()]
    total_cost = 0.0
    for p,q in fruits + add_list:
        total_cost += _avg_cost(conn, st.session_state.get("store"), p) * q
    unit_cost = (total_cost/qty_out) if qty_out>0 else 0.0
    return fruits + add_list, unit_cost

def page_sanxuat(conn: Connection, user: dict):
    st.markdown("## 🏭 Sản xuất (CỐT 1 bước, MỨT 2 nguồn)")
    if not has_perm(user, "INV_EDIT"):
        st.warning("Bạn không có quyền sản xuất.")
        return

    store = st.session_state.get("store")
    tabs = st.tabs(["CỐT", "MỨT từ TRÁI CÂY", "MỨT từ CỐT", "Lịch sử lô"])
    # ===== CỐT =====
    with tabs[0]:
        st.subheader("Sản xuất CỐT")
        fm = _formulas(conn, "COT")
        if fm.empty:
            st.info("Chưa có công thức CỐT.")
        else:
            pick = st.selectbox("Chọn công thức", fm["code"]+" — "+fm["name"], index=0, key="fm_cot")
            out_qty = st.number_input("Số lượng TP (kg)", value=0.0, step=0.1, min_value=0.0, key="cot_qty")
            batch_id = st.text_input("Mã lô", value=_lot_new_id(conn, "COT"), key="cot_lot")
            row = fm.iloc[list(fm["code"]).index(pick.split(" — ",1)[0])]
            preview, ucost = _preview_inputs_for_cot(conn, row, out_qty)
            st.caption("• Nguyên liệu tiêu hao (preview):")
            prev_df = pd.DataFrame(preview, columns=["pcode","qty"])
            st.dataframe(prev_df, use_container_width=True, hide_index=True)
            st.caption(f"• Dự tính giá thành: ~ **{ucost:,.0f} đ/kg**")

            # check âm
            ok_stock = True
            errs = []
            for p,q in preview:
                cur = _stock_of(conn, store, p)
                if q > cur + 1e-9:
                    ok_stock = False
                    errs.append(f"{p}: yêu cầu {q:.2f} > tồn {cur:.2f}")
            if not ok_stock:
                st.error("Thiếu NVL:\n- " + "\n- ".join(errs))

            if st.button("✅ Thực hiện SX CỐT", type="primary", disabled=(out_qty<=0 or not ok_stock)):
                # NVL OUT
                for p,q in preview:
                    run_sql(conn, """
                        INSERT INTO inventory_ledger(store,ts,kind,reason,pcode,qty,price_in,lot_id,note)
                        VALUES (:s,NOW(),'OUT','SX_COT',:p,:q,NULL,:lot,NULL)
                    """, {"s":store,"p":p,"q":q,"lot":batch_id})
                # TP IN
                run_sql(conn, """
                    INSERT INTO inventory_ledger(store,ts,kind,reason,pcode,qty,price_in,lot_id,note)
                    VALUES (:s,NOW(),'IN','SX_COT',:p,:q,:pr,:lot,NULL)
                """, {"s":store,"p":row["output_pcode"],"q":out_qty,"pr":ucost,"lot":batch_id})
                # save batch
                run_sql(conn, """
                    INSERT INTO production_batches(batch_id,store,ts,type,formula_code,output_pcode,qty_out,unit_cost,input_detail)
                    VALUES (:id,:s,NOW(),'COT',:fc,:op,:qo,:uc,:inp)
                """, {"id":batch_id,"s":store,"fc":row["code"],"op":row["output_pcode"],
                      "qo":out_qty,"uc":ucost,"inp":json.dumps(preview, ensure_ascii=False)})
                write_audit(conn,"MAKE_COT", f"{batch_id}:{row['output_pcode']}:{out_qty}")
                st.success("Đã tạo lô CỐT.")
                st.rerun()

    # ===== MỨT từ TRÁI CÂY =====
    with tabs[1]:
        st.subheader("Sản xuất MỨT (nguồn TRÁI CÂY)")
        fm = _formulas(conn, "MUT", "TRAI_CAY")
        if fm.empty:
            st.info("Chưa có công thức MỨT (SRC=TRAI_CAY).")
        else:
            pick = st.selectbox("Chọn công thức", fm["code"]+" — "+fm["name"], index=0, key="fm_mut_tc")
            out_qty = st.number_input("Số lượng TP (kg)", value=0.0, step=0.1, min_value=0.0, key="mut_tc_qty")
            batch_id = st.text_input("Mã lô", value=_lot_new_id(conn, "MUTTC"), key="mut_tc_lot")
            row = fm.iloc[list(fm["code"]).index(pick.split(" — ",1)[0])]
            preview, ucost = _preview_inputs_for_mut_from(conn, row, out_qty, "TRAI_CAY")
            st.caption("• Nguyên liệu tiêu hao (preview):")
            st.dataframe(pd.DataFrame(preview, columns=["pcode","qty"]), use_container_width=True, hide_index=True)
            st.caption(f"• Dự tính giá thành: ~ **{ucost:,.0f} đ/kg**")

            ok_stock = True; errs=[]
            for p,q in preview:
                cur = _stock_of(conn, store, p)
                if q > cur + 1e-9:
                    ok_stock=False; errs.append(f"{p}: yêu cầu {q:.2f} > tồn {cur:.2f}")
            if not ok_stock:
                st.error("Thiếu NVL:\n- " + "\n- ".join(errs))

            if st.button("✅ Thực hiện SX MỨT (TC)", type="primary", disabled=(out_qty<=0 or not ok_stock)):
                for p,q in preview:
                    run_sql(conn, """
                        INSERT INTO inventory_ledger(store,ts,kind,reason,pcode,qty,price_in,lot_id,note)
                        VALUES (:s,NOW(),'OUT','SX_MUT_TC',:p,:q,NULL,:lot,NULL)
                    """, {"s":store,"p":p,"q":q,"lot":batch_id})
                run_sql(conn, """
                    INSERT INTO inventory_ledger(store,ts,kind,reason,pcode,qty,price_in,lot_id,note)
                    VALUES (:s,NOW(),'IN','SX_MUT_TC',:p,:q,:pr,:lot,NULL)
                """, {"s":store,"p":row["output_pcode"],"q":out_qty,"pr":ucost,"lot":batch_id})
                run_sql(conn, """
                    INSERT INTO production_batches(batch_id,store,ts,type,formula_code,output_pcode,qty_out,unit_cost,input_detail)
                    VALUES (:id,:s,NOW(),'MUT_TC',:fc,:op,:qo,:uc,:inp)
                """, {"id":batch_id,"s":store,"fc":row["code"],"op":row["output_pcode"],
                      "qo":out_qty,"uc":ucost,"inp":json.dumps(preview, ensure_ascii=False)})
                write_audit(conn,"MAKE_MUT_TC", f"{batch_id}:{row['output_pcode']}:{out_qty}")
                st.success("Đã tạo lô MỨT (TC).")
                st.rerun()

    # ===== MỨT từ CỐT =====
    with tabs[2]:
        st.subheader("Sản xuất MỨT (nguồn CỐT)")
        fm = _formulas(conn, "MUT", "COT")
        if fm.empty:
            st.info("Chưa có công thức MỨT (SRC=COT).")
        else:
            pick = st.selectbox("Chọn công thức", fm["code"]+" — "+fm["name"], index=0, key="fm_mut_ct")
            out_qty = st.number_input("Số lượng TP (kg)", value=0.0, step=0.1, min_value=0.0, key="mut_ct_qty")
            batch_id = st.text_input("Mã lô", value=_lot_new_id(conn, "MUTCT"), key="mut_ct_lot")
            row = fm.iloc[list(fm["code"]).index(pick.split(" — ",1)[0])]
            preview, ucost = _preview_inputs_for_mut_from(conn, row, out_qty, "COT")
            st.caption("• Nguyên liệu tiêu hao (preview):")
            st.dataframe(pd.DataFrame(preview, columns=["pcode","qty"]), use_container_width=True, hide_index=True)
            st.caption(f"• Dự tính giá thành: ~ **{ucost:,.0f} đ/kg**")

            ok_stock = True; errs=[]
            for p,q in preview:
                cur = _stock_of(conn, store, p)
                if q > cur + 1e-9:
                    ok_stock=False; errs.append(f"{p}: yêu cầu {q:.2f} > tồn {cur:.2f}")
            if not ok_stock:
                st.error("Thiếu NVL:\n- " + "\n- ".join(errs))

            if st.button("✅ Thực hiện SX MỨT (CỐT)", type="primary", disabled=(out_qty<=0 or not ok_stock)):
                for p,q in preview:
                    run_sql(conn, """
                        INSERT INTO inventory_ledger(store,ts,kind,reason,pcode,qty,price_in,lot_id,note)
                        VALUES (:s,NOW(),'OUT','SX_MUT_CT',:p,:q,NULL,:lot,NULL)
                    """, {"s":store,"p":p,"q":q,"lot":batch_id})
                run_sql(conn, """
                    INSERT INTO inventory_ledger(store,ts,kind,reason,pcode,qty,price_in,lot_id,note)
                    VALUES (:s,NOW(),'IN','SX_MUT_CT',:p,:q,:pr,:lot,NULL)
                """, {"s":store,"p":row["output_pcode"],"q":out_qty,"pr":ucost,"lot":batch_id})
                run_sql(conn, """
                    INSERT INTO production_batches(batch_id,store,ts,type,formula_code,output_pcode,qty_out,unit_cost,input_detail)
                    VALUES (:id,:s,NOW(),'MUT_CT',:fc,:op,:qo,:uc,:inp)
                """, {"id":batch_id,"s":store,"fc":row["code"],"op":row["output_pcode"],
                      "qo":out_qty,"uc":ucost,"inp":json.dumps(preview, ensure_ascii=False)})
                write_audit(conn,"MAKE_MUT_CT", f"{batch_id}:{row['output_pcode']}:{out_qty}")
                st.success("Đã tạo lô MỨT (CỐT).")
                st.rerun()

    # ===== LỊCH SỬ LÔ =====
    with tabs[3]:
        st.subheader("Lịch sử lô sản xuất")
        df = fetch_df(conn, """
            SELECT batch_id, ts, type, formula_code, output_pcode, qty_out, unit_cost
            FROM production_batches
            WHERE store=:s
            ORDER BY ts DESC
            LIMIT 500
        """, {"s": store})
        st.dataframe(df, use_container_width=True, hide_index=True)# Đang dùng email làm khóa chính => truyền val_col="email"

