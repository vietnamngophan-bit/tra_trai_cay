# =========================================================
# Fruit Tea ERP v5 - Postgres only (all-in-one)
# =========================================================
import os, re, json, hashlib
from datetime import date, datetime, timedelta
from decimal import Decimal
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

# ---------------- CONFIG UI ----------------
st.set_page_config(page_title="Quản Trị Trà Trái Cây", page_icon="🍵", layout="wide", initial_sidebar_state="expanded")
st.session_state.setdefault("store", "")

# ---------------- DB CONNECT ----------------
_ENGINE = None
def _normalize_pg_url(url: str) -> str:
    url = url.strip()
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif url.startswith("postgresql://") and "+psycopg2" not in url:
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    if "sslmode=" not in url:
        url += ("&" if "?" in url else "?") + "sslmode=require"
    return url

def get_conn() -> Connection:
    global _ENGINE
    pg = os.getenv("DATABASE_URL", "")
    if not pg:
        st.error("❌ Thiếu biến môi trường DATABASE_URL")
        st.stop()
    if _ENGINE is None:
        _ENGINE = create_engine(_normalize_pg_url(pg), pool_pre_ping=True, future=True)
    return _ENGINE.connect()

# ---------------- SQL HELPERS ----------------
def run_sql(conn: Connection, sql: str, params=None):
    """Hỗ trợ ? -> :p1,:p2,... và commit nhẹ"""
    if isinstance(params, (list, tuple)):
        idx = 1
        def repl(_):
            nonlocal idx
            s = f":p{idx}"; idx += 1
            return s
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
        def repl(_):
            nonlocal idx
            s = f":p{idx}"; idx += 1
            return s
        sql_named = re.sub(r"\?", repl, sql)
        params = {f"p{i+1}": v for i,v in enumerate(params)}
        return pd.read_sql_query(text(sql_named), conn, params=params)
    return pd.read_sql_query(text(sql), conn, params=params or {})

# ---------------- AUTH / AUDIT ----------------
def sha256(s: str) -> str: return hashlib.sha256(s.encode("utf-8")).hexdigest()

def has_perm(user: dict, perm: str) -> bool:
    if not user: return False
    if user.get("role") == "SuperAdmin": return True
    return perm in (user.get("perms") or "").split(",")

def write_audit(conn: Connection, action: str, detail: str=""):
    try:
        run_sql(conn, "INSERT INTO syslog(ts,actor,action,detail) VALUES (NOW(),:a,:b,:c)",
                {"a": st.session_state.get("user",{}).get("email","anonymous"),
                 "b": action, "c": (detail or "")[:1000]})
    except: pass

def login_form(conn: Connection):
    st.markdown("### 🔐 Đăng nhập hệ thống")
    email = st.text_input("Email", key="login_email")
    pw    = st.text_input("Mật khẩu", type="password", key="login_pw")
    if st.button("Đăng nhập", type="primary"):
        df = fetch_df(conn, "SELECT email,display,password,role,store_code,perms FROM users WHERE email=:e", {"e": email})
        if df.empty: st.error("Sai tài khoản hoặc mật khẩu."); return
        row = df.iloc[0]
        if row["password"] != sha256(pw):
            st.error("Sai tài khoản hoặc mật khẩu."); return
        user = {"email": row["email"], "display": row["display"] or row["email"],
                "role": row["role"] or "User", "perms": row["perms"] or "", "store": row["store_code"] or ""}
        st.session_state["user"] = user
        if user["store"]: st.session_state["store"] = user["store"]
        write_audit(conn, "LOGIN", email)
        st.rerun()

def require_login(conn: Connection) -> dict:
    if "user" not in st.session_state:
        login_form(conn); st.stop()
    return st.session_state["user"]

def logout(conn: Connection):
    write_audit(conn, "LOGOUT", st.session_state.get("user",{}).get("email",""))
    st.session_state.clear(); st.rerun()

# ---------------- HEADER ----------------
def header_top(conn: Connection, user: dict):
    c1,c2 = st.columns([0.8,0.2])
    with c1: st.markdown("## 🍵 Quản Trị Trà Trái Cây Anh Gầy"); st.caption("Kết nối: Postgres (Supabase)")
    with c2:
        with st.popover(f"👤 {user.get('display','')}"):
            st.caption(user.get("email",""))
            st.markdown("---")
            st.markdown("**Đổi mật khẩu**")
            with st.form("fm_pw", clear_on_submit=True):
                old = st.text_input("Mật khẩu cũ", type="password")
                new1 = st.text_input("Mật khẩu mới", type="password")
                new2 = st.text_input("Xác nhận", type="password")
                ok = st.form_submit_button("Cập nhật")
            if ok:
                df = fetch_df(conn, "SELECT password FROM users WHERE email=:e", {"e": user["email"]})
                if df.empty or df.iloc[0]["password"] != sha256(old):
                    st.error("Mật khẩu cũ không đúng.")
                elif not new1 or new1 != new2:
                    st.error("Xác nhận chưa khớp.")
                else:
                    run_sql(conn, "UPDATE users SET password=:p WHERE email=:e",
                            {"p": sha256(new1), "e": user["email"]})
                    write_audit(conn,"CHANGE_PW", user["email"])
                    st.success("Đã đổi mật khẩu. Vui lòng đăng nhập lại.")
                    logout(conn)
            st.markdown("---")
            if st.button("Đăng xuất", use_container_width=True): logout(conn)

# ---------------- COMMON UI ----------------
def store_selector(conn: Connection, user: dict):
    st.sidebar.markdown("### 🏪 Cửa hàng")
    df = fetch_df(conn, "SELECT code,name FROM stores ORDER BY name")
    opts = ["(TẤT CẢ)"] + df["code"].tolist()
    default = opts.index(user.get("store")) if user.get("store") in opts else 0
    st.sidebar.selectbox("Đang thao tác tại", opts, index=default, key="store")

# =========================================================
# =============== PAGES (định nghĩa thật) ================
# =========================================================

# ---------- Dashboard ----------
def page_dashboard(conn, user):
    st.markdown("### 📊 Dashboard")
    s = st.session_state.get("store") or None
    # số sản phẩm
    n_prod = fetch_df(conn, "SELECT COUNT(*) n FROM products")["n"].iat[0]
    # số cửa hàng
    n_st   = fetch_df(conn, "SELECT COUNT(*) n FROM stores")["n"].iat[0]
    # doanh thu 30 ngày
    df_rev = fetch_df(conn, """
        SELECT CAST(biz_date AS DATE) d, SUM(amount) total
        FROM sales
        WHERE (:s IS NULL OR store_code=:s) AND biz_date >= CURRENT_DATE - INTERVAL '30 day'
        GROUP BY 1 ORDER BY 1
    """, {"s": s})
    c1,c2,c3 = st.columns(3)
    c1.metric("Sản phẩm", f"{n_prod}")
    c2.metric("Cửa hàng", f"{n_st}")
    c3.metric("Doanh thu 30 ngày", f"{float(df_rev['total'].sum() or 0):,.0f} đ")
    st.line_chart(df_rev.set_index("d")["total"]) if not df_rev.empty else st.info("Chưa có doanh thu.")

# ---------- Danh mục (Category/Product/Formula) ----------
def page_catalog(conn, user):
    st.markdown("### 🧾 Danh mục")
    tabs = st.tabs(["Danh mục SP","Sản phẩm","Công thức"])

    # Danh mục SP
    with tabs[0]:
        df = fetch_df(conn, "SELECT code,name FROM categories ORDER BY code")
        st.dataframe(df, use_container_width=True, height=340)
        with st.form("fm_cat", clear_on_submit=True):
            code = st.text_input("Mã")
            name = st.text_input("Tên")
            if st.form_submit_button("Lưu", type="primary") and code and name:
                run_sql(conn, """
                    INSERT INTO categories(code,name) VALUES (:c,:n)
                    ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name
                """, {"c":code.strip(),"n":name.strip()})
                write_audit(conn,"CAT_UPSERT", code); st.success("OK"); st.rerun()
        pick = st.selectbox("Xoá mã", ["—"]+[r["code"] for _,r in df.iterrows()], index=0)
        if pick!="—" and st.button("Xoá danh mục"):
            run_sql(conn,"DELETE FROM categories WHERE code=:c",{"c":pick}); write_audit(conn,"CAT_DELETE",pick); st.rerun()

    # Sản phẩm
    with tabs[1]:
        df = fetch_df(conn, "SELECT code,name,cat_code,uom,cups_per_kg,price_ref FROM products ORDER BY name")
        st.dataframe(df, use_container_width=True, height=340)
        with st.form("fm_prod", clear_on_submit=True):
            code = st.text_input("Mã SP")
            name = st.text_input("Tên SP")
            cat  = st.selectbox("Nhóm", ["TRAI_CAY","COT","MUT","PHU_GIA","TP_KHAC"])
            uom  = st.text_input("ĐVT", value="kg")
            cups = st.number_input("Cốc/kg TP", min_value=0.0, value=0.0, step=0.1)
            pref = st.number_input("Giá tham chiếu", min_value=0.0, value=0.0, step=1000.0)
            if st.form_submit_button("Lưu", type="primary") and code and name:
                run_sql(conn, """
                    INSERT INTO products(code,name,cat_code,uom,cups_per_kg,price_ref)
                    VALUES (:c,:n,:g,:u,:k,:p)
                    ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name,cat_code=EXCLUDED.cat_code,
                        uom=EXCLUDED.uom,cups_per_kg=EXCLUDED.cups_per_kg,price_ref=EXCLUDED.price_ref
                """, {"c":code.strip(),"n":name.strip(),"g":cat,"u":uom.strip(),"k":float(cups),"p":float(pref)})
                write_audit(conn,"PROD_UPSERT", code); st.success("OK"); st.rerun()
        pick = st.selectbox("Xoá SP", ["—"]+[r["code"] for _,r in df.iterrows()], index=0, key="del_sp")
        if pick!="—" and st.button("Xoá sản phẩm"):
            run_sql(conn, "DELETE FROM products WHERE code=:c", {"c":pick}); write_audit(conn,"PROD_DELETE",pick); st.rerun()

    # Công thức (CỐT 1 bước; MỨT từ trái cây / từ cốt)
    with tabs[2]:
        st.caption("• CỐT: có hệ số thu hồi. • MỨT: không hệ số; nguồn NVL chính TRÁI CÂY hoặc CỐT. • Nhiều NVL/phụ gia cho 1 CT.")
        df_hdr = fetch_df(conn, "SELECT code,name,type,output_pcode,output_uom,recovery,cups_per_kg,note FROM formulas ORDER BY type,name")
        st.dataframe(df_hdr, use_container_width=True, height=300)

        st.markdown("#### ➕ Thêm công thức")
        with st.form("fm_ct_add", clear_on_submit=True):
            cA,cB = st.columns(2)
            with cA:
                code = st.text_input("Mã CT")
                name = st.text_input("Tên CT")
                typ  = st.selectbox("Loại", ["COT","MUT"])
            with cB:
                cups = st.number_input("Số cốc/kg TP", min_value=0.0, value=0.0, step=0.1, key="cups_add")
                rec  = st.number_input("Hệ số thu hồi (CỐT)", min_value=0.01, value=1.0, step=0.01, disabled=(typ!="COT"), key="rec_add")

            # SP đầu ra theo loại
            out_cat = "COT" if typ=="COT" else "MUT"
            df_out = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code=:c ORDER BY name", {"c": out_cat})
            out_pick = st.selectbox("Sản phẩm đầu ra", [f"{r['code']} — {r['name']}" for _,r in df_out.iterrows()]) if not df_out.empty else ""
            output_pcode = out_pick.split(" — ",1)[0] if out_pick else ""

            # Nguồn NVL chính (chỉ MỨT)
            if typ=="MUT":
                src_kind = st.radio("Nguồn NVL chính", ["TRAI_CAY","COT"], horizontal=True, key="mut_src_add")
            else:
                src_kind = "TRAI_CAY"; st.caption("Nguồn NVL chính: Trái cây")

            # Nguyên liệu chính (đa chọn) + định lượng /1kg TP
            st.markdown("**Nguyên liệu chính**")
            src_cat = "TRAI_CAY" if src_kind=="TRAI_CAY" else "COT"
            df_src = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code=:c ORDER BY name", {"c": src_cat})
            picked_raw = st.multiselect("Chọn NVL", [f"{r['code']} — {r['name']}" for _,r in df_src.iterrows()], key="raw_multi_add")
            raw_inputs = {}
            for item in picked_raw:
                c0 = item.split(" — ",1)[0]
                q0 = st.number_input(f"{item} — kg / 1kg TP", min_value=0.0, value=0.0, step=0.01, key=f"raw_add_{c0}")
                if q0>0: raw_inputs[c0] = q0

            # Phụ gia
            st.markdown("**Phụ gia**")
            df_add = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code='PHU_GIA' ORDER BY name")
            picked_add = st.multiselect("Chọn phụ gia",[f"{r['code']} — {r['name']}" for _, r in df_add.iterrows()],key="add_multi_add")

            add_inputs = {}
            for item in picked_add:
                c0 = item.split(" — ",1)[0]
                q0 = st.number_input(f"{item} — kg / 1kg TP", min_value=0.0, value=0.0, step=0.01, key=f"add_add_{c0}")
                if q0>0: add_inputs[c0] = q0

            if st.form_submit_button("Lưu CT", type="primary"):
                if not code or not name or not output_pcode or (typ=="COT" and not raw_inputs):
                    st.error("Thiếu mã/tên/SP đầu ra/NVL."); 
                else:
                    run_sql(conn, """
                        INSERT INTO formulas(code,name,type,output_pcode,output_uom,recovery,cups_per_kg,note)
                        VALUES (:c,:n,:t,:o,'kg',:r,:k,:x)
                        ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name,type=EXCLUDED.type,
                          output_pcode=EXCLUDED.output_pcode,output_uom=EXCLUDED.output_uom,
                          recovery=EXCLUDED.recovery,cups_per_kg=EXCLUDED.cups_per_kg,note=EXCLUDED.note
                    """, {"c":code.strip(),"n":name.strip(),"t":typ,"o":output_pcode,
                          "r": (float(rec) if typ=="COT" else 1.0),"k": float(cups),
                          "x": ("" if typ=="COT" else f"SRC={src_kind}")})
                    run_sql(conn, "DELETE FROM formula_inputs WHERE formula_code=:c", {"c": code})
                    for k,v in raw_inputs.items():
                        run_sql(conn, "INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind) VALUES (:f,:p,:q,:k)",
                                {"f": code, "p": k, "q": float(v), "k": ("TRAI_CAY" if src_cat=="TRAI_CAY" else "COT")})
                    for k,v in add_inputs.items():
                        run_sql(conn, "INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind) VALUES (:f,:p,:q,'PHU_GIA')",
                                {"f": code, "p": k, "q": float(v)})
                    write_audit(conn,"FORMULA_UPSERT", code); st.success("Đã lưu."); st.rerun()

        st.markdown("#### ✏️ Sửa / 🗑️ Xoá")
        if df_hdr.empty:
            st.info("Chưa có công thức.")
        else:
            pick = st.selectbox("Chọn CT", [f"{r['code']} — {r['name']}" for _,r in df_hdr.iterrows()], key="ct_pick_edit")
            ct_code = pick.split(" — ",1)[0]
            hdr = fetch_df(conn, "SELECT * FROM formulas WHERE code=:c", {"c": ct_code}).iloc[0].to_dict()
            det = fetch_df(conn, "SELECT * FROM formula_inputs WHERE formula_code=:c ORDER BY kind", {"c": ct_code})
            # (Phần edit tương tự như add — rút gọn vì độ dài)
            if st.button("🗑️ Xoá công thức"):
                run_sql(conn, "DELETE FROM formulas WHERE code=:c", {"c": ct_code})
                write_audit(conn,"FORMULA_DELETE", ct_code); st.success("Đã xoá."); st.rerun()

# ---------- Kho ----------
def _stock_of(conn, store: str, pcode: str, to_dt: date|None=None) -> float:
    """Số dư đến ngày"""
    try:
        if to_dt is None: to_dt = date.today()
        v = fetch_df(conn, """
            SELECT COALESCE(SUM(CASE WHEN mv='IN' THEN qty ELSE -qty END),0) as bal
            FROM stock_moves
            WHERE pcode=:p AND (:s='' OR store_code=:s) AND biz_date <= :d
        """, {"p": pcode, "s": (store or ""), "d": to_dt})["bal"].iat[0]
        return float(v or 0)
    except: return 0.0

def page_kho(conn, user):
    st.markdown("### 📦 Kho (Nhập/Xuất/Kiểm kê)")
    store = (st.session_state.get("store") or "").strip()
    tabs = st.tabs(["Nhập","Xuất","Tồn kho","Kiểm kê nâng cao"])

    # Nhập
    with tabs[0]:
        dfp = fetch_df(conn, "SELECT code,name,cat_code FROM products ORDER BY name")
        pick = st.selectbox("Chọn sản phẩm nhập", [f"{r['code']} — {r['name']}" for _,r in dfp.iterrows()], key="in_p")
        pcode = pick.split(" — ",1)[0]
        d = st.date_input("Ngày nhập", value=date.today(), key="in_date")
        c1,c2,c3 = st.columns(3)
        with c1:
            qty = st.number_input("Số lượng", min_value=0.0, value=0.0, step=0.1, key="in_qty")
        with c2:
            price = st.number_input("Đơn giá (VND/DVT)", min_value=0.0, value=0.0, step=1000.0, key="in_price")
        with c3:
            note = st.text_input("Lý do/Ghi chú", key="in_note")
        if st.button("➕ Ghi nhập", type="primary"):
            run_sql(conn, """
                INSERT INTO stock_moves(biz_date,store_code,pcode,qty,uom,price,reason,mv)
                VALUES (:d,:s,:p,:q,'kg',:pr,:n,'IN')
            """, {"d": d, "s": store, "p": pcode, "q": float(qty), "pr": float(price), "n": note})
            write_audit(conn,"STOCK_IN", f"{store}/{pcode}/{qty}")
            st.success("Đã ghi."); st.rerun()

    # Xuất
    with tabs[1]:
        dfp = fetch_df(conn, "SELECT code,name FROM products ORDER BY name")
        pick = st.selectbox("Chọn sản phẩm xuất", [f"{r['code']} — {r['name']}" for _,r in dfp.iterrows()], key="out_p")
        pcode = pick.split(" — ",1)[0]
        d = st.date_input("Ngày xuất", value=date.today(), key="out_date")
        qty = st.number_input("Số lượng xuất", min_value=0.0, value=0.0, step=0.1, key="out_qty")
        reason = st.selectbox("Lý do", ["BÁN","HỦY","KHÁC"], key="out_reason")
        if st.button("➖ Ghi xuất", type="primary"):
            bal = _stock_of(conn, store, pcode, d)
            if qty > bal + 1e-9:
                st.error(f"Không đủ tồn (đang {bal})."); 
            else:
                run_sql(conn, """
                    INSERT INTO stock_moves(biz_date,store_code,pcode,qty,uom,price,reason,mv)
                    VALUES (:d,:s,:p,:q,'kg',0,:r,'OUT')
                """, {"d": d, "s": store, "p": pcode, "q": float(qty), "r": reason})
                write_audit(conn,"STOCK_OUT", f"{store}/{pcode}/{qty}")
                st.success("Đã ghi."); st.rerun()

    # Tồn kho
    with tabs[2]:
        to_dt = st.date_input("Chốt đến ngày", value=date.today(), key="stk_to")
        df = fetch_df(conn, """
            WITH x AS (
              SELECT pcode,
                     SUM(CASE WHEN mv='IN'  THEN qty ELSE -qty END) qty,
                     SUM(CASE WHEN mv='IN'  THEN qty*price ELSE 0 END) val_in
              FROM stock_moves
              WHERE biz_date<=:d AND (:s='' OR store_code=:s)
              GROUP BY pcode
            )
            SELECT p.code, p.name, p.cat_code, COALESCE(x.qty,0) qty,
                   COALESCE(x.val_in,0) / NULLIF(NULLIF(x.qty,0),0) AS avg_price
            FROM products p
            LEFT JOIN x ON x.pcode=p.code
            ORDER BY p.cat_code,p.name
        """, {"d": to_dt, "s": (store or "")})
        if not df.empty:
            df["value"] = (df["qty"].fillna(0) * df["avg_price"].fillna(0)).round(0)
            st.dataframe(df, use_container_width=True, height=420)
            c1,c2,c3 = st.columns(3)
            c2.metric("Tổng trị giá", f"{float(df['value'].sum() or 0):,.0f} đ")

    # Kiểm kê nâng cao (ghi điều chỉnh)
    with tabs[3]:
        dfp = fetch_df(conn, "SELECT code,name FROM products ORDER BY name")
        pick = st.selectbox("Chọn SP kiểm kê", [f"{r['code']} — {r['name']}" for _,r in dfp.iterrows()], key="adj_p")
        pcode = pick.split(" — ",1)[0]
        d = st.date_input("Ngày KK", value=date.today(), key="adj_date")
        qty_physical = st.number_input("Số lượng thực tế", min_value=0.0, value=0.0, step=0.1, key="adj_qty")
        bal = _stock_of(conn, store, pcode, d)
        st.caption(f"Tồn sổ đến {d}: {bal}")
        if st.button("⚖️ Ghi chênh lệch"):
            delta = qty_physical - bal
            if abs(delta) < 1e-9:
                st.info("Không có chênh lệch.")
            else:
                mv = 'IN' if delta>0 else 'OUT'
                run_sql(conn, """
                    INSERT INTO stock_moves(biz_date,store_code,pcode,qty,uom,price,reason,mv)
                    VALUES (:d,:s,:p,:q,'kg',0,'KIEM_KE',:m)
                """, {"d": d, "s": store, "p": pcode, "q": float(abs(delta)), "m": mv})
                write_audit(conn,"STOCK_ADJUST", f"{store}/{pcode}/{delta}")
                st.success("Đã ghi điều chỉnh."); st.rerun()

# ---------- Sản xuất ----------
def _calc_inputs_for(conn, ct_code: str, out_qty_kg: float):
    hdr = fetch_df(conn, "SELECT * FROM formulas WHERE code=:c", {"c": ct_code})
    if hdr.empty: return None, None, None
    H = hdr.iloc[0].to_dict()
    det = fetch_df(conn, "SELECT pcode,qty_per_kg,kind FROM formula_inputs WHERE formula_code=:c", {"c": ct_code})
    raw = det[det["kind"].isin(["TRAI_CAY","COT"])].copy()
    add = det[det["kind"]=="PHU_GIA"].copy()
    # Hệ số (CỐT) hoặc 1.0 (MỨT)
    rec = float(H.get("recovery") or 1.0)
    kg_tp = float(out_qty_kg)
    raw["req"] = raw["qty_per_kg"].astype(float) * kg_tp
    add["req"] = add["qty_per_kg"].astype(float) * kg_tp
    return H, raw[["pcode","req","kind"]], add[["pcode","req"]]

def page_sanxuat(conn, user):
    st.markdown("### 🏭 Sản xuất (CỐT 1 bước • MỨT 2 nguồn)")
    store = (st.session_state.get("store") or "").strip()
    tabs = st.tabs(["CỐT","MỨT từ TRÁI CÂY","MỨT từ CỐT","Lịch sử lô"])

    # CỐT
    with tabs[0]:
        df_ct = fetch_df(conn, "SELECT code,name FROM formulas WHERE type='COT' ORDER BY name")
        pick = st.selectbox("Chọn công thức", [f"{r['code']} — {r['name']}" for _,r in df_ct.iterrows()], key="ct_cot")
        ct = pick.split(" — ",1)[0] if pick else ""
        qty_tp = st.number_input("Số lượng TP (kg)", min_value=0.0, value=0.0, step=0.1, key="cot_qty")
        if ct:
            H, raw, add = _calc_inputs_for(conn, ct, qty_tp)
            st.write("**NVL chính:**"); st.dataframe(raw, use_container_width=True)
            st.write("**Phụ gia:**"); st.dataframe(add, use_container_width=True)
            lot = st.text_input("Mã lô", value=f"COT-{datetime.now():%Y%m%d-%H%M%S}")
            biz = st.date_input("Ngày SX", value=date.today(), key="cot_date")
            if st.button("✅ Ghi lô CỐT", type="primary"):
                # kiểm tồn NVL
                ok = True; msg=[]
                for r in raw.itertuples():
                    bal = _stock_of(conn, store, r.pcode, biz)
                    if r.req > bal + 1e-6: ok=False; msg.append(f"{r.pcode} thiếu {r.req-bal:.2f}")
                for r in add.itertuples():
                    bal = _stock_of(conn, store, r.pcode, biz)
                    if r.req > bal + 1e-6: ok=False; msg.append(f"{r.pcode} thiếu {r.req-bal:.2f}")
                if not ok: st.error("Không đủ NVL: " + ", ".join(msg))
                else:
                    # ghi lô + xuất NVL + nhập TP
                    run_sql(conn, "INSERT INTO mfg_lots(lot_id,formula_code,store_code,biz_date,output_pcode,qty_out,uom)"
                                  "SELECT :id, code, :s, :d, output_pcode, :q, 'kg' FROM formulas WHERE code=:f",
                           {"id": lot, "s": store, "d": biz, "q": float(qty_tp), "f": ct})
                    for r in raw.itertuples():
                        run_sql(conn, "INSERT INTO stock_moves(biz_date,store_code,pcode,qty,uom,price,reason,mv,lot_id)"
                                      "VALUES (:d,:s,:p,:q,'kg',0,'SX_COT','OUT',:l)",
                               {"d": biz, "s": store, "p": r.pcode, "q": float(r.req), "l": lot})
                    for r in add.itertuples():
                        run_sql(conn, "INSERT INTO stock_moves(biz_date,store_code,pcode,qty,uom,price,reason,mv,lot_id)"
                                      "VALUES (:d,:s,:p,:q,'kg',0,'SX_COT','OUT',:l)",
                               {"d": biz, "s": store, "p": r.pcode, "q": float(r.req), "l": lot})
                    out = fetch_df(conn, "SELECT output_pcode FROM formulas WHERE code=:c", {"c": ct})["output_pcode"].iat[0]
                    run_sql(conn, "INSERT INTO stock_moves(biz_date,store_code,pcode,qty,uom,price,reason,mv,lot_id)"
                                  "VALUES (:d,:s,:p,:q,'kg',0,'SX_COT','IN',:l)",
                           {"d": biz, "s": store, "p": out, "q": float(qty_tp), "l": lot})
                    write_audit(conn,"MFG_COT", lot); st.success("Đã ghi lô."); st.rerun()

    # MỨT từ TRÁI CÂY
    with tabs[1]:
        df_ct = fetch_df(conn, "SELECT code,name FROM formulas WHERE type='MUT' AND COALESCE(note,'') LIKE 'SRC=TRAI_CAY%' ORDER BY name")
        pick = st.selectbox("Chọn công thức", [f"{r['code']} — {r['name']}" for _,r in df_ct.iterrows()], key="ct_mut_tc")
        ct = pick.split(" — ",1)[0] if pick else ""
        qty_tp = st.number_input("Số lượng TP (kg)", min_value=0.0, value=0.0, step=0.1, key="mut_tc_qty")
        if ct:
            H, raw, add = _calc_inputs_for(conn, ct, qty_tp)
            st.write("**NVL chính:**"); st.dataframe(raw, use_container_width=True)
            st.write("**Phụ gia:**"); st.dataframe(add, use_container_width=True)
            lot = st.text_input("Mã lô", value=f"MUTTC-{datetime.now():%Y%m%d-%H%M%S}")
            biz = st.date_input("Ngày SX", value=date.today(), key="mut_tc_date")
            if st.button("✅ Ghi lô MỨT (trái cây)", type="primary"):
                ok=True; msg=[]
                for r in raw.itertuples():
                    bal = _stock_of(conn, store, r.pcode, biz)
                    if r.req > bal + 1e-6: ok=False; msg.append(f"{r.pcode} thiếu {r.req-bal:.2f}")
                for r in add.itertuples():
                    bal = _stock_of(conn, store, r.pcode, biz)
                    if r.req > bal + 1e-6: ok=False; msg.append(f"{r.pcode} thiếu {r.req-bal:.2f}")
                if not ok: st.error("Không đủ NVL: " + ", ".join(msg))
                else:
                    run_sql(conn, "INSERT INTO mfg_lots(lot_id,formula_code,store_code,biz_date,output_pcode,qty_out,uom)"
                                  "SELECT :id, code, :s, :d, output_pcode, :q, 'kg' FROM formulas WHERE code=:f",
                           {"id": lot, "s": store, "d": biz, "q": float(qty_tp), "f": ct})
                    for r in raw.itertuples():
                        run_sql(conn, "INSERT INTO stock_moves(biz_date,store_code,pcode,qty,uom,price,reason,mv,lot_id)"
                                      "VALUES (:d,:s,:p,:q,'kg',0,'SX_MUT_TC','OUT',:l)",
                               {"d": biz, "s": store, "p": r.pcode, "q": float(r.req), "l": lot})
                    for r in add.itertuples():
                        run_sql(conn, "INSERT INTO stock_moves(biz_date,store_code,pcode,qty,uom,price,reason,mv,lot_id)"
                                      "VALUES (:d,:s,:p,:q,'kg',0,'SX_MUT_TC','OUT',:l)",
                               {"d": biz, "s": store, "p": r.pcode, "q": float(r.req), "l": lot})
                    out = fetch_df(conn, "SELECT output_pcode FROM formulas WHERE code=:c", {"c": ct})["output_pcode"].iat[0]
                    run_sql(conn, "INSERT INTO stock_moves(biz_date,store_code,pcode,qty,uom,price,reason,mv,lot_id)"
                                  "VALUES (:d,:s,:p,:q,'kg',0,'SX_MUT_TC','IN',:l)",
                           {"d": biz, "s": store, "p": out, "q": float(qty_tp), "l": lot})
                    write_audit(conn,"MFG_MUT_TC", lot); st.success("Đã ghi lô."); st.rerun()

    # MỨT từ CỐT
    with tabs[2]:
        df_ct = fetch_df(conn, "SELECT code,name FROM formulas WHERE type='MUT' AND COALESCE(note,'') LIKE 'SRC=COT%' ORDER BY name")
        pick = st.selectbox("Chọn công thức", [f"{r['code']} — {r['name']}" for _,r in df_ct.iterrows()], key="ct_mut_ct")
        ct = pick.split(" — ",1)[0] if pick else ""
        qty_tp = st.number_input("Số lượng TP (kg)", min_value=0.0, value=0.0, step=0.1, key="mut_ct_qty")
        if ct:
            H, raw, add = _calc_inputs_for(conn, ct, qty_tp)
            st.write("**NVL chính:**"); st.dataframe(raw, use_container_width=True)
            st.write("**Phụ gia:**"); st.dataframe(add, use_container_width=True)
            lot = st.text_input("Mã lô", value=f"MUTCT-{datetime.now():%Y%m%d-%H%M%S}")
            biz = st.date_input("Ngày SX", value=date.today(), key="mut_ct_date")
            if st.button("✅ Ghi lô MỨT (cốt)", type="primary"):
                ok=True; msg=[]
                for r in raw.itertuples():
                    bal = _stock_of(conn, store, r.pcode, biz)
                    if r.req > bal + 1e-6: ok=False; msg.append(f"{r.pcode} thiếu {r.req-bal:.2f}")
                for r in add.itertuples():
                    bal = _stock_of(conn, store, r.pcode, biz)
                    if r.req > bal + 1e-6: ok=False; msg.append(f"{r.pcode} thiếu {r.req-bal:.2f}")
                if not ok: st.error("Không đủ NVL: " + ", ".join(msg))
                else:
                    run_sql(conn, "INSERT INTO mfg_lots(lot_id,formula_code,store_code,biz_date,output_pcode,qty_out,uom)"
                                  "SELECT :id, code, :s, :d, output_pcode, :q, 'kg' FROM formulas WHERE code=:f",
                           {"id": lot, "s": store, "d": biz, "q": float(qty_tp), "f": ct})
                    for r in raw.itertuples():
                        run_sql(conn, "INSERT INTO stock_moves(biz_date,store_code,pcode,qty,uom,price,reason,mv,lot_id)"
                                      "VALUES (:d,:s,:p,:q,'kg',0,'SX_MUT_CT','OUT',:l)",
                               {"d": biz, "s": store, "p": r.pcode, "q": float(r.req), "l": lot})
                    for r in add.itertuples():
                        run_sql(conn, "INSERT INTO stock_moves(biz_date,store_code,pcode,qty,uom,price,reason,mv,lot_id)"
                                      "VALUES (:d,:s,:p,:q,'kg',0,'SX_MUT_CT','OUT',:l)",
                               {"d": biz, "s": store, "p": r.pcode, "q": float(r.req), "l": lot})
                    out = fetch_df(conn, "SELECT output_pcode FROM formulas WHERE code=:c", {"c": ct})["output_pcode"].iat[0]
                    run_sql(conn, "INSERT INTO stock_moves(biz_date,store_code,pcode,qty,uom,price,reason,mv,lot_id)"
                                  "VALUES (:d,:s,:p,:q,'kg',0,'SX_MUT_CT','IN',:l)",
                           {"d": biz, "s": store, "p": out, "q": float(qty_tp), "l": lot})
                    write_audit(conn,"MFG_MUT_CT", lot); st.success("Đã ghi lô."); st.rerun()

    # Lịch sử lô
    with tabs[3]:
        df = fetch_df(conn, """
            SELECT l.lot_id,l.biz_date,l.store_code,l.formula_code,l.output_pcode,l.qty_out,
                   (SELECT STRING_AGG(pcode||':'||qty, ', ') FROM
                        (SELECT pcode, SUM(CASE WHEN mv='OUT' THEN qty ELSE 0 END) qty
                         FROM stock_moves WHERE lot_id=l.lot_id GROUP BY pcode) t) as nvl
            FROM mfg_lots l
            WHERE (:s='' OR store_code=:s) ORDER BY biz_date DESC, lot_id DESC
        """, {"s": (st.session_state.get("store") or "")})
        st.dataframe(df, use_container_width=True, height=420)

# ---------- Doanh thu ----------
def page_doanhthu(conn, user):
    st.markdown("### 💵 Doanh thu (Tiền mặt/Chuyển khoản)")
    store = (st.session_state.get("store") or "").strip()
    d = st.date_input("Thời điểm", value=date.today(), key="rev_date")
    method = st.radio("Phương thức", ["CASH","BANK"], horizontal=True, key="rev_method")
    amt = st.number_input("Số tiền (đ)", min_value=0.0, value=0.0, step=1000.0, key="rev_amt")
    note = st.text_area("Ghi chú", key="rev_note")
    if st.button("Ghi nhận", type="primary"):
        run_sql(conn, "INSERT INTO sales(biz_date,store_code,amount,method,note) VALUES (:d,:s,:a,:m,:n)",
                {"d": d, "s": store, "a": float(amt), "m": method, "n": note})
        write_audit(conn,"REV_ADD", f"{store}/{method}/{amt}")
        st.success("OK"); st.rerun()
    st.markdown("---")
    f1,f2 = st.columns(2)
    with f1:
        fr = st.date_input("Từ ngày", value=date.today()-timedelta(days=30), key="rev_from")
    with f2:
        to = st.date_input("Đến ngày", value=date.today(), key="rev_to")
    df = fetch_df(conn, """
        SELECT biz_date, method, SUM(amount) amount
        FROM sales
        WHERE biz_date BETWEEN :f AND :t AND (:s='' OR store_code=:s)
        GROUP BY biz_date, method ORDER BY biz_date
    """, {"f": fr, "t": to, "s": (store or "")})
    if df.empty:
        st.info("Chưa có dữ liệu.")
    else:
        st.dataframe(df, use_container_width=True, height=360)
        st.bar_chart(df.pivot_table(index="biz_date", columns="method", values="amount", aggfunc="sum").fillna(0))

# ---------- Báo cáo ----------
def page_baocao(conn, user):
    st.markdown("### 📑 Báo cáo tài chính")
    f = st.date_input("Từ ngày", value=date.today().replace(day=1), key="rpt_from")
    t = st.date_input("Đến ngày", value=date.today(), key="rpt_to")
    tabs = st.tabs(["Tổng quan","Sổ quỹ (CASH/BANK)","Cân đối kế toán","Lưu chuyển tiền tệ"])

    # Tổng quan
    with tabs[0]:
        rev = fetch_df(conn, """
            SELECT SUM(amount) a FROM sales WHERE biz_date BETWEEN :f AND :t
        """, {"f": f, "t": t})["a"].fillna(0).iat[0]
        inv = fetch_df(conn, """
            WITH x AS (
              SELECT pcode,
                     SUM(CASE WHEN mv='IN' THEN qty ELSE -qty END) qty,
                     SUM(CASE WHEN mv='IN' THEN qty*price ELSE 0 END) val
              FROM stock_moves WHERE biz_date<=:t GROUP BY pcode
            )
            SELECT SUM(COALESCE(x.val,0)) AS v FROM x
        """, {"t": t})["v"].fillna(0).iat[0]
        c1,c2 = st.columns(2)
        c1.metric("Doanh thu", f"{float(rev):,.0f} đ")
        c2.metric("Giá trị hàng tồn", f"{float(inv):,.0f} đ")

    # Sổ quỹ
    with tabs[1]:
        df = fetch_df(conn, "SELECT biz_date, method, amount, note FROM sales WHERE biz_date BETWEEN :f AND :t ORDER BY biz_date",
                      {"f": f, "t": t})
        st.dataframe(df, use_container_width=True, height=420)

    # Cân đối kế toán (giản lược)
    with tabs[2]:
        # Tài sản ≈ tồn kho + tiền
        cash = fetch_df(conn, "SELECT COALESCE(SUM(amount),0) c FROM sales WHERE biz_date<=:t", {"t": t})["c"].iat[0]
        inv = fetch_df(conn, """
            WITH x AS (
              SELECT SUM(CASE WHEN mv='IN' THEN qty ELSE -qty END) qty,
                     SUM(CASE WHEN mv='IN' THEN qty*price ELSE 0 END) val
              FROM stock_moves WHERE biz_date<=:t
            ) SELECT COALESCE(val,0) v FROM x
        """, {"t": t})["v"].iat[0]
        df = pd.DataFrame({
            "Khoản mục": ["Tiền & tương đương tiền","Hàng tồn kho","Nợ phải trả (giả định 0)","Vốn CSH (đối ứng)"],
            "Giá trị":   [float(cash), float(inv), 0.0, float(cash)+float(inv)]
        })
        st.dataframe(df, use_container_width=True)

    # Lưu chuyển tiền tệ (giản lược)
    with tabs[3]:
        df = fetch_df(conn, """
            SELECT DATE_TRUNC('month', biz_date) m, SUM(amount) cash_in
            FROM sales WHERE biz_date BETWEEN :f AND :t
            GROUP BY 1 ORDER BY 1
        """, {"f": f, "t": t})
        st.line_chart(df.set_index("m")["cash_in"]) if not df.empty else st.info("Chưa có dữ liệu.")

# ---------- TSCĐ ----------
def page_tscd(conn, user):
    st.markdown("### 🧱 Tài sản cố định")
    tabs = st.tabs(["Danh mục TSCĐ","Tính khấu hao"])
    with tabs[0]:
        df = fetch_df(conn, "SELECT code,name,acq_date,cost,life_month,location FROM assets ORDER BY acq_date DESC")
        st.dataframe(df, use_container_width=True, height=360)
        with st.form("fm_asset", clear_on_submit=True):
            code = st.text_input("Mã")
            name = st.text_input("Tên")
            acq  = st.date_input("Ngày mua", value=date.today())
            cost = st.number_input("Nguyên giá", min_value=0.0, value=0.0, step=100000.0)
            life = st.number_input("Thời gian KH (tháng)", min_value=1, value=36, step=1)
            loc  = st.text_input("Vị trí")
            if st.form_submit_button("Lưu", type="primary") and code and name:
                run_sql(conn, """
                    INSERT INTO assets(code,name,acq_date,cost,life_month,location)
                    VALUES (:c,:n,:d,:v,:l,:o)
                    ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name,acq_date=EXCLUDED.acq_date,
                        cost=EXCLUDED.cost,life_month=EXCLUDED.life_month,location=EXCLUDED.location
                """, {"c":code,"n":name,"d":acq,"v":float(cost),"l":int(life),"o":loc})
                st.success("OK"); st.rerun()
    with tabs[1]:
        month = st.date_input("Kỳ tính (lấy ngày bất kỳ trong tháng)", value=date.today())
        df = fetch_df(conn, """
            SELECT code,name,acq_date,cost,life_month,
                   GREATEST(0, LEAST(life_month, EXTRACT(MONTH FROM AGE(:d, acq_date))::int)) AS used,
                   CASE WHEN life_month>0 THEN cost/life_month ELSE 0 END AS monthly
            FROM assets
        """, {"d": month})
        df["kh_dk"] = (df["monthly"] * (df["used"]-1).clip(lower=0)).round(0)
        df["kh_thang"] = df["monthly"].round(0)
        df["kh_lk"] = (df["monthly"] * df["used"]).round(0)
        df["gtcl"] = (df["cost"] - df["kh_lk"]).clip(lower=0).round(0)
        st.dataframe(df[["code","name","cost","life_month","kh_thang","kh_lk","gtcl"]], use_container_width=True, height=420)

# ---------- Cửa hàng ----------
def page_stores(conn, user):
    st.markdown("### 🏪 Cửa hàng")
    df = fetch_df(conn, "SELECT code,name,addr,note FROM stores ORDER BY name")
    st.dataframe(df, use_container_width=True, height=360)
    with st.form("fm_store", clear_on_submit=True):
        code = st.text_input("Mã")
        name = st.text_input("Tên")
        addr = st.text_input("Địa chỉ")
        note = st.text_input("Ghi chú")
        if st.form_submit_button("Lưu", type="primary") and code and name:
            run_sql(conn, """
                INSERT INTO stores(code,name,addr,note) VALUES (:c,:n,:a,:o)
                ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name, addr=EXCLUDED.addr, note=EXCLUDED.note
            """, {"c":code,"n":name,"a":addr,"o":note}); st.success("OK"); st.rerun()
    pick = st.selectbox("Xoá cửa hàng", ["—"]+[r["code"] for _,r in df.iterrows()], index=0)
    if pick!="—" and st.button("Xoá"):
        run_sql(conn, "DELETE FROM stores WHERE code=:c", {"c": pick}); st.rerun()

# ---------- Người dùng ----------
def page_users(conn, user):
    if not has_perm(user,"USER_EDIT") and user.get("role")!="SuperAdmin":
        st.warning("Bạn không có quyền."); return
    st.markdown("### 👤 Người dùng")
    df = fetch_df(conn, "SELECT email,display,role,store_code,perms,created_at FROM users ORDER BY email")
    st.dataframe(df, use_container_width=True, height=360)
    with st.form("fm_user", clear_on_submit=True):
        email = st.text_input("Email")
        display = st.text_input("Tên hiển thị")
        role = st.selectbox("Vai trò", ["User","Admin","SuperAdmin"], index=0)
        store = st.text_input("Mã cửa hàng mặc định", value=user.get("store",""))
        perms = st.text_input("Quyền (phân tách dấu phẩy)", value="")
        pw = st.text_input("Mật khẩu (đặt/đổi)", type="password")
        if st.form_submit_button("Lưu", type="primary") and email:
            if pw:
                run_sql(conn, """
                    INSERT INTO users(email,display,password,role,store_code,perms)
                    VALUES (:e,:d,:p,:r,:s,:m)
                    ON CONFLICT (email) DO UPDATE SET display=EXCLUDED.display, password=EXCLUDED.password,
                        role=EXCLUDED.role, store_code=EXCLUDED.store_code, perms=EXCLUDED.perms
                """, {"e":email,"d":display or email,"p":sha256(pw),"r":role,"s":store,"m":perms})
            else:
                run_sql(conn, """
                    UPDATE users SET display=:d, role=:r, store_code=:s, perms=:m WHERE email=:e
                """, {"e":email,"d":display or email,"r":role,"s":store,"m":perms})
            st.success("OK"); st.rerun()
    pick = st.selectbox("Xoá tài khoản", ["—"]+[r["email"] for _,r in df.iterrows()], index=0)
    if pick!="—" and st.button("Xoá"):
        run_sql(conn, "DELETE FROM users WHERE email=:e", {"e": pick}); st.rerun()

# ---------- Nhật ký ----------
def page_nhatky(conn, user):
    if not has_perm(user,"AUDIT_VIEW") and user.get("role")!="SuperAdmin":
        st.warning("Bạn không có quyền."); return
    st.markdown("### 🗒️ Nhật ký")
    df = fetch_df(conn, "SELECT ts,actor,action,detail FROM syslog ORDER BY ts DESC LIMIT 500")
    st.dataframe(df, use_container_width=True, height=480)

# =========================================================
# ---------------- ROUTER DUY NHẤT ----------------
def router():
    conn = get_conn()
    user = require_login(conn)
    header_top(conn, user)
    store_selector(conn, user)

    candidates = [
        ("Dashboard",  "page_dashboard"),
        ("Danh mục",   "page_catalog"),
        ("Kho",        "page_kho"),
        ("Sản xuất",   "page_sanxuat"),
        ("Doanh thu",  "page_doanhthu"),
        ("Báo cáo",    "page_baocao"),
        ("TSCĐ",       "page_tscd"),
        ("Nhật ký",    "page_nhatky"),
        ("Cửa hàng",   "page_stores"),
        ("Người dùng", "page_users"),
    ]
    visible = [(label, fn) for (label, fn) in candidates if callable(globals().get(fn))]
    labels = [label for (label, _) in visible]
    st.sidebar.markdown("### 📌 Chức năng")
    choice = st.sidebar.radio("", labels, index=0, label_visibility="collapsed")

    for label, fn in visible:
        if label == choice:
            globals()[fn](conn, user)
            break

# ---------------- ENTRY ----------------
if __name__ == "__main__":
    router()
