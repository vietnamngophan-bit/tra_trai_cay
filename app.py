# =============================================
# app.py v5 – Fruit Tea ERP (SQLite / Supabase)
# =============================================

import os, re, io, zipfile, json, datetime
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection as _SAConnection
import json as jsonlib

st.set_page_config(
    page_title="Fruit Tea ERP v5",
    page_icon="🍹",
    layout="wide"
)

# AI nâng cao
from sentence_transformers import SentenceTransformer
import faiss
import numpy as np

# ====================================================
# DB BRIDGE (SQLite local <-> Supabase/Postgres online)
# ====================================================

# ==========================
# Postgres-only (Supabase) DB layer
# ==========================
_ENGINE = None

# --- ép dùng pooler + ssl, và chuẩn hoá scheme ---
def _force_pooler(url: str) -> str:
    """
    Chuẩn hoá postgres URL cho SQLAlchemy + chuyển host sang Session Pooler của Supabase
    và thêm sslmode=require.
    - Hỗ trợ đầu vào: postgres://..., postgresql://..., postgresql+psycopg2://...
    - Trả về: postgresql+psycopg2://user:pass@aws-1-ap-southeast-1.pooler.supabase.com:6543/db?sslmode=require
    """
    # 1) scheme → SQLAlchemy
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)

    try:
        from urllib.parse import urlparse, urlunparse, quote_plus
        p = urlparse(url)

        # 2) nếu là host *.supabase.co → ép sang pooler
        host = (p.hostname or "").lower()
        if host.endswith(".supabase.co"):
            pooler = "aws-1-ap-southeast-1.pooler.supabase.com"
            user = p.username or ""
            pw = quote_plus(p.password) if p.password else None
            creds = user if user else ""
            if pw is not None:
                creds += f":{pw}"
            if creds:
                creds += "@"
            netloc = f"{creds}{pooler}:6543"
            p = p._replace(netloc=netloc)

        # 3) chắc chắn có sslmode=require
        q = p.query or ""
        if "sslmode=" not in q:
            q = (q + "&" if q else "") + "sslmode=require"
        p = p._replace(query=q)

        return urlunparse(p)
    except Exception:
        # Nếu có lỗi parse vẫn trả về url cũ (đã chuẩn hoá scheme ở trên)
        return url

def get_conn():
    """
    Luôn trả về kết nối Postgres (SQLAlchemy connection).
    YÊU CẦU: đặt DATABASE_URL trong Streamlit Secrets/ENV.
    Gợi ý value (dùng trực tiếp URI Pooler của Supabase):
      postgresql://postgres:<PASSWORD>@aws-1-ap-southeast-1.pooler.supabase.com:6543/postgres?sslmode=require
    Hoặc dán URI primary, hàm này sẽ tự ép sang pooler.
    """
    global _ENGINE
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL is not set. Add it in Streamlit Secrets.")

    url = _force_pooler(url)
    if _ENGINE is None:
        _ENGINE = create_engine(url, pool_pre_ping=True, future=True)
    return _ENGINE.connect()

# --- hỗ trợ chuyển dấu hỏi (?) → tham số đặt tên (:p1, :p2, ...) khi chạy trên Postgres ---
def _qmark_to_named(sql: str, params):
    if not isinstance(params, (list, tuple)):
        return sql, (params or {})
    idx = 1
    def repl(_):
        nonlocal idx
        s = f":p{idx}"
        idx += 1
        return s
    sql2 = re.sub(r"\?", repl, sql)
    params2 = {f"p{i+1}": v for i, v in enumerate(params)}
    return sql2, params2

# --- vá pandas.read_sql_query để chấp nhận chuỗi + params list/tuple khi dùng Postgres ---
_ORIG_PD_READ = pd.read_sql_query
def _pd_read_sql_query_any(sql, conn, params=None, *args, **kwargs):
    # conn là SQLAlchemy Connection (Postgres)
    if isinstance(sql, str):
        if isinstance(params, (list, tuple)):
            sql, params = _qmark_to_named(sql, params)
        return _ORIG_PD_READ(text(sql), conn, params=params or {}, *args, **kwargs)
    # fallback (không nên chạy tới nhánh này với Postgres)
    return _ORIG_PD_READ(sql, conn, params=params, *args, **kwargs)
pd.read_sql_query = _pd_read_sql_query_any

# --- vá Connection.execute để nhận chuỗi SQL + auto chuyển ? →
_ORIG_SA_EXEC = _SAConnection.execute
def _sa_exec_auto(self, statement, *multiparams, **kwargs):
    if isinstance(statement, str):
        # Chuyển "INSERT OR REPLACE" (SQLite style) → UPSERT trên PG nếu bạn còn dùng ở đâu đó
        up = statement.upper()
        if "INSERT OR REPLACE" in up:
            stmt = statement.replace("INSERT OR REPLACE", "INSERT")
            m = re.search(r"INSERT\s+INTO\s+(\w+)", stmt, re.I)
            if m:
                table = m.group(1).lower()
                conflict = "code" if table not in ["wip_cost"] else "batch_id"
                cols = re.findall(r"\((.*?)\)", stmt)[0].split(",")
                sets = [f"{c.strip()}=EXCLUDED.{c.strip()}" for c in cols if c.strip() != conflict]
                statement = stmt + f" ON CONFLICT ({conflict}) DO UPDATE SET " + ", ".join(sets)

        # Nếu đối số params truyền kiểu positional list/tuple → đổi sang đặt tên
        if multiparams and isinstance(multiparams[0], (list, tuple)):
            sql, params = _qmark_to_named(statement, multiparams[0])
            return _ORIG_SA_EXEC(self, text(sql), params)

        return _ORIG_SA_EXEC(self, text(statement), **kwargs)
    return _ORIG_SA_EXEC(self, statement, *multiparams, **kwargs)
_SAConnection.execute = _sa_exec_auto

# --- helpers ngắn gọn ---
def run_sql(conn, sql, params=None):
    if isinstance(params, (list, tuple)):
        sql, params = _qmark_to_named(sql, params)
    res = conn.execute(text(sql) if isinstance(sql, str) else sql, params or {})
    try: conn.commit()
    except: pass
    return res

def fetch_df(conn, sql, params=None):
    return pd.read_sql_query(sql, conn, params=params or {})


def avg_cost(conn, store, pcode):
    dfc = fetch_df(conn, """
        SELECT kind,qty,price_in FROM inventory_ledger
        WHERE store=? AND pcode=? ORDER BY ts
    """, (store, pcode))
    stock = 0.0; cost = 0.0
    for _, r in dfc.iterrows():
        if r["kind"] == "IN":
            q = float(r["qty"] or 0); p = float(r["price_in"] or 0)
            if q > 0:
                total = cost*stock + p*q
                stock += q
                cost = (total/stock) if stock>0 else 0.0
        else:
            stock -= float(r["qty"] or 0)
            if stock < 0: stock = 0.0
    return cost
# ==== END DB BRIDGE ====conn = get_conn()
import streamlit as st
st.caption("DB: " + ("Postgres" if os.getenv("DATABASE_URL") else "SQLite"))

# --- Helpers cho dropdown sản phẩm ---
def _prod_options(conn, cat=None):
    """
    Trả về danh sách options dạng [(code, "CODE – Tên"), ...]
    - cat: lọc theo cat_code nếu truyền vào (TRAI_CAY | PHU_GIA | COT | MUT)
    """
    if cat:
        df = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code=? ORDER BY code", (cat,))
    else:
        df = fetch_df(conn, "SELECT code,name FROM products ORDER BY cat_code, code")
    if df.empty:
        return []
    return [(r["code"], f'{r["code"]} – {r["name"]}') for _, r in df.iterrows()]

def _opt_index(options, code, default=0):
    """Tìm index theo code trong options [(code,label),...] để set default cho selectbox."""
    for i, (c, _) in enumerate(options):
        if c == code:
            return i
    return default

# quyền
def has_perm(user, code):
    # SuperAdmin: full quyền
    if (user.get("role") or "").lower() == "superadmin":
        return True
    p = (user.get("perms") or "")
    return code in p.split(",")


def guard_perm(user, code, msg="Bạn không có quyền truy cập mục này."):
    if not has_perm(user, code):
        st.warning(msg)
        st.stop()
# --- Lấy map cups/kg cho TP (COT, MUT) ---
def cups_map(conn):
    df = fetch_df(conn, """
        SELECT output_pcode AS pcode, MAX(cups_per_kg) AS cups_per_kg
        FROM formulas
        WHERE type IN ('COT','MUT') AND output_pcode IS NOT NULL
        GROUP BY output_pcode
    """)
    return {r["pcode"]: float(r["cups_per_kg"] or 0.0) for _, r in df.iterrows()}

# --- Tính tồn kho theo mã, kèm đơn giá bình quân ---
def stock_df(conn, store, from_date=None, to_date=None):
    # lọc khoảng ngày nếu cần (tồn đến ngày -> to_date)
    cond = ["store=?"]; params=[store]
    if to_date: 
        cond.append("date(ts) <= ?"); params.append(str(to_date))
    if from_date:
        cond.append("date(ts) >= ?"); params.append(str(from_date))
    where = " AND ".join(cond)

    df = fetch_df(conn, f"""
        WITH m AS (
          SELECT pcode,
                 SUM(CASE WHEN kind='IN'  THEN qty ELSE -qty END) AS ton,
                 SUM(CASE WHEN kind='IN'  THEN qty*COALESCE(price_in,0) ELSE 0 END) AS cost_in
          FROM inventory_ledger
          WHERE {where}
          GROUP BY pcode
        ),
        last_in AS (
          SELECT pcode,
                 MAX(CASE WHEN kind='IN' THEN ts END) AS last_ts
          FROM inventory_ledger
          WHERE {where}
          GROUP BY pcode
        )
        SELECT p.code, p.name, p.cat_code,
               COALESCE(m.ton,0) AS ton,
               CASE WHEN COALESCE(m.ton,0)>0
                    THEN ROUND(COALESCE(m.cost_in,0)/NULLIF(m.ton,0),2)
                    ELSE 0 END AS avg_price
        FROM products p
        LEFT JOIN m ON m.pcode=p.code
        WHERE COALESCE(m.ton,0)<>0 OR p.cat_code IN ('COT','MUT')
        ORDER BY p.code
    """, tuple(params))

    # tính số cốc
    cmap = cups_map(conn)
    df["cups_per_kg"] = df["code"].map(lambda c: cmap.get(c, 0.0))
    df["cups"] = (df["ton"].astype(float) * df["cups_per_kg"].astype(float)).round(2)
    df["value"] = (df["ton"].astype(float) * df["avg_price"].astype(float)).round(0)
    return df

# ==========================
# Auth & session
# ==========================
def login_form():
    st.subheader("Đăng nhập hệ thống")
    email = st.text_input("Email")
    pw = st.text_input("Mật khẩu", type="password")
    if st.button("Đăng nhập"):
        with get_conn() as conn:
            df = fetch_df(conn, "SELECT * FROM users WHERE email=?", (email,))
            if not df.empty and df.iloc[0]["password"] == pw:
                st.session_state["user"] = dict(df.iloc[0])
                st.experimental_rerun()
            else:
                st.error("Sai tài khoản hoặc mật khẩu")

def require_login():
    if "user" not in st.session_state:
        login_form()
        st.stop()
    return st.session_state["user"]

# ==========================
# Sidebar
# ==========================
def sidebar_menu(user):
    st.sidebar.title("Menu")
    st.sidebar.write(f"👤 {user['display']} ({user['role']})")

    # danh sách cửa hàng
    with get_conn() as conn:
        stores = fetch_df(conn, "SELECT code,name FROM stores ORDER BY code")
    cur_store = st.sidebar.selectbox("Cửa hàng", stores["code"] if not stores.empty else ["HOSEN"])
    st.session_state["store"] = cur_store

    items = []
    if has_perm(user, "KHO"): items.append("Kho")
    if has_perm(user, "SANXUAT"): items.append("Sản xuất")
    if has_perm(user, "DANHMUC"): items.append("Danh mục")
    if has_perm(user, "DOANHTHU"): items.append("Doanh thu")
    if has_perm(user, "BAOCAO"): items.append("Báo cáo")
    if has_perm(user, "TSCD"): items.append("Tài sản cố định")
    if has_perm(user, "TAICHINH"): items.append("Tài chính")
    if has_perm(user, "USERS"): items.append("Người dùng")
    if user["role"] == "SuperAdmin": items.append("Cửa hàng")
    items += ["Sao lưu/Phục hồi", "Xuất báo cáo", "AI hỏi đáp", "Đổi mật khẩu"]

    choice = st.sidebar.radio("Chức năng", items)
    if st.sidebar.button("Đăng xuất"):
        st.session_state.pop("user")
        st.experimental_rerun()
    return choice

# ==========================
# Quản lý Cửa hàng (SuperAdmin)
# ==========================
def page_stores(conn):
    user = st.session_state["user"]
    if user["role"] != "SuperAdmin":
        st.warning("Chỉ SuperAdmin mới được quản lý cửa hàng.")
        return

    st.subheader("🏪 Quản lý cửa hàng")
    df = fetch_df(conn, "SELECT * FROM stores ORDER BY code")
    st.dataframe(df, use_container_width=True)

    with st.form("store_form"):
        code = st.text_input("Mã cửa hàng")
        name = st.text_input("Tên cửa hàng")
        addr = st.text_input("Địa chỉ")
        note = st.text_input("Ghi chú", "")
        allow = st.checkbox("Cho phép sản xuất", True)
        if st.form_submit_button("Lưu"):
            run_sql(conn, """INSERT OR REPLACE INTO stores(code,name,address,note,allow_production)
                             VALUES(?,?,?,?,?)""",
                    (code, name, addr, note, allow))
            st.success("Đã lưu cửa hàng")
            st.experimental_rerun()

    del_store = st.text_input("Mã cửa hàng cần xóa")
    if st.button("Xóa cửa hàng"):
        run_sql(conn, "DELETE FROM stores WHERE code=?", (del_store,))
        st.success("Đã xóa")
        st.experimental_rerun()

# ==========================
# Quản lý Người dùng (CRUD)
# ==========================
def page_users(conn):
    guard_perm(st.session_state["user"], "USERS")
    st.subheader("👥 Người dùng")

    df = fetch_df(conn, "SELECT email,display,role,store_code,perms FROM users ORDER BY email")
    st.dataframe(df, use_container_width=True)

    st.markdown("### Thêm/Sửa user")
    with st.form("user_form"):
        email = st.text_input("Email")
        display = st.text_input("Tên hiển thị")
        pw = st.text_input("Mật khẩu (để trống nếu không đổi)")
        role = st.selectbox("Vai trò", ["User","Admin","SuperAdmin"])
        store = st.text_input("Cửa hàng mặc định", st.session_state.get("store","HOSEN"))
        perms_all = ["KHO","SANXUAT","DANHMUC","DOANHTHU","BAOCAO","USERS","TSCD","TAICHINH","CT_EDIT"]
        perms = st.multiselect("Quyền", perms_all, default=["KHO","SANXUAT"])
        if st.form_submit_button("Lưu"):
            if pw.strip():
                run_sql(conn, """INSERT OR REPLACE INTO users(email,display,password,role,store_code,perms)
                                 VALUES(?,?,?,?,?,?)""",
                        (email,display,pw,role,store,",".join(perms)))
            else:
                old = fetch_df(conn, "SELECT password FROM users WHERE email=?", (email,))
                pwd = old.iloc[0]["password"] if not old.empty else ""
                run_sql(conn, """INSERT OR REPLACE INTO users(email,display,password,role,store_code,perms)
                                 VALUES(?,?,?,?,?,?)""",
                        (email,display,pwd,role,store,",".join(perms)))
            st.success("Đã lưu user")
            st.experimental_rerun()

    st.markdown("### Xóa user")
    del_u = st.text_input("Email cần xóa")
    if st.button("Xóa user"):
        run_sql(conn, "DELETE FROM users WHERE email=?", (del_u,))
        st.success("Đã xóa user")
        st.experimental_rerun()

# ==========================
# Đổi mật khẩu
# ==========================
def page_change_password(conn):
    st.subheader("🔐 Đổi mật khẩu")
    user = st.session_state["user"]
    old = st.text_input("Mật khẩu cũ", type="password")
    new1 = st.text_input("Mật khẩu mới", type="password")
    new2 = st.text_input("Nhập lại mật khẩu mới", type="password")
    if st.button("Đổi mật khẩu"):
        cur = fetch_df(conn, "SELECT password FROM users WHERE email=?", (user["email"],))
        if cur.empty or cur.iloc[0]["password"] != old:
            st.error("Mật khẩu cũ không đúng")
        elif new1 != new2 or not new1:
            st.error("Mật khẩu mới không khớp / rỗng")
        else:
            run_sql(conn, "UPDATE users SET password=? WHERE email=?", (new1, user["email"]))
            st.success("Đã đổi mật khẩu")
# ==========================
# Kho (phiếu Nhập – Xuất – Tồn, lọc nâng cao)
# ==========================
def page_kho(conn):
    guard_perm(st.session_state["user"],"KHO")
    store = st.session_state["store"]
    st.subheader(f"📦 Quản lý kho – {store}")
    tab_in, tab_out, tab_ton = st.tabs(["Phiếu nhập","Phiếu xuất","Tồn kho"])

    # --- Phiếu nhập ---
    with tab_in:
        all_opts = _prod_options(conn)  # tất cả SP
        codes = [c for c, _ in all_opts]
        labels = [l for _, l in all_opts]
        with st.form("rcp_in_v2"):
            ts = st.date_input("Ngày nhập", datetime.date.today())
            idx = st.selectbox("Sản phẩm nhập", range(len(labels)), format_func=lambda i: labels[i])
            pcode = codes[idx]
            qty = st.number_input("Số lượng", 0.0, step=0.1)
            price = st.number_input("Đơn giá nhập (VNĐ/đvt)", 0.0, step=1000.0)
            note = st.text_input("Ghi chú","")
            ok = st.form_submit_button("Lưu phiếu nhập")
        if ok:
            run_sql(conn,"INSERT INTO receipt_in(ts,store,pcode,qty,unit_cost,note) VALUES(?,?,?,?,?,?)",
                    (ts,store,pcode,qty,price,note))
            run_sql(conn,"INSERT INTO inventory_ledger(store,pcode,ts,kind,qty,price_in,note) VALUES(?,?,?,?,?,?,?)",
                    (store,pcode,ts,"IN",qty,price,"Phiếu nhập"))
            st.success(f"Đã nhập {pcode}")

    # --- Phiếu xuất ---
    with tab_out:
        all_opts = _prod_options(conn)
        codes = [c for c, _ in all_opts]
        labels = [l for _, l in all_opts]
        with st.form("rcp_out_v2"):
            ts = st.date_input("Ngày xuất", datetime.date.today())
            idx = st.selectbox("Sản phẩm xuất", range(len(labels)), format_func=lambda i: labels[i])
            pcode = codes[idx]
            qty = st.number_input("Số lượng", 0.0, step=0.1)
            note = st.text_input("Ghi chú","")
            ok = st.form_submit_button("Lưu phiếu xuất")
        if ok:
            run_sql(conn,"INSERT INTO receipt_out(ts,store,pcode,qty,note) VALUES(?,?,?,?,?)",
                    (ts,store,pcode,qty,note))
            run_sql(conn,"INSERT INTO inventory_ledger(store,pcode,ts,kind,qty,note) VALUES(?,?,?,?,?,?)",
                    (store,pcode,ts,"OUT",qty,"Phiếu xuất"))
            st.success(f"Đã xuất {pcode}")

    # --- Tồn kho & lọc nâng cao ---
    with tab_ton:
        st.markdown("### Bộ lọc nâng cao")
        with st.form("inv_filter"):
            d1 = st.date_input("Từ ngày", datetime.date.today().replace(day=1))
            d2 = st.date_input("Đến ngày", datetime.date.today())
            cat = st.multiselect("Nhóm", ["TRAI_CAY","PHU_GIA","COT","MUT"])
            kw = st.text_input("Mã/Tên chứa ...","")
            st.form_submit_button("Lọc")
        cond = "WHERE l.store=? AND l.ts BETWEEN ? AND ?"
        params = [store,d1,d2]
        if cat:
            cond += f" AND p.cat_code IN ({','.join(['?']*len(cat))})"; params+=cat
        if kw:
            cond += " AND (p.code LIKE ? OR p.name LIKE ?)"; params += [f"%{kw}%",f"%{kw}%"]
        df = fetch_df(conn, f"""
            SELECT p.code, p.name, p.cat_code,
                   COALESCE(SUM(CASE WHEN l.kind='IN' THEN l.qty ELSE -l.qty END),0) AS ton
            FROM products p LEFT JOIN inventory_ledger l
              ON l.pcode=p.code
            {cond}
            GROUP BY p.code,p.name,p.cat_code ORDER BY p.cat_code,p.code
        """, params)
        if df.empty:
            st.info("Không có dữ liệu.")
        else:
            df["avg_price"] = df["code"].apply(lambda c: avg_cost(conn, store, c))
            df["Giá trị tồn"] = df["ton"] * df["avg_price"]
            st.dataframe(df, use_container_width=True)

# ==========================
# Sản xuất (CỐT, Mứt từ trái cây, Mứt từ CỐT, Công thức)
# ==========================
def page_sanxuat(conn):
    guard_perm(st.session_state["user"], "SANXUAT")
    st.subheader("🏭 Sản xuất")
    store = st.session_state["store"]

    def _avg_cost(store_code, pcode):
        dfc = fetch_df(conn, """
            SELECT kind,qty,price_in FROM inventory_ledger
            WHERE store=? AND pcode=? ORDER BY ts, id
        """, (store_code, pcode))
        stock=0.0; cost=0.0
        for _,r in dfc.iterrows():
            if r["kind"]=="IN":
                q=float(r["qty"] or 0); p=float(r["price_in"] or 0)
                if q>0:
                    total=cost*stock + p*q; stock+=q
                    cost=(total/stock) if stock>0 else 0.0
            else:
                stock-=float(r["qty"] or 0)
                if stock<0: stock=0.0
        return cost

    tab_cot, tab_mut_fruit, tab_mut_cot, tab_ct = st.tabs(
        ["Thành phẩm (CỐT)", "Mứt từ trái cây", "Mứt từ CỐT", "Công thức"]
    )

    # ===== CỐT (1 bước, CÓ hệ số thu hồi) =====
    with tab_cot:
        st.markdown("#### CỐT – Xuất NVL + phụ gia ⇒ Nhập TP (có hệ số thu hồi)")
        f = fetch_df(conn, "SELECT * FROM formulas WHERE type='COT' ORDER BY code")
        if f.empty:
            st.info("Chưa có công thức CỐT.")
        else:
            f_sel = st.selectbox("Chọn công thức CỐT", f["code"])
            frow = f[f["code"]==f_sel].iloc[0]

            fruits = [c for c in (frow["fruits_csv"] or "").split(",") if c]
            cols = st.columns(max(1, min(3, max(1,len(fruits)))))
            raw_inputs = {}
            for i, pcode in enumerate(fruits):
                pname = fetch_df(conn, "SELECT name FROM products WHERE code=?", (pcode,))
                label = f"{pcode} – {(pname.iloc[0]['name'] if not pname.empty else '')}"
                with cols[i % len(cols)]:
                    raw_inputs[pcode] = st.number_input(label, 0.0, step=0.1, key=f"cot_{pcode}")

            kg_after = st.number_input("Tổng KG sau sơ chế", 0.0, step=0.1, key="cot_after")
            try:
                adds = jsonlib.loads(frow["additives_json"] or "{}")
            except:
                adds = {}
                st.warning("Phụ gia JSON không hợp lệ → xem như rỗng.")

            if st.checkbox("Xem khối lượng phụ gia sẽ xuất", True):
                df_add = pd.DataFrame([{"Mã":k,"SL (kg)":v*kg_after} for k,v in adds.items()]) if adds else pd.DataFrame()
                st.dataframe(df_add, use_container_width=True)

            if st.button("➕ Thực hiện SX CỐT"):
                total_cost = 0.0
                # Xuất trái cây
                for pcode, kg in raw_inputs.items():
                    if kg<=0: continue
                    avg = _avg_cost(store, pcode)
                    run_sql(conn, "INSERT INTO inventory_ledger(store,pcode,ts,kind,qty,note) VALUES(?,?,?,?,?,?)",
                            (store, pcode, datetime.datetime.now(), "OUT", kg, f"Xuất NVL COT {f_sel}"))
                    total_cost += kg * (avg or 0)
                # Xuất phụ gia
                for pcode, ratio in adds.items():
                    qty = ratio * kg_after
                    if qty<=0: continue
                    avg = _avg_cost(store, pcode)
                    run_sql(conn, "INSERT INTO inventory_ledger(store,pcode,ts,kind,qty,note) VALUES(?,?,?,?,?,?)",
                            (store, pcode, datetime.datetime.now(), "OUT", qty, f"Xuất PG COT {f_sel}"))
                    total_cost += qty * (avg or 0)

                recovery = float(frow["recovery"] or 1.0)
                cups_per = float(frow["cups_per_kg"] or 0.0)
                kg_out = kg_after * recovery
                cups = kg_out * cups_per
                unit_cost = (total_cost / kg_out) if kg_out>0 else 0.0

                run_sql(conn, "INSERT INTO inventory_ledger(store,pcode,ts,kind,qty,price_in,cups,note) VALUES(?,?,?,?,?,?,?,?)",
                        (store, frow["output_pcode"], datetime.datetime.now(), "IN", kg_out, unit_cost, cups, f"Nhập TP COT {f_sel}"))
                run_sql(conn, """INSERT INTO prod_log(ts,store,kind,fcode,fname,raw_json,kg_after,additives_json,kg_output,cups,status,user_email,note,batch_id)
                                 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (datetime.datetime.now(), store, "COT", frow["code"], frow["name"],
                         jsonlib.dumps(raw_inputs), kg_after, jsonlib.dumps({k: v*kg_after for k,v in adds.items()}),
                         kg_out, cups, "HOANTHANH", st.session_state["user"]["email"], "", None))
                st.success(f"✅ Đã nhập {kg_out} kg {frow['output_pcode']} (đơn giá {unit_cost:,.0f}đ/kg).")

    # ===== MỨT từ TRÁI CÂY (2 bước, KHÔNG recovery) =====
    with tab_mut_fruit:
        st.markdown("#### Mứt từ trái cây – Lô TẠM (xuất NVL) → Hoàn thành (nhập TP có giá)")
        f_all = fetch_df(conn, "SELECT * FROM formulas WHERE type='MUT' AND note LIKE 'SRC=TRAI_CAY%' ORDER BY code")
        if f_all.empty:
            st.info("Chưa có CT MỨT trái cây.")
        else:
            f_sel = st.selectbox("Chọn CT MỨT (trái cây)", f_all["code"], key="mutf_sel")
            frow = f_all[f_all["code"]==f_sel].iloc[0]

            fruits = [c for c in (frow["fruits_csv"] or "").split(",") if c]
            cols = st.columns(max(1, min(3, max(1,len(fruits)))))
            raw_inputs = {}
            for i, pcode in enumerate(fruits):
                pname = fetch_df(conn, "SELECT name FROM products WHERE code=?", (pcode,))
                label = f"{pcode} – {(pname.iloc[0]['name'] if not pname.empty else '')}"
                with cols[i % len(cols)]:
                    raw_inputs[pcode] = st.number_input(label, 0.0, step=0.1, key=f"mutf_{pcode}")
            kg_after = st.number_input("Tổng KG sau sơ chế", 0.0, step=0.1, key="mutf_after")

            try:
                adds = jsonlib.loads(frow["additives_json"] or "{}")
            except:
                adds = {}

            if st.button("➕ Tạo lô TẠM (xuất NVL)"):
                total_cost = 0.0
                # Xuất trái cây
                for pcode, kg in raw_inputs.items():
                    if kg<=0: continue
                    avg = _avg_cost(store, pcode)
                    run_sql(conn, "INSERT INTO inventory_ledger(store,pcode,ts,kind,qty,note) VALUES(?,?,?,?,?,?)",
                            (store, pcode, datetime.datetime.now(), "OUT", kg, f"WIP MUT-FRUIT {f_sel}"))
                    total_cost += kg * (avg or 0)
                # Xuất phụ gia
                for pcode, ratio in adds.items():
                    qty = ratio * kg_after
                    if qty<=0: continue
                    avg = _avg_cost(store, pcode)
                    run_sql(conn, "INSERT INTO inventory_ledger(store,pcode,ts,kind,qty,note) VALUES(?,?,?,?,?,?)",
                            (store, pcode, datetime.datetime.now(), "OUT", qty, f"WIP PG MUT-FRUIT {f_sel}"))
                    total_cost += qty * (avg or 0)

                batch_id = f"WIP-MUTF-{int(datetime.datetime.now().timestamp())}"
                run_sql(conn, """INSERT OR REPLACE INTO wip_cost(batch_id,store,fcode,cost,cups_per_kg,output_pcode)
                                 VALUES(?,?,?,?,?,?)""",
                        (batch_id, store, frow["code"], total_cost, float(frow["cups_per_kg"] or 0.0), frow["output_pcode"]))
                run_sql(conn, """INSERT INTO prod_log(ts,store,kind,fcode,fname,raw_json,kg_after,additives_json,kg_output,cups,status,user_email,note,batch_id)
                                 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (datetime.datetime.now(), store, "MUT_FRUIT", frow["code"], frow["name"],
                         jsonlib.dumps(raw_inputs), kg_after, jsonlib.dumps({k: v*kg_after for k,v in adds.items()}),
                         0, 0, "WIP", st.session_state["user"]["email"], "", batch_id))
                st.success(f"✅ Đã tạo lô TẠM: {batch_id}")

            st.markdown("**Hoàn thành lô MỨT (trái cây)**")
            dfw = fetch_df(conn, "SELECT batch_id,fcode,fname,kg_after FROM prod_log WHERE status='WIP' AND store=? AND kind='MUT_FRUIT' ORDER BY id DESC", (store,))
            if not dfw.empty:
                batch = st.selectbox("Chọn lô", dfw["batch_id"], key="mutf_batch")
                kg_tp = st.number_input("KG thành phẩm nhập kho", 0.0, step=0.1, key="mutf_kg")
                if st.button("✅ Hoàn thành (trái cây)"):
                    w = fetch_df(conn, "SELECT * FROM wip_cost WHERE batch_id=?", (batch,))
                    if not w.empty:
                        w = w.iloc[0]
                        unit_cost = (float(w["cost"] or 0)/kg_tp) if kg_tp>0 else 0.0
                        cups = kg_tp * float(w["cups_per_kg"] or 0.0)
                        run_sql(conn, "INSERT INTO inventory_ledger(store,pcode,ts,kind,qty,price_in,cups,note) VALUES(?,?,?,?,?,?,?,?)",
                                (store, w["output_pcode"], datetime.datetime.now(), "IN", kg_tp, unit_cost, cups, f"Nhập TP MUT-FRUIT {w['fcode']}"))
                        run_sql(conn, "UPDATE prod_log SET status='HOANTHANH', kg_output=?, cups=? WHERE batch_id=?", (kg_tp, cups, batch))
                        run_sql(conn, "DELETE FROM wip_cost WHERE batch_id=?", (batch,))
                        st.success(f"Đã nhập TP MỨT (trái cây) – đơn giá {unit_cost:,.0f}đ/kg.")

    # ===== MỨT từ CỐT (2 bước, KHÔNG recovery) =====
    with tab_mut_cot:
        st.markdown("#### Mứt từ CỐT – Lô TẠM (xuất CỐT + PG) → Hoàn thành (nhập TP có giá)")
        f_all = fetch_df(conn, "SELECT * FROM formulas WHERE type='MUT' AND note LIKE 'SRC=COT%' ORDER BY code")
        if f_all.empty:
            st.info("Chưa có CT MỨT từ CỐT.")
        else:
            f_sel = st.selectbox("Chọn CT MỨT (từ CỐT)", f_all["code"], key="mutc_sel")
            frow = f_all[f_all["code"]==f_sel].iloc[0]

            fruits = [c for c in (frow["fruits_csv"] or "").split(",") if c]  # ở đây là mã CỐT
            cols = st.columns(max(1, min(3, max(1,len(fruits)))))
            raw_inputs = {}
            for i, pcode in enumerate(fruits):
                pname = fetch_df(conn, "SELECT name FROM products WHERE code=?", (pcode,))
                label = f"{pcode} – {(pname.iloc[0]['name'] if not pname.empty else '')}"
                with cols[i % len(cols)]:
                    raw_inputs[pcode] = st.number_input(label, 0.0, step=0.1, key=f"mutc_{pcode}")
            kg_after = st.number_input("Tổng KG sau sơ chế", 0.0, step=0.1, key="mutc_after")

            try:
                adds = jsonlib.loads(frow["additives_json"] or "{}")
            except:
                adds = {}

            if st.button("➕ Tạo lô TẠM (từ CỐT)"):
                total_cost = 0.0
                for pcode, kg in raw_inputs.items():
                    if kg<=0: continue
                    avg = _avg_cost(store, pcode)
                    run_sql(conn, "INSERT INTO inventory_ledger(store,pcode,ts,kind,qty,note) VALUES(?,?,?,?,?,?)",
                            (store, pcode, datetime.datetime.now(), "OUT", kg, f"WIP MUT-COT {f_sel}"))
                    total_cost += kg * (avg or 0)
                for pcode, ratio in adds.items():
                    qty = ratio * kg_after
                    if qty<=0: continue
                    avg = _avg_cost(store, pcode)
                    run_sql(conn, "INSERT INTO inventory_ledger(store,pcode,ts,kind,qty,note) VALUES(?,?,?,?,?,?)",
                            (store, pcode, datetime.datetime.now(), "OUT", qty, f"WIP PG MUT-COT {f_sel}"))
                    total_cost += qty * (avg or 0)

                batch_id = f"WIP-MUTC-{int(datetime.datetime.now().timestamp())}"
                run_sql(conn, """INSERT OR REPLACE INTO wip_cost(batch_id,store,fcode,cost,cups_per_kg,output_pcode)
                                 VALUES(?,?,?,?,?,?)""",
                        (batch_id, store, frow["code"], total_cost, float(frow["cups_per_kg"] or 0.0), frow["output_pcode"]))
                run_sql(conn, """INSERT INTO prod_log(ts,store,kind,fcode,fname,raw_json,kg_after,additives_json,kg_output,cups,status,user_email,note,batch_id)
                                 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (datetime.datetime.now(), store, "MUT_COT", frow["code"], frow["name"],
                         jsonlib.dumps(raw_inputs), kg_after, jsonlib.dumps({k: v*kg_after for k,v in adds.items()}),
                         0, 0, "WIP", st.session_state["user"]["email"], "", batch_id))
                st.success(f"✅ Đã tạo lô TẠM: {batch_id}")

            st.markdown("**Hoàn thành lô MỨT (từ CỐT)**")
            dfw = fetch_df(conn, "SELECT batch_id,fcode,fname,kg_after FROM prod_log WHERE status='WIP' AND store=? AND kind='MUT_COT' ORDER BY id DESC", (store,))
            if not dfw.empty:
                batch = st.selectbox("Chọn lô", dfw["batch_id"], key="mutc_batch")
                kg_tp = st.number_input("KG thành phẩm nhập kho", 0.0, step=0.1, key="mutc_kg")
                if st.button("✅ Hoàn thành (từ CỐT)"):
                    w = fetch_df(conn, "SELECT * FROM wip_cost WHERE batch_id=?", (batch,))
                    if not w.empty:
                        w = w.iloc[0]
                        unit_cost = (float(w["cost"] or 0)/kg_tp) if kg_tp>0 else 0.0
                        cups = kg_tp * float(w["cups_per_kg"] or 0.0)
                        run_sql(conn, "INSERT INTO inventory_ledger(store,pcode,ts,kind,qty,price_in,cups,note) VALUES(?,?,?,?,?,?,?,?)",
                                (store, w["output_pcode"], datetime.datetime.now(), "IN", kg_tp, unit_cost, cups, f"Nhập TP MUT-COT {w['fcode']}"))
                        run_sql(conn, "UPDATE prod_log SET status='HOANTHANH', kg_output=?, cups=? WHERE batch_id=?", (kg_tp, cups, batch))
                        run_sql(conn, "DELETE FROM wip_cost WHERE batch_id=?", (batch,))
                        st.success(f"Đã nhập TP MỨT (từ CỐT) – đơn giá {unit_cost:,.0f}đ/kg.")

   # ===== CÔNG THỨC (CRUD; ẩn/khóa recovery khi là MỨT) =====
    with tab_ct:
        import json
        if not has_perm(st.session_state["user"], "CT_EDIT"):
            st.warning("Bạn không có quyền truy cập Công thức.")
        else:
            st.markdown("#### Công thức – Thêm / Sửa / Xóa")

            # Options dropdown cho sản phẩm đầu ra
            def _prod_opts(cat_code):
                dfp = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code=? ORDER BY code", (cat_code,))
                if dfp.empty: 
                    return [], []
                codes = dfp["code"].tolist()
                labels = [f"{r['code']} – {r['name']}" for _, r in dfp.iterrows()]
                return codes, labels

            df_ct = fetch_df(conn, "SELECT code,name,type,output_pcode,output_uom,recovery,cups_per_kg,fruits_csv,additives_json,note FROM formulas ORDER BY code")
            st.dataframe(df_ct, use_container_width=True)

            mode = st.radio("Chế độ", ["Tạo mới", "Sửa/Xóa"], horizontal=True)

            # ------------------ TẠO MỚI ------------------
            if mode == "Tạo mới":
                code_ct = st.text_input("Mã CT")
                name = st.text_input("Tên CT")
                typ = st.selectbox("Loại CT", ["COT","MUT"])

                # SP đầu ra: dropdown theo loại
                if typ == "COT":
                    out_codes, out_labels = _prod_opts("COT")
                    out_idx = st.selectbox("SP đầu ra (CỐT)", list(range(len(out_labels))),
                                           format_func=lambda i: out_labels[i]) if out_labels else None
                    outp = out_codes[out_idx] if out_labels else ""
                else:  # MUT
                    out_codes, out_labels = _prod_opts("MUT")
                    out_idx = st.selectbox("SP đầu ra (MỨT)", list(range(len(out_labels))),
                                           format_func=lambda i: out_labels[i]) if out_labels else None
                    outp = out_codes[out_idx] if out_labels else ""

                uom = st.text_input("ĐVT TP", "kg")

                # Recovery: chỉ CỐT mới có
                if typ == "COT":
                    rec = st.number_input("Hệ số thu hồi (CỐT)", min_value=0.0, max_value=1.0, value=1.0, step=0.01, key="rec_cot_new")
                else:
                    rec = 1.0  # MỨT không dùng, cố định 1.0

                cups = st.number_input("Cốc/kg TP", 0.0, step=1.0)

                # Nguồn NVL cho MỨT
                mut_source = None
                if typ == "MUT":
                    mut_source = st.radio("Nguồn NVL (chỉ cho MỨT)", ["TRAI_CAY","COT"], index=0, horizontal=True)

                # NVL chính theo loại/nguồn
                if typ == "COT" or mut_source == "TRAI_CAY":
                    raw_list = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code='TRAI_CAY' ORDER BY code")
                elif mut_source == "COT":
                    raw_list = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code='COT' ORDER BY code")
                else:
                    raw_list = fetch_df(conn, "SELECT code,name FROM products WHERE 1=0")  # trống

                raw_opts = raw_list["code"].tolist() if not raw_list.empty else []
                raw_sel = st.multiselect("Nguyên liệu (mã)", raw_opts)

                # Phụ gia giữ nguyên (bth)
                adds_codes = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code='PHU_GIA' ORDER BY code")
                add_opts = adds_codes["code"].tolist() if not adds_codes.empty else []
                add_sel = st.multiselect("Phụ gia", add_opts)
                add_q = {}
                for c in add_sel:
                    add_q[c] = st.number_input(f"{c} – kg / 1kg sau sơ", 0.0, step=0.1, key=f"add_{c}")

                if st.button("💾 Lưu công thức"):
                    note = f"SRC={mut_source}" if typ=="MUT" else ""
                    run_sql(conn, """INSERT OR REPLACE INTO formulas
                                     (code,name,type,output_pcode,output_uom,recovery,cups_per_kg,fruits_csv,additives_json,note)
                                     VALUES(?,?,?,?,?,?,?,?,?,?)""",
                            (code_ct.strip(), name.strip(), typ, outp, uom,
                             (rec if typ=="COT" else 1.0),
                             cups, ",".join(raw_sel), jsonlib.dumps(add_q), note))
                    st.success("Đã lưu công thức")
                    st.experimental_rerun()

            # ------------------ SỬA / XOÁ ------------------
            else:
                if df_ct.empty:
                    st.info("Chưa có công thức.")
                else:
                    choose = st.selectbox("Chọn CT", df_ct["code"])
                    row = fetch_df(conn, "SELECT * FROM formulas WHERE code=?", (choose,)).iloc[0]

                    name = st.text_input("Tên CT", row["name"])
                    typ = st.selectbox("Loại CT", ["COT","MUT"], index=(0 if row["type"]=="COT" else 1))

                    # SP đầu ra: dropdown theo loại + set default đúng mã đang lưu
                    if typ == "COT":
                        out_codes, out_labels = _prod_opts("COT")
                        try:
                            def_idx = out_codes.index(row["output_pcode"]) if row["output_pcode"] in out_codes else 0
                        except: 
                            def_idx = 0
                        out_idx = st.selectbox("SP đầu ra (CỐT)", list(range(len(out_labels))),
                                               index=def_idx if out_labels else 0,
                                               format_func=lambda i: out_labels[i]) if out_labels else None
                        outp = out_codes[out_idx] if out_labels else ""
                    else:
                        out_codes, out_labels = _prod_opts("MUT")
                        try:
                            def_idx = out_codes.index(row["output_pcode"]) if row["output_pcode"] in out_codes else 0
                        except:
                            def_idx = 0
                        out_idx = st.selectbox("SP đầu ra (MỨT)", list(range(len(out_labels))),
                                               index=def_idx if out_labels else 0,
                                               format_func=lambda i: out_labels[i]) if out_labels else None
                        outp = out_codes[out_idx] if out_labels else ""

                    uom = st.text_input("ĐVT TP", row["output_uom"] or "kg")

                    # Recovery: chỉ CỐT bật, MỨT thì khóa/ẩn (ở đây khóa)
                    rec = st.number_input("Hệ số thu hồi (CỐT)",
                                          min_value=0.0, max_value=1.0,
                                          value=float(row["recovery"] or 1.0),
                                          step=0.01,
                                          disabled=(typ!="COT"),
                                          key="rec_edit")

                    cups = st.number_input("Cốc/kg TP", float(row["cups_per_kg"] or 0.0), step=1.0)

                    # Nguồn NVL cho MỨT
                    mut_source = None
                    if typ=="MUT":
                        src_now = "TRAI_CAY"
                        if (row["note"] or "").startswith("SRC="):
                            src_now = (row["note"] or "").split("=",1)[1]
                        mut_source = st.radio("Nguồn NVL (chỉ MỨT)", ["TRAI_CAY","COT"],
                                              index=(0 if src_now=="TRAI_CAY" else 1), horizontal=True)

                    # NVL theo loại/nguồn
                    if typ == "COT" or mut_source == "TRAI_CAY":
                        raw_list = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code='TRAI_CAY' ORDER BY code")
                    elif mut_source == "COT":
                        raw_list = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code='COT' ORDER BY code")
                    else:
                        raw_list = fetch_df(conn, "SELECT code,name FROM products WHERE 1=0")

                    exists_codes = raw_list["code"].tolist() if not raw_list.empty else []
                    sel_default = [c for c in (row["fruits_csv"] or "").split(",") if c and (c in exists_codes)]
                    raw_sel = st.multiselect("Nguyên liệu (mã)", exists_codes, default=sel_default)

                    # Phụ gia giữ nguyên (bth)
                    try:
                        adds0 = jsonlib.loads(row["additives_json"] or "{}")
                    except:
                        adds0 = {}
                    adds_codes = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code='PHU_GIA' ORDER BY code")
                    exist_adds = adds_codes["code"].tolist() if not adds_codes.empty else []
                    add_sel = st.multiselect("Phụ gia", exist_adds, default=[c for c in adds0.keys() if c in exist_adds])
                    add_q = {}
                    for c in add_sel:
                        add_q[c] = st.number_input(f"{c} – kg / 1kg sau sơ", float(adds0.get(c,0.0)), step=0.1, key=f"add_edit_{c}")

                    colA, colB = st.columns(2)
                    with colA:
                        if st.button("💾 Cập nhật"):
                            note = f"SRC={mut_source}" if typ=="MUT" else ""
                            run_sql(conn, """INSERT OR REPLACE INTO formulas
                                             (code,name,type,output_pcode,output_uom,recovery,cups_per_kg,fruits_csv,additives_json,note)
                                             VALUES(?,?,?,?,?,?,?,?,?,?)""",
                                    (choose, name.strip(), typ, outp, uom,
                                     (rec if typ=="COT" else 1.0),
                                     cups, ",".join(raw_sel), jsonlib.dumps(add_q), note))
                            st.success("Đã cập nhật")
                            st.experimental_rerun()
                    with colB:
                        if st.button("🗑️ Xóa công thức"):
                            run_sql(conn, "DELETE FROM formulas WHERE code=?", (choose,))
                            st.success("Đã xóa công thức")
                            st.experimental_rerun()

# ==========================
# Danh mục sản phẩm (CRUD)
# ==========================
def page_danhmuc(conn):
    guard_perm(st.session_state["user"], "DANHMUC")
    st.subheader("📋 Danh mục sản phẩm")
    df = fetch_df(conn, "SELECT * FROM products ORDER BY code")
    st.dataframe(df, use_container_width=True)

    tab_new, tab_edit, tab_del = st.tabs(["Tạo mới", "Sửa", "Xóa"])

    with tab_new:
        with st.form("prod_new"):
            code = st.text_input("Mã SP")
            name = st.text_input("Tên SP")
            uom = st.text_input("Đơn vị", "kg")
            cat = st.selectbox("Nhóm", ["TRAI_CAY","PHU_GIA","COT","MUT"])
            if st.form_submit_button("💾 Lưu mới"):
                run_sql(conn, "INSERT OR REPLACE INTO products(code,name,uom,cat_code) VALUES(?,?,?,?)",
                        (code,name,uom,cat))
                st.success("Đã lưu sản phẩm")

    with tab_edit:
        if df.empty:
            st.info("Chưa có sản phẩm.")
        else:
            sel = st.selectbox("Chọn SP", df["code"])
            row = df[df["code"]==sel].iloc[0]
            with st.form("prod_edit"):
                name = st.text_input("Tên SP", row["name"])
                uom = st.text_input("Đơn vị", row["uom"] or "kg")
                cat = st.selectbox("Nhóm", ["TRAI_CAY","PHU_GIA","COT","MUT"], index=["TRAI_CAY","PHU_GIA","COT","MUT"].index(row["cat_code"]))
                if st.form_submit_button("💾 Cập nhật"):
                    run_sql(conn, "INSERT OR REPLACE INTO products(code,name,uom,cat_code) VALUES(?,?,?,?)",
                            (sel,name,uom,cat))
                    st.success("Đã cập nhật")

    with tab_del:
        del_code = st.text_input("Mã SP cần xóa")
        if st.button("🗑️ Xóa SP"):
            run_sql(conn, "DELETE FROM products WHERE code=?", (del_code,))
            st.success("Đã xóa SP")


# ==========================
# Doanh thu (CASH / BANK)
# ==========================
def page_doanhthu(conn):
    guard_perm(st.session_state["user"], "DOANHTHU")
    st.subheader("💵 Doanh thu (chỉ nhập SỐ TIỀN + kênh thanh toán)")
    store = st.session_state["store"]

    # --- Nhập thu tiền ---
    with st.form("add_rev_money"):
        ts = st.date_input("Ngày", datetime.date.today())
        amount = st.number_input("Số tiền thu (VNĐ)", 0.0, step=1000.0)
        pay = st.selectbox("Kênh thanh toán", ["CASH","BANK"])
        note = st.text_input("Ghi chú", "")
        ok = st.form_submit_button("Lưu thu tiền")
    if ok:
        # Lưu vào revenue: không quan tâm sản phẩm -> gán pcode='THU', qty=1, unit_price=amount
        run_sql(conn,
            "INSERT INTO revenue(ts,store,pcode,qty,unit_price,pay_method,note) VALUES(?,?,?,?,?,?,?)",
            (ts, store, "THU", 1, amount, pay, note)
        )
        st.success("Đã ghi nhận thu tiền")

    # --- Lọc và xem lịch sử ---
    st.markdown("### Lịch sử thu tiền")
    with st.form("rev_filter"):
        d1 = st.date_input("Từ ngày", datetime.date.today().replace(day=1))
        d2 = st.date_input("Đến ngày", datetime.date.today())
        payf = st.multiselect("Kênh", ["CASH","BANK"], default=["CASH","BANK"])
        submit = st.form_submit_button("Lọc")
    if not payf:
        payf = ["CASH","BANK"]
    qmarks = ",".join(["?"]*len(payf))

    df = fetch_df(conn, f"""
        SELECT ts, store, pay_method,
               note,
               (qty*unit_price) AS amount
        FROM revenue
        WHERE store=? AND ts BETWEEN ? AND ? AND pay_method IN ({qmarks})
        ORDER BY ts DESC
    """, (store, d1, d2, *payf))
    # Tổng theo kênh
    total = df.groupby("pay_method", as_index=False)["amount"].sum() if not df.empty else pd.DataFrame(columns=["pay_method","amount"])
    col1, col2 = st.columns([2,1])
    with col1:
        st.dataframe(df, use_container_width=True)
    with col2:
        st.markdown("**Tổng theo kênh**")
        st.dataframe(total, use_container_width=True)
        st.info(f"**Tổng cộng:** {float(df['amount'].sum() if not df.empty else 0):,.0f} đ")

# ========== TÀI SẢN CỐ ĐỊNH (nâng cao) ==========
def page_tscd(conn):
    guard_perm(st.session_state["user"], "TSCD")
    st.subheader("🏗️ Tài sản cố định (nâng cao)")
    with st.form("add_tscd"):
        name = st.text_input("Tên TSCD")
        group_code = st.selectbox("Nhóm", ["MAY_MOC","PHUONG_TIEN","NHA_XUONG","KHAC"])
        cost = st.number_input("Nguyên giá", 0.0, step=100000.0)
        life = st.number_input("Thời gian KH (tháng)", 0, step=1)
        dep = st.number_input("KH / tháng", 0.0, step=10000.0)
        buy_date = st.date_input("Ngày mua", datetime.date.today())
        if st.form_submit_button("Thêm"):
            run_sql(conn, "INSERT INTO tscd(name,group_code,cost,acc_life_months,dep_per_month,buy_date,acc_depr) VALUES(?,?,?,?,?,?,?)",
                    (name, group_code, cost, life, dep, buy_date, 0))
            st.success("Đã thêm TSCD")
    df = fetch_df(conn, "SELECT * FROM tscd ORDER BY id DESC")
    st.dataframe(df, use_container_width=True)
    agg = fetch_df(conn, "SELECT COALESCE(SUM(dep_per_month),0) dep_month, COALESCE(SUM(acc_depr),0) acc_dep FROM tscd")
    st.info(f"KH tháng: {float(agg.iloc[0]['dep_month']):,.0f} – Lũy kế: {float(agg.iloc[0]['acc_dep']):,.0f}")

# ========== BÁO CÁO ==========
def _avg_cost(store, pcode, conn_obj=None):
    c = conn if conn_obj is None else conn_obj
    dfc = fetch_df(c, """
        SELECT kind,qty,price_in FROM inventory_ledger
        WHERE store=? AND pcode=? ORDER BY ts, id
    """, (store, pcode))
    stock=0.0; cost=0.0
    for _,r in dfc.iterrows():
        if r["kind"]=="IN":
            q=float(r["qty"] or 0); p=float(r["price_in"] or 0)
            if q>0:
                total=cost*stock + p*q; stock+=q
                cost=(total/stock) if stock>0 else 0.0
        else:
            stock-=float(r["qty"] or 0)
            if stock<0: stock=0.0
    return cost

def page_baocao(conn):
    guard_perm(st.session_state["user"],"BAOCAO")
    st.subheader("📑 Báo cáo tồn kho & trị giá")
    store = st.session_state["store"]
    with st.form("bc_filter"):
        d1 = st.date_input("Từ ngày", datetime.date.today().replace(day=1))
        d2 = st.date_input("Đến ngày", datetime.date.today())
        st.form_submit_button("Lọc")

    inv = fetch_df(conn, """
        SELECT p.code,p.name,p.cat_code,
               COALESCE(SUM(CASE WHEN l.kind='IN' THEN l.qty ELSE -l.qty END),0) AS ton
        FROM products p LEFT JOIN inventory_ledger l
          ON l.pcode=p.code AND l.store=? AND l.ts BETWEEN ? AND ?
        GROUP BY p.code,p.name,p.cat_code
        ORDER BY p.cat_code,p.code
    """, (store,d1,d2))

    if inv.empty:
        st.info("Chưa có phát sinh."); 
        return

    # >>> CHỈNH Ở ĐÂY: truyền conn vào avg_cost, dùng default arg để giữ scope trong apply
    inv["avg_cost"] = inv["code"].apply(lambda c, _conn=conn: avg_cost(_conn, store, c))
    inv["amount"] = inv["ton"] * inv["avg_cost"]

    st.dataframe(inv, use_container_width=True)
    grp = inv.groupby("cat_code", as_index=False).agg(ton=("ton","sum"), amount=("amount","sum"))
    st.dataframe(grp, use_container_width=True)

def page_taichinh(conn):
    guard_perm(st.session_state["user"],"TAICHINH")
    st.subheader("📘 Báo cáo tài chính (rút gọn)")
    store = st.session_state["store"]

    inv = fetch_df(conn, """
        SELECT p.code, COALESCE(SUM(CASE WHEN l.kind='IN' THEN l.qty ELSE -l.qty END),0) AS ton
        FROM products p LEFT JOIN inventory_ledger l 
             ON l.pcode=p.code AND l.store=?
        GROUP BY p.code
    """, (store,))

    if inv.empty:
        ton_gia_tri = 0.0
    else:
        # >>> CHỈNH Ở ĐÂY: truyền conn vào avg_cost
        inv["avg_cost"] = inv["code"].apply(lambda c, _conn=conn: avg_cost(_conn, store, c))
        inv["amount"]  = inv["ton"] * inv["avg_cost"]
        ton_gia_tri = float(inv["amount"].sum())

    d1 = datetime.date.today().replace(day=1)
    rev = fetch_df(conn, "SELECT COALESCE(SUM(qty*unit_price),0) amt FROM revenue WHERE store=? AND ts>=?", (store,d1))
    doanh_thu_thang = float(rev.iloc[0]["amt"] or 0.0)

    dep = fetch_df(conn, "SELECT COALESCE(SUM(dep_per_month),0) dep FROM tscd")
    kh = float(dep.iloc[0]["dep"] or 0.0)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("### Cân đối kế toán")
        st.dataframe(pd.DataFrame({
            "Chỉ tiêu": ["Hàng tồn kho (giá vốn)"],
            "Số tiền": [ton_gia_tri]
        }), use_container_width=True)

    with col2:
        st.markdown("### Lưu chuyển tiền tệ")
        st.dataframe(pd.DataFrame({
            "Khoản mục": ["Tiền thu bán hàng (tháng)", "Chi khấu hao (tháng)"],
            "Tiền": [doanh_thu_thang, -kh]
        }), use_container_width=True)

# ========== XUẤT FILE ==========
def page_export(conn):
    st.subheader("📤 Xuất báo cáo")
    store = st.session_state["store"]
    what = st.selectbox("Chọn báo cáo", ["Tồn kho", "Doanh thu", "TSCD"])
    d1 = st.date_input("Từ ngày", datetime.date.today().replace(day=1))
    d2 = st.date_input("Đến ngày", datetime.date.today())
    if what == "Tồn kho":
        df = fetch_df(conn, """
            SELECT p.code,p.name,p.cat_code,
                   COALESCE(SUM(CASE WHEN l.kind='IN' THEN l.qty ELSE -l.qty END),0) AS ton
            FROM products p LEFT JOIN inventory_ledger l
              ON l.pcode=p.code AND l.store=? AND l.ts BETWEEN ? AND ?
            GROUP BY p.code,p.name,p.cat_code ORDER BY p.cat_code,p.code
        """, (store, d1, d2))
    elif what == "Doanh thu":
        df = fetch_df(conn, "SELECT * FROM revenue WHERE store=? AND ts BETWEEN ? AND ? ORDER BY ts DESC", (store, d1, d2))
    else:
        df = fetch_df(conn, "SELECT * FROM tscd ORDER BY id DESC")
    st.dataframe(df, use_container_width=True)
    # Excel
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Report")
    st.download_button("⬇️ Tải Excel", data=buf.getvalue(), file_name=f"{what}_{store}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    # CSV (in thay PDF)
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Tải CSV", data=csv, file_name=f"{what}_{store}.csv", mime="text/csv")

# ========== SAO LƯU / PHỤC HỒI ==========
def page_backup_restore(conn):
    st.subheader("💾 Sao lưu / Phục hồi (CSV .zip)")
    tables = ["stores","users","products","formulas","inventory_ledger","prod_log","wip_cost","revenue","tscd","backup_log","receipt_in","receipt_out"]
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Tạo backup (.zip)"):
            zip_buf = io.BytesIO()
            with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as z:
                for t in tables:
                    df = fetch_df(conn, f"SELECT * FROM {t}")
                    z.writestr(f"{t}.csv", df.to_csv(index=False))
            run_sql(conn, "INSERT INTO backup_log(user_email,note) VALUES(?,?)", (st.session_state["user"]["email"], "manual"))
            st.download_button("⬇️ Tải backup.zip", data=zip_buf.getvalue(), file_name="backup.zip", mime="application/zip")
    with col2:
        up = st.file_uploader("Tải backup.zip để phục hồi", type=["zip"])
        if up and st.button("Phục hồi"):
            with zipfile.ZipFile(up) as z:
                for t in tables:
                    try:
                        df = pd.read_csv(z.open(f"{t}.csv"))
                        run_sql(conn, f"DELETE FROM {t}")
                        if not df.empty:
                            cols = ",".join(df.columns)
                            qmarks = ",".join(["?"]*len(df.columns))
                            for row in df.itertuples(index=False):
                                run_sql(conn, f"INSERT INTO {t} ({cols}) VALUES ({qmarks})", tuple(row))
                    except KeyError:
                        pass
            st.success("Đã phục hồi từ backup")

# ========== AI NÂNG CAO (Embeddings + FAISS, offline) ==========
@st.cache_resource
def _ai_load():
    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
    intents = [
        ("ton kho [pcode] tai [store]", "inv_item"),
        ("gia tri ton kho tai cua hang", "inv_value"),
        ("doanh thu thang nay", "rev_month"),
        ("doanh thu tu [d1] den [d2]", "rev_range"),
        ("san luong san xuat cot thang nay", "prod_cot_month"),
        ("san luong san xuat mut thang nay", "prod_mut_month"),
    ]
    X = model.encode([x[0] for x in intents], normalize_embeddings=True)
    index = faiss.IndexFlatIP(X.shape[1])
    index.add(np.array(X, dtype="float32"))
    return model, index, intents

def _ai_match(q):
    model, index, intents = _ai_load()
    x = model.encode([q], normalize_embeddings=True).astype("float32")
    D, I = index.search(x, 1)
    intent = intents[int(I[0][0])][1] if D[0][0] > 0.4 else None
    return intent

def page_ai(conn):
    st.subheader("🤖 AI hỏi đáp")
    store = st.session_state["store"]
    q = st.text_input("Hỏi gì cũng được (VD: 'Tồn kho MUT_CAM?', 'Doanh thu tháng này?')")
    if st.button("Hỏi") and q.strip():
        ql = q.lower()
        intent = _ai_match(ql)

        # fallback regex đơn giản
        if "tồn" in ql and ("kho" in ql or "ton" in ql):
            # lấy mã sp cuối câu nếu có
            tokens = q.strip().upper().split()
            cand = [t for t in tokens if t.isalnum() and len(t)<=20]
            pcode = cand[-1] if cand else ""
            df = fetch_df(conn, """
                SELECT COALESCE(SUM(CASE WHEN kind='IN' THEN qty ELSE -qty END),0) AS ton
                FROM inventory_ledger WHERE store=? AND pcode=?
            """, (store, pcode))
            st.info(f"Tồn {pcode}: {float(df.iloc[0]['ton'] or 0)}")
            return

        if intent == "inv_item":
            st.write("Hỏi ‘tồn kho [mã]’. Ví dụ: Tồn kho MUT_CAM?")
        elif intent == "inv_value":
            inv = fetch_df(conn, """
                SELECT p.code,
                       COALESCE(SUM(CASE WHEN l.kind='IN' THEN l.qty ELSE -l.qty END),0) AS ton
                FROM products p LEFT JOIN inventory_ledger l ON l.pcode=p.code AND l.store=?
                GROUP BY p.code
            """, (store,))
            inv["avg_cost"] = inv["code"].apply(lambda c: _avg_cost(store, c))
            inv["amount"] = inv["ton"] * inv["avg_cost"]
            st.info(f"Giá trị tồn kho hiện tại: {float(inv['amount'].sum()):,.0f} đ")
        elif intent == "rev_month":
            d1 = datetime.date.today().replace(day=1)
            rev = fetch_df(conn, "SELECT COALESCE(SUM(qty*unit_price),0) amt FROM revenue WHERE store=? AND ts>=?", (store, d1))
            st.info(f"Doanh thu tháng này: {float(rev.iloc[0]['amt']):,.0f} đ")
        elif intent == "rev_range":
            st.write("Nhập rõ khoảng thời gian ‘doanh thu từ YYYY-MM-DD đến YYYY-MM-DD’.")
        elif intent == "prod_cot_month":
            d1 = datetime.date.today().replace(day=1)
            df = fetch_df(conn, "SELECT COALESCE(SUM(kg_output),0) kg FROM prod_log WHERE store=? AND kind='COT' AND ts>=?", (store, d1))
            st.info(f"Sản lượng CỐT (tháng): {float(df.iloc[0]['kg']):,.2f} kg")
        elif intent == "prod_mut_month":
            d1 = datetime.date.today().replace(day=1)
            df = fetch_df(conn, "SELECT COALESCE(SUM(kg_output),0) kg FROM prod_log WHERE store=? AND kind IN ('MUT_FRUIT','MUT_COT') AND ts>=?", (store, d1))
            st.info(f"Sản lượng MỨT (tháng): {float(df.iloc[0]['kg']):,.2f} kg")
        else:
            st.write("Ví dụ: ‘Tồn kho MUT_CAM?’, ‘Doanh thu tháng này?’, ‘Giá trị tồn kho?’")

# ========== ROUTER & MAIN ==========
def main():

    conn = get_conn()
    user = require_login()
    choice = sidebar_menu(user)

    if choice == "Kho":
        page_kho(conn_local)
    elif choice == "Sản xuất":
        page_sanxuat(conn_local)
    elif choice == "Danh mục":
        page_danhmuc(conn_local)
    elif choice == "Doanh thu":
        page_doanhthu(conn_local)
    elif choice == "Báo cáo":
        page_baocao(conn_local)
    elif choice == "Tài sản cố định":
        page_tscd(conn_local)
    elif choice == "Tài chính":
        page_taichinh(conn_local)
    elif choice == "Người dùng":
        page_users(conn_local)
    elif choice == "Cửa hàng":
        page_stores(conn_local)
    elif choice == "Sao lưu/Phục hồi":
        page_backup_restore(conn_local)
    elif choice == "Xuất báo cáo":
        page_export(conn_local)
    elif choice == "AI hỏi đáp":
        page_ai(conn_local)
    elif choice == "Đổi mật khẩu":
        page_change_password(conn_local)
    else:
        st.info("Chọn trang ở sidebar.")

if __name__ == "__main__":
    main()
# ======== QUICKFIX PATCH (APPEND-ONLY) ========
# (Dán khối này vào CUỐI FILE app.py, không cần sửa gì ở trên)

import streamlit as _st
# 1) Alias cho bản Streamlit cũ (hết lỗi experimental_rerun)
if not hasattr(_st, "experimental_rerun"):
    _st.experimental_rerun = _st.rerun

# 2) SuperAdmin có full quyền
def has_perm(user, code):
    if (user.get("role") or "").lower() == "superadmin":
        return True
    p = (user.get("perms") or "")
    return code in p.split(",")

# 3) Đảm bảo schema doanh thu có cột pay_method / note (SQLite & Postgres đều OK)
def ensure_migrations():
    try:
        with get_conn() as c:
            try:
                run_sql(c, "ALTER TABLE revenue ADD COLUMN pay_method TEXT")
            except Exception:
                pass
            try:
                run_sql(c, "ALTER TABLE revenue ADD COLUMN note TEXT")
            except Exception:
                pass
    except Exception:
        pass

ensure_migrations()

# 4) Doanh thu CHỈ nhập SỐ TIỀN + kênh thanh toán (CASH/BANK)
def page_doanhthu(conn):
    guard_perm(st.session_state["user"], "DOANHTHU")
    st.subheader("💵 Doanh thu (chỉ số tiền + kênh thanh toán)")
    store = st.session_state["store"]

    # --- Ghi nhận thu tiền ---
    with st.form("add_rev_money"):
        ts = st.date_input("Ngày", datetime.date.today())
        amount = st.number_input("Số tiền thu (VNĐ)", 0.0, step=1000.0)
        pay = st.selectbox("Kênh thanh toán", ["CASH","BANK"])
        note = st.text_input("Ghi chú", "")
        ok = st.form_submit_button("Lưu thu tiền")
    if ok:
        # Không quan tâm sản phẩm: pcode='THU', qty=1, unit_price=amount
        run_sql(conn,
            "INSERT INTO revenue(ts,store,pcode,qty,unit_price,pay_method,note) VALUES(?,?,?,?,?,?,?)",
            (ts, store, "THU", 1, amount, pay, note)
        )
        st.success("Đã ghi nhận thu tiền")

    # --- Lịch sử & lọc ---
    st.markdown("### Lịch sử thu tiền")
    with st.form("rev_filter"):
        d1 = st.date_input("Từ ngày", datetime.date.today().replace(day=1))
        d2 = st.date_input("Đến ngày", datetime.date.today())
        payf = st.multiselect("Kênh", ["CASH","BANK"], default=["CASH","BANK"])
        st.form_submit_button("Lọc")
    if not payf:
        payf = ["CASH","BANK"]
    qmarks = ",".join(["?"]*len(payf))

    df = fetch_df(conn, f"""
        SELECT ts, store, pay_method, note, (qty*unit_price) AS amount
        FROM revenue
        WHERE store=? AND ts BETWEEN ? AND ? AND pay_method IN ({qmarks})
        ORDER BY ts DESC
    """, (store, d1, d2, *payf))

    col1, col2 = st.columns([2,1])
    with col1:
        st.dataframe(df, use_container_width=True)
    with col2:
        if not df.empty:
            tot = df.groupby("pay_method", as_index=False)["amount"].sum()
            st.markdown("**Tổng theo kênh**")
            st.dataframe(tot, use_container_width=True)
            st.info(f"**Tổng cộng:** {float(df['amount'].sum()):,.0f} đ")
        else:
            st.info("Chưa có thu tiền trong khoảng lọc.")

# 5) Cho chắc: nếu trang Công thức có chặn theo quyền, SuperAdmin vẫn vào được
#    (Nếu bạn đã dùng check 'has_perm(..., \"CT_EDIT\")', SuperAdmin đã pass ở mục 2)
# ======== END QUICKFIX PATCH ========
# ======================= HOTFIX (append-only) =======================
# Mục tiêu: 
# - Không còn "name 'conn' is not defined"
# - Bổ sung cột TSCD nếu thiếu
# - Chuẩn hoá hàm tính giá vốn TB theo kết nối được truyền vào

import streamlit as _st
import datetime, pandas as _pd

# 0) Alias cho bản Streamlit cũ (nếu chưa có)
if not hasattr(_st, "experimental_rerun"):
    _st.experimental_rerun = _st.rerun

# 1) Bổ sung cột thiếu (an toàn cho SQLite/Postgres)
def _ensure_schema_fix():
    try:
        with get_conn() as c:
            # TSCD nâng cao
            try: run_sql(c, "ALTER TABLE tscd ADD COLUMN acc_depr NUMERIC")
            except Exception: pass
            try: run_sql(c, "ALTER TABLE tscd ADD COLUMN group_code TEXT")
            except Exception: pass
            try: run_sql(c, "ALTER TABLE tscd ADD COLUMN acc_life_months INT")
            except Exception: pass
            # Revenue kênh thanh toán
            try: run_sql(c, "ALTER TABLE revenue ADD COLUMN pay_method TEXT")
            except Exception: pass
            try: run_sql(c, "ALTER TABLE revenue ADD COLUMN note TEXT")
            except Exception: pass
    except Exception:
        pass

_ensure_schema_fix()

# 2) Hàm tính giá vốn trung bình di động – BẮT BUỘC truyền conn
def avg_cost(conn, store, pcode):
    dfc = fetch_df(conn, """
        SELECT kind, qty, price_in
        FROM inventory_ledger
        WHERE store=? AND pcode=?
        ORDER BY ts
    """, (store, pcode))
    stock = 0.0
    cost  = 0.0
    for _, r in dfc.iterrows():
        k  = r["kind"]
        q  = float(r["qty"] or 0)
        pi = float(r["price_in"] or 0)
        if k == "IN":
            if q > 0:
                total = cost * stock + pi * q
                stock += q
                cost = (total / stock) if stock > 0 else 0.0
        else:  # OUT
            stock -= q
            if stock < 0:
                stock = 0.0
    return cost

# 3) Thay thế trang TSCD: KH lũy kế tính theo thời gian, không cần cột có sẵn
def page_tscd(conn):
    guard_perm(st.session_state["user"], "TSCD")
    st.subheader("🏗️ Tài sản cố định (nâng cao)")

    with st.form("add_tscd"):
        name = st.text_input("Tên TSCD")
        group_code = st.selectbox("Nhóm", ["MAY_MOC","PHUONG_TIEN","NHA_XUONG","KHAC"])
        cost = st.number_input("Nguyên giá", 0.0, step=100000.0)
        life = st.number_input("Thời gian KH (tháng)", 0, step=1)
        dep = st.number_input("Khấu hao / tháng", 0.0, step=10000.0)
        buy_date = st.date_input("Ngày mua", datetime.date.today())
        if st.form_submit_button("Thêm"):
            run_sql(conn, """
                INSERT INTO tscd(name,group_code,cost,acc_life_months,dep_per_month,buy_date,acc_depr)
                VALUES(?,?,?,?,?,?,?)
            """, (name, group_code, cost, life, dep, buy_date, 0))
            st.success("Đã thêm TSCD")

    df = fetch_df(conn, "SELECT id,name,group_code,cost,dep_per_month,buy_date FROM tscd ORDER BY id DESC")
    if df.empty:
        st.info("Chưa có TSCD.")
    else:
        # tính số tháng đã dùng & KH lũy kế động (khỏi lệ thuộc cột acc_depr)
        def _months(buy):
            if not isinstance(buy, (datetime.date, datetime.datetime)):
                buy = datetime.datetime.fromisoformat(str(buy)).date()
            today = datetime.date.today()
            return max(0, (today.year-buy.year)*12 + (today.month-buy.month))
        df["months_used"] = df["buy_date"].apply(_months)
        df["acc_dep_calc"] = df["dep_per_month"] * df["months_used"]
        st.dataframe(df, use_container_width=True)

        agg = {
            "Khấu hao tháng": float(df["dep_per_month"].sum()),
            "Khấu hao lũy kế (tính)": float(df["acc_dep_calc"].sum())
        }
        st.info(f"KH tháng: {agg['Khấu hao tháng']:,.0f} – KH lũy kế: {agg['Khấu hao lũy kế (tính)']:,.0f}")

# 4) Báo cáo tồn kho & trị giá – dùng đúng conn truyền vào
def page_baocao(conn):
    """Báo cáo tồn kho & trị giá – dùng đúng conn truyền vào."""
    guard_perm(st.session_state["user"], "BAOCAO")
    st.subheader("📑 Báo cáo tồn kho & trị giá")
    store = st.session_state.get("store", "")

    # Bộ lọc
    with st.form("inv_filter_bc"):
        d1 = st.date_input("Từ ngày", datetime.date.today().replace(day=1))
        d2 = st.date_input("Đến ngày", datetime.date.today())
        submit = st.form_submit_button("Lọc")

    # Tính tồn (IN - OUT) trong khoảng
    inv = fetch_df(conn, """
        SELECT p.code, p.name, p.cat_code,
               COALESCE(SUM(CASE WHEN l.kind='IN' THEN l.qty ELSE -l.qty END),0) AS ton
        FROM products p
        LEFT JOIN inventory_ledger l
               ON l.pcode=p.code AND l.store=? AND l.ts BETWEEN ? AND ?
        GROUP BY p.code,p.name,p.cat_code
        ORDER BY p.cat_code,p.code
    """, (store, d1, d2))

    if inv.empty:
        st.info("Chưa có phát sinh trong khoảng lọc.")
        return

    # GIÁ VỐN TB hiện hành cho từng mã (tính từ toàn bộ lịch sử đến hiện tại)
    # LƯU Ý: dùng helper avg_cost(conn, store, pcode)
    inv["avg_cost"] = inv["code"].apply(lambda c: avg_cost(conn, store, c))
    inv["amount"] = inv["ton"] * inv["avg_cost"]

    st.dataframe(inv, use_container_width=True)

    # Tổng hợp theo nhóm
    grp = inv.groupby("cat_code", as_index=False).agg(ton=("ton","sum"), amount=("amount","sum"))
    col1, col2 = st.columns([2,1])
    with col1:
        st.markdown("### Tổng hợp theo nhóm")
        st.dataframe(grp, use_container_width=True)
    with col2:
        st.markdown("### Tổng cộng")
        st.metric("Giá trị tồn", f"{float(inv['amount'].sum()):,.0f} đ")


# 5) Báo cáo tài chính (rút gọn) – dùng avg_cost(conn, ...)
def page_taichinh(conn):
    """Báo cáo tài chính rút gọn – dùng đúng conn truyền vào."""
    guard_perm(st.session_state["user"], "TAICHINH")
    st.subheader("📘 Báo cáo tài chính (rút gọn)")
    store = st.session_state.get("store", "")

    # 1) Giá trị hàng tồn kho (theo giá vốn TB hiện hành)
    inv = fetch_df(conn, """
        SELECT p.code,
               COALESCE(SUM(CASE WHEN l.kind='IN' THEN l.qty ELSE -l.qty END),0) AS ton
        FROM products p
        LEFT JOIN inventory_ledger l ON l.pcode=p.code AND l.store=?
        GROUP BY p.code
    """, (store,))
    if inv.empty:
        ton_gia_tri = 0.0
    else:
        inv["avg_cost"] = inv["code"].apply(lambda c: avg_cost(conn, store, c))
        inv["amount"] = inv["ton"] * inv["avg_cost"]
        ton_gia_tri = float(inv["amount"].sum())

    # 2) Dòng tiền đơn giản (tháng hiện tại): thu từ revenue
    d1 = datetime.date.today().replace(day=1)
    rev = fetch_df(conn, """
        SELECT COALESCE(SUM(qty*unit_price),0) AS amt
        FROM revenue WHERE store=? AND ts>=?
    """, (store, d1))
    doanh_thu_thang = float(rev.iloc[0]["amt"] or 0.0)

    # 3) Khấu hao tháng (TSCD): lấy tổng dep_per_month
    dep = fetch_df(conn, "SELECT COALESCE(SUM(dep_per_month),0) AS dep FROM tscd")
    khau_hao_thang = float(dep.iloc[0]["dep"] or 0.0)

    # Hiển thị
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("### Cân đối kế toán (rút gọn)")
        st.dataframe(pd.DataFrame({
            "Chỉ tiêu": ["Hàng tồn kho (giá vốn)"],
            "Số tiền": [ton_gia_tri]
        }), use_container_width=True)
    with col2:
        st.markdown("### Lưu chuyển tiền tệ (giản lược)")
        st.dataframe(pd.DataFrame({
            "Khoản mục": ["Tiền thu bán hàng (tháng)", "Chi khấu hao (tháng)"],
            "Tiền": [doanh_thu_thang, -khau_hao_thang]
        }), use_container_width=True)

    st.info(f"Giá trị tồn kho: {ton_gia_tri:,.0f} đ · Thu tháng: {doanh_thu_thang:,.0f} đ · KH tháng: {khau_hao_thang:,.0f} đ")

# ==================== END HOTFIX ====================
