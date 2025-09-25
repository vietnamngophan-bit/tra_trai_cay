# =========================
# app.py — PART 1/5 (Core)
# =========================
import os, re, json, hashlib
from datetime import datetime, date, timedelta
from typing import Dict, Any, Tuple, Optional

import pandas as pd
import streamlit as st

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection as _SAConnection

# ---- UI base config (must be first Streamlit call)
st.set_page_config(page_title="Fruit Tea ERP v5", page_icon="🧃", layout="wide")

# ==========================================================
# DB BRIDGE (SQLite local <-> Postgres online via Supabase)
# ==========================================================
_ENGINE = None
_IS_PG = False

def _normalize_pg_url(url: str) -> str:
    # Streamlit/HF secrets thường là "postgresql://", SQLAlchemy khuyên "postgresql+psycopg2://"
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql+psycopg2://", 1)
    if url.startswith("postgresql://") and "+psycopg2" not in url:
        return url.replace("postgresql://", "postgresql+psycopg2://", 1)
    return url

def get_conn():
    """
    Trả về kết nối SQLAlchemy Connection (Postgres) hoặc sqlite3.Connection (local).
    """
    global _ENGINE, _IS_PG
    url = os.getenv("DATABASE_URL", "").strip()
    if url:
        _IS_PG = True
        url = _normalize_pg_url(url)
        if _ENGINE is None:
            _ENGINE = create_engine(url, pool_pre_ping=True, future=True)
        return _ENGINE.connect()
    else:
        # offline/local fallback
        import sqlite3
        os.makedirs("data", exist_ok=True)
        return sqlite3.connect(os.path.join("data", "app.db"), check_same_thread=False)

def _qmark_to_named(sql: str, params):
    """Đổi ? -> :p1, :p2,... cho Postgres khi params là list/tuple."""
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

# ---- Patch pandas.read_sql_query để đọc cả sqlite & pg bằng cùng cú pháp
_ORIG_PD_READ = pd.read_sql_query
def _pd_read_sql_query_any(sql, conn, params=None, *args, **kwargs):
    if _IS_PG:
        if isinstance(params, (list, tuple)):
            sql, params = _qmark_to_named(sql, params)
        return _ORIG_PD_READ(text(sql), conn, params=params or {}, *args, **kwargs)
    return _ORIG_PD_READ(sql, conn, params=params, *args, **kwargs)
pd.read_sql_query = _pd_read_sql_query_any

# ---- Patch Connection.execute để hỗ trợ "INSERT OR REPLACE" trên Postgres (thành UPSERT)
_ORIG_SA_EXEC = _SAConnection.execute
def _sa_exec_auto(self, statement, *multiparams, **kwargs):
    if isinstance(statement, str):
        stmt_upper = statement.upper()
        # Chuyển "INSERT OR REPLACE INTO table(cols...)" -> INSERT ... ON CONFLICT (...) DO UPDATE ...
        if _IS_PG and "INSERT OR REPLACE" in stmt_upper:
            # Suy luận cột conflict phổ biến: code / email / batch_id, có thể chỉnh sau
            m = re.search(r"INSERT\s+OR\s+REPLACE\s+INTO\s+(\w+)\s*\((.*?)\)", statement, re.I|re.S)
            if m:
                table = m.group(1).strip()
                cols = [c.strip() for c in m.group(2).split(",")]
                # Ưu tiên các khóa thường dùng
                conflict = "code"
                if "email" in cols: conflict = "email"
                if "batch_id" in cols: conflict = "batch_id"
                # build upsert
                statement = re.sub(r"INSERT\s+OR\s+REPLACE", "INSERT", statement, flags=re.I)
                update_sets = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c != conflict])
                statement += f" ON CONFLICT ({conflict}) DO UPDATE SET {update_sets}"
        # Đổi multiparams dạng list -> named cho PG
        if _IS_PG:
            if multiparams and isinstance(multiparams[0], (list, tuple)):
                sql2, params2 = _qmark_to_named(statement, multiparams[0])
                return _ORIG_SA_EXEC(self, text(sql2), params2)
            return _ORIG_SA_EXEC(self, text(statement), **kwargs)
        # sqlite: thử exec driver trước để tận dụng ? param
        try:
            return self.exec_driver_sql(statement, *multiparams, **kwargs)
        except Exception:
            return _ORIG_SA_EXEC(self, text(statement), **kwargs)
    return _ORIG_SA_EXEC(self, statement, *multiparams, **kwargs)
_SAConnection.execute = _sa_exec_auto

def run_sql(conn, sql, params=None):
    """Execute a SQL string on both PG/SQLite. With PG we pass dict params."""
    if _IS_PG and isinstance(params, (list, tuple)):
        sql, params = _qmark_to_named(sql, params)
    res = conn.execute(text(sql) if _IS_PG else sql, params or {})
    try:
        conn.commit()
    except:
        pass
    return res


def fetch_df(conn, sql: str, params=None) -> pd.DataFrame:
    """Đọc DataFrame. Dùng chung cho PG & SQLite."""
    return pd.read_sql_query(sql, conn, params=params)

# ==========================
# SCHEMA (đảm bảo tối thiểu)
# ==========================
MIN_SCHEMA_SQL = """
-- cửa hàng
CREATE TABLE IF NOT EXISTS stores(
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    address TEXT,
    note TEXT
);
-- người dùng
CREATE TABLE IF NOT EXISTS users(
    email TEXT PRIMARY KEY,
    display TEXT,
    password TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'user',
    store_code TEXT,
    perms TEXT
);
-- danh mục sản phẩm
CREATE TABLE IF NOT EXISTS products(
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    uom  TEXT NOT NULL DEFAULT 'kg',
    cat_code TEXT NOT NULL
);
-- sổ kho (có cả số cốc)
CREATE TABLE IF NOT EXISTS inventory_ledger(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    store TEXT NOT NULL,
    pcode TEXT NOT NULL,
    kind TEXT NOT NULL,                -- IN / OUT / ADJ
    qty DOUBLE PRECISION NOT NULL,     -- kg (+/-)
    price_in DOUBLE PRECISION,         -- giá nhập/kg khi IN
    cups DOUBLE PRECISION DEFAULT 0.0, -- số cốc (+/-) cho CỐT & MỨT
    ref TEXT                           -- tham chiếu giao dịch
);
-- doanh thu đơn giản (CASH/BANK)
CREATE TABLE IF NOT EXISTS revenue(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts DATE NOT NULL,
    store TEXT NOT NULL,
    amount DOUBLE PRECISION NOT NULL DEFAULT 0,
    pay_method TEXT NOT NULL           -- CASH | BANK
);
-- công thức
CREATE TABLE IF NOT EXISTS formulas(
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT NOT NULL,                -- COT | MUT
    output_pcode TEXT NOT NULL,
    output_uom TEXT NOT NULL DEFAULT 'kg',
    recovery DOUBLE PRECISION,         -- chỉ dùng cho CỐT
    cups_per_kg DOUBLE PRECISION,      -- số cốc / 1 kg TP
    fruits_csv TEXT,                   -- mã NL chính (trái cây hoặc cốt) dạng "A,B,C"
    additives_json TEXT,               -- {ma_phu_gia: dinh_luong_kg_cho_1kg_sau_so}
    note TEXT
);
-- WIP batch (mẻ đang làm)
CREATE TABLE IF NOT EXISTS wip_batches(
    batch_id TEXT PRIMARY KEY,
    store TEXT NOT NULL,
    ct_code TEXT NOT NULL,
    type TEXT NOT NULL,                -- COT | MUT
    src TEXT,                          -- TRAI_CAY | COT (cho MỨT)
    kg_input DOUBLE PRECISION NOT NULL DEFAULT 0,
    kg_after DOUBLE PRECISION NOT NULL DEFAULT 0,  -- kg sau sơ chế (cho MỨT)
    ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
-- chi phí WIP (nếu cần)
CREATE TABLE IF NOT EXISTS wip_cost(
    batch_id TEXT PRIMARY KEY,
    total_cost DOUBLE PRECISION NOT NULL DEFAULT 0
);
-- nhật ký hệ thống
CREATE TABLE IF NOT EXISTS syslog(
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    actor TEXT,
    action TEXT,
    detail TEXT
);
"""

def ensure_min_schema(conn):
    if _IS_PG:
        # ép kiểu auto increment
        sql = MIN_SCHEMA_SQL.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "BIGSERIAL PRIMARY KEY") \
                            .replace("DOUBLE PRECISION", "DOUBLE PRECISION")
        run_sql(conn, sql)
    else:
        run_sql(conn, MIN_SCHEMA_SQL)

# ==========================
# LOGGING & UTILS
# ==========================
def log_action(conn, actor: str, action: str, detail: str = ""):
    run_sql(conn, "INSERT INTO syslog(actor,action,detail) VALUES(?,?,?)",
            (actor or "", action or "", detail or ""))

def sha256(txt: str) -> str:
    return hashlib.sha256((txt or "").encode("utf-8")).hexdigest()

# Quyền đơn giản: chuỗi perms phẩy, hoặc role=SuperAdmin
def has_perm(user: Dict[str, Any], perm: str) -> bool:
    if not user: return False
    if (user.get("role") or "").lower() in ["superadmin", "admin", "super_admin"]:
        return True
    perms = (user.get("perms") or "").split(",")
    return perm in [p.strip().upper() for p in perms if p.strip()]

# ==========================
# AUTH (login/logout)
# ==========================
def login_form(conn) -> Optional[Dict[str, Any]]:
    st.header("Đăng nhập hệ thống")
    email = st.text_input("Email", value="admin@example.com")
    pw = st.text_input("Mật khẩu", type="password")
    btn = st.button("Đăng nhập")
    if btn:
        row = fetch_df(conn, "SELECT * FROM users WHERE email=?", (email,))
        if row.empty:
            st.error("Sai tài khoản / mật khẩu")
            return None
        r = row.iloc[0].to_dict()
        if (r.get("password") in [pw, sha256(pw)]):  # chấp nhận plain hoặc sha256 (để chuyển dần)
            st.success("Đăng nhập thành công")
            return {
                "email": r.get("email"),
                "display": r.get("display") or r.get("email"),
                "role": r.get("role") or "user",
                "store": r.get("store_code") or "",
                "perms": r.get("perms") or ""
            }
        st.error("Sai tài khoản / mật khẩu")
    return None

def require_login(conn) -> Dict[str, Any]:
    if "user" not in st.session_state or not st.session_state["user"]:
        u = login_form(conn)
        if not u:
            st.stop()
        st.session_state["user"] = u
        log_action(conn, u["email"], "LOGIN", "User logged in")
    return st.session_state["user"]

# ==========================
# COMMON DATA HELPERS
# ==========================
def prod_options(conn, cat_code: Optional[str] = None) -> pd.DataFrame:
    if cat_code:
        return fetch_df(conn, "SELECT code,name,uom,cat_code FROM products WHERE cat_code=? ORDER BY code", (cat_code,))
    return fetch_df(conn, "SELECT code,name,uom,cat_code FROM products ORDER BY code")

def store_options(conn) -> pd.DataFrame:
    return fetch_df(conn, "SELECT code,name FROM stores ORDER BY code")

def inv_balance(conn, store: str, pcode: str) -> Tuple[float, float]:
    """
    Trả về (tồn kg, tồn cốc) hiện tại từ sổ kho.
    """
    df = fetch_df(conn, """
        SELECT COALESCE(SUM(qty),0) qty, COALESCE(SUM(cups),0) cups
        FROM inventory_ledger WHERE store=? AND pcode=?
    """, (store, pcode))
    if df.empty: return 0.0, 0.0
    return float(df.iloc[0]["qty"] or 0.0), float(df.iloc[0]["cups"] or 0.0)

def post_ledger(conn, store: str, pcode: str, kind: str, qty: float, price_in: Optional[float] = None,
                cups: float = 0.0, ref: str = ""):
    """
    Ghi 1 dòng kho với cả số cốc (cups). kind: IN/OUT/ADJ
    """
    # kiểm tra SP có tồn tại
    p = fetch_df(conn, "SELECT code FROM products WHERE code=?", (pcode,))
    if p.empty:
        raise ValueError("SP không tồn tại")
    run_sql(conn, "INSERT INTO inventory_ledger(ts,store,pcode,kind,qty,price_in,cups,ref) VALUES(CURRENT_TIMESTAMP,?,?,?,?,?,?,?)",
            (store, pcode, kind, float(qty or 0), price_in, float(cups or 0), ref))
# =========================
# app.py — PART 2/5 (Setup + Danh mục)
# =========================

# --- mở kết nối & đảm bảo schema
conn = get_conn()
ensure_min_schema(conn)

# --- seed dữ liệu tối thiểu (an toàn, ON CONFLICT/REPLACE)
def seed_min_data(conn):
    # 1 store mặc định
    run_sql(conn, """
        INSERT OR REPLACE INTO stores(code,name,address,note)
        VALUES('HOSEN','Kho HOSEN','Đà Nẵng','store mặc định')
    """)
    # tài khoản admin mặc định
    run_sql(conn, """
        INSERT OR REPLACE INTO users(email,display,password,role,store_code,perms)
        VALUES(?,?,?,?,?,?)
    """, ("admin@example.com","SuperAdmin","admin","SuperAdmin","HOSEN",
          "KHO,BAOCAO,SANXUAT,DM,USERS,CT_EDIT"))
    # vài sản phẩm mẫu để chạy thử
    samples = [
        ("TRA_TUOI", "Trà tươi", "kg", "TRAI_CAY"),
        ("DUONG", "Đường", "kg", "PHU_GIA"),
        ("COT_CAM", "Cốt cam", "kg", "COT"),
        ("MUT_CAM", "Mứt cam", "kg", "MUT"),
    ]
    for r in samples:
        run_sql(conn, "INSERT OR REPLACE INTO products(code,name,uom,cat_code) VALUES(?,?,?,?)", r)

seed_min_data(conn)

# --- trạng thái người dùng
user = require_login(conn)
st.caption("DB: " + ("Postgres" if os.getenv("DATABASE_URL") else "SQLite"))

# --- chọn cửa hàng ở sidebar
with st.sidebar:
    st.markdown("### 🏬 Cửa hàng")
    stores_df = store_options(conn)
    if stores_df.empty:
        st.warning("Chưa có cửa hàng. Vào Danh mục → Cửa hàng để tạo mới.")
        current_store = "HOSEN"
    else:
        store_list = stores_df["code"].tolist()
        defidx = store_list.index(user.get("store") or "HOSEN") if (user.get("store") or "HOSEN") in store_list else 0
        current_store = st.selectbox("Chọn kho/cửa hàng", store_list, index=defidx)
    if "store" not in st.session_state or st.session_state["store"] != current_store:
        st.session_state["store"] = current_store
        # lưu store ưu tiên cho user (tùy chọn)
        try:
            run_sql(conn, "UPDATE users SET store_code=? WHERE email=?", (current_store, user["email"]))
        except Exception:
            pass

# ==========================================================
# DANH MỤC (Cửa hàng / Sản phẩm / Người dùng & Quyền)
# ==========================================================
def page_danhmuc(conn):
    st.header("📚 Danh mục")
    tabs = st.tabs(["Cửa hàng", "Sản phẩm", "Người dùng & Quyền"])

    # -------- CỬA HÀNG --------
    with tabs[0]:
        st.subheader("Cửa hàng")
        df = store_options(conn)
        st.dataframe(df, use_container_width=True)
        st.markdown("**Thêm / Sửa**")
        col1, col2 = st.columns(2)
        with col1:
            code = st.text_input("Mã cửa hàng", value=(df["code"].iloc[0] if not df.empty else "HOSEN"))
            name = st.text_input("Tên cửa hàng")
            addr = st.text_input("Địa chỉ")
            note = st.text_input("Ghi chú")
        with col2:
            if st.button("💾 Lưu cửa hàng"):
                if not code or not name:
                    st.error("Mã và Tên là bắt buộc")
                else:
                    run_sql(conn, "INSERT OR REPLACE INTO stores(code,name,address,note) VALUES(?,?,?,?)",
                            (code.strip(), name.strip(), addr.strip(), note.strip()))
                    log_action(conn, user["email"], "DM_STORE_UPSERT", f"{code} - {name}")
                    st.success("Đã lưu")
                    st.experimental_rerun()
            del_code = st.text_input("Xóa cửa hàng (nhập mã)")
            if st.button("🗑️ Xóa cửa hàng"):
                run_sql(conn, "DELETE FROM stores WHERE code=?", (del_code.strip(),))
                log_action(conn, user["email"], "DM_STORE_DELETE", del_code.strip())
                st.success("Đã xóa")
                st.experimental_rerun()

    # -------- SẢN PHẨM --------
    with tabs[1]:
        st.subheader("Sản phẩm")
        dfp = fetch_df(conn, "SELECT code,name,uom,cat_code FROM products ORDER BY code")
        st.dataframe(dfp, use_container_width=True)
        st.markdown("**Thêm / Sửa**")
        col1, col2 = st.columns(2)
        with col1:
            pcode = st.text_input("Mã SP")
            pname = st.text_input("Tên SP")
            uom = st.text_input("ĐVT", value="kg")
            cat = st.selectbox("Nhóm SP", ["TRAI_CAY","PHU_GIA","COT","MUT"], index=0)
        with col2:
            if st.button("💾 Lưu SP"):
                if not pcode or not pname:
                    st.error("Mã và Tên là bắt buộc")
                else:
                    run_sql(conn, "INSERT OR REPLACE INTO products(code,name,uom,cat_code) VALUES(?,?,?,?)",
                            (pcode.strip(), pname.strip(), uom.strip(), cat.strip()))
                    log_action(conn, user["email"], "DM_PRODUCT_UPSERT", pcode.strip())
                    st.success("Đã lưu")
                    st.experimental_rerun()
            del_p = st.text_input("Xóa SP (nhập mã)")
            if st.button("🗑️ Xóa SP"):
                run_sql(conn, "DELETE FROM products WHERE code=?", (del_p.strip(),))
                log_action(conn, user["email"], "DM_PRODUCT_DELETE", del_p.strip())
                st.success("Đã xóa")
                st.experimental_rerun()

    # -------- NGƯỜI DÙNG & QUYỀN --------
    with tabs[2]:
        if not has_perm(user, "USERS"):
            st.warning("Bạn không có quyền truy cập mục Người dùng.")
        else:
            st.subheader("Người dùng & Quyền")
            dfu = fetch_df(conn, "SELECT email,display,role,store_code,perms FROM users ORDER BY email")
            st.dataframe(dfu, use_container_width=True)
            st.markdown("**Thêm / Sửa**")
            col1, col2 = st.columns(2)
            with col1:
                u_email = st.text_input("Email", value="")
                u_display = st.text_input("Tên hiển thị", value="")
                u_role = st.selectbox("Vai trò", ["SuperAdmin","admin","user"], index=2)
                u_store = st.text_input("Store mặc định", value=st.session_state.get("store","HOSEN"))
            with col2:
                u_perms = st.text_area("Quyền riêng (phẩy):\nVD: KHO,BAOCAO,SANXUAT,DM,USERS,CT_EDIT", height=80)
                u_pw = st.text_input("Mật khẩu (đặt/xóa trắng = giữ nguyên)", type="password", value="")
                if st.button("💾 Lưu người dùng"):
                    if not u_email:
                        st.error("Cần nhập email")
                    else:
                        # nếu không nhập PW => giữ nguyên; nếu có => lưu plain và hash tùy bạn chuyển đổi sau
                        existed = fetch_df(conn, "SELECT email,password FROM users WHERE email=?", (u_email,))
                        pw_save = (existed.iloc[0]["password"] if (not existed.empty and not u_pw) else (u_pw or "123456"))
                        run_sql(conn, """
                            INSERT OR REPLACE INTO users(email,display,password,role,store_code,perms)
                            VALUES(?,?,?,?,?,?)
                        """, (u_email.strip(), u_display.strip() or u_email.strip(), pw_save,
                              u_role, u_store.strip(), u_perms.strip()))
                        log_action(conn, user["email"], "DM_USER_UPSERT", u_email.strip())
                        st.success("Đã lưu")
                        st.experimental_rerun()
                del_u = st.text_input("Xóa user (email)")
                if st.button("🗑️ Xóa user"):
                    run_sql(conn, "DELETE FROM users WHERE email=?", (del_u.strip(),))
                    log_action(conn, user["email"], "DM_USER_DELETE", del_u.strip())
                    st.success("Đã xóa")
                    st.experimental_rerun()
# =========================
# app.py — PART 3/5 (Kho + Báo cáo nâng cao)
# =========================

# ---------- TIỆN ÍCH DỮ LIỆU KHO ----------
def product_list(conn, cat=None, keyword=""):
    sql = "SELECT code,name,uom,cat_code FROM products"
    cond, params = [], []
    if cat:
        cond.append("cat_code=?"); params.append(cat)
    if keyword:
        cond.append("(code LIKE ? OR name LIKE ?)"); params += [f"%{keyword}%", f"%{keyword}%"]
    if cond:
        sql += " WHERE " + " AND ".join(cond)
    sql += " ORDER BY code"
    return fetch_df(conn, sql, tuple(params) if params else None)

def write_ledger(conn, ts, store, pcode, kind, qty, price_in=0.0, note="", cups=0.0):
    run_sql(conn, """
        INSERT INTO inventory_ledger(ts,store,pcode,kind,qty,price_in,note,cups)
        VALUES(?,?,?,?,?,?,?,?)
    """, (ts, store, pcode, kind, qty, price_in, note, cups))
    log_action(conn, st.session_state["user"]["email"], f"KHO_{kind}", f"{store}-{pcode}-{qty}")

def cups_per_kg_of(conn, pcode):
    # lấy số cốc/kg từ CT (ưu tiên CT loại COT/MUT có output_pcode = pcode)
    df = fetch_df(conn, "SELECT cups_per_kg FROM formulas WHERE output_pcode=? ORDER BY code LIMIT 1", (pcode,))
    if not df.empty and pd.notna(df.iloc[0]["cups_per_kg"]):
        return float(df.iloc[0]["cups_per_kg"] or 0.0)
    return 0.0

def stock_snapshot(conn, store, to_date=None):
    """Tồn kho đến ngày to_date (<=). Tính cả 'số cốc' cho nhóm COT/MUT."""
    params = [store]
    sql = """
        SELECT pcode,
               SUM(CASE WHEN kind='IN'  THEN qty ELSE -qty END) AS ton_qty,
               SUM(CASE WHEN kind='IN'  THEN cups ELSE -cups END) AS ton_cups
        FROM inventory_ledger
        WHERE store=? {date_filter}
        GROUP BY pcode
    """
    date_filter = ""
    if to_date:
        date_filter = "AND date(ts)<=?"
        params.append(to_date.strftime("%Y-%m-%d"))
    df = fetch_df(conn, sql.format(date_filter=date_filter), tuple(params))
    if df.empty:
        return pd.DataFrame(columns=["pcode","name","uom","cat_code","ton_qty","avg_cost","value","ton_cups"])

    # gắn thông tin sản phẩm
    prods = fetch_df(conn, "SELECT code,name,uom,cat_code FROM products")
    df = df.merge(prods, left_on="pcode", right_on="code", how="left").drop(columns=["code"])
    # avg cost & giá trị
    df["avg_cost"] = df["pcode"].apply(lambda c: avg_cost(conn, store, c))
    df["value"] = df["avg_cost"] * df["ton_qty"]
    # nếu một số bản ghi cups thiếu (0) thì suy từ ton_qty * cups/kg
    def ensure_cups(row):
        cups = float(row.get("ton_cups") or 0.0)
        if cups == 0.0 and row["cat_code"] in ("COT","MUT"):
            cups = float(row["ton_qty"] or 0.0) * cups_per_kg_of(conn, row["pcode"])
        return cups
    df["ton_cups"] = df.apply(ensure_cups, axis=1)
    return df[["pcode","name","uom","cat_code","ton_qty","avg_cost","value","ton_cups"]].sort_values("pcode")

# ---------- TRANG KHO ----------
def page_kho(conn):
    st.header(f"📦 Quản lý kho – {st.session_state.get('store','')}")
    tab_in, tab_out, tab_ton = st.tabs(["Phiếu nhập", "Phiếu xuất", "Tồn kho"])

    # ====== PHIẾU NHẬP ======
    with tab_in:
        st.subheader("Phiếu nhập")
        c1, c2, c3 = st.columns([1,2,1])
        with c1:
            ngay = st.date_input("Ngày", datetime.today().date())
        with c2:
            kw = st.text_input("Tìm SP (mã/tên) để nhập")
            df_opts = product_list(conn, keyword=kw)
            sp = st.selectbox("Chọn sản phẩm", df_opts["code"].tolist() if not df_opts.empty else [], format_func=lambda m: f"{m} - {df_opts.set_index('code').loc[m,'name']}" if not df_opts.empty and m in df_opts["code"].values else m)
        with c3:
            qty = st.number_input("Số lượng nhập", 0.0, step=0.1)
            price = st.number_input("Đơn giá nhập (VND/ĐVT)", 0.0, step=100.0)

        # 'số cốc' ghi thẳng vào ledger (nếu là COT/MUT) = qty * cups/kg
        cups_calc = 0.0
        if sp:
            pd_info = fetch_df(conn, "SELECT cat_code FROM products WHERE code=?", (sp,))
            if not pd_info.empty and pd_info.iloc[0]["cat_code"] in ("COT","MUT"):
                cups_calc = qty * cups_per_kg_of(conn, sp)
        st.caption(f"👉 Số cốc ghi nhận: {cups_calc:.0f}")

        note = st.text_input("Ghi chú")
        if st.button("💾 Lưu phiếu nhập"):
            if not sp or qty <= 0:
                st.error("Chọn SP và số lượng > 0")
            else:
                write_ledger(conn, ngay, st.session_state["store"], sp, "IN", qty, price_in=price, note=note, cups=cups_calc)
                st.success("Đã lưu phiếu nhập")
                st.experimental_rerun()

        st.markdown("**Lịch sử nhập gần đây**")
        df_in = fetch_df(conn, """
            SELECT ts,pcode,qty,price_in,note,cups FROM inventory_ledger
            WHERE store=? AND kind='IN' ORDER BY ts DESC LIMIT 200
        """, (st.session_state["store"],))
        st.dataframe(df_in, use_container_width=True)

    # ====== PHIẾU XUẤT ======
    with tab_out:
        st.subheader("Phiếu xuất")
        c1, c2, c3 = st.columns([1,2,1])
        with c1:
            ngay2 = st.date_input("Ngày xuất", datetime.today().date(), key="ngayx")
        with c2:
            kw2 = st.text_input("Tìm SP (mã/tên) để xuất")
            df_opts2 = product_list(conn, keyword=kw2)
            sp2 = st.selectbox("Chọn sản phẩm xuất", df_opts2["code"].tolist() if not df_opts2.empty else [], key="spx",
                               format_func=lambda m: f"{m} - {df_opts2.set_index('code').loc[m,'name']}" if not df_opts2.empty and m in df_opts2["code"].values else m)
        with c3:
            qty2 = st.number_input("Số lượng xuất", 0.0, step=0.1, key="qtyx")

        cups_out = 0.0
        if sp2:
            pd_info2 = fetch_df(conn, "SELECT cat_code FROM products WHERE code=?", (sp2,))
            if not pd_info2.empty and pd_info2.iloc[0]["cat_code"] in ("COT","MUT"):
                cups_out = qty2 * cups_per_kg_of(conn, sp2)
        st.caption(f"👉 Số cốc trừ kho: {cups_out:.0f}")

        note2 = st.text_input("Ghi chú xuất")
        if st.button("📤 Lưu phiếu xuất"):
            if not sp2 or qty2 <= 0:
                st.error("Chọn SP và số lượng > 0")
            else:
                write_ledger(conn, ngay2, st.session_state["store"], sp2, "OUT", qty2, price_in=0.0, note=note2, cups=cups_out)
                st.success("Đã lưu phiếu xuất")
                st.experimental_rerun()

        st.markdown("**Lịch sử xuất gần đây**")
        df_out = fetch_df(conn, """
            SELECT ts,pcode,qty,note,cups FROM inventory_ledger
            WHERE store=? AND kind='OUT' ORDER BY ts DESC LIMIT 200
        """, (st.session_state["store"],))
        st.dataframe(df_out, use_container_width=True)

    # ====== TỒN KHO ======
    with tab_ton:
        st.subheader("Tồn kho (có số cốc)")
        c1, c2, c3, c4 = st.columns([1,1,1,2])
        with c1:
            fr = st.date_input("Từ ngày", datetime.today().date().replace(day=1))
        with c2:
            to = st.date_input("Đến ngày", datetime.today().date())
        with c3:
            catf = st.selectbox("Nhóm SP", ["TẤT CẢ","TRAI_CAY","PHU_GIA","COT","MUT"])
        with c4:
            name_like = st.text_input("Mã/Tên chứa ...")

        df_ton = stock_snapshot(conn, st.session_state["store"], to)
        # lọc nâng cao
        if catf != "TẤT CẢ":
            df_ton = df_ton[df_ton["cat_code"] == catf]
        if name_like:
            df_ton = df_ton[df_ton["pcode"].str.contains(name_like, case=False) | df_ton["name"].str.contains(name_like, case=False)]

        st.dataframe(df_ton, use_container_width=True)
        colx, coly = st.columns(2)
        with colx:
            total_val = float(df_ton["value"].sum()) if not df_ton.empty else 0.0
            total_cups = float(df_ton["ton_cups"].sum()) if not df_ton.empty else 0.0
            st.metric("Tổng giá trị tồn (VND)", f"{total_val:,.0f}")
        with coly:
            st.metric("Tổng số cốc quy đổi", f"{total_cups:,.0f}")

        # Xuất CSV
        if not df_ton.empty:
            st.download_button(
                "⬇️ Xuất tồn kho (CSV)",
                data=df_ton.to_csv(index=False).encode("utf-8"),
                file_name=f"ton_kho_{to}.csv",
                mime="text/csv"
            )

# ---------- BÁO CÁO NÂNG CAO ----------
def page_baocao(conn):
    st.header("📈 Báo cáo nâng cao")
    tab_tonkho, tab_taichinh = st.tabs(["Tồn kho & Trị giá", "Tài chính (doanh thu – COGS – lãi gộp)"])

    # ---- TỒN & TRỊ GIÁ ----
    with tab_tonkho:
        to = st.date_input("Chốt đến ngày", datetime.today().date(), key="ton_to")
        df_ton = stock_snapshot(conn, st.session_state["store"], to)
        st.dataframe(df_ton, use_container_width=True)
        st.metric("Tổng trị giá", f"{df_ton['value'].sum():,.0f} VND")
        st.metric("Tổng số cốc", f"{df_ton['ton_cups'].sum():,.0f}")
        if not df_ton.empty:
            st.download_button("⬇️ CSV", df_ton.to_csv(index=False).encode("utf-8"),
                               file_name=f"bao_cao_ton_{to}.csv", mime="text/csv")

    # ---- TÀI CHÍNH NÂNG CAO ----
    with tab_taichinh:
        colF = st.columns(3)
        fr = st.date_input("Từ ngày", datetime.today().date().replace(day=1), key="tc_fr")
        to = st.date_input("Đến ngày", datetime.today().date(), key="tc_to")
        pay = st.multiselect("Kênh thanh toán", ["CASH","BANK"], default=["CASH","BANK"])

        # Doanh thu ghi ở bảng revenue(ts,store,pcode,qty,unit_price,pay_method)
        # Nếu bạn chỉ cần tổng tiền theo kênh thanh toán: không cần pcode
        rev = fetch_df(conn, f"""
            SELECT date(ts) d, pay_method, SUM(qty*unit_price) amount, SUM(qty) total_qty
            FROM revenue
            WHERE store=? AND date(ts) BETWEEN ? AND ?
                  AND pay_method IN ({",".join(["?"]*len(pay))})
            GROUP BY date(ts), pay_method
            ORDER BY d
        """, (st.session_state["store"], fr.strftime("%Y-%m-%d"), to.strftime("%Y-%m-%d"), *pay)) if len(pay)>0 else pd.DataFrame(columns=["d","pay_method","amount","total_qty"])
        st.subheader("Doanh thu theo ngày & phương thức")
        st.dataframe(rev, use_container_width=True)
        st.metric("Tổng doanh thu", f"{(rev['amount'].sum() if not rev.empty else 0):,.0f} VND")

        # COGS (giá vốn) ~ tổng xuất kho loại 'OUT_SALE' (nếu bạn dùng kind riêng) hoặc từ revenue.qty * avg_cost(pcode)
        # Ở đây: nếu có cột pcode/qty trong revenue => ước tính giá vốn theo avg_cost tại thời điểm báo cáo
        rev_detail = fetch_df(conn, """
            SELECT pcode, SUM(qty) qty FROM revenue
            WHERE store=? AND date(ts) BETWEEN ? AND ?
            GROUP BY pcode
        """, (st.session_state["store"], fr.strftime("%Y-%m-%d"), to.strftime("%Y-%m-%d")))
        if not rev_detail.empty:
            rev_detail["avg_cost"] = rev_detail["pcode"].apply(lambda c: avg_cost(conn, st.session_state["store"], c))
            rev_detail["cogs"] = rev_detail["qty"] * rev_detail["avg_cost"]
            cogs_total = rev_detail["cogs"].sum()
        else:
            cogs_total = 0.0

        doanh_thu = float(rev["amount"].sum() if not rev.empty else 0.0)
        lai_gop = doanh_thu - cogs_total
        col1, col2, col3 = st.columns(3)
        col1.metric("Doanh thu", f"{doanh_thu:,.0f} VND")
        col2.metric("Giá vốn (ước tính)", f"{cogs_total:,.0f} VND")
        col3.metric("Lãi gộp", f"{lai_gop:,.0f} VND")

        # Xuất CSV các bảng
        if not rev.empty:
            st.download_button("⬇️ Doanh thu CSV", rev.to_csv(index=False).encode("utf-8"),
                               file_name=f"doanh_thu_{fr}_{to}.csv", mime="text/csv")
        if not rev_detail.empty:
            st.download_button("⬇️ COGS chi tiết CSV", rev_detail.to_csv(index=False).encode("utf-8"),
                               file_name=f"cogs_chi_tiet_{fr}_{to}.csv", mime="text/csv")
# =========================
# app.py — PART 4/5 (Công thức + Sản xuất 3 loại)
# =========================

# ---- avg_cost: bình quân di động cho 1 mã hàng ----
def avg_cost(conn, store: str, pcode: str) -> float:
    dfc = fetch_df(conn, """
        SELECT kind, qty, price_in
        FROM inventory_ledger
        WHERE store = ? AND pcode = ?
        ORDER BY ts
    """, (store, pcode))
    stock = 0.0
    cost  = 0.0
    for _, r in dfc.iterrows():
        k = r["kind"]; q = float(r["qty"] or 0); p = float(r["price_in"] or 0)
        if k == "IN":
            if q > 0:
                total = cost * stock + p * q
                stock += q
                cost = (total / stock) if stock > 0 else 0.0
        else:  # OUT / ADJ-
            stock -= q
            if stock < 0: stock = 0.0
    return float(cost)

# ---- tiện ích lấy list sản phẩm theo cat (để làm selectbox đẹp) ----
def _prod_select(conn, cats):
    df = fetch_df(conn,
        "SELECT code, name FROM products WHERE cat_code IN (" +
        ",".join(["?"]*len(cats)) + ") ORDER BY code", tuple(cats))
    opts = df["code"].tolist() if not df.empty else []
    def fmt(x): 
        if df.empty or x not in df["code"].values: return x
        return f"{x} — {df.set_index('code').loc[x,'name']}"
    return opts, fmt

# ==========================================
# CÔNG THỨC (CRUD) — chuẩn hóa theo yêu cầu
# ==========================================
def page_congthuc(conn):
    st.header("🧪 Công thức (COT / MUT)")
    if not has_perm(st.session_state.get("user"), "CT_EDIT"):
        st.warning("Bạn không có quyền truy cập Công thức.")
        return

    df_ct = fetch_df(conn, """
        SELECT code,name,type,output_pcode,output_uom,recovery,cups_per_kg,fruits_csv,additives_json,note
        FROM formulas ORDER BY code
    """)
    st.dataframe(df_ct, use_container_width=True)

    mode = st.radio("Chế độ", ["Tạo mới", "Sửa/Xóa"], horizontal=True)

    # ----- chọn SP đầu ra theo loại -----
    def out_opts_by_type(t):
        cats = ["COT"] if t == "COT" else ["MUT"]
        return _prod_select(conn, cats)

    if mode == "Tạo mới":
        colL, colR = st.columns([1,1])
        with colL:
            code = st.text_input("Mã CT")
            name = st.text_input("Tên CT")
            typ  = st.selectbox("Loại CT", ["COT","MUT"])
            out_list, out_fmt = out_opts_by_type(typ)
            outp = st.selectbox("SP đầu ra (mã)", out_list, format_func=out_fmt)
            uom  = st.text_input("ĐVT TP", "kg")
        with colR:
            rec  = st.number_input("Hệ số thu hồi (chỉ CỐT)", 1.0, step=0.1, disabled=(typ!="COT"))
            cups = st.number_input("Số cốc / 1kg TP", 0.0, step=0.1)
            mut_src = st.radio("Nguồn NVL (chỉ MỨT)", ["TRAI_CAY","COT"], index=0, horizontal=True)

        # ----- NVL theo loại & nguồn -----
        if typ == "COT" or mut_src == "TRAI_CAY":
            raw_opts, raw_fmt = _prod_select(conn, ["TRAI_CAY"])
        else:
            raw_opts, raw_fmt = _prod_select(conn, ["COT"])
        raw_sel = st.multiselect("Nguyên liệu chính", raw_opts, format_func=raw_fmt)

        # ----- phụ gia (tùy chọn) -----
        add_opts, add_fmt = _prod_select(conn, ["PHU_GIA"])
        add_sel = st.multiselect("Phụ gia", add_opts, format_func=add_fmt)
        add_q = {}
        if add_sel:
            st.caption("Định lượng phụ gia (kg / 1kg sau sơ chế)")
            for c in add_sel:
                add_q[c] = st.number_input(f"{add_fmt(c)}", 0.0, step=0.01, key=f"add_{c}")

        if st.button("💾 Lưu công thức"):
            if not code or not name or not outp:
                st.error("Thiếu mã, tên hoặc SP đầu ra.")
            else:
                note = f"SRC={mut_src}" if typ=="MUT" else ""
                run_sql(conn, """
                    INSERT OR REPLACE INTO formulas
                        (code,name,type,output_pcode,output_uom,recovery,cups_per_kg,fruits_csv,additives_json,note)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                """, (code.strip(), name.strip(), typ, outp, uom,
                      (rec if typ=="COT" else 1.0), cups,
                      ",".join(raw_sel), json.dumps(add_q), note))
                log_action(conn, st.session_state["user"]["email"], "CT_SAVE", code)
                st.success("Đã lưu công thức")
                st.experimental_rerun()

    else:  # Sửa/Xóa
        if df_ct.empty:
            st.info("Chưa có công thức."); return
        pick = st.selectbox("Chọn CT", df_ct["code"])
        row  = df_ct[df_ct["code"]==pick].iloc[0]
        typ  = st.selectbox("Loại CT", ["COT","MUT"], index=(0 if row["type"]=="COT" else 1))

        out_list, out_fmt = out_opts_by_type(typ)
        try:
            def_idx = out_list.index(row["output_pcode"]) if row["output_pcode"] in out_list else 0
        except Exception:
            def_idx = 0

        colL, colR = st.columns([1,1])
        with colL:
            name = st.text_input("Tên CT", row["name"])
            outp = st.selectbox("SP đầu ra (mã)", out_list, index=def_idx, format_func=out_fmt)
            uom  = st.text_input("ĐVT TP", row["output_uom"])
        with colR:
            rec  = st.number_input("Hệ số thu hồi (CỐT)", float(row["recovery"] or 1.0), step=0.1, disabled=(typ!="COT"), key="rec_edit")
            cups = st.number_input("Số cốc / 1kg TP", float(row["cups_per_kg"] or 0.0), step=0.1)
            src0 = "TRAI_CAY"
            if typ=="MUT" and (row["note"] or "").startswith("SRC="):
                src0 = (row["note"] or "").split("=",1)[1]
            mut_src = st.radio("Nguồn NVL (MỨT)", ["TRAI_CAY","COT"], index=(0 if src0=="TRAI_CAY" else 1), horizontal=True)

        # NVL khởi tạo
        if typ == "COT" or mut_src == "TRAI_CAY":
            raw_opts, raw_fmt = _prod_select(conn, ["TRAI_CAY"])
        else:
            raw_opts, raw_fmt = _prod_select(conn, ["COT"])
        current_raws = [x for x in (row["fruits_csv"] or "").split(",") if x]
        raw_sel = st.multiselect("Nguyên liệu chính", raw_opts, default=[r for r in current_raws if r in raw_opts], format_func=raw_fmt)

        # Phụ gia
        try:
            adds0 = json.loads(row["additives_json"] or "{}")
        except Exception:
            adds0 = {}
        add_opts, add_fmt = _prod_select(conn, ["PHU_GIA"])
        add_pick = st.multiselect("Phụ gia", add_opts, default=list(adds0.keys()), format_func=add_fmt)
        add_q = {}
        if add_pick:
            st.caption("Định lượng phụ gia (kg / 1kg sau sơ chế)")
            for c in add_pick:
                add_q[c] = st.number_input(f"{add_fmt(c)}", float(adds0.get(c,0.0)), step=0.01, key=f"add_edit_{c}")

        c1, c2 = st.columns(2)
        with c1:
            if st.button("💾 Cập nhật"):
                note = f"SRC={mut_src}" if typ=="MUT" else ""
                run_sql(conn, """
                    INSERT OR REPLACE INTO formulas
                        (code,name,type,output_pcode,output_uom,recovery,cups_per_kg,fruits_csv,additives_json,note)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                """, (pick, name.strip(), typ, outp, uom,
                      (rec if typ=="COT" else 1.0), cups,
                      ",".join(raw_sel), json.dumps(add_q), note))
                log_action(conn, st.session_state["user"]["email"], "CT_UPDATE", pick)
                st.success("Đã cập nhật")
                st.experimental_rerun()
        with c2:
            if st.button("🗑️ Xóa công thức"):
                run_sql(conn, "DELETE FROM formulas WHERE code=?", (pick,))
                log_action(conn, st.session_state["user"]["email"], "CT_DELETE", pick)
                st.success("Đã xóa")
                st.experimental_rerun()

# ==========================================
# SẢN XUẤT — 3 luồng: CỐT / MỨT từ TRÁI CÂY / MỨT từ CỐT
# ==========================================
def _consume_materials(conn, store, items: Dict[str, float], ref: str):
    """Xuất NVL theo dict {pcode: qty}, cups=0 (NVL & phụ gia không tính cốc)."""
    for p, q in items.items():
        if q > 0:
            post_ledger(conn, store, p, "OUT", q, price_in=0.0, cups=0.0, ref=ref)

def _receive_finish(conn, store, pcode, kg_out, unit_cost, cups_per_kg, ref):
    """Nhập TP, cups = kg_out * cups_per_kg."""
    cups_in = max(0.0, float(kg_out or 0.0)) * max(0.0, float(cups_per_kg or 0.0))
    post_ledger(conn, store, pcode, "IN", kg_out, price_in=float(unit_cost or 0.0), cups=cups_in, ref=ref)

def _avg_cost_from_raws(conn, store, raws: list) -> float:
    """Lấy bình quân đơn giá các NVL chính (ước tính giá TP)."""
    if not raws: return 0.0
    vals = []
    for r in raws:
        c = avg_cost(conn, store, r)
        if c > 0: vals.append(c)
        else:
            # fallback: đơn giá nhập gần nhất
            last = fetch_df(conn, """
                SELECT price_in FROM inventory_ledger
                WHERE store=? AND pcode=? AND kind='IN'
                ORDER BY ts DESC LIMIT 1
            """, (store, r))
            if not last.empty:
                vals.append(float(last.iloc[0]["price_in"] or 0))
    return float(sum(vals)/len(vals)) if vals else 0.0

def page_sanxuat(conn):
    st.header("🏭 Sản xuất")
    store = st.session_state.get("store","")

    tab_cot, tab_mut_tc, tab_mut_ct = st.tabs(["Thành phẩm CỐT", "Mứt từ TRÁI CÂY", "Mứt từ CỐT"])

    # ======== 1) CỐT ========
    with tab_cot:
        st.subheader("SX CỐT (có hệ số thu hồi)")
        cts = fetch_df(conn, "SELECT * FROM formulas WHERE type='COT' ORDER BY code")
        ct_pick = st.selectbox("Chọn CT CỐT", cts["code"].tolist() if not cts.empty else [])
        if ct_pick:
            row = cts[cts["code"]==ct_pick].iloc[0].to_dict()
            out_p = row["output_pcode"]
            rec   = float(row["recovery"] or 1.0)
            cups  = float(row["cups_per_kg"] or 0.0)
            raws  = [x for x in (row["fruits_csv"] or "").split(",") if x]
            adds  = json.loads(row["additives_json"] or "{}")

            kg_sau_so = st.number_input("KG sau sơ chế (đầu vào)", 0.0, step=0.1)
            kg_tp     = st.number_input("KG thành phẩm (tính theo hệ số)", value=kg_sau_so*rec, step=0.1)
            st.caption(f"Hệ số thu hồi = {rec}. Cốc/1kg TP = {cups}.")

            if st.button("✅ Ghi sổ SX CỐT"):
                # Xuất NVL: chia đều theo số NVL chính (có thể nâng cấp thêm định mức riêng từng NVL nếu có)
                n = max(1, len(raws))
                consume = {r: (kg_sau_so / n) for r in raws}
                # Phụ gia: định lượng theo kg sau sơ
                for pg, per1 in adds.items():
                    consume[pg] = consume.get(pg, 0.0) + float(per1 or 0.0) * kg_sau_so
                _consume_materials(conn, store, consume, ref=f"PRD_COT:{ct_pick}")

                # Giá thành TP = bình quân đơn giá NVL chính (ước tính)
                unit_cost = _avg_cost_from_raws(conn, store, raws)
                _receive_finish(conn, store, out_p, kg_tp, unit_cost, cups_per_kg=cups, ref=f"PRD_COT:{ct_pick}")

                log_action(conn, st.session_state["user"]["email"], "PRD_COT", f"{ct_pick} -> {out_p} {kg_tp}kg @~{unit_cost}")
                st.success("Đã ghi sổ SX CỐT & nhập kho thành phẩm.")
                st.experimental_rerun()

    # ======== 2) MỨT từ TRÁI CÂY ========
    with tab_mut_tc:
        st.subheader("SX MỨT (nguồn TRÁI CÂY) — KHÔNG có hệ số thu hồi")
        cts = fetch_df(conn, "SELECT * FROM formulas WHERE type='MUT' AND (note LIKE 'SRC=TRAI_CAY%' OR note='' OR note IS NULL) ORDER BY code")
        ct_pick = st.selectbox("Chọn CT MỨT (TRÁI CÂY)", cts["code"].tolist() if not cts.empty else [], key="ct_mut_tc")
        if ct_pick:
            row = cts[cts["code"]==ct_pick].iloc[0].to_dict()
            out_p = row["output_pcode"]
            cups  = float(row["cups_per_kg"] or 0.0)
            raws  = [x for x in (row["fruits_csv"] or "").split(",") if x]   # trái cây
            adds  = json.loads(row["additives_json"] or "{}")

            kg_sau_so = st.number_input("KG sau sơ chế (đầu vào)", 0.0, step=0.1, key="mut_tc_in")
            kg_tp     = st.number_input("KG thành phẩm MỨT", 0.0, step=0.1, key="mut_tc_out")

            if st.button("✅ Ghi sổ SX MỨT (trái cây)"):
                n = max(1, len(raws))
                consume = {r: (kg_sau_so / n) for r in raws}
                for pg, per1 in adds.items():
                    consume[pg] = consume.get(pg, 0.0) + float(per1 or 0.0) * kg_sau_so
                _consume_materials(conn, store, consume, ref=f"PRD_MUT_TC:{ct_pick}")

                unit_cost = _avg_cost_from_raws(conn, store, raws)
                _receive_finish(conn, store, out_p, kg_tp, unit_cost, cups_per_kg=cups, ref=f"PRD_MUT_TC:{ct_pick}")

                log_action(conn, st.session_state["user"]["email"], "PRD_MUT_TC", f"{ct_pick} -> {out_p} {kg_tp}kg @~{unit_cost}")
                st.success("Đã ghi sổ SX MỨT (TRÁI CÂY) & nhập kho.")
                st.experimental_rerun()

    # ======== 3) MỨT từ CỐT ========
    with tab_mut_ct:
        st.subheader("SX MỨT (nguồn CỐT) — KHÔNG có hệ số thu hồi")
        cts = fetch_df(conn, "SELECT * FROM formulas WHERE type='MUT' AND note LIKE 'SRC=COT%' ORDER BY code")
        ct_pick = st.selectbox("Chọn CT MỨT (CỐT)", cts["code"].tolist() if not cts.empty else [], key="ct_mut_ct")
        if ct_pick:
            row = cts[cts["code"]==ct_pick].iloc[0].to_dict()
            out_p = row["output_pcode"]
            cups  = float(row["cups_per_kg"] or 0.0)
            raws  = [x for x in (row["fruits_csv"] or "").split(",") if x]   # danh mục CỐT dùng làm NVL
            adds  = json.loads(row["additives_json"] or "{}")

            kg_cot = st.number_input("KG CỐT dùng", 0.0, step=0.1)
            kg_tp  = st.number_input("KG thành phẩm MỨT", 0.0, step=0.1)

            if st.button("✅ Ghi sổ SX MỨT (CỐT)"):
                n = max(1, len(raws))
                consume = {r: (kg_cot / n) for r in raws}
                for pg, per1 in adds.items():
                    consume[pg] = consume.get(pg, 0.0) + float(per1 or 0.0) * kg_cot
                _consume_materials(conn, store, consume, ref=f"PRD_MUT_CT:{ct_pick}")

                unit_cost = _avg_cost_from_raws(conn, store, raws)  # bình quân đơn giá các CỐT dùng
                _receive_finish(conn, store, out_p, kg_tp, unit_cost, cups_per_kg=cups, ref=f"PRD_MUT_CT:{ct_pick}")

                log_action(conn, st.session_state["user"]["email"], "PRD_MUT_CT", f"{ct_pick} -> {out_p} {kg_tp}kg @~{unit_cost}")
                st.success("Đã ghi sổ SX MỨT (CỐT) & nhập kho.")
                st.experimental_rerun()
# =========================
# app.py — PART 5/5 (Doanh thu + TSCD + Dashboard + Main)
# =========================

# ---- Nâng cấp bảng revenue để tương thích báo cáo nâng cao (Phần 3) ----
def ensure_revenue_enhanced(conn):
    # Thêm cột nếu thiếu: pcode, qty, unit_price (để query qty*unit_price không lỗi)
    try:
        run_sql(conn, "ALTER TABLE revenue ADD COLUMN pcode TEXT")
    except Exception:
        pass
    try:
        run_sql(conn, "ALTER TABLE revenue ADD COLUMN qty DOUBLE PRECISION")
    except Exception:
        pass
    try:
        run_sql(conn, "ALTER TABLE revenue ADD COLUMN unit_price DOUBLE PRECISION")
    except Exception:
        pass

# ---- Doanh thu: 2 chế độ nhập ----
def page_doanhthu(conn):
    st.header("💵 Doanh thu")
    ensure_revenue_enhanced(conn)
    store = st.session_state.get("store","")

    tab_simple, tab_detail = st.tabs(["Ghi tổng tiền (CASH/BANK)", "Ghi chi tiết theo SP (tùy chọn)"])

    # ----- 1) Ghi tổng tiền -----
    with tab_simple:
        st.subheader("Ghi tổng tiền theo ngày & phương thức")
        d = st.date_input("Ngày", date.today(), key="rev_d")
        pay = st.selectbox("Thanh toán", ["CASH","BANK"], key="rev_pay")
        amt = st.number_input("Số tiền (VND)", 0.0, step=1000.0, key="rev_amt")
        note = st.text_input("Ghi chú", key="rev_note")
        if st.button("💾 Lưu (tổng tiền)"):
            run_sql(conn, """
                INSERT INTO revenue(ts,store,amount,pay_method,pcode,qty,unit_price,note)
                VALUES (?,?,?,?,?,?,?,?)
            """, (d, store, amt, pay, None, None, None, note))
            log_action(conn, st.session_state["user"]["email"], "REV_ADD_SUM", f"{d} {pay} {amt}")
            st.success("Đã lưu")
            st.experimental_rerun()

        st.markdown("**Nhật ký (30 ngày gần đây)**")
        df = fetch_df(conn, """
            SELECT ts, pay_method, amount, note
            FROM revenue
            WHERE store=? AND ts>=?
            ORDER BY ts DESC
        """, (store, (date.today() - timedelta(days=30)).strftime("%Y-%m-%d")))
        st.dataframe(df, use_container_width=True)

    # ----- 2) Ghi chi tiết theo SP (phục vụ báo cáo nâng cao/COGS) -----
    with tab_detail:
        st.subheader("Ghi theo sản phẩm (tùy chọn)")
        d2 = st.date_input("Ngày", date.today(), key="rev2_d")
        pay2 = st.selectbox("Thanh toán", ["CASH","BANK"], key="rev2_pay")
        # chọn SP
        kw = st.text_input("Tìm SP", key="rev2_kw")
        opts = product_list(conn, keyword=kw)
        def fmt(m):
            if opts.empty or m not in opts["code"].values: return m
            return f"{m} — {opts.set_index('code').loc[m,'name']}"
        pcode = st.selectbox("Sản phẩm", opts["code"].tolist() if not opts.empty else [], format_func=fmt)
        qty = st.number_input("Số lượng", 0.0, step=0.1, key="rev2_qty")
        unit_price = st.number_input("Đơn giá bán (VND/ĐVT)", 0.0, step=500.0, key="rev2_price")
        note2 = st.text_input("Ghi chú", key="rev2_note")

        colA, colB = st.columns(2)
        with colA:
            if st.button("💾 Lưu (chi tiết SP)"):
                amount = qty * unit_price
                run_sql(conn, """
                    INSERT INTO revenue(ts,store,amount,pay_method,pcode,qty,unit_price,note)
                    VALUES (?,?,?,?,?,?,?,?)
                """, (d2, store, amount, pay2, pcode, qty, unit_price, note2))
                log_action(conn, st.session_state["user"]["email"], "REV_ADD_DETAIL", f"{d2} {pcode} {qty} x {unit_price}")
                st.success("Đã lưu")
                st.experimental_rerun()
        with colB:
            # xuất CSV nhanh để kiểm tra
            dfrom = st.date_input("Từ ngày", date.today().replace(day=1), key="rev2_from")
            dto   = st.date_input("Đến ngày", date.today(), key="rev2_to")
            df2 = fetch_df(conn, """
                SELECT ts, pcode, qty, unit_price, amount, pay_method, note
                FROM revenue
                WHERE store=? AND ts BETWEEN ? AND ?
                ORDER BY ts DESC
            """, (store, dfrom.strftime("%Y-%m-%d"), dto.strftime("%Y-%m-%d")))
            st.dataframe(df2, use_container_width=True)
            if not df2.empty:
                st.download_button("⬇️ Export CSV", df2.to_csv(index=False).encode("utf-8"),
                                   file_name=f"revenue_detail_{dfrom}_{dto}.csv", mime="text/csv")

# ---- TSCD ----
def page_tscd(conn):
    st.header("🏗️ Tài sản cố định")
    df = fetch_df(conn, "SELECT id,name,cost,dep_per_month,buy_date FROM tscd ORDER BY id DESC")
    st.dataframe(df, use_container_width=True)

    c1,c2,c3,c4 = st.columns(4)
    with c1:
        name = st.text_input("Tên TS")
    with c2:
        cost = st.number_input("Nguyên giá", 0.0, step=100000.0)
    with c3:
        dep = st.number_input("Khấu hao/tháng", 0.0, step=10000.0)
    with c4:
        buy = st.date_input("Ngày mua", date.today())

    if st.button("💾 Thêm TSCD"):
        run_sql(conn, "INSERT INTO tscd(name,cost,dep_per_month,buy_date) VALUES(?,?,?,?)",
                (name, cost, dep, buy))
        log_action(conn, st.session_state["user"]["email"], "TSCD_ADD", name)
        st.success("Đã lưu"); st.experimental_rerun()

    # Tổng hợp nhanh
    st.subheader("Tổng hợp khấu hao & giá trị ròng (ước tính)")
    def months_between(d0: date, d1: date) -> int:
        return max(0, (d1.year - d0.year)*12 + (d1.month - d0.month))
    today = date.today()
    kh_lk = 0.0; ng = 0.0
    for _, r in df.iterrows():
        ng += float(r["cost"] or 0.0)
        kh_lk += min(float(r["cost"] or 0.0), months_between(pd.to_datetime(r["buy_date"]).date(), today) * float(r["dep_per_month"] or 0.0))
    st.metric("Nguyên giá", f"{ng:,.0f} VND")
    st.metric("Khấu hao lũy kế (ước)", f"{kh_lk:,.0f} VND")
    st.metric("Giá trị ròng", f"{max(0.0, ng - kh_lk):,.0f} VND")

# ---- Dashboard ----
def page_dashboard(conn):
    st.title("🧃 Fruit Tea ERP v5 – Dashboard")
    store = st.session_state.get("store","")

    col1, col2, col3 = st.columns(3)

    # Giá trị tồn kho (đến hôm nay)
    inv = fetch_df(conn, """
        SELECT pcode, kind, qty, price_in, ts
        FROM inventory_ledger
        WHERE store=? ORDER BY ts
    """, (store,))
    avg_cost_map, stock_map = {}, {}
    for _, r in inv.iterrows():
        p=r["pcode"]; q=float(r["qty"] or 0); k=r["kind"]; pr=float(r["price_in"] or 0)
        if p not in stock_map: stock_map[p]=0.0; avg_cost_map[p]=0.0
        if k=="IN":
            total=avg_cost_map[p]*stock_map[p]+pr*q
            stock_map[p]+=q
            avg_cost_map[p]=(total/stock_map[p]) if stock_map[p]>0 else 0.0
        else:
            stock_map[p]-=q
            if stock_map[p]<0: stock_map[p]=0.0
    total_value = sum(max(0.0, stock_map.get(p,0.0))*avg_cost_map.get(p,0.0) for p in stock_map)

    # Doanh thu 14 ngày gần nhất (tổng amount)
    df_rev = fetch_df(conn, """
        SELECT ts::date d, SUM(COALESCE(amount,0)) amount
        FROM revenue
        WHERE store=? AND ts>=?
        GROUP BY ts::date
        ORDER BY d
    """, (store, (date.today()-timedelta(days=13)).strftime("%Y-%m-%d")))
    rev_today = float(df_rev[df_rev["d"]==pd.to_datetime(date.today())]["amount"].sum()) if not df_rev.empty else 0.0

    col1.metric("Giá trị tồn kho", f"{total_value:,.0f} VND")
    col2.metric("Doanh thu hôm nay", f"{rev_today:,.0f} VND")
    col3.metric("Cửa hàng hiện tại", st.session_state.get("store",""))

    st.subheader("Doanh thu 14 ngày gần nhất")
    if not df_rev.empty:
        st.line_chart(df_rev.set_index("d")["amount"])
    else:
        st.info("Chưa có dữ liệu doanh thu.")

# ---- Menu & main() ----
def main_app():
    # Đảm bảo schema & nâng cấp revenue
    ensure_min_schema(conn)
    ensure_revenue_enhanced(conn)

    user = require_login(conn)

    # Sidebar
    with st.sidebar:
        st.markdown(f"**👤 {user.get('display','')}**  \n`{user.get('email','')}`")
        st.markdown("---")
        menu = st.radio("Menu", [
            "Dashboard",
            "Danh mục",
            "Kho",
            "Sản xuất",
            "Doanh thu",
            "Báo cáo",
            "TSCD",
            "Nhật ký",
            "Đăng xuất"
        ])
        st.markdown("---")
        st.caption("DB: " + ("Postgres" if os.getenv("DATABASE_URL") else "SQLite"))

    # Router
    if menu == "Dashboard":
        page_dashboard(conn)
    elif menu == "Danh mục":
        page_danhmuc(conn)
    elif menu == "Kho":
        page_kho(conn)
    elif menu == "Sản xuất":
        page_sanxuat(conn)
    elif menu == "Doanh thu":
        page_doanhthu(conn)
    elif menu == "Báo cáo":
        page_baocao(conn)
    elif menu == "TSCD":
        page_tscd(conn)
    elif menu == "Nhật ký":
        st.header("🧾 Nhật ký hệ thống")
        df = fetch_df(conn, "SELECT ts,actor,action,detail FROM syslog ORDER BY ts DESC LIMIT 500")
        st.dataframe(df, use_container_width=True)
    else:
        log_action(conn, user["email"], "LOGOUT", "")
        st.session_state.clear()
        st.experimental_rerun()

# ---- Entry point ----
if __name__ == "__main__":
    main_app()
