# ============================================================
# app.py — PART 1/5 (Core PG-only: Kết nối, Helpers, Auth, Kho)
# KHÔNG tạo bảng, KHÔNG seed. YÊU CẦU: bảng đã có sẵn trên Supabase.
# ============================================================
import os, re, json, hashlib
from datetime import datetime, date, timedelta
from typing import Dict, Any, Optional, Tuple

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

# ---- UI config
st.set_page_config(page_title="FruitTea ERP v5 (PG only)", page_icon="🧃", layout="wide")

# ============================================================
# KẾT NỐI POSTGRES (Supabase Pooler) — KHÔNG dùng SQLite
# ============================================================
_ENGINE = None
def _normalize_pg_url(url: str) -> str:
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif url.startswith("postgresql://") and "+psycopg2" not in url:
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    if "sslmode=" not in url:
        sep = "&" if "?" in url else "?"
        url = url + f"{sep}sslmode=require"
    return url

def get_conn_pg():
    """Kết nối Postgres (Supabase Session Pooler)."""
    global _ENGINE
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        st.error("Thiếu DATABASE_URL (Supabase Pooler). Vào Settings → Secrets để đặt."); st.stop()
    url = _normalize_pg_url(url)
    if _ENGINE is None:
        _ENGINE = create_engine(url, pool_pre_ping=True, future=True)
    try:
        conn = _ENGINE.connect()
        conn.execute(text("select 1"))
        return conn
    except Exception as e:
        st.error(f"Không kết nối được Postgres: {e}"); st.stop()

# ============================================================
# TIỆN ÍCH SQL (PG-only) — hỗ trợ ? → :p1
# ============================================================
def _qmark_to_named(sql: str, params):
    if not isinstance(params, (list, tuple)):
        return sql, (params or {})
    idx = 1
    def repl(_):
        nonlocal idx
        s = f":p{idx}"; idx += 1; return s
    sql2 = re.sub(r"\?", repl, sql)
    params2 = {f"p{i+1}": v for i, v in enumerate(params)}
    return sql2, params2

def run_sql(conn, sql: str, params=None):
    """Execute WRITE. Dùng dict param cho PG; chấp nhận list/tuple rồi tự đổi."""
    if isinstance(params, (list, tuple)):
        sql, params = _qmark_to_named(sql, params)
    res = conn.execute(text(sql), params or {})
    try: conn.commit()
    except: pass
    return res

def fetch_df(conn, sql: str, params=None) -> pd.DataFrame:
    """Execute READ → DataFrame."""
    if isinstance(params, (list, tuple)):
        sql, params = _qmark_to_named(sql, params)
    return pd.read_sql_query(text(sql), conn, params=params or {})

# ============================================================
# AUTH / QUYỀN / LOG
# ============================================================
def sha256(txt: str) -> str:
    return hashlib.sha256((txt or "").encode("utf-8")).hexdigest()

def has_perm(user: Dict[str, Any], perm: str) -> bool:
    if not user: return False
    if (user.get("role") or "").lower() in ("superadmin","admin"): return True
    perms = (user.get("perms") or "").upper().split(",")
    return perm.upper() in [p.strip() for p in perms if p.strip()]

def log_action(conn, actor: str, action: str, detail: str = ""):
    run_sql(conn, "INSERT INTO syslog(user_email,action,detail) VALUES(:u,:a,:d)",
            {"u": actor or "", "a": action or "", "d": detail or ""})

def login_form(conn) -> Optional[Dict[str, Any]]:
    st.header("Đăng nhập")
    email = st.text_input("Email", value="admin@example.com")
    pw    = st.text_input("Mật khẩu", type="password", value="admin")
    if st.button("Đăng nhập"):
        df = fetch_df(conn, "SELECT * FROM users WHERE email=:e", {"e": email})
        if df.empty: st.error("Sai tài khoản / mật khẩu"); return None
        r = df.iloc[0].to_dict()
        ok = (pw == r.get("password")) or (sha256(pw) == r.get("password"))
        if not ok: st.error("Sai tài khoản / mật khẩu"); return None
        st.success("Đăng nhập thành công")
        return {
            "email": r["email"], "display": r.get("display") or r["email"],
            "role": r.get("role") or "user",
            "store": r.get("store_code") or "HOSEN",
            "perms": r.get("perms") or ""
        }
    return None

def require_login(conn) -> Dict[str, Any]:
    if "user" not in st.session_state or not st.session_state["user"]:
        u = login_form(conn)
        if not u: st.stop()
        st.session_state["user"] = u
        log_action(conn, u["email"], "LOGIN", "ok")
    return st.session_state["user"]

# ============================================================
# HELPERS DANH MỤC / KHO (có số cốc) / GIÁ BQ / TỒN
# ============================================================
def store_options(conn) -> pd.DataFrame:
    return fetch_df(conn, "SELECT code,name FROM stores ORDER BY code")

def prod_options(conn, cat_code: Optional[str] = None) -> pd.DataFrame:
    if cat_code:
        return fetch_df(conn, "SELECT code,name,uom,cat_code FROM products WHERE cat_code=:c ORDER BY code", {"c": cat_code})
    return fetch_df(conn, "SELECT code,name,uom,cat_code FROM products ORDER BY code")

def product_list(conn, cat: str | None = None, keyword: str = "") -> pd.DataFrame:
    sql = "SELECT code,name,uom,cat_code FROM products"
    where, params = [], {}
    if cat: where.append("cat_code = :cat"); params["cat"] = cat
    if keyword: where.append("(code ILIKE :kw OR name ILIKE :kw)"); params["kw"] = f"%{keyword}%"
    if where: sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY code"
    return fetch_df(conn, sql, params)

def post_ledger(conn, store: str, pcode: str, kind: str, qty: float,
                price_in: float = 0.0, cups: float = 0.0, ref: str = "", note: str = ""):
    run_sql(conn, """
        INSERT INTO inventory_ledger(store,pcode,kind,qty,price_in,cups,ref,note)
        VALUES (:s,:p,:k,:q,:pr,:c,:r,:n)
    """, {"s": store, "p": pcode, "k": kind, "q": float(qty or 0.0),
          "pr": float(price_in or 0.0), "c": float(cups or 0.0),
          "r": ref or "", "n": note or ""})

def cups_per_kg_of(conn, pcode: str) -> float:
    """Ưu tiên lấy cups/kg từ công thức gắn với output_pcode."""
    df = fetch_df(conn, "SELECT cups_per_kg FROM formulas WHERE output_pcode=:p LIMIT 1", {"p": pcode})
    if df.empty: return 0.0
    try: return float(df.iloc[0]["cups_per_kg"] or 0.0)
    except: return 0.0

def inv_balance(conn, store: str, pcode: str) -> Tuple[float, float]:
    """(tồn kg, tồn cốc) đến hiện tại."""
    df = fetch_df(conn, """
        SELECT
          COALESCE(SUM(CASE WHEN kind='IN' THEN qty ELSE -qty END),0)  AS ton_qty,
          COALESCE(SUM(CASE WHEN kind='IN' THEN cups ELSE -cups END),0) AS ton_cups
        FROM inventory_ledger
        WHERE store=:s AND pcode=:p
    """, {"s": store, "p": pcode})
    if df.empty: return 0.0, 0.0
    return float(df.iloc[0]["ton_qty"] or 0.0), float(df.iloc[0]["ton_cups"] or 0.0)

def avg_cost(conn, store: str, pcode: str) -> float:
    """Giá bình quân di động theo thứ tự sổ kho."""
    df = fetch_df(conn, """
        SELECT kind, qty, price_in
        FROM inventory_ledger
        WHERE store=:s AND pcode=:p
        ORDER BY ts, id
    """, {"s": store, "p": pcode})
    stock = 0.0; cost = 0.0
    for _, r in df.iterrows():
        k = r["kind"]; q = float(r["qty"] or 0); p = float(r["price_in"] or 0)
        if k == "IN":
            if q > 0:
                total = cost*stock + p*q
                stock += q
                cost = (total/stock) if stock>0 else 0.0
        else:
            stock -= q
            if stock < 0: stock = 0.0
    return float(cost)

def stock_snapshot(conn, store: str, to_date: date | None = None) -> pd.DataFrame:
    """
    Ảnh chốt tồn đến ngày (nếu có). Trả về: pcode,name,uom,cat_code,ton_qty,avg_cost,value,ton_cups.
    Nếu cups=0 với COT/MUT → suy ra từ cups_per_kg * ton_qty.
    """
    params = {"s": store}
    date_filter = ""
    if to_date:
        date_filter = "AND ts::date <= :d"; params["d"] = to_date.strftime("%Y-%m-%d")
    df = fetch_df(conn, f"""
        SELECT pcode,
               SUM(CASE WHEN kind='IN' THEN qty ELSE -qty END)  AS ton_qty,
               SUM(CASE WHEN kind='IN' THEN cups ELSE -cups END) AS ton_cups
        FROM inventory_ledger
        WHERE store=:s {date_filter}
        GROUP BY pcode
        HAVING ABS(SUM(CASE WHEN kind='IN' THEN qty ELSE -qty END)) <> 0
            OR ABS(SUM(CASE WHEN kind='IN' THEN cups ELSE -cups END)) <> 0
        ORDER BY pcode
    """, params)
    if df.empty:
        return pd.DataFrame(columns=["pcode","name","uom","cat_code","ton_qty","avg_cost","value","ton_cups"])
    prods = fetch_df(conn, "SELECT code,name,uom,cat_code FROM products")
    df = df.merge(prods, left_on="pcode", right_on="code", how="left").drop(columns=["code"])
    df["avg_cost"] = df["pcode"].apply(lambda c: avg_cost(conn, store, c))
    df["value"]    = df["avg_cost"] * df["ton_qty"]
    def ensure_cups(row):
        cups = float(row.get("ton_cups") or 0.0)
        if cups == 0.0 and row["cat_code"] in ("COT","MUT"):
            cups = float(row["ton_qty"] or 0.0) * cups_per_kg_of(conn, row["pcode"])
        return cups
    df["ton_cups"] = df.apply(ensure_cups, axis=1)
    return df[["pcode","name","uom","cat_code","ton_qty","avg_cost","value","ton_cups"]]

# ============================================================
# KẾT NỐI
# ============================================================
conn = get_conn_pg()
st.caption("✅ DB: Postgres (Supabase) — connected")
# ============================================================
# app.py — PART 2/5 (Sidebar + Danh mục: Cửa hàng / Sản phẩm / Người dùng)
# ============================================================

# ---------- Sidebar & Header ----------
def build_sidebar_and_get_menu(conn) -> str:
    user = require_login(conn)

    with st.sidebar:
        st.markdown(f"### 👤 {user.get('display','')}")
        st.caption(user.get("email",""))
        st.divider()

        # Chọn cửa hàng
        stores_df = store_options(conn)
        if stores_df.empty:
            st.warning("⚠️ Chưa có cửa hàng. Tạo tại Danh mục → Cửa hàng.")
            current_store = user.get("store") or "HOSEN"
        else:
            store_list = stores_df["code"].tolist()
            default_code = user.get("store") or (store_list[0] if store_list else "HOSEN")
            idx = store_list.index(default_code) if default_code in store_list else 0
            current_store = st.selectbox("🏬 Cửa hàng", store_list, index=idx, help="Áp dụng cho toàn bộ nghiệp vụ")

        # Đồng bộ session + user.store_code
        if st.session_state.get("store") != current_store:
            st.session_state["store"] = current_store
            try:
                run_sql(conn, "UPDATE users SET store_code=:s WHERE email=:e",
                        {"s": current_store, "e": user["email"]})
            except Exception:
                pass

        st.divider()
        menu = st.radio(
            "📚 Menu",
            ["Dashboard", "Danh mục", "Kho", "Sản xuất", "Doanh thu", "Báo cáo", "TSCD", "Nhật ký", "Đăng xuất"],
            index=1
        )
        st.divider()
        st.caption("DB: Postgres (Supabase)")
    return menu

# ---------- Danh mục ----------
def page_danhmuc(conn):
    st.header("📚 Danh mục (Master Data)")
    tabs = st.tabs(["🏬 Cửa hàng", "📦 Sản phẩm", "👥 Người dùng & Quyền"])

    # ====== TAB 1: CỬA HÀNG ======
    with tabs[0]:
        st.subheader("🏬 Cửa hàng")
        colf1, colf2 = st.columns([1,3])
        with colf1:
            kw = st.text_input("Tìm mã/tên", placeholder="Nhập mã hoặc tên...")
        with colf2:
            st.caption("Tạo/sửa/xóa cửa hàng. Lưu ý: mã là duy nhất.")

        df = fetch_df(conn, "SELECT code,name,address,note FROM stores ORDER BY code")
        if kw:
            df = df[df["code"].str.contains(kw, case=False) | df["name"].str.contains(kw, case=False)]
        st.dataframe(df, use_container_width=True, height=300)

        st.markdown("#### ➕ Thêm / ✏️ Sửa")
        with st.form("store_form", clear_on_submit=False):
            c1, c2 = st.columns([1,2])
            with c1:
                code = st.text_input("Mã cửa hàng*", value=(df["code"].iloc[0] if not df.empty else "HOSEN"))
            with c2:
                name = st.text_input("Tên cửa hàng*")
            address = st.text_input("Địa chỉ")
            note = st.text_input("Ghi chú")
            ok = st.form_submit_button("💾 Lưu")
        if ok:
            if not code or not name:
                st.error("⚠️ Mã và Tên bắt buộc.")
            else:
                run_sql(conn, """
                    INSERT INTO stores(code,name,address,note)
                    VALUES(:c,:n,:a,:no)
                    ON CONFLICT (code) DO UPDATE SET
                        name=EXCLUDED.name, address=EXCLUDED.address, note=EXCLUDED.note
                """, {"c": code.strip(), "n": name.strip(), "a": address.strip(), "no": note.strip()})
                log_action(conn, st.session_state["user"]["email"], "DM_STORE_UPSERT", code.strip())
                st.success("✅ Đã lưu cửa hàng.")
                st.experimental_rerun()

        with st.expander("🗑️ Xóa cửa hàng", expanded=False):
            del_code = st.text_input("Nhập mã cần xóa")
            if st.button("Xác nhận xóa cửa hàng"):
                if not del_code:
                    st.warning("Nhập mã trước khi xóa.")
                else:
                    run_sql(conn, "DELETE FROM stores WHERE code=:c", {"c": del_code.strip()})
                    log_action(conn, st.session_state["user"]["email"], "DM_STORE_DELETE", del_code.strip())
                    st.success("Đã xóa.")
                    st.experimental_rerun()

    # ====== TAB 2: SẢN PHẨM ======
    with tabs[1]:
        st.subheader("📦 Sản phẩm")
        colf1, colf2, colf3 = st.columns([1,1,2])
        with colf1:
            cat_filter = st.selectbox("Nhóm", ["TẤT CẢ","TRAI_CAY","PHU_GIA","COT","MUT","KHAC"], index=0)
        with colf2:
            kwp = st.text_input("Tìm mã/tên", placeholder="VD: CAM, DUONG ...")
        with colf3:
            st.caption("Lưu ý: **COT/MUT** có thêm **số cốc** trong kho & báo cáo.")

        dfp = fetch_df(conn, "SELECT code,name,uom,cat_code FROM products ORDER BY code")
        if cat_filter != "TẤT CẢ":
            dfp = dfp[dfp["cat_code"] == cat_filter]
        if kwp:
            dfp = dfp[dfp["code"].str.contains(kwp, case=False) | dfp["name"].str.contains(kwp, case=False)]
        st.dataframe(dfp, use_container_width=True, height=320)

        st.markdown("#### ➕ Thêm / ✏️ Sửa sản phẩm")
        with st.form("product_form", clear_on_submit=False):
            c1, c2, c3, c4 = st.columns([1.2,2,1,1])
            with c1:
                pcode = st.text_input("Mã SP*")
            with c2:
                pname = st.text_input("Tên SP*")
            with c3:
                uom = st.text_input("ĐVT*", value="kg")
            with c4:
                cat = st.selectbox("Nhóm*", ["TRAI_CAY","PHU_GIA","COT","MUT","KHAC"])
            okp = st.form_submit_button("💾 Lưu SP")
        if okp:
            if not pcode or not pname or not uom:
                st.error("⚠️ Mã/Tên/ĐVT bắt buộc.")
            else:
                run_sql(conn, """
                    INSERT INTO products(code,name,uom,cat_code)
                    VALUES(:c,:n,:u,:cat)
                    ON CONFLICT (code) DO UPDATE SET
                        name=EXCLUDED.name, uom=EXCLUDED.uom, cat_code=EXCLUDED.cat_code
                """, {"c": pcode.strip(), "n": pname.strip(), "u": uom.strip(), "cat": cat})
                log_action(conn, st.session_state["user"]["email"], "DM_PRODUCT_UPSERT", pcode.strip())
                st.success("✅ Đã lưu sản phẩm.")
                st.experimental_rerun()

        with st.expander("🗑️ Xóa sản phẩm", expanded=False):
            del_p = st.text_input("Nhập mã SP cần xóa")
            if st.button("Xác nhận xóa SP"):
                if not del_p:
                    st.warning("Nhập mã trước khi xóa.")
                else:
                    run_sql(conn, "DELETE FROM products WHERE code=:c", {"c": del_p.strip()})
                    log_action(conn, st.session_state["user"]["email"], "DM_PRODUCT_DELETE", del_p.strip())
                    st.success("Đã xóa.")
                    st.experimental_rerun()

    # ====== TAB 3: NGƯỜI DÙNG & QUYỀN ======
    with tabs[2]:
        if not has_perm(st.session_state.get("user"), "USERS"):
            st.warning("⛔ Bạn không có quyền truy cập mục Người dùng.")
        else:
            st.subheader("👥 Người dùng & Quyền")
            kwu = st.text_input("Tìm email/tên", placeholder="Nhập email hoặc tên hiển thị...")
            dfu = fetch_df(conn, "SELECT email,display,role,store_code,perms FROM users ORDER BY email")
            if kwu:
                dfu = dfu[dfu["email"].str.contains(kwu, case=False) | dfu["display"].str.contains(kwu, case=False)]
            st.dataframe(dfu, use_container_width=True, height=320)

            st.markdown("#### ➕ Thêm / ✏️ Sửa người dùng")
            with st.form("user_form", clear_on_submit=False):
                c1, c2 = st.columns([2,1])
                with c1:
                    u_email = st.text_input("Email*", value="")
                    u_display = st.text_input("Tên hiển thị", value="")
                    u_store = st.text_input("Store mặc định", value=st.session_state.get("store","HOSEN"))
                with c2:
                    u_role = st.selectbox("Vai trò", ["SuperAdmin","admin","user"], index=2)
                    u_pw = st.text_input("Mật khẩu (để trống = giữ nguyên nếu đã tồn tại)", type="password")
                    perms_hint = "CSV quyền: KHO,SANXUAT,DANHMUC,DOANHTHU,BAOCAO,USERS,TSCD,TAICHINH,CT_EDIT"
                    u_perms = st.text_area("Quyền riêng (CSV)", value="", height=70, help=perms_hint)

                ok_u = st.form_submit_button("💾 Lưu người dùng")
            if ok_u:
                if not u_email:
                    st.error("⚠️ Email bắt buộc.")
                else:
                    existed = fetch_df(conn, "SELECT email,password FROM users WHERE email=:e", {"e": u_email.strip()})
                    pw_save = (existed.iloc[0]["password"] if (not existed.empty and not u_pw)
                               else (u_pw or "123456"))
                    run_sql(conn, """
                        INSERT INTO users(email,display,password,role,store_code,perms)
                        VALUES(:e,:d,:p,:r,:s,:m)
                        ON CONFLICT (email) DO UPDATE SET
                            display=EXCLUDED.display,
                            password=EXCLUDED.password,
                            role=EXCLUDED.role,
                            store_code=EXCLUDED.store_code,
                            perms=EXCLUDED.perms
                    """, {"e": u_email.strip(), "d": (u_display or u_email).strip(),
                          "p": pw_save, "r": u_role, "s": u_store.strip(), "m": (u_perms or "").strip()})
                    log_action(conn, st.session_state["user"]["email"], "DM_USER_UPSERT", u_email.strip())
                    st.success("✅ Đã lưu người dùng.")
                    st.experimental_rerun()

            with st.expander("🔑 Đổi mật khẩu nhanh", expanded=False):
                me = st.session_state["user"]["email"]
                old = st.text_input("Mật khẩu cũ", type="password")
                new1 = st.text_input("Mật khẩu mới", type="password")
                new2 = st.text_input("Nhập lại mật khẩu mới", type="password")
                if st.button("Cập nhật mật khẩu"):
                    if not new1 or new1 != new2:
                        st.error("Mật khẩu mới không trùng khớp.")
                    else:
                        # Nếu đang lưu plain-text: bỏ check old. Nếu dùng hash thì so sánh sha256(old) với DB.
                        run_sql(conn, "UPDATE users SET password=:p WHERE email=:e", {"p": new1, "e": me})
                        log_action(conn, me, "USER_CHANGE_PASSWORD", "")
                        st.success("Đã đổi mật khẩu.")

            with st.expander("🗑️ Xóa người dùng", expanded=False):
                del_u = st.text_input("Email cần xóa")
                if st.button("Xác nhận xóa user"):
                    if not del_u:
                        st.warning("Nhập email trước khi xóa.")
                    else:
                        run_sql(conn, "DELETE FROM users WHERE email=:e", {"e": del_u.strip()})
                        log_action(conn, st.session_state["user"]["email"], "DM_USER_DELETE", del_u.strip())
                        st.success("Đã xóa.")
                        st.experimental_rerun()

# =============== Router tạm thời (sau Part 2) ===============
if "menu_inited" not in st.session_state:
    st.session_state["menu_inited"] = True
_menu = build_sidebar_and_get_menu(conn)

if _menu == "Danh mục":
    page_danhmuc(conn)
elif _menu == "Đăng xuất":
    log_action(conn, st.session_state["user"]["email"], "LOGOUT", "")
    st.session_state.clear()
    st.experimental_rerun()
else:
    st.info("Tiếp tục dán Phần 3/5, 4/5, 5/5 để hoàn thiện Kho, Sản xuất, Doanh thu, Báo cáo, TSCD, Nhật ký.")
# ============================================================
# app.py — PART 3/5 (Kho + Báo cáo nâng cao | PG only)
# ============================================================

# ---------- TIỆN ÍCH CHỌN SẢN PHẨM (dropdown có tìm kiếm) ----------
def _product_selector(conn, key_prefix: str = "", placeholder: str = "Chọn sản phẩm…",
                      cat_filter: str | None = None):
    kw = st.text_input("Tìm (mã/tên)", key=f"kw_{key_prefix}", placeholder="Gõ vài ký tự để lọc…")
    df_opts = product_list(conn, cat=cat_filter, keyword=kw)
    codes = df_opts["code"].tolist() if not df_opts.empty else []
    def fmt(c):
        if df_opts.empty or c not in df_opts["code"].values: return c
        row = df_opts.set_index("code").loc[c]
        return f"{c} — {row['name']} ({row['uom']})"
    sel = st.selectbox(placeholder, codes, key=f"sel_{key_prefix}", format_func=fmt)
    return sel, (df_opts.set_index("code").loc[sel].to_dict() if sel else None)

# ---------- GHI SỔ TIỆN LỢI ----------
def _write_ledger(conn, ts: date, store: str, pcode: str, kind: str,
                  qty: float, price_in: float = 0.0, cups: float = 0.0,
                  ref: str = "", note: str = ""):
    post_ledger(conn, store=store, pcode=pcode, kind=kind,
                qty=qty, price_in=price_in, cups=cups, ref=ref, note=note)
    log_action(conn, st.session_state["user"]["email"], f"KHO_{kind}",
               f"{store}-{pcode}-{qty} ({'cups='+str(cups) if cups else ''})")

# ---------- KHO ----------
def page_kho(conn):
    st.header(f"📦 Quản lý kho – {st.session_state.get('store','')}")
    tab_in, tab_out, tab_ton = st.tabs(["Phiếu nhập", "Phiếu xuất", "Tồn kho (nâng cao)"])

    # ====== PHIẾU NHẬP ======
    with tab_in:
        st.subheader("Phiếu nhập")
        c1, c2, c3 = st.columns([1.2, 2, 1.2])
        with c1:
            ngay = st.date_input("Ngày nhập", datetime.today().date(), key="in_date")
        with c2:
            pcode, _ = _product_selector(conn, key_prefix="in", placeholder="Chọn sản phẩm nhập")
        with c3:
            qty = st.number_input("Số lượng", 0.0, step=0.1, min_value=0.0, key="in_qty")
            price = st.number_input("Đơn giá nhập (VND/ĐVT)", 0.0, step=100.0, min_value=0.0, key="in_price")

        # cups tự tính cho COT/MUT
        cups_in = 0.0
        if pcode:
            info = fetch_df(conn, "SELECT cat_code FROM products WHERE code=:c", {"c": pcode})
            if not info.empty and info.iloc[0]["cat_code"] in ("COT","MUT"):
                cups_in = qty * cups_per_kg_of(conn, pcode)
        st.caption(f"👉 Số cốc ghi nhận: **{cups_in:.0f}**")

        note = st.text_input("Ghi chú (tuỳ chọn)", key="in_note")
        if st.button("💾 Lưu phiếu nhập"):
            if not pcode or qty <= 0:
                st.error("Chọn sản phẩm và nhập số lượng > 0.")
            else:
                _write_ledger(conn, ngay, st.session_state["store"], pcode, "IN", qty,
                              price_in=price, cups=cups_in, ref="PURCHASE", note=note)
                st.success("✅ Đã lưu phiếu nhập.")
                st.experimental_rerun()

        st.markdown("**Lịch sử nhập gần đây**")
        df_in = fetch_df(conn, """
            SELECT ts::timestamp(0) AS ts, pcode, qty, price_in, cups, note
            FROM inventory_ledger
            WHERE store=:s AND kind='IN'
            ORDER BY ts DESC
            LIMIT 200
        """, {"s": st.session_state["store"]})
        st.dataframe(df_in, use_container_width=True)

    # ====== PHIẾU XUẤT ======
    with tab_out:
        st.subheader("Phiếu xuất")
        c1, c2, c3 = st.columns([1.2, 2, 1.2])
        with c1:
            ngay2 = st.date_input("Ngày xuất", datetime.today().date(), key="out_date")
        with c2:
            pcode2, _ = _product_selector(conn, key_prefix="out", placeholder="Chọn sản phẩm xuất")
        with c3:
            qty2 = st.number_input("Số lượng", 0.0, step=0.1, min_value=0.0, key="out_qty")

        cups_out = 0.0
        if pcode2:
            info2 = fetch_df(conn, "SELECT cat_code FROM products WHERE code=:c", {"c": pcode2})
            if not info2.empty and info2.iloc[0]["cat_code"] in ("COT","MUT"):
                cups_out = qty2 * cups_per_kg_of(conn, pcode2)
        st.caption(f"👉 Số cốc trừ kho: **{cups_out:.0f}**")

        note2 = st.text_input("Ghi chú (tuỳ chọn)", key="out_note")
        if st.button("📤 Lưu phiếu xuất"):
            if not pcode2 or qty2 <= 0:
                st.error("Chọn sản phẩm và nhập số lượng > 0.")
            else:
                _write_ledger(conn, ngay2, st.session_state["store"], pcode2, "OUT", qty2,
                              price_in=0.0, cups=cups_out, ref="ISSUE", note=note2)
                st.success("✅ Đã lưu phiếu xuất.")
                st.experimental_rerun()

        st.markdown("**Lịch sử xuất gần đây**")
        df_out = fetch_df(conn, """
            SELECT ts::timestamp(0) AS ts, pcode, qty, cups, note
            FROM inventory_ledger
            WHERE store=:s AND kind='OUT'
            ORDER BY ts DESC
            LIMIT 200
        """, {"s": st.session_state["store"]})
        st.dataframe(df_out, use_container_width=True)

    # ====== TỒN KHO (NÂNG CAO) ======
    with tab_ton:
        st.subheader("Tồn kho (nâng cao)")
        c1, c2, c3, c4 = st.columns([1,1,1,2])
        with c1:
            to = st.date_input("Chốt đến ngày", datetime.today().date(), key="ton_to")
        with c2:
            catf = st.selectbox("Nhóm SP", ["TẤT CẢ","TRAI_CAY","PHU_GIA","COT","MUT","KHAC"], key="ton_cat")
        with c3:
            name_like = st.text_input("Mã/Tên chứa ...", key="ton_kw")
        with c4:
            st.caption("COT/MUT hiển thị thêm **số cốc**; trị giá dùng **giá bình quân di động**.")

        df_ton = stock_snapshot(conn, st.session_state["store"], to)
        if catf != "TẤT CẢ":
            df_ton = df_ton[df_ton["cat_code"] == catf]
        if name_like:
            df_ton = df_ton[df_ton["pcode"].str.contains(name_like, case=False) | df_ton["name"].str.contains(name_like, case=False)]

        st.dataframe(df_ton, use_container_width=True)

        colx, coly, colz = st.columns(3)
        total_val = float(df_ton["value"].sum()) if not df_ton.empty else 0.0
        total_qty = float(df_ton["ton_qty"].sum()) if not df_ton.empty else 0.0
        total_cups = float(df_ton["ton_cups"].sum()) if not df_ton.empty else 0.0
        colx.metric("Tổng giá trị tồn (VND)", f"{total_val:,.0f}")
        coly.metric("Tổng số lượng (kg)", f"{total_qty:,.2f}")
        colz.metric("Tổng số cốc (COT/MUT)", f"{total_cups:,.0f}")

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
    tab_tonkho, tab_taichinh = st.tabs(["Tồn kho & Trị giá", "Tài chính (Doanh thu – COGS – Lãi gộp)"])

    # ---- TỒN KHO & TRỊ GIÁ ----
    with tab_tonkho:
        to = st.date_input("Chốt đến ngày", datetime.today().date(), key="rpt_ton_to")
        df_ton = stock_snapshot(conn, st.session_state["store"], to)
        st.dataframe(df_ton, use_container_width=True, height=380)
        st.metric("Tổng trị giá", f"{df_ton['value'].sum():,.0f} VND")
        st.metric("Tổng số cốc", f"{df_ton['ton_cups'].sum():,.0f}")
        if not df_ton.empty:
            st.download_button("⬇️ CSV", df_ton.to_csv(index=False).encode("utf-8"),
                               file_name=f"bao_cao_ton_{to}.csv", mime="text/csv")

    # ---- TÀI CHÍNH: Doanh thu, COGS, Lãi gộp ----
    with tab_taichinh:
        c1, c2, c3 = st.columns(3)
        with c1:
            fr = st.date_input("Từ ngày", datetime.today().date().replace(day=1), key="tc_fr")
        with c2:
            to = st.date_input("Đến ngày", datetime.today().date(), key="tc_to")
        with c3:
            pay = st.multiselect("Kênh thanh toán", ["CASH","BANK"], default=["CASH","BANK"], key="tc_pay")

        # Doanh thu tổng theo ngày & phương thức
        if pay:
            rev = fetch_df(conn, f"""
                SELECT ts::date AS d, pay_method, SUM(COALESCE(amount,0)) AS amount
                FROM revenue
                WHERE store=:s AND ts BETWEEN :fr AND :to AND pay_method = ANY(:pay)
                GROUP BY d, pay_method
                ORDER BY d
            """, {"s": st.session_state["store"], "fr": fr.strftime("%Y-%m-%d"),
                  "to": to.strftime("%Y-%m-%d"), "pay": pay})
        else:
            rev = pd.DataFrame(columns=["d","pay_method","amount"])
        st.subheader("Doanh thu theo ngày & phương thức")
        st.dataframe(rev, use_container_width=True)
        doanh_thu = float(rev["amount"].sum() if not rev.empty else 0.0)

        # COGS ước tính từ doanh thu chi tiết (nếu có pcode/qty)
        rev_detail = fetch_df(conn, """
            SELECT pcode, SUM(COALESCE(qty,0)) AS qty
            FROM revenue
            WHERE store=:s AND ts BETWEEN :fr AND :to AND pcode IS NOT NULL
            GROUP BY pcode
        """, {"s": st.session_state["store"], "fr": fr.strftime("%Y-%m-%d"), "to": to.strftime("%Y-%m-%d")})
        if not rev_detail.empty:
            rev_detail["avg_cost"] = rev_detail["pcode"].apply(lambda c: avg_cost(conn, st.session_state["store"], c))
            rev_detail["cogs"] = rev_detail["qty"] * rev_detail["avg_cost"]
            cogs_total = float(rev_detail["cogs"].sum())
        else:
            cogs_total = 0.0

        lai_gop = doanh_thu - cogs_total

        m1, m2, m3 = st.columns(3)
        m1.metric("Doanh thu", f"{doanh_thu:,.0f} VND")
        m2.metric("Giá vốn (ước tính)", f"{cogs_total:,.0f} VND")
        m3.metric("Lãi gộp", f"{lai_gop:,.0f} VND")

        st.markdown("**Chi tiết COGS (nếu có bán theo SP)**")
        if not rev_detail.empty:
            st.dataframe(rev_detail, use_container_width=True)
            st.download_button("⬇️ COGS chi tiết CSV", rev_detail.to_csv(index=False).encode("utf-8"),
                               file_name=f"cogs_detail_{fr}_{to}.csv", mime="text/csv")
        else:
            st.info("Chưa có doanh thu chi tiết theo sản phẩm. Ghi ở mục **Doanh thu → Chi tiết SP** (Phần 5).")

# =============== Router cập nhật (Kho/Báo cáo) ===============
if _menu == "Kho":
    page_kho(conn)
elif _menu == "Báo cáo":
    page_baocao(conn)
# ============================================================
# app.py — PART 4/5 (Công thức CRUD + Sản xuất 3 luồng | PG only)
# ============================================================

# ---------- Helper chọn theo nhóm ----------
def _prod_select(conn, cats: list[str]):
    df = fetch_df(conn,
        "SELECT code,name FROM products WHERE cat_code = ANY(:cats) ORDER BY code",
        {"cats": cats})
    opts = df["code"].tolist() if not df.empty else []
    def fmt(x):
        if df.empty or x not in df["code"].values: return x
        return f"{x} — {df.set_index('code').loc[x,'name']}"
    return opts, fmt

# ---------- Ước tính đơn giá TP từ NVL chính ----------
def _avg_cost_from_raws(conn, store: str, raws: list[str]) -> float:
    if not raws: return 0.0
    vals = []
    for r in raws:
        c = avg_cost(conn, store, r)
        if c > 0: vals.append(c)
        else:
            last = fetch_df(conn, """
                SELECT price_in FROM inventory_ledger
                WHERE store=:s AND pcode=:p AND kind='IN'
                ORDER BY ts DESC LIMIT 1
            """, {"s": store, "p": r})
            if not last.empty:
                vals.append(float(last.iloc[0]["price_in"] or 0.0))
    return float(sum(vals)/len(vals)) if vals else 0.0

# ---------- Xuất NVL & Nhập TP ----------
def _consume_materials(conn, store: str, ts: date, items: dict[str, float], ref: str):
    for p, q in items.items():
        if q > 0:
            post_ledger(conn, store, p, "OUT", q, price_in=0.0, cups=0.0, ref=ref, note="")

def _receive_finish(conn, store: str, ts: date, pcode: str, kg_out: float, unit_cost: float, cups_per_kg: float, ref: str):
    cups_in = max(0.0, float(kg_out or 0.0)) * max(0.0, float(cups_per_kg or 0.0))
    post_ledger(conn, store, pcode, "IN", kg_out, price_in=unit_cost, cups=cups_in, ref=ref, note="")

# ============================================================
# CÔNG THỨC (CRUD) — chỉ người có quyền CT_EDIT
# ============================================================
def page_congthuc(conn):
    st.subheader("🧪 Công thức (CỐT / MỨT)")
    if not has_perm(st.session_state.get("user"), "CT_EDIT"):
        st.info("Bạn không có quyền sửa Công thức.")
        return

    df_ct = fetch_df(conn, """
        SELECT code,name,type,output_pcode,output_uom,recovery,cups_per_kg,fruits_csv,additives_json,note
        FROM formulas ORDER BY code
    """)
    st.dataframe(df_ct, use_container_width=True, height=260)

    mode = st.radio("Chế độ", ["Tạo mới", "Sửa/Xóa"], horizontal=True)

    def out_opts_by_type(t: str):
        return _prod_select(conn, ["COT"] if t == "COT" else ["MUT"])

    # -------- TẠO MỚI --------
    if mode == "Tạo mới":
        c1, c2 = st.columns(2)
        with c1:
            code = st.text_input("Mã CT*")
            name = st.text_input("Tên CT*")
            typ  = st.selectbox("Loại CT*", ["COT","MUT"])
            out_list, out_fmt = out_opts_by_type(typ)
            outp = st.selectbox("SP đầu ra (mã)*", out_list, format_func=out_fmt)
            uom  = st.text_input("ĐVT TP*", "kg")
        with c2:
            rec  = st.number_input("Hệ số thu hồi (chỉ CỐT)", 1.0, step=0.1, disabled=(typ!="COT"))
            cups = st.number_input("Số cốc / 1kg TP", 0.0, step=0.1)
            mut_src = st.radio("Nguồn NVL (chỉ cho MỨT)", ["TRÁI_CÂY","CỐT"], index=0, horizontal=True)

        # NVL theo loại/nguồn
        if typ == "COT" or mut_src == "TRÁI_CÂY":
            raw_opts, raw_fmt = _prod_select(conn, ["TRAI_CÂY"])
        else:
            raw_opts, raw_fmt = _prod_select(conn, ["COT"])
        raw_sel = st.multiselect("Nguyên liệu chính*", raw_opts, format_func=raw_fmt)

        # Phụ gia (kg / 1kg sau sơ)
        add_opts, add_fmt = _prod_select(conn, ["PHU_GIA"])
        add_pick = st.multiselect("Phụ gia (tùy chọn)", add_opts, format_func=add_fmt)
        add_q = {}
        if add_pick:
            st.caption("Định lượng phụ gia (kg / 1kg sau sơ)")
            for c in add_pick:
                add_q[c] = st.number_input(f"{add_fmt(c)}", 0.0, step=0.01, key=f"add_{c}")

        if st.button("💾 Lưu công thức"):
            if not code or not name or not outp or not raw_sel:
                st.error("Thiếu dữ liệu bắt buộc (Mã/Tên/SP đầu ra/NVL).")
            else:
                note = f"SRC={'TRÁI_CÂY' if typ=='MUT' and mut_src=='TRÁI_CÂY' else ('CỐT' if typ=='MUT' else '')}"
                run_sql(conn, """
                    INSERT INTO formulas
                        (code,name,type,output_pcode,output_uom,recovery,cups_per_kg,fruits_csv,additives_json,note)
                    VALUES (:c,:n,:t,:o,:u,:r,:cpk,:fr,:aj,:no)
                    ON CONFLICT (code) DO UPDATE SET
                        name=EXCLUDED.name, type=EXCLUDED.type, output_pcode=EXCLUDED.output_pcode,
                        output_uom=EXCLUDED.output_uom, recovery=EXCLUDED.recovery, cups_per_kg=EXCLUDED.cups_per_kg,
                        fruits_csv=EXCLUDED.fruits_csv, additives_json=EXCLUDED.additives_json, note=EXCLUDED.note
                """, {"c": code.strip(), "n": name.strip(), "t": typ, "o": outp, "u": uom,
                      "r": (rec if typ=="COT" else 1.0), "cpk": cups,
                      "fr": ",".join(raw_sel), "aj": json.dumps(add_q), "no": note})
                log_action(conn, st.session_state["user"]["email"], "CT_SAVE", code.strip())
                st.success("✅ Đã lưu công thức.")
                st.experimental_rerun()

    # -------- SỬA / XÓA --------
    else:
        if df_ct.empty:
            st.info("Chưa có công thức."); return
        ct_pick = st.selectbox("Chọn CT", df_ct["code"].tolist())
        row = df_ct[df_ct["code"]==ct_pick].iloc[0].to_dict()

        typ  = st.selectbox("Loại CT", ["COT","MUT"], index=(0 if row["type"]=="COT" else 1))
        out_list, out_fmt = out_opts_by_type(typ)
        def_idx = out_list.index(row["output_pcode"]) if row["output_pcode"] in out_list else 0

        c1, c2 = st.columns(2)
        with c1:
            name = st.text_input("Tên CT", row["name"])
            outp = st.selectbox("SP đầu ra (mã)", out_list, index=def_idx, format_func=out_fmt)
            uom  = st.text_input("ĐVT TP", row["output_uom"] or "kg")
        with c2:
            rec  = st.number_input("Hệ số thu hồi (CỐT)", float(row["recovery"] or 1.0),
                                   step=0.1, disabled=(typ!="COT"), key="rec_edit")
            cups = st.number_input("Số cốc / 1kg TP", float(row["cups_per_kg"] or 0.0), step=0.1)
            src0 = "TRÁI_CÂY"
            if typ=="MUT" and (row["note"] or "").startswith("SRC="):
                src0 = (row["note"] or "").split("=",1)[1]
            mut_src = st.radio("Nguồn NVL (MỨT)", ["TRÁI_CÂY","CỐT"], index=(0 if src0 in ("TRÁI_CÂY","TRAI_CAY") else 1), horizontal=True)

        # NVL theo nguồn
        if typ == "COT" or mut_src in ("TRÁI_CÂY","TRAI_CAY"):
            raw_opts, raw_fmt = _prod_select(conn, ["TRAI_CAY"])
        else:
            raw_opts, raw_fmt = _prod_select(conn, ["COT"])
        current_raws = [x for x in (row["fruits_csv"] or "").split(",") if x]
        raw_sel = st.multiselect("Nguyên liệu chính", raw_opts,
                                 default=[r for r in current_raws if r in raw_opts], format_func=raw_fmt)

        # Phụ gia
        try:
            adds0 = json.loads(row["additives_json"] or "{}")
        except Exception:
            adds0 = {}
        add_opts, add_fmt = _prod_select(conn, ["PHU_GIA"])
        add_pick = st.multiselect("Phụ gia", add_opts, default=list(adds0.keys()), format_func=add_fmt)
        add_q = {}
        if add_pick:
            st.caption("Định lượng phụ gia (kg / 1kg sau sơ)")
            for c in add_pick:
                add_q[c] = st.number_input(f"{add_fmt(c)}", float(adds0.get(c,0.0)), step=0.01, key=f"add_edit_{c}")

        colA, colB = st.columns(2)
        with colA:
            if st.button("💾 Cập nhật"):
                note = f"SRC={'TRÁI_CÂY' if typ=='MUT' and mut_src in ('TRÁI_CÂY','TRAI_CAY') else ('CỐT' if typ=='MUT' else '')}"
                run_sql(conn, """
                    UPDATE formulas
                    SET name=:n, type=:t, output_pcode=:o, output_uom=:u,
                        recovery=:r, cups_per_kg=:cpk, fruits_csv=:fr, additives_json=:aj, note=:no
                    WHERE code=:c
                """, {"n": name.strip(), "t": typ, "o": outp, "u": uom,
                      "r": (rec if typ=="COT" else 1.0), "cpk": cups,
                      "fr": ",".join(raw_sel), "aj": json.dumps(add_q), "no": note, "c": row["code"]})
                log_action(conn, st.session_state["user"]["email"], "CT_UPDATE", row["code"])
                st.success("✅ Đã cập nhật.")
                st.experimental_rerun()
        with colB:
            if st.button("🗑️ Xóa công thức"):
                run_sql(conn, "DELETE FROM formulas WHERE code=:c", {"c": row["code"]})
                log_action(conn, st.session_state["user"]["email"], "CT_DELETE", row["code"])
                st.success("Đã xóa.")
                st.experimental_rerun()

# ============================================================
# SẢN XUẤT — 3 luồng: CỐT / MỨT (TRÁI CÂY) / MỨT (CỐT)
# ============================================================
def page_sanxuat(conn):
    st.header("🏭 Sản xuất")
    user = st.session_state.get("user") or {}
    if not has_perm(user, "SANXUAT"):
        st.warning("⛔ Bạn không có quyền vào mục Sản xuất.")
        return
    store = st.session_state.get("store","HOSEN")

    # Tabs (nếu có quyền CT_EDIT sẽ có thêm tab Công thức)
    if has_perm(user, "CT_EDIT"):
        tabs = st.tabs(["🧪 Công thức (CRUD)", "CỐT", "MỨT từ TRÁI CÂY", "MỨT từ CỐT"])
        with tabs[0]:
            page_congthuc(conn)
        t_cot, t_mut_tc, t_mut_ct = tabs[1], tabs[2], tabs[3]
    else:
        t_cot, t_mut_tc, t_mut_ct = st.tabs(["CỐT", "MỨT từ TRÁI CÂY", "MỨT từ CỐT"])

    # ======== CỐT ========
    with t_cot:
        st.subheader("SX CỐT (có hệ số thu hồi)")
        cts = fetch_df(conn, "SELECT * FROM formulas WHERE type='COT' ORDER BY code")
        ct_pick = st.selectbox("Chọn CT CỐT", cts["code"].tolist() if not cts.empty else [])
        ts = st.date_input("Ngày ghi sổ", datetime.today().date(), key="prd_cot_dt")
        if ct_pick:
            row = cts[cts["code"]==ct_pick].iloc[0].to_dict()
            out_p = row["output_pcode"]
            rec   = float(row["recovery"] or 1.0)
            cups  = float(row["cups_per_kg"] or 0.0)
            raws  = [x for x in (row["fruits_csv"] or "").split(",") if x]
            adds  = json.loads(row["additives_json"] or "{}")

            kg_sau_so = st.number_input("KG sau sơ chế (đầu vào)", 0.0, step=0.1, key="cot_in")
            kg_tp     = st.number_input("KG thành phẩm (auto = kg_sau_sơ × hệ số)", value=kg_sau_so*rec, step=0.1, key="cot_out")
            st.caption(f"HS thu hồi = {rec:.2f} • Cốc/1kg TP = {cups:.2f}")

            if st.button("✅ Ghi sổ SX CỐT"):
                # Xuất NVL chính chia đều; phụ gia theo kg_sau_so
                consume = {}
                n = max(1, len(raws))
                for r in raws:
                    consume[r] = consume.get(r,0.0) + (kg_sau_so / n)
                for pg, per1 in adds.items():
                    consume[pg] = consume.get(pg,0.0) + float(per1 or 0.0)*kg_sau_so
                _consume_materials(conn, store, ts, consume, ref=f"PRD_COT:{ct_pick}")

                unit_cost = _avg_cost_from_raws(conn, store, raws)
                _receive_finish(conn, store, ts, out_p, kg_tp, unit_cost, cups_per_kg=cups, ref=f"PRD_COT:{ct_pick}")

                log_action(conn, user["email"], "PRD_COT", f"{ct_pick} -> {out_p} {kg_tp}kg @~{unit_cost}")
                st.success("✅ Đã ghi sổ SX CỐT & nhập kho TP.")
                st.experimental_rerun()

    # ======== MỨT từ TRÁI CÂY (không có hệ số) ========
    with t_mut_tc:
        st.subheader("SX MỨT (nguồn TRÁI CÂY) — KHÔNG có hệ số thu hồi")
        cts = fetch_df(conn, "SELECT * FROM formulas WHERE type='MUT' AND (note LIKE 'SRC=TRÁI_CÂY%' OR note LIKE 'SRC=TRAI_CAY%' OR note='' OR note IS NULL) ORDER BY code")
        ct_pick = st.selectbox("Chọn CT MỨT (TRÁI CÂY)", cts["code"].tolist() if not cts.empty else [], key="ct_mut_tc")
        ts2 = st.date_input("Ngày ghi sổ", datetime.today().date(), key="prd_mut_tc_dt")
        if ct_pick:
            row = cts[cts["code"]==ct_pick].iloc[0].to_dict()
            out_p = row["output_pcode"]
            cups  = float(row["cups_per_kg"] or 0.0)
            raws  = [x for x in (row["fruits_csv"] or "").split(",") if x]  # trái cây
            adds  = json.loads(row["additives_json"] or "{}")

            kg_in  = st.number_input("KG sau sơ chế (đầu vào)", 0.0, step=0.1, key="mut_tc_in")
            kg_out = st.number_input("KG thành phẩm MỨT", 0.0, step=0.1, key="mut_tc_out")

            if st.button("✅ Ghi sổ SX MỨT (TRÁI CÂY)"):
                consume = {}
                n = max(1, len(raws))
                for r in raws:
                    consume[r] = consume.get(r,0.0) + (kg_in / n)
                for pg, per1 in adds.items():
                    consume[pg] = consume.get(pg,0.0) + float(per1 or 0.0)*kg_in
                _consume_materials(conn, store, ts2, consume, ref=f"PRD_MUT_TC:{ct_pick}")

                unit_cost = _avg_cost_from_raws(conn, store, raws)
                _receive_finish(conn, store, ts2, out_p, kg_out, unit_cost, cups_per_kg=cups, ref=f"PRD_MUT_TC:{ct_pick}")

                log_action(conn, user["email"], "PRD_MUT_TC", f"{ct_pick} -> {out_p} {kg_out}kg @~{unit_cost}")
                st.success("✅ Đã ghi sổ SX MỨT (TRÁI CÂY) & nhập kho.")
                st.experimental_rerun()

    # ======== MỨT từ CỐT (không có hệ số) ========
    with t_mut_ct:
        st.subheader("SX MỨT (nguồn CỐT) — KHÔNG có hệ số thu hồi")
        cts = fetch_df(conn, "SELECT * FROM formulas WHERE type='MUT' AND (note LIKE 'SRC=CỐT%' OR note LIKE 'SRC=COT%') ORDER BY code")
        ct_pick = st.selectbox("Chọn CT MỨT (CỐT)", cts["code"].tolist() if not cts.empty else [], key="ct_mut_ct")
        ts3 = st.date_input("Ngày ghi sổ", datetime.today().date(), key="prd_mut_ct_dt")
        if ct_pick:
            row = cts[cts["code"]==ct_pick].iloc[0].to_dict()
            out_p = row["output_pcode"]
            cups  = float(row["cups_per_kg"] or 0.0)
            raws  = [x for x in (row["fruits_csv"] or "").split(",") if x]  # danh mục CỐT dùng làm NVL
            adds  = json.loads(row["additives_json"] or "{}")

            kg_cot = st.number_input("KG CỐT dùng", 0.0, step=0.1, key="mut_ct_in")
            kg_out = st.number_input("KG thành phẩm MỨT", 0.0, step=0.1, key="mut_ct_out")

            if st.button("✅ Ghi sổ SX MỨT (CỐT)"):
                consume = {}
                n = max(1, len(raws))
                for r in raws:
                    consume[r] = consume.get(r,0.0) + (kg_cot / n)
                for pg, per1 in adds.items():
                    consume[pg] = consume.get(pg,0.0) + float(per1 or 0.0)*kg_cot
                _consume_materials(conn, store, ts3, consume, ref=f"PRD_MUT_CT:{ct_pick}")

                unit_cost = _avg_cost_from_raws(conn, store, raws)
                _receive_finish(conn, store, ts3, out_p, kg_out, unit_cost, cups_per_kg=cups, ref=f"PRD_MUT_CT:{ct_pick}")

                log_action(conn, user["email"], "PRD_MUT_CT", f"{ct_pick} -> {out_p} {kg_out}kg @~{unit_cost}")
                st.success("✅ Đã ghi sổ SX MỨT (CỐT) & nhập kho.")
                st.experimental_rerun()

# =============== Router cập nhật (thêm Sản xuất/CT) ===============
if _menu == "Sản xuất":
    page_sanxuat(conn)
elif _menu == "Danh mục":
    page_danhmuc(conn)
elif _menu == "Kho":
    page_kho(conn)
elif _menu == "Báo cáo":
    page_baocao(conn)

