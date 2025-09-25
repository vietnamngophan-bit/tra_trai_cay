# ============================================================
# app.py — PHẦN 1/5: Hạ tầng & Giao diện khung (Postgres only)
# ============================================================
# LƯU Ý:
# - Đặt file này là duy nhất chạy app (không có router cũ ở cuối).
# - Các trang nghiệp vụ sẽ được thêm ở Phần 2–5 thông qua các hàm route_*.
# - Không dùng SQLite. Chỉ Postgres qua biến môi trường DATABASE_URL.
# ============================================================

import os, re, json, hashlib
from datetime import datetime
from typing import Dict, Any
from datetime import date, datetime, timedelta
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

# ------------------- CẤU HÌNH TRANG (PHẢI Ở TRÊN CÙNG) -------------------
st.set_page_config(
    page_title="Fruit Tea ERP v5",
    page_icon="🍵",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ------------------- KẾT NỐI POSTGRES -------------------
_ENGINE = None  # SQLAlchemy Engine (global duy nhất)

def _normalize_pg_url(url: str) -> str:
    """Chuẩn hoá URL Postgres → driver psycopg2 + ép sslmode=require nếu thiếu."""
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif url.startswith("postgresql://") and "+psycopg2" not in url:
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    if "sslmode=" not in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}sslmode=require"
    return url

def get_conn() -> Connection:
    """Tạo 1 kết nối (connection) từ Engine. Chỉ dùng Postgres."""
    global _ENGINE
    pg_url = os.getenv("DATABASE_URL", "").strip()
    if not pg_url:
        st.error("❌ Thiếu biến môi trường **DATABASE_URL** (Postgres).")
        st.stop()
    if _ENGINE is None:
        _ENGINE = create_engine(_normalize_pg_url(pg_url), pool_pre_ping=True, future=True)
    return _ENGINE.connect()

# ------------------- TIỆN ÍCH SQL -------------------
def _qmark_to_named(sql: str, params):
    """Đổi ? → :p1, :p2... để dùng với sqlalchemy.text()."""
    if not isinstance(params, (list, tuple)):
        return sql, (params or {})
    idx = 1
    def repl(_):
        nonlocal idx
        s = f":p{idx}"
        idx += 1
        return s
    sql_named = re.sub(r"\?", repl, sql)
    named_params = {f"p{i+1}": v for i, v in enumerate(params)}
    return sql_named, named_params

def run_sql(conn: Connection, sql: str, params=None):
    """Thực thi SQL (INSERT/UPDATE/DELETE). Tự commit."""
    if isinstance(params, (list, tuple)):
        sql, params = _qmark_to_named(sql, params)
    res = conn.execute(text(sql), params or {})
    try:
        conn.commit()
    except Exception:
        pass
    return res

def fetch_df(conn: Connection, sql: str, params=None) -> pd.DataFrame:
    """SELECT trả DataFrame (hỗ trợ ? params)."""
    if isinstance(params, (list, tuple)):
        sql, params = _qmark_to_named(sql, params)
    return pd.read_sql_query(text(sql), conn, params=params or {})

# ------------------- AUTH, PHÂN QUYỀN, NHẬT KÝ -------------------
PERM_ALL = [
    # Danh mục / Sản phẩm / Công thức / Người dùng / Cửa hàng
    "CAT_VIEW","CAT_EDIT",
    "SKU_VIEW","SKU_EDIT",
    "CT_VIEW","CT_EDIT",
    "USER_VIEW","USER_EDIT",
    "STORE_VIEW","STORE_EDIT",

    # Kho
    "INV_VIEW","INV_IN","INV_OUT","INV_ADJUST",

    # Sản xuất
    "MFG_VIEW","MFG_EXEC","MFG_CLOSE","MFG_WIP_VIEW",

    # Doanh thu
    "REV_VIEW","REV_EDIT",

    # Báo cáo
    "RPT_INV","RPT_FIN",

    # Tài sản cố định
    "FA_VIEW","FA_EDIT",

    # Nhật ký
    "AUDIT_VIEW",
]

def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def has_perm(user: Dict[str, Any], perm: str) -> bool:
    if not user:
        return False
    if user.get("role") == "SuperAdmin":
        return True
    perms = (user.get("perms") or "").split(",")
    return perm in perms

def write_audit(conn: Connection, action: str, detail: str = ""):
    """Ghi nhật ký hệ thống. Không chặn nếu lỗi để không làm gián đoạn nghiệp vụ."""
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

# ------------------- SESSION DEFAULTS -------------------
def _ensure_session_defaults():
    ss = st.session_state
    ss.setdefault("user", None)
    ss.setdefault("store", "")
    ss.setdefault("menu", "Dashboard")

# ------------------- FORM ĐĂNG NHẬP -------------------
def login_form(conn: Connection):
    st.markdown("### 🔐 Đăng nhập hệ thống")
    email = st.text_input("Email", key="login_email")
    pw    = st.text_input("Mật khẩu", type="password", key="login_pw")

    if st.button("Đăng nhập", type="primary", use_container_width=True):
        df = fetch_df(conn,
                      "SELECT email, display, password, role, store_code, perms "
                      "FROM users WHERE email=:e",
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
        st.session_state["store"] = user["store"]
        write_audit(conn, "LOGIN", user["email"])
        st.success("Đăng nhập thành công.")
        st.rerun()

def require_login(conn: Connection) -> Dict[str, Any]:
    if not st.session_state.get("user"):
        login_form(conn)
        st.stop()
    return st.session_state["user"]

def logout(conn: Connection):
    u = st.session_state.get("user", {})
    write_audit(conn, "LOGOUT", u.get("email", ""))
    st.session_state.clear()
    st.rerun()

# ------------------- TIỆU ĐỀ & MENU PHẢI (POPOVER TÀI KHOẢN) -------------------
def header_top(conn: Connection, user: Dict[str, Any]):
    left, right = st.columns([0.8, 0.2])
    with left:
        st.markdown("## 🍵 Fruit Tea ERP v5")
        st.caption("Kết nối: **Postgres (Supabase)**")
    with right:
        with st.popover(f"👤 {user.get('display','')}", use_container_width=True):
            st.caption(user.get("email", ""))
            st.markdown("---")
            st.markdown("**Đổi mật khẩu**")
            with st.form("form_change_pw", clear_on_submit=True):
                old = st.text_input("Mật khẩu cũ", type="password")
                new1 = st.text_input("Mật khẩu mới", type="password")
                new2 = st.text_input("Xác nhận mật khẩu", type="password")
                ok = st.form_submit_button("Cập nhật")
            if ok:
                df = fetch_df(conn, "SELECT password FROM users WHERE email=:e", {"e": user["email"]})
                if df.empty or df.iloc[0]["password"] != sha256(old):
                    st.error("Mật khẩu cũ không đúng.")
                elif not new1 or new1 != new2:
                    st.error("Xác nhận mật khẩu chưa khớp.")
                else:
                    run_sql(conn, "UPDATE users SET password=:p WHERE email=:e",
                            {"p": sha256(new1), "e": user["email"]})
                    write_audit(conn, "CHANGE_PASSWORD", user["email"])
                    st.success("Đã đổi mật khẩu. Vui lòng đăng nhập lại.")
                    logout(conn)

            st.markdown("---")
            if st.button("Đăng xuất", use_container_width=True):
                logout(conn)

# ------------------- SIDEBAR: CỬA HÀNG + MENU CHÍNH -------------------
def sidebar_menu(conn: Connection, user: Dict[str, Any]) -> str:
    st.sidebar.markdown("### 🏬 Cửa hàng")
    stores = fetch_df(conn, "SELECT code, name FROM stores ORDER BY name")
    store_map = {r["name"] if r["name"] else r["code"]: r["code"] for _, r in stores.iterrows()}
    disp_list = list(store_map.keys()) or ["(chưa có cửa hàng)"]

    # Nếu user có store mặc định thì chọn sẵn
    default_label = None
    if st.session_state.get("store"):
        for k, v in store_map.items():
            if v == st.session_state["store"]:
                default_label = k
                break

    chosen = st.sidebar.selectbox("Đang thao tác tại", disp_list, index=(
        disp_list.index(default_label) if default_label in disp_list else 0
    ), key="sidebar_store_select")

    # Lưu code cửa hàng vào session
    st.session_state["store"] = store_map.get(chosen, "")

    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📌 Chức năng")

    menu = st.sidebar.radio(
        "Điều hướng",
        [
            "Dashboard",
            "Danh mục",
            "Kho",
            "Sản xuất",
            "Doanh thu",
            "Báo cáo",
            "TSCD",
            "Nhật ký",
            "Cửa hàng",
            "Người dùng",
        ],
        index=0,
        label_visibility="collapsed",
        key="main_menu_radio",
    )

    st.sidebar.markdown("---")
    st.sidebar.caption("DB: Postgres (Supabase)")

    return menu



def route_audit(conn: Connection, user: Dict[str, Any], menu: str):
    if menu == "Nhật ký":
        if has_perm(user, "AUDIT_VIEW"):
            df = fetch_df(conn,
                          "SELECT ts, actor, action, detail "
                          "FROM syslog ORDER BY ts DESC LIMIT 300")
            st.markdown("### 🗒️ Nhật ký hệ thống (mới nhất)")
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.warning("Bạn không có quyền xem nhật ký.")

# ------------------- ROUTER DUY NHẤT -------------------
def router():
    _ensure_session_defaults()
    conn = get_conn()

    # Nếu chưa đăng nhập → dừng tại form login
    user = require_login(conn)

    # Tiêu đề + popover tài khoản (đổi mật khẩu/đăng xuất)
    header_top(conn, user)

    # Sidebar: chọn cửa hàng & menu
    menu = sidebar_menu(conn, user)

    # Điều hướng (Phần 2–5 sẽ override các placeholder này)
    route_part2_placeholder(menu)
    route_part3_placeholder(menu)
    route_part4_placeholder(menu)
    route_part5_placeholder(menu)
    route_audit(conn, user, menu)

# ------------------- ENTRY -------------------
if __name__ == "__main__":
    router()
# ============================================================
# PHẦN 2/5 — Dashboard + Danh mục + Cửa hàng + Người dùng
# (CRUD đầy đủ, dùng các helper/perm/audit từ Phần 1)
# ============================================================

# ---------- DASHBOARD (nhẹ, tổng quan) ----------
def page_dashboard(conn: Connection, user: dict):
    st.markdown("### 📊 Tổng quan nhanh")
    c1, c2, c3, c4 = st.columns(4)
    # Tổng số SKU
    n_sku = fetch_df(conn, "SELECT COUNT(*) n FROM products").iloc[0]["n"]
    n_ct  = fetch_df(conn, "SELECT COUNT(*) n FROM formulas").iloc[0]["n"]
    n_st  = fetch_df(conn, "SELECT COUNT(*) n FROM stores").iloc[0]["n"]
    n_user= fetch_df(conn, "SELECT COUNT(*) n FROM users").iloc[0]["n"]
    c1.metric("Sản phẩm (SKU)", n_sku)
    c2.metric("Công thức", n_ct)
    c3.metric("Cửa hàng", n_st)
    c4.metric("Người dùng", n_user)

    st.divider()
    st.caption("Hoạt động gần đây")
    df = fetch_df(conn, "SELECT ts, actor, action, detail FROM syslog ORDER BY ts DESC LIMIT 20")
    st.dataframe(df, use_container_width=True, hide_index=True)

# ---------- DANH MỤC: Categories + Products + Formulas ----------
def page_danhmuc(conn: Connection, user: dict):
    st.markdown("### 📚 Danh mục")
    tabs = st.tabs(["📁 Nhóm hàng", "📦 Sản phẩm (SKU)", "🧪 Công thức (CỐT/MỨT)"])

    # --- 1) Nhóm hàng (categories) ---
    with tabs[0]:
        st.subheader("📁 Nhóm hàng")
        df = fetch_df(conn, "SELECT code,name FROM categories ORDER BY code")
        st.dataframe(df, use_container_width=True, hide_index=True)

        if has_perm(user, "CAT_EDIT"):
            with st.form("cat_add", clear_on_submit=True):
                st.markdown("**Thêm / Sửa nhóm**")
                code = st.text_input("Mã nhóm", key="cat_code")
                name = st.text_input("Tên nhóm", key="cat_name")
                colA, colB, colC = st.columns(3)
                ok_add  = colA.form_submit_button("💾 Lưu (thêm/sửa)", use_container_width=True)
                ok_del  = colB.form_submit_button("🗑️ Xoá", use_container_width=True)
                cancel  = colC.form_submit_button("HUỶ", use_container_width=True)

            if ok_add and code and name:
                run_sql(conn, "INSERT INTO categories(code,name) VALUES(:c,:n) "
                              "ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name",
                              {"c":code.strip(), "n":name.strip()})
                write_audit(conn, "CAT_UPSERT", f"{code}={name}")
                st.success("Đã lưu nhóm hàng.")
                st.rerun()
            if ok_del and code:
                run_sql(conn, "DELETE FROM categories WHERE code=:c", {"c": code.strip()})
                write_audit(conn, "CAT_DELETE", code)
                st.success("Đã xoá.")
                st.rerun()
        else:
            st.info("Bạn không có quyền chỉnh sửa nhóm (CAT_EDIT).")

    # --- 2) Sản phẩm (products) ---
    with tabs[1]:
        st.subheader("📦 Sản phẩm")
        dfp = fetch_df(conn, """
            SELECT p.code, p.name, p.cat_code, p.uom, COALESCE(p.is_active,true) is_active
            FROM products p ORDER BY p.code
        """)
        st.dataframe(dfp, use_container_width=True, hide_index=True)

        if has_perm(user, "SKU_EDIT"):
            st.markdown("**Thêm / Sửa / Xoá sản phẩm**")
            cats = fetch_df(conn, "SELECT code,name FROM categories ORDER BY code")
            cat_opts = [f"{r['code']} — {r['name']}" for _,r in cats.iterrows()] if not cats.empty else []

            with st.form("sku_edit", clear_on_submit=True):
                col1, col2, col3 = st.columns([2,2,1])
                code = col1.text_input("Mã SP", key="sku_code")
                name = col2.text_input("Tên SP", key="sku_name")
                uom  = col3.text_input("ĐVT", value="kg", key="sku_uom")
                cat_label = st.selectbox("Nhóm", cat_opts, index=0 if cat_opts else None, key="sku_cat")
                active = st.checkbox("Đang dùng", value=True, key="sku_active")
                cA, cB, cC = st.columns(3)
                ok = cA.form_submit_button("💾 Lưu", use_container_width=True)
                rm = cB.form_submit_button("🗑️ Xoá", use_container_width=True)
                _  = cC.form_submit_button("HUỶ", use_container_width=True)

            if ok and code and name and cat_opts:
                cat_code = cat_label.split(" — ",1)[0]
                run_sql(conn, """
                    INSERT INTO products(code,name,cat_code,uom,is_active)
                    VALUES(:c,:n,:cat,:u,:a)
                    ON CONFLICT (code)
                    DO UPDATE SET name=EXCLUDED.name, cat_code=EXCLUDED.cat_code,
                                  uom=EXCLUDED.uom, is_active=EXCLUDED.is_active
                """, {"c":code.strip(),"n":name.strip(),"cat":cat_code,"u":uom.strip(),"a":bool(active)})
                write_audit(conn,"SKU_UPSERT",code)
                st.success("Đã lưu sản phẩm.")
                st.rerun()
            if rm and code:
                run_sql(conn,"DELETE FROM products WHERE code=:c",{"c":code.strip()})
                write_audit(conn,"SKU_DELETE",code)
                st.success("Đã xoá.")
                st.rerun()
        else:
            st.info("Bạn không có quyền chỉnh sửa sản phẩm (SKU_EDIT).")

    # --- 3) Công thức (formulas) ---
    with tabs[2]:
        st.subheader("🧪 Công thức (CỐT / MỨT)")
        dff = fetch_df(conn, """
            SELECT code,name,type,output_pcode,output_uom,recovery,cups_per_kg,
                   fruits_csv, additives_json, note
            FROM formulas ORDER BY code
        """)
        st.dataframe(dff, use_container_width=True, hide_index=True)

        if has_perm(user, "CT_EDIT"):
            st.markdown("**Thêm / Sửa / Xoá công thức**")
            # danh sách đầu ra theo loại
            prod_all = fetch_df(conn, "SELECT code,name,cat_code FROM products ORDER BY code")
            cot_list = prod_all[prod_all["cat_code"]=="COT"] if not prod_all.empty else pd.DataFrame()
            mut_list = prod_all[prod_all["cat_code"]=="MUT"] if not prod_all.empty else pd.DataFrame()
            trai_list= prod_all[prod_all["cat_code"]=="TRAI_CAY"] if not prod_all.empty else pd.DataFrame()
            pg_list  = prod_all[prod_all["cat_code"]=="PHU_GIA"] if not prod_all.empty else pd.DataFrame()

            with st.form("ct_edit", clear_on_submit=True):
                col1, col2, col3 = st.columns(3)
                code = col1.text_input("Mã CT", key="ct_code")
                name = col2.text_input("Tên CT", key="ct_name")
                typ  = col3.selectbox("Loại", ["COT","MUT"], key="ct_type")

                # Output product theo loại
                if typ == "COT":
                    opts = [f"{r.code} — {r.name}" for _,r in cot_list.iterrows()]
                else:
                    opts = [f"{r.code} — {r.name}" for _,r in mut_list.iterrows()]
                out_label = st.selectbox("SP đầu ra", opts, index=0 if opts else None, key="ct_out")

                uom = st.text_input("ĐVT TP", value="kg", key="ct_uom")

                if typ == "COT":
                    rec = st.number_input("Hệ số thu hồi (CỐT)", value=1.0, step=0.1, key="ct_rec")
                else:
                    rec = 1.0  # mứt không dùng hệ số

                cups = st.number_input("Cốc / 1kg TP", value=0.0, step=1.0, key="ct_cups")

                # Nguồn NVL cho MỨT
                src = st.radio("Nguồn NVL cho MỨT", ["TRAI_CAY","COT"], index=0, horizontal=True, key="ct_src")

                # Nguyên liệu chính
                if typ=="COT" or src=="TRAI_CAY":
                    raw_pool = trai_list
                else:
                    raw_pool = cot_list
                raw_opts = [f"{r.code} — {r.name}" for _,r in raw_pool.iterrows()]
                raw_sel = st.multiselect("Nguyên liệu (mã)", raw_opts, key="ct_raw")

                # Phụ gia + định lượng
                pg_opts = [f"{r.code} — {r.name}" for _,r in pg_list.iterrows()]
                pg_sel = st.multiselect("Phụ gia", pg_opts, key="ct_pg")
                add_q = {}
                for label in pg_sel:
                    c = label.split(" — ",1)[0]
                    add_q[c] = st.number_input(f"{c} (kg / 1kg sau sơ chế)", value=0.0, step=0.1, key=f"ct_pg_{c}")

                colA, colB, colC = st.columns(3)
                ok = colA.form_submit_button("💾 Lưu", use_container_width=True)
                rm = colB.form_submit_button("🗑️ Xoá", use_container_width=True)
                _  = colC.form_submit_button("HUỶ", use_container_width=True)

            if ok and code and out_label:
                out_code = out_label.split(" — ",1)[0]
                fruits_csv = ",".join([x.split(" — ",1)[0] for x in raw_sel])
                note = f"SRC={src}" if typ=="MUT" else ""
                run_sql(conn, """
                    INSERT INTO formulas(code,name,type,output_pcode,output_uom,recovery,
                                         cups_per_kg,fruits_csv,additives_json,note)
                    VALUES(:c,:n,:t,:op,:u,:r,:cups,:fr,:adds,:note)
                    ON CONFLICT (code) DO UPDATE SET
                      name=EXCLUDED.name, type=EXCLUDED.type, output_pcode=EXCLUDED.output_pcode,
                      output_uom=EXCLUDED.output_uom, recovery=EXCLUDED.recovery,
                      cups_per_kg=EXCLUDED.cups_per_kg, fruits_csv=EXCLUDED.fruits_csv,
                      additives_json=EXCLUDED.additives_json, note=EXCLUDED.note
                """, {
                    "c":code.strip(),"n":name.strip(),"t":typ,"op":out_code,"u":uom.strip(),
                    "r":float(rec),"cups":float(cups),"fr":fruits_csv,
                    "adds":json.dumps(add_q, ensure_ascii=False),"note":note
                })
                write_audit(conn,"CT_UPSERT",code)
                st.success("Đã lưu công thức.")
                st.rerun()

            if rm and code:
                run_sql(conn,"DELETE FROM formulas WHERE code=:c",{"c":code.strip()})
                write_audit(conn,"CT_DELETE",code)
                st.success("Đã xoá công thức.")
                st.rerun()
        else:
            st.info("Bạn không có quyền chỉnh sửa công thức (CT_EDIT).")

# ---------- CỬA HÀNG (stores) ----------
def page_cuahang(conn: Connection, user: dict):
    st.markdown("### 🏬 Cửa hàng")
    df = fetch_df(conn, "SELECT code,name,address,phone,COALESCE(is_active,true) is_active FROM stores ORDER BY code")
    st.dataframe(df, use_container_width=True, hide_index=True)

    if has_perm(user,"STORE_EDIT"):
        st.markdown("**Thêm / Sửa / Xoá cửa hàng**")
        with st.form("store_edit", clear_on_submit=True):
            col1, col2 = st.columns([1,2])
            code = col1.text_input("Mã cửa hàng", key="st_code")
            name = col2.text_input("Tên cửa hàng", key="st_name")
            address = st.text_input("Địa chỉ", key="st_addr")
            phone   = st.text_input("Điện thoại", key="st_phone")
            active  = st.checkbox("Đang hoạt động", value=True, key="st_active")
            cA, cB, cC = st.columns(3)
            ok = cA.form_submit_button("💾 Lưu", use_container_width=True)
            rm = cB.form_submit_button("🗑️ Xoá", use_container_width=True)
            _  = cC.form_submit_button("HUỶ", use_container_width=True)

        if ok and code and name:
            run_sql(conn, """
                INSERT INTO stores(code,name,address,phone,is_active)
                VALUES(:c,:n,:a,:p,:act)
                ON CONFLICT (code) DO UPDATE SET
                    name=EXCLUDED.name, address=EXCLUDED.address,
                    phone=EXCLUDED.phone, is_active=EXCLUDED.is_active
            """, {"c":code.strip(),"n":name.strip(),"a":address,"p":phone,"act":bool(active)})
            write_audit(conn,"STORE_UPSERT",code)
            st.success("Đã lưu cửa hàng.")
            st.rerun()
        if rm and code:
            run_sql(conn,"DELETE FROM stores WHERE code=:c",{"c":code.strip()})
            write_audit(conn,"STORE_DELETE",code)
            st.success("Đã xoá.")
            st.rerun()
    else:
        st.info("Bạn không có quyền chỉnh sửa cửa hàng (STORE_EDIT).")

# ---------- NGƯỜI DÙNG (users) ----------
def page_nguoidung(conn: Connection, user: dict):
    st.markdown("### 👥 Người dùng")
    df = fetch_df(conn, """
        SELECT email, display, role, store_code, perms
        FROM users ORDER BY email
    """)
    st.dataframe(df, use_container_width=True, hide_index=True)

    if has_perm(user, "USER_EDIT"):
        st.markdown("**Thêm / Sửa / Xoá người dùng**")
        stores = fetch_df(conn, "SELECT code,name FROM stores ORDER BY name")
        store_opts = ["(Không gán)"] + [f"{r.code} — {r.name}" for _,r in stores.iterrows()]

        with st.form("user_edit", clear_on_submit=True):
            col1, col2 = st.columns(2)
            email   = col1.text_input("Email (đăng nhập)", key="us_email")
            display = col2.text_input("Tên hiển thị", key="us_disp")
            role    = st.selectbox("Vai trò", ["User","Admin","SuperAdmin"], key="us_role")
            store_lb= st.selectbox("Cửa hàng mặc định", store_opts, key="us_store")
            perms   = st.text_area("Quyền (phân tách dấu phẩy)", value=",".join(PERM_ALL if role=="Admin" else []), key="us_perms")
            pw_new  = st.text_input("Mật khẩu (để trống nếu không đổi)", type="password", key="us_pw")
            cA,cB,cC = st.columns(3)
            ok = cA.form_submit_button("💾 Lưu", use_container_width=True)
            rm = cB.form_submit_button("🗑️ Xoá", use_container_width=True)
            _  = cC.form_submit_button("HUỶ", use_container_width=True)

        if ok and email:
            store_code = None if store_lb=="(Không gán)" else store_lb.split(" — ",1)[0]
            if pw_new:
                run_sql(conn, """
                    INSERT INTO users(email,display,password,role,store_code,perms)
                    VALUES(:e,:d,:p,:r,:s,:pm)
                    ON CONFLICT (email) DO UPDATE SET
                        display=EXCLUDED.display, password=EXCLUDED.password,
                        role=EXCLUDED.role, store_code=EXCLUDED.store_code,
                        perms=EXCLUDED.perms
                """, {"e":email.strip(),"d":display or email.strip(),"p":sha256(pw_new),
                      "r":role,"s":store_code,"pm":perms.strip()})
            else:
                run_sql(conn, """
                    INSERT INTO users(email,display,password,role,store_code,perms)
                    VALUES(:e,:d,COALESCE((SELECT password FROM users WHERE email=:e), :p_keep),:r,:s,:pm)
                    ON CONFLICT (email) DO UPDATE SET
                        display=EXCLUDED.display,
                        role=EXCLUDED.role, store_code=EXCLUDED.store_code,
                        perms=EXCLUDED.perms
                """, {"e":email.strip(),"d":display or email.strip(),"p_keep":sha256("changeme"),
                      "r":role,"s":store_code,"pm":perms.strip()})
            write_audit(conn,"USER_UPSERT",email)
            st.success("Đã lưu người dùng.")
            st.rerun()

        if rm and email:
            if email.strip().lower()==st.session_state.get("user",{}).get("email","").lower():
                st.error("Không thể xoá tài khoản đang đăng nhập.")
            else:
                run_sql(conn,"DELETE FROM users WHERE email=:e",{"e":email.strip()})
                write_audit(conn,"USER_DELETE",email)
                st.success("Đã xoá.")
                st.rerun()
    else:
        st.info("Bạn không có quyền chỉnh sửa người dùng (USER_EDIT).")

# ---------- ROUTER CẬP NHẬT (thay cho placeholder ở Phần 1) ----------
def router():
    """Router duy nhất: gọi trang theo menu."""
    _ensure_session_defaults()
    conn = get_conn()
    user = require_login(conn)
    header_top(conn, user)
    menu = sidebar_menu(conn, user)

    if menu == "Dashboard":
        page_dashboard(conn, user)
    elif menu == "Danh mục":
        page_danhmuc(conn, user)
    elif menu == "Cửa hàng":
        page_cuahang(conn, user)
    elif menu == "Người dùng":
        page_nguoidung(conn, user)
    elif menu == "Nhật ký":
        if has_perm(user, "AUDIT_VIEW"):
            df = fetch_df(conn, "SELECT ts,actor,action,detail FROM syslog ORDER BY ts DESC LIMIT 300")
            st.markdown("### 🗒️ Nhật ký hệ thống")
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.warning("Bạn không có quyền xem nhật ký.")
    elif menu == "Kho":
        st.info("Kho sẽ được cung cấp đầy đủ ở **Phần 3**.")
    elif menu == "Sản xuất":
        st.info("Sản xuất (CỐT/MỨT) sẽ ở **Phần 3**.")
    elif menu == "Báo cáo":
        st.info("Báo cáo sẽ ở **Phần 4**.")
    elif menu == "TSCD":
        st.info("TSCD sẽ ở **Phần 4**.")
    elif menu == "Doanh thu":
        st.info("Doanh thu sẽ ở **Phần 5**.")
# ============================================================
# PHẦN 3/5 — KHO (Nhập/Xuất/Tồn/KK) + SẢN XUẤT (CỐT & MỨT)
# ============================================================

# ========= Helpers riêng cho Kho/SX =========
def _get_products(conn, cat=None):
    if cat:
        return fetch_df(conn, "SELECT code,name,uom,cat_code FROM products WHERE cat_code=:c ORDER BY code", {"c": cat})
    return fetch_df(conn, "SELECT code,name,uom,cat_code FROM products ORDER BY code")

def _product_selector(conn, cat=None, placeholder="Chọn sản phẩm...", key_prefix=""):
    df = _get_products(conn, cat)
    opts = [f"{r.code} — {r.name} ({r.uom})" for _, r in df.iterrows()]
    lb = st.selectbox(placeholder, opts, index=0 if opts else None, key=f"{key_prefix}p")
    code = lb.split(" — ", 1)[0] if lb else None
    uom = df[df["code"] == code]["uom"].iloc[0] if (lb and not df.empty) else ""
    return code, uom

def _cups_per_kg_for_pcode(conn, pcode: str) -> float:
    """Lấy cốc/kg TP từ công thức – ưu tiên công thức mới nhất của đúng pcode."""
    try:
        df = fetch_df(conn, """
            SELECT cups_per_kg FROM formulas
            WHERE output_pcode=:p ORDER BY code DESC LIMIT 1
        """, {"p": pcode})
        return float(df.iloc[0]["cups_per_kg"]) if not df.empty else 0.0
    except Exception:
        return 0.0

def _stock_of(conn, store: str, pcode: str, to_dt: date | None = None):
    """Tồn & cốc đến hết ngày to_dt (nếu None => tới hiện tại)."""
    if to_dt is None:
        to_dt = date.today()
    df = fetch_df(conn, """
        SELECT
          SUM(CASE WHEN kind='IN'  THEN qty ELSE -qty END)  AS qty,
          SUM(CASE WHEN kind='IN'  THEN COALESCE(cups,0) ELSE -COALESCE(cups,0) END) AS cups,
          SUM(CASE WHEN kind='IN'  THEN qty*COALESCE(price_in,0) ELSE 0 END)         AS val_in
        FROM inventory_ledger
        WHERE store=:s AND pcode=:p AND ts::date<=:d
    """, {"s": store, "p": pcode, "d": to_dt})
    if df.empty:
        return 0.0, 0.0, 0.0
    q = float(df.iloc[0]["qty"] or 0.0)
    c = float(df.iloc[0]["cups"] or 0.0)
    v = float(df.iloc[0]["val_in"] or 0.0)
    return q, c, v

def _snapshot_stock(conn, store: str, to_dt: date | None = None, cat: str | None = None):
    if to_dt is None: to_dt = date.today()
    cond_cat = "" if not cat else "AND p.cat_code=:cat"
    sql = f"""
      SELECT p.code, p.name, p.cat_code, p.uom,
             COALESCE(SUM(CASE WHEN l.kind='IN'  THEN l.qty ELSE -l.qty END),0)        AS qty,
             COALESCE(SUM(CASE WHEN l.kind='IN'  THEN COALESCE(l.cups,0)
                               ELSE -COALESCE(l.cups,0) END),0)                         AS cups,
             COALESCE(SUM(CASE WHEN l.kind='IN'  THEN l.qty*COALESCE(l.price_in,0)
                               ELSE 0 END),0)                                           AS total_in_value
      FROM products p
      LEFT JOIN inventory_ledger l
             ON l.pcode=p.code AND l.store=:s AND l.ts::date<=:d
      WHERE 1=1 {cond_cat}
      GROUP BY p.code,p.name,p.cat_code,p.uom
      HAVING COALESCE(SUM(CASE WHEN l.kind='IN' THEN l.qty ELSE -l.qty END),0)<>0
          OR COALESCE(SUM(CASE WHEN l.kind='IN' THEN COALESCE(l.cups,0)
                               ELSE -COALESCE(l.cups,0) END),0)<>0
      ORDER BY p.code
    """
    params = {"s": store, "d": to_dt}
    if cat: params["cat"] = cat
    return fetch_df(conn, sql, params)

def _prevent_negative(conn, store: str, pcode: str, qty_out: float):
    stock, _, _ = _stock_of(conn, store, pcode)
    return qty_out <= stock + 1e-9

def _new_batch_id(conn, store: str, typ: str):
    df = fetch_df(conn, "SELECT TO_CHAR(NOW(),'YYMMDDHH24MISS') AS t")
    t = df.iloc[0]["t"]
    return f"{store}-{typ}-{t}"

# =============== KHO ===============
def page_kho(conn: Connection, user: dict):
    st.markdown("### 🧳 Quản lý kho")
    tabs = st.tabs(["🧾 Phiếu nhập", "📤 Phiếu xuất", "📦 Tồn kho", "🧮 Kiểm kê nâng cao"])

    # ---------- Phiếu nhập ----------
    with tabs[0]:
        st.subheader("🧾 Phiếu nhập")
        if not has_perm(user, "WH_IN"):
            st.warning("Bạn không có quyền nhập kho (WH_IN).")
        else:
            col1, col2, col3 = st.columns([1.2, 2, 1])
            with col1:
                in_date = st.date_input("Ngày nhập", value=date.today(), key="in_date")
            with col2:
                _ = st.text_input("Gõ vài ký tự để lọc…", key="in_find")
            with col3:
                store = st.session_state.get("store") or st.selectbox(
                    "Cửa hàng", fetch_df(conn, "SELECT code FROM stores ORDER BY code")["code"].tolist(),
                    key="in_store_select"
                )
            st.caption("Chọn sản phẩm nhập")
            pcode, uom = _product_selector(conn, placeholder="— chọn sản phẩm —", key_prefix="in_")
            c1, c2, c3 = st.columns([1, 1, 2])
            with c1:
                qty = st.number_input("Số lượng", value=0.0, step=0.1, min_value=0.0, key="in_qty")
            with c2:
                price = st.number_input("Đơn giá nhập (VND/ĐVT)", value=0.0, step=100.0, min_value=0.0, key="in_price")
            with c3:
                note = st.text_input("Ghi chú", key="in_note")

            # Cốc: chỉ hiển thị nếu người dùng muốn tự nhập cho CỐT/MỨT; mặc định để 0 – SX sẽ tính tự động
            manual_cups = st.checkbox("Nhập số cốc thủ công (chỉ khi nhập thành phẩm CỐT/MỨT)", key="in_cup_manual")
            cups_in = 0.0
            if manual_cups and pcode:
                cups_in = st.number_input("Số cốc (+)", value=0.0, step=1.0, min_value=0.0, key="in_cups")

            if st.button("💾 Lưu phiếu nhập", type="primary", key="in_btn"):
                if not pcode or qty <= 0:
                    st.error("Chọn sản phẩm và số lượng > 0.")
                else:
                    run_sql(conn, """
                        INSERT INTO inventory_ledger(ts, store, pcode, kind, qty, price_in, reason, cups)
                        VALUES (NOW(), :s, :p, 'IN', :q, :pr, :rs, :cups)
                    """, {"s": store, "p": pcode, "q": qty, "pr": price, "rs": note, "cups": cups_in})
                    write_audit(conn, "WH_IN", f"{store} {pcode} +{qty} {uom} ({price}) cups+{cups_in}")
                    st.success("Đã lưu phiếu nhập.")

    # ---------- Phiếu xuất ----------
    with tabs[1]:
        st.subheader("📤 Phiếu xuất")
        if not has_perm(user, "WH_OUT"):
            st.warning("Bạn không có quyền xuất kho (WH_OUT).")
        else:
            col1, col2, col3 = st.columns([1.2, 2, 1])
            with col1:
                out_date = st.date_input("Ngày xuất", value=date.today(), key="out_date")
            with col2:
                _ = st.text_input("Gõ vài ký tự để lọc…", key="out_find")
            with col3:
                store = st.session_state.get("store") or st.selectbox(
                    "Cửa hàng", fetch_df(conn, "SELECT code FROM stores ORDER BY code")["code"].tolist(),
                    key="out_store_select"
                )
            st.caption("Chọn sản phẩm xuất")
            pcode, uom = _product_selector(conn, placeholder="— chọn sản phẩm —", key_prefix="out_")

            c1, c2 = st.columns([1, 2])
            with c1:
                qty = st.number_input("Số lượng", value=0.0, step=0.1, min_value=0.0, key="out_qty")
            with c2:
                reason = st.selectbox("Lý do xuất", ["BÁN_LẺ", "BÁN_SỈ", "HỦY", "KHÁC"], key="out_reason")
            auto_cups = 0.0
            if pcode:
                auto_cups = _cups_per_kg_for_pcode(conn, pcode) * qty
            cups_override = st.checkbox("Nhập số cốc thủ công (nếu cần)", key="out_cup_manual")
            cups = st.number_input("Số cốc (-)", value=float(auto_cups), step=1.0, min_value=0.0,
                                   key="out_cups", disabled=not cups_override)

            if st.button("💾 Lưu phiếu xuất", type="primary", key="out_btn"):
                if not pcode or qty <= 0:
                    st.error("Chọn sản phẩm và số lượng > 0.")
                elif not _prevent_negative(conn, store, pcode, qty):
                    st.error("Xuất âm kho! Kiểm tra lại số lượng tồn.")
                else:
                    run_sql(conn, """
                        INSERT INTO inventory_ledger(ts, store, pcode, kind, qty, reason, cups)
                        VALUES (NOW(), :s, :p, 'OUT', :q, :rs, :cups)
                    """, {"s": store, "p": pcode, "q": qty, "rs": reason, "cups": (float(cups) if cups_override else auto_cups)})
                    write_audit(conn, "WH_OUT", f"{store} {pcode} -{qty} {uom} cups-{cups}")
                    st.success("Đã lưu phiếu xuất.")

    # ---------- Tồn kho ----------
    with tabs[2]:
        st.subheader("📦 Báo cáo tồn kho")
        with st.expander("🔎 Bộ lọc (chỉ áp khi bấm **Áp dụng**)", expanded=False):
            colf1, colf2, colf3, colf4 = st.columns([1, 1, 1, 1])
            to_date   = colf1.date_input("Chốt đến ngày", value=date.today(), key="stk_to")
            cat       = colf2.selectbox("Nhóm", ["(Tất cả)", "TRAI_CAY", "COT", "MUT", "PHU_GIA"], key="stk_cat")
            store     = colf3.selectbox("Cửa hàng", fetch_df(conn, "SELECT code FROM stores ORDER BY code")["code"].tolist(),
                                        key="stk_store")
            do_apply  = colf4.button("Áp dụng", key="stk_apply")
        if 'stk_cache' not in st.session_state or do_apply:
            cat_val = None if cat == "(Tất cả)" else cat
            st.session_state['stk_cache'] = _snapshot_stock(conn, store, to_date, cat_val)
        df_stk = st.session_state.get('stk_cache', pd.DataFrame())
        if df_stk.empty:
            st.info("Chưa có số liệu.")
        else:
            df_stk["Giá trị tồn (ước)"] = df_stk["total_in_value"]  # tổng giá vốn đã nhập tới thời điểm đó
            df_stk = df_stk.rename(columns={
                "code":"Mã", "name":"Tên", "cat_code":"Nhóm", "uom":"ĐVT",
                "qty":"Tồn SL", "cups":"Tồn cốc"
            })
            st.dataframe(df_stk, use_container_width=True, hide_index=True)
            st.caption(f"Tổng số dòng: {len(df_stk)} | Tổng giá trị (nhập): {df_stk['Giá trị tồn (ước)'].sum():,.0f} VND")

    # ---------- Kiểm kê nâng cao ----------
    with tabs[3]:
        st.subheader("🧮 Kiểm kê nâng cao")
        if not has_perm(user, "WH_AUDIT"):
            st.warning("Bạn không có quyền kiểm kê (WH_AUDIT).")
        else:
            store = st.session_state.get("store") or st.selectbox(
                "Cửa hàng", fetch_df(conn, "SELECT code FROM stores ORDER BY code")["code"].tolist(),
                key="kk_store"
            )
            pcode, uom = _product_selector(conn, placeholder="— chọn sản phẩm kiểm kê —", key_prefix="kk_")
            if pcode:
                st.caption("Số liệu hệ thống đến hiện tại:")
                qty_sys, cups_sys, _ = _stock_of(conn, store, pcode)
                st.info(f"Tồn hệ thống: {qty_sys:.3f} {uom} | Cốc: {cups_sys:.0f}")

            col1, col2 = st.columns([1, 1])
            with col1:
                qty_real = st.number_input("Số lượng thực tế", value=0.0, step=0.1, min_value=0.0, key="kk_qty")
            with col2:
                cups_real = st.number_input("Cốc thực tế (nếu có)", value=0.0, step=1.0, min_value=0.0, key="kk_cups")
            note = st.text_input("Ghi chú", key="kk_note")

            if st.button("📌 Ghi chênh lệch", type="primary", key="kk_btn"):
                if not pcode:
                    st.error("Chọn sản phẩm.")
                else:
                    qty_sys, cups_sys, _ = _stock_of(conn, store, pcode)
                    diff_q = qty_real - qty_sys
                    diff_c = cups_real - cups_sys
                    if abs(diff_q) < 1e-9 and abs(diff_c) < 1e-9:
                        st.info("Không có chênh lệch.")
                    else:
                        kind = 'IN' if diff_q >= 0 else 'OUT'
                        qty = abs(diff_q)
                        cups = abs(diff_c)
                        run_sql(conn, """
                            INSERT INTO inventory_ledger(ts, store, pcode, kind, qty, reason, cups)
                            VALUES (NOW(), :s, :p, :k, :q, :rs, :cups)
                        """, {"s": store, "p": pcode, "k": kind, "q": qty,
                              "rs": f"KIEM_KE: {note}", "cups": (cups if diff_c != 0 else 0)})
                        write_audit(conn, "WH_AUDIT",
                                    f"{store} {pcode} diff_qty={diff_q:.3f} diff_cups={diff_c:.0f}")
                        st.success("Đã ghi kiểm kê.")

# =============== SẢN XUẤT ===============
def _parse_formula_row(row) -> dict:
    src = "TRAI_CAY"
    if (row.get("note") or "").startswith("SRC="):
        src = (row.get("note") or "").split("=",1)[1]
    adds = {}
    try:
        adds = json.loads(row.get("additives_json") or "{}")
    except Exception:
        adds = {}
    return {
        "code": row["code"], "name": row["name"], "type": row["type"],
        "out_pcode": row["output_pcode"], "out_uom": row["output_uom"] or "kg",
        "recovery": float(row["recovery"] or 1.0),
        "cups_per_kg": float(row["cups_per_kg"] or 0.0),
        "fruits": [x for x in (row["fruits_csv"] or "").split(",") if x],
        "additives": adds,
        "src": src
    }

def _formula_options(conn, typ: str):
    df = fetch_df(conn, "SELECT * FROM formulas WHERE type=:t ORDER BY code", {"t": typ})
    return df

def _preview_inputs_for_output(conn, formula: dict, qty_out: float):
    """Tính nguyên liệu cần cho qty_out TP. Trả về df_inputs, cups_out."""
    cups = formula["cups_per_kg"] * qty_out
    inputs = []
    # Nguyên liệu chính: nếu có nhiều, cho tỉ lệ đều — phần UI sẽ cho sửa tỉ lệ trước khi ghi
    need_raw = qty_out if formula["type"] == "MUT" else (qty_out / (formula["recovery"] or 1.0))
    if len(formula["fruits"]) > 0:
        per = 1.0 / len(formula["fruits"])
        for pc in formula["fruits"]:
            inputs.append({"pcode": pc, "qty": need_raw * per, "kind":"RAW"})
    # Phụ gia (kg / 1kg sau sơ chế)
    for pc, perkg in (formula["additives"] or {}).items():
        if perkg and float(perkg) > 0:
            inputs.append({"pcode": pc, "qty": float(perkg) * qty_out, "kind":"ADD"})
    df = pd.DataFrame(inputs) if inputs else pd.DataFrame(columns=["pcode","qty","kind"])
    return df, cups

def page_sanxuat(conn: Connection, user: dict):
    st.markdown("### 🏭 Sản xuất")
    tabs = st.tabs(["🧪 CỐT (1 bước)", "🍯 MỨT từ TRÁI CÂY", "🍯 MỨT từ CỐT", "🧾 Lịch sử lô"])

    # ---------- CỐT ----------
    with tabs[0]:
        st.subheader("🧪 CỐT (1 bước)")
        if not has_perm(user, "PROD_RUN"):
            st.warning("Bạn không có quyền sản xuất (PROD_RUN).")
        else:
            df_ct = _formula_options(conn, "COT")
            if df_ct.empty:
                st.info("Chưa có công thức CỐT.")
            else:
                opts = [f"{r.output_pcode} — {r.name} ({r.code})" for _, r in df_ct.iterrows()]
                lb = st.selectbox("Chọn công thức", opts, key="cot_formula")
                sel = df_ct.iloc[opts.index(lb)]
                f = _parse_formula_row(sel)
                qty_out = st.number_input("Sản lượng TP (kg)", value=0.0, step=0.1, min_value=0.0, key="cot_qty")
                df_in, cups = _preview_inputs_for_output(conn, f, qty_out)

                # Phân bổ tỉ lệ nguyên liệu chính (nếu nhiều)
                if not df_in[df_in["kind"]=="RAW"].empty:
                    st.caption("Tỉ lệ nguyên liệu chính (tổng = 100%)")
                    raws = df_in[df_in["kind"]=="RAW"].copy()
                    ratios = []
                    for i, row in raws.iterrows():
                        r = st.slider(f"{row['pcode']}", min_value=0, max_value=100,
                                      value=int(100/len(raws)), key=f"cot_ratio_{row['pcode']}")
                        ratios.append(r)
                    s = sum(ratios) or 1
                    raws["qty"] = (qty_out/(f["recovery"] or 1.0)) * (pd.Series(ratios)/s)
                    df_in.update(raws)

                st.markdown("**Nguyên liệu dự kiến xuất kho**")
                st.dataframe(df_in, use_container_width=True, hide_index=True)
                st.info(f"Dự kiến cốc tạo ra: {cups:.0f}")

                store = st.session_state.get("store") or st.selectbox(
                    "Cửa hàng", fetch_df(conn, "SELECT code FROM stores ORDER BY code")["code"].tolist(),
                    key="cot_store"
                )
                note = st.text_input("Ghi chú", key="cot_note")

                if st.button("🚀 Thực hiện SX CỐT", type="primary", key="cot_do"):
                    # kiểm tồn tất cả NVL
                    ok = True
                    for _, r in df_in.iterrows():
                        if not _prevent_negative(conn, store, r["pcode"], float(r["qty"])):
                            ok = False
                            st.error(f"Xuất âm kho: {r['pcode']}")
                    if qty_out <= 0:
                        ok = False
                        st.error("Sản lượng phải > 0.")
                    if ok:
                        lot = _new_batch_id(conn, store, "COT")
                        # xuất NVL
                        for _, r in df_in.iterrows():
                            run_sql(conn, """
                                INSERT INTO inventory_ledger(ts, store, pcode, kind, qty, reason, lot_id)
                                VALUES (NOW(), :s, :p, 'OUT', :q, 'SX_COT', :lot)
                            """, {"s": store, "p": r["pcode"], "q": float(r["qty"]), "lot": lot})
                        # nhập TP
                        run_sql(conn, """
                            INSERT INTO inventory_ledger(ts, store, pcode, kind, qty, reason, cups, lot_id)
                            VALUES (NOW(), :s, :p, 'IN', :q, 'SX_COT_DONE', :cups, :lot)
                        """, {"s": store, "p": f["out_pcode"], "q": float(qty_out), "cups": float(cups), "lot": lot})
                        # ghi batch
                        run_sql(conn, """
                            INSERT INTO prod_batches(batch_id, store, type, ts, output_pcode, qty_out, cups_out, status, note)
                            VALUES(:id,:s,'COT',NOW(),:p,:q,:c,'DONE',:n)
                        """, {"id": lot, "s": store, "p": f["out_pcode"], "q": float(qty_out), "c": float(cups), "n": note})
                        write_audit(conn, "PROD_COT", f"{lot} {f['out_pcode']} +{qty_out}kg cups+{cups}")
                        st.success(f"Đã tạo lô {lot}.")

    # ---------- MỨT từ TRÁI CÂY ----------
    with tabs[1]:
        st.subheader("🍯 MỨT từ TRÁI CÂY")
        df_ct = _formula_options(conn, "MUT")
        df_ct = df_ct[df_ct["note"].fillna("").str.startswith("SRC=TRAI_CAY")]
        if df_ct.empty:
            st.info("Chưa có công thức MỨT nguồn TRÁI_CÂY.")
        else:
            lb = st.selectbox("Chọn công thức", [f"{r.output_pcode} — {r.name} ({r.code})" for _,r in df_ct.iterrows()],
                              key="mut_tc_formula")
            sel = df_ct.iloc[[i for i,_ in enumerate(df_ct.index)] [ [f"{r.output_pcode} — {r.name} ({r.code})" for _,r in df_ct.iterrows()].index(lb) ]]
            f = _parse_formula_row(sel)
            qty_out = st.number_input("Sản lượng TP (kg)", value=0.0, step=0.1, min_value=0.0, key="mut_tc_qty")
            df_in, cups = _preview_inputs_for_output(conn, f, qty_out)

            # Phân bổ tỉ lệ trái cây (nếu nhiều)
            if not df_in[df_in["kind"]=="RAW"].empty:
                st.caption("Tỉ lệ trái cây (tổng = 100%)")
                raws = df_in[df_in["kind"]=="RAW"].copy()
                ratios = []
                for i, row in raws.iterrows():
                    r = st.slider(f"{row['pcode']}", min_value=0, max_value=100,
                                  value=int(100/len(raws)), key=f"mut_tc_ratio_{row['pcode']}")
                    ratios.append(r)
                s = sum(ratios) or 1
                raws["qty"] = qty_out * (pd.Series(ratios)/s)  # rec=1.0 cho MUT
                df_in.update(raws)

            st.markdown("**Nguyên liệu dự kiến**")
            st.dataframe(df_in, use_container_width=True, hide_index=True)
            st.info(f"Dự kiến cốc tạo ra: {cups:.0f}")

            store = st.session_state.get("store") or st.selectbox(
                "Cửa hàng", fetch_df(conn, "SELECT code FROM stores ORDER BY code")["code"].tolist(),
                key="mut_tc_store"
            )
            note = st.text_input("Ghi chú", key="mut_tc_note")

            if st.button("🚀 Thực hiện SX MỨT (từ TRÁI CÂY)", type="primary", key="mut_tc_do"):
                ok = True
                for _, r in df_in.iterrows():
                    if not _prevent_negative(conn, store, r["pcode"], float(r["qty"])):
                        ok = False
                        st.error(f"Xuất âm kho: {r['pcode']}")
                if qty_out <= 0: ok = False; st.error("Sản lượng phải > 0.")
                if ok:
                    lot = _new_batch_id(conn, store, "MUT")
                    for _, r in df_in.iterrows():
                        run_sql(conn, """
                            INSERT INTO inventory_ledger(ts, store, pcode, kind, qty, reason, lot_id)
                            VALUES (NOW(), :s, :p, 'OUT', :q, 'SX_MUT_TC', :lot)
                        """, {"s": store, "p": r["pcode"], "q": float(r["qty"]), "lot": lot})
                    run_sql(conn, """
                        INSERT INTO inventory_ledger(ts, store, pcode, kind, qty, reason, cups, lot_id)
                        VALUES (NOW(), :s, :p, 'IN', :q, 'SX_MUT_TC_DONE', :cups, :lot)
                    """, {"s": store, "p": f["out_pcode"], "q": float(qty_out), "cups": float(cups), "lot": lot})
                    run_sql(conn, """
                        INSERT INTO prod_batches(batch_id, store, type, ts, output_pcode, qty_out, cups_out, status, note)
                        VALUES(:id,:s,'MUT_TC',NOW(),:p,:q,:c,'DONE',:n)
                    """, {"id": lot, "s": store, "p": f["out_pcode"], "q": float(qty_out), "c": float(cups), "n": note})
                    write_audit(conn, "PROD_MUT_TC", f"{lot} {f['out_pcode']} +{qty_out}kg cups+{cups}")
                    st.success(f"Đã tạo lô {lot}.")

    # ---------- MỨT từ CỐT ----------
    with tabs[2]:
        st.subheader("🍯 MỨT từ CỐT")
        df_ct = _formula_options(conn, "MUT")
        df_ct = df_ct[df_ct["note"].fillna("").str.startswith("SRC=COT")]
        if df_ct.empty:
            st.info("Chưa có công thức MỨT nguồn CỐT.")
        else:
            lb = st.selectbox("Chọn công thức", [f"{r.output_pcode} — {r.name} ({r.code})" for _,r in df_ct.iterrows()],
                              key="mut_cot_formula")
            sel = df_ct.iloc[[i for i,_ in enumerate(df_ct.index)] [ [f"{r.output_pcode} — {r.name} ({r.code})" for _,r in df_ct.iterrows()].index(lb) ]]
            f = _parse_formula_row(sel)
            qty_out = st.number_input("Sản lượng TP (kg)", value=0.0, step=0.1, min_value=0.0, key="mut_cot_qty")
            df_in, cups = _preview_inputs_for_output(conn, f, qty_out)
            st.markdown("**Nguyên liệu dự kiến**")
            st.dataframe(df_in, use_container_width=True, hide_index=True)
            st.info(f"Dự kiến cốc tạo ra: {cups:.0f}")

            store = st.session_state.get("store") or st.selectbox(
                "Cửa hàng", fetch_df(conn, "SELECT code FROM stores ORDER BY code")["code"].tolist(),
                key="mut_cot_store"
            )
            note = st.text_input("Ghi chú", key="mut_cot_note")

            if st.button("🚀 Thực hiện SX MỨT (từ CỐT)", type="primary", key="mut_cot_do"):
                ok = True
                for _, r in df_in.iterrows():
                    if not _prevent_negative(conn, store, r["pcode"], float(r["qty"])):
                        ok = False
                        st.error(f"Xuất âm kho: {r['pcode']}")
                if qty_out <= 0: ok = False; st.error("Sản lượng phải > 0.")
                if ok:
                    lot = _new_batch_id(conn, store, "MUT")
                    for _, r in df_in.iterrows():
                        run_sql(conn, """
                            INSERT INTO inventory_ledger(ts, store, pcode, kind, qty, reason, lot_id)
                            VALUES (NOW(), :s, :p, 'OUT', :q, 'SX_MUT_COT', :lot)
                        """, {"s": store, "p": r["pcode"], "q": float(r["qty"]), "lot": lot})
                    run_sql(conn, """
                        INSERT INTO inventory_ledger(ts, store, pcode, kind, qty, reason, cups, lot_id)
                        VALUES (NOW(), :s, :p, 'IN', :q, 'SX_MUT_COT_DONE', :cups, :lot)
                    """, {"s": store, "p": f["out_pcode"], "q": float(qty_out), "cups": float(cups), "lot": lot})
                    run_sql(conn, """
                        INSERT INTO prod_batches(batch_id, store, type, ts, output_pcode, qty_out, cups_out, status, note)
                        VALUES(:id,:s,'MUT_COT',NOW(),:p,:q,:c,'DONE',:n)
                    """, {"id": lot, "s": store, "p": f["out_pcode"], "q": float(qty_out), "c": float(cups), "n": note})
                    write_audit(conn, "PROD_MUT_COT", f"{lot} {f['out_pcode']} +{qty_out}kg cups+{cups}")
                    st.success(f"Đã tạo lô {lot}.")

    # ---------- Lịch sử lô ----------
    with tabs[3]:
        st.subheader("🧾 Lịch sử lô")
        col1, col2, col3 = st.columns([1,1,2])
        d_from = col1.date_input("Từ ngày", value=date.today()-timedelta(days=30), key="his_from")
        d_to   = col2.date_input("Đến ngày", value=date.today(), key="his_to")
        store  = st.session_state.get("store") or col3.selectbox(
            "Cửa hàng", fetch_df(conn, "SELECT code FROM stores ORDER BY code")["code"].tolist(),
            key="his_store"
        )
        df = fetch_df(conn, """
            SELECT batch_id, store, type, ts, output_pcode, qty_out, cups_out, status, note
            FROM prod_batches
            WHERE ts::date BETWEEN :f AND :t AND store=:s
            ORDER BY ts DESC
        """, {"f": d_from, "t": d_to, "s": store})
        st.dataframe(df, use_container_width=True, hide_index=True)

        if has_perm(user, "PROD_DELETE"):
            st.warning("⚠️ Hoàn tác lô sẽ đảo chiều chứng từ kho liên quan.")
            lot = st.text_input("Nhập mã lô để hoàn tác", key="his_rm_lot")
            if st.button("🧨 Hoàn tác lô", key="his_rm_btn"):
                if not lot:
                    st.error("Nhập mã lô.")
                else:
                    # đảo chiều ledger của lot
                    df_legs = fetch_df(conn, "SELECT * FROM inventory_ledger WHERE lot_id=:id", {"id": lot})
                    if df_legs.empty:
                        st.error("Không tìm thấy chứng từ.")
                    else:
                        for _, r in df_legs.iterrows():
                            kind = "OUT" if r["kind"]=="IN" else "IN"
                            run_sql(conn, """
                                INSERT INTO inventory_ledger(ts, store, pcode, kind, qty, price_in, reason, cups, lot_id)
                                VALUES (NOW(), :s, :p, :k, :q, :pr, :rs, :cups, :lot)
                            """, {"s": r["store"], "p": r["pcode"], "k": kind, "q": float(r["qty"]),
                                  "pr": float(r.get("price_in") or 0), "rs": f"UNDO:{r['reason']}",
                                  "cups": float(r.get("cups") or 0), "lot": f"UNDO-{lot}"})
                        run_sql(conn, "UPDATE prod_batches SET status='VOID' WHERE batch_id=:id", {"id": lot})
                        write_audit(conn, "PROD_UNDO", lot)
                        st.success("Đã hoàn tác.")

# ---------- Gắn vào router ----------
def router():
    _ensure_session_defaults()
    conn = get_conn()
    user = require_login(conn)
    header_top(conn, user)
    menu = sidebar_menu(conn, user)

    if menu == "Dashboard":
        page_dashboard(conn, user)
    elif menu == "Danh mục":
        page_danhmuc(conn, user)
    elif menu == "Cửa hàng":
        page_cuahang(conn, user)
    elif menu == "Người dùng":
        page_nguoidung(conn, user)
    elif menu == "Kho":
        page_kho(conn, user)
    elif menu == "Sản xuất":
        page_sanxuat(conn, user)
    elif menu == "Báo cáo":
        st.info("Báo cáo sẽ ở **Phần 4**.")
    elif menu == "TSCD":
        st.info("TSCD sẽ ở **Phần 4**.")
    elif menu == "Doanh thu":
        st.info("Doanh thu sẽ ở **Phần 5**.")
    elif menu == "Nhật ký":
        if has_perm(user, "AUDIT_VIEW"):
            df = fetch_df(conn, "SELECT ts,actor,action,detail FROM syslog ORDER BY ts DESC LIMIT 300")
            st.markdown("### 🗒️ Nhật ký hệ thống")
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.warning("Bạn không có quyền xem nhật ký.")
# ============================================================
# PHẦN 4/5 — BÁO CÁO TÀI CHÍNH + TÀI SẢN CỐ ĐỊNH
# ============================================================

# ---------- Helpers tài chính ----------
def _stock_value(conn, store: str, to_dt: date):
    """Giá trị hàng tồn đến hết ngày to_dt (giá vốn nhập)."""
    df = fetch_df(conn, """
        SELECT p.code,
               SUM(CASE WHEN l.kind='IN'  THEN l.qty ELSE -l.qty END) AS qty,
               SUM(CASE WHEN l.kind='IN'  THEN l.qty*COALESCE(l.price_in,0) ELSE 0 END) AS val_in
        FROM products p
        LEFT JOIN inventory_ledger l ON l.pcode=p.code AND l.store=:s AND l.ts::date<=:d
        GROUP BY p.code
    """, {"s": store, "d": to_dt})
    # Giá trị tồn = phân bổ theo phương pháp bình quân: nếu qty>0 => val_in * (qty / tổng qty nhập) ~ xấp xỉ
    # Đơn giản hơn: dùng tỷ lệ giá vốn nhập / SL nhập * SL tồn (xấp xỉ bình quân).
    df_in = fetch_df(conn, """
        SELECT pcode,
               SUM(CASE WHEN kind='IN' THEN qty ELSE 0 END) AS in_qty,
               SUM(CASE WHEN kind='IN' THEN qty*COALESCE(price_in,0) ELSE 0 END) AS in_val
        FROM inventory_ledger
        WHERE store=:s AND ts::date<=:d
        GROUP BY pcode
    """, {"s": store, "d": to_dt})
    m = pd.merge(df, df_in, how="left", left_on="code", right_on="pcode")
    m["avg_cost"] = m.apply(lambda r: (float(r["in_val"]) / float(r["in_qty"])) if float(r.get("in_qty") or 0)>0 else 0.0, axis=1)
    m["stock_val"] = m["avg_cost"] * m["qty"].astype(float)
    return float(m["stock_val"].sum() if not m.empty else 0.0)

def _purchases_value(conn, store: str, d_from: date, d_to: date):
    df = fetch_df(conn, """
        SELECT SUM(qty*COALESCE(price_in,0)) AS v
        FROM inventory_ledger
        WHERE store=:s AND kind='IN' AND ts::date BETWEEN :f AND :t
    """, {"s": store, "f": d_from, "t": d_to})
    return float(df.iloc[0]["v"] or 0.0) if not df.empty else 0.0

def _sales_value(conn, store: str, d_from: date, d_to: date):
    """Doanh thu gộp lấy theo bảng receipts/payments (sẽ bù ở Phần 5). Ở đây cho trường hợp đã có bảng sales_receipts."""
    df = fetch_df(conn, """
        SELECT SUM(amount) AS v
        FROM payments
        WHERE store=:s AND pay_date BETWEEN :f AND :t AND method IN ('CASH','BANK') AND direction='IN'
    """, {"s": store, "f": d_from, "t": d_to})
    return float(df.iloc[0]["v"] or 0.0) if not df.empty else 0.0

def _expenses_value(conn, store: str, d_from: date, d_to: date):
    """Chi phí tiền mặt/CK (không gồm hàng hóa) – direction OUT."""
    df = fetch_df(conn, """
        SELECT SUM(amount) AS v
        FROM payments
        WHERE store=:s AND pay_date BETWEEN :f AND :t AND method IN ('CASH','BANK') AND direction='OUT'
    """, {"s": store, "f": d_from, "t": d_to})
    return float(df.iloc[0]["v"] or 0.0) if not df.empty else 0.0

# ---------- Trang Báo cáo ----------
def page_baocao(conn: Connection, user: dict):
    st.markdown("### 📊 Báo cáo tài chính")
    store = st.session_state.get("store") or st.selectbox(
        "Cửa hàng", fetch_df(conn, "SELECT code FROM stores ORDER BY code")["code"].tolist(), key="rpt_store"
    )
    colA, colB, colC = st.columns([1,1,1])
    with colA:
        d_from = st.date_input("Từ ngày", value=date.today().replace(day=1), key="rpt_from")
    with colB:
        d_to   = st.date_input("Đến ngày", value=date.today(), key="rpt_to")
    with colC:
        view = st.selectbox("Chọn báo cáo", ["Kết quả kinh doanh (P&L)", "Bảng cân đối kế toán", "Lưu chuyển tiền tệ"], key="rpt_view")

    # ----- KQKD -----
    if view == "Kết quả kinh doanh (P&L)":
        stock_open = _stock_value(conn, store, d_from - timedelta(days=1))
        stock_close = _stock_value(conn, store, d_to)
        purchases = _purchases_value(conn, store, d_from, d_to)
        revenue   = _sales_value(conn, store, d_from, d_to)

        cogs = stock_open + purchases - stock_close
        gross = revenue - cogs
        expenses = _expenses_value(conn, store, d_from, d_to)
        depreciation = float(fetch_df(conn, """
            SELECT COALESCE(SUM(depr_amount),0) AS v
            FROM tscd_depr_log
            WHERE store=:s AND depr_date BETWEEN :f AND :t
        """, {"s": store, "f": d_from, "t": d_to}).iloc[0]["v"] or 0.0)
        op_profit = gross - expenses - depreciation

        df = pd.DataFrame([
            ["Doanh thu", revenue],
            ["Giá vốn (tính theo tồn)", -cogs],
            ["Lãi gộp", gross],
            ["Chi phí vận hành", -expenses],
            ["Khấu hao", -depreciation],
            ["Lợi nhuận thuần", op_profit]
        ], columns=["Khoản mục","Giá trị (VND)"])
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.metric("Lợi nhuận thuần", f"{op_profit:,.0f} VND")

    # ----- Bảng cân đối -----
    elif view == "Bảng cân đối kế toán":
        inv = _stock_value(conn, store, d_to)
        cash = float(fetch_df(conn, """
            SELECT COALESCE(SUM(CASE WHEN direction='IN' THEN amount ELSE -amount END),0) AS v
            FROM payments
            WHERE store=:s AND pay_date<=:t AND method IN ('CASH','BANK')
        """, {"s": store, "t": d_to}).iloc[0]["v"] or 0.0)
        # TSCD:
        tscd_df = fetch_df(conn, "SELECT cost,accum_dep FROM tscd WHERE store=:s", {"s": store})
        fa_cost = float(tscd_df["cost"].sum() if not tscd_df.empty else 0.0)
        fa_acc  = float(tscd_df["accum_dep"].sum() if not tscd_df.empty else 0.0)
        fa_net  = fa_cost - fa_acc
        assets = inv + cash + fa_net

        # Nguồn vốn (đơn giản: Vốn chủ sở hữu = tổng tài sản vì không theo dõi công nợ)
        df = pd.DataFrame([
            ["Tài sản ngắn hạn - Tiền", cash],
            ["Tài sản ngắn hạn - Hàng tồn", inv],
            ["Tài sản dài hạn - TSCD (ròng)", fa_net],
            ["Tổng tài sản", assets],
            ["Vốn chủ sở hữu (tương đương)", assets]
        ], columns=["Khoản mục","Giá trị (VND)"])
        st.dataframe(df, use_container_width=True, hide_index=True)

    # ----- Lưu chuyển tiền tệ -----
    else:
        df = fetch_df(conn, """
            SELECT pay_date, method, direction, ref, note, amount
            FROM payments
            WHERE store=:s AND pay_date BETWEEN :f AND :t
            ORDER BY pay_date
        """, {"s": store, "f": d_from, "t": d_to})
        st.dataframe(df, use_container_width=True, hide_index=True)
        cash_in  = float(df[df["direction"]=="IN"]["amount"].sum() if not df.empty else 0.0)
        cash_out = float(df[df["direction"]=="OUT"]["amount"].sum() if not df.empty else 0.0)
        st.metric("Tiền vào", f"{cash_in:,.0f} VND")
        st.metric("Tiền ra", f"{cash_out:,.0f} VND")
        st.metric("Lưu chuyển thuần", f"{(cash_in-cash_out):,.0f} VND")

# ---------- TSCD ----------
def _tscd_monthly_depr(cost: float, life_months: int) -> float:
    if not cost or not life_months or life_months <= 0: return 0.0
    return float(cost) / float(life_months)

def page_tscd(conn: Connection, user: dict):
    st.markdown("### 🧱 Tài sản cố định")
    tabs = st.tabs(["📄 Danh mục TSCD", "🧮 Tính/ghi khấu hao"])
    # --- Danh mục (CRUD) ---
    with tabs[0]:
        if not has_perm(user, "ASSET_EDIT"):
            st.warning("Bạn không có quyền quản lý TSCD (ASSET_EDIT).")
        else:
            store = st.session_state.get("store") or st.selectbox(
                "Cửa hàng", fetch_df(conn, "SELECT code FROM stores ORDER BY code")["code"].tolist(), key="fa_store"
            )
            df = fetch_df(conn, "SELECT * FROM tscd WHERE store=:s ORDER BY code", {"s": store})
            st.dataframe(df, use_container_width=True, hide_index=True)

            st.markdown("#### ➕ Thêm mới")
            col1, col2, col3, col4 = st.columns([1,2,1,1])
            code = col1.text_input("Mã TS", key="fa_code")
            name = col2.text_input("Tên TS", key="fa_name")
            cost = col3.number_input("Nguyên giá", value=0.0, step=1_000_000.0, min_value=0.0, key="fa_cost")
            life = col4.number_input("Số tháng KH", value=60, step=1, min_value=1, key="fa_life")
            col5, col6 = st.columns([1,1])
            acq  = col5.date_input("Ngày mua", value=date.today(), key="fa_acq")
            note = col6.text_input("Ghi chú", key="fa_note")
            if st.button("💾 Lưu TSCD", type="primary", key="fa_save"):
                if not code or not name:
                    st.error("Nhập mã & tên.")
                else:
                    run_sql(conn, """
                        INSERT INTO tscd(code,store,name,acq_date,cost,life_months,accum_dep,note)
                        VALUES(:c,:s,:n,:d,:cost,:life,0,:note)
                        ON CONFLICT (code) DO UPDATE
                        SET store=:s, name=:n, acq_date=:d, cost=:cost, life_months=:life, note=:note
                    """, {"c": code, "s": store, "n": name, "d": acq, "cost": cost, "life": int(life), "note": note})
                    write_audit(conn, "ASSET_SAVE", code)
                    st.success("Đã lưu TSCD.")

            st.markdown("#### 🗑️ Xoá")
            if not df.empty:
                del_code = st.selectbox("Chọn TS cần xoá", df["code"].tolist(), key="fa_del")
                if st.button("Xoá TSCD", key="fa_del_btn"):
                    run_sql(conn, "DELETE FROM tscd WHERE code=:c", {"c": del_code})
                    write_audit(conn, "ASSET_DELETE", del_code)
                    st.success("Đã xoá.")

    # --- Khấu hao ---
    with tabs[1]:
        store = st.session_state.get("store") or st.selectbox(
            "Cửa hàng", fetch_df(conn, "SELECT code FROM stores ORDER BY code")["code"].tolist(), key="kh_store"
        )
        period = st.date_input("Kỳ khấu hao (lấy theo tháng)", value=date.today(), key="kh_period")
        ym = period.replace(day=1)
        df = fetch_df(conn, "SELECT code,name,cost,life_months,accum_dep FROM tscd WHERE store=:s ORDER BY code", {"s": store})
        if df.empty:
            st.info("Chưa có TSCD.")
        else:
            df["kh_thang"] = df.apply(lambda r: _tscd_monthly_depr(float(r["cost"]), int(r["life_months"])), axis=1)
            st.dataframe(df, use_container_width=True, hide_index=True)
            if st.button("📌 Ghi khấu hao tháng này", type="primary", key="kh_post"):
                for _, r in df.iterrows():
                    kh = float(r["kh_thang"])
                    if kh <= 0: continue
                    # log khấu hao
                    run_sql(conn, """
                        INSERT INTO tscd_depr_log(depr_date, store, code, depr_amount)
                        VALUES(:d, :s, :c, :v)
                        ON CONFLICT DO NOTHING
                    """, {"d": ym, "s": store, "c": r["code"], "v": kh})
                    # cộng dồn
                    run_sql(conn, "UPDATE tscd SET accum_dep=COALESCE(accum_dep,0)+:v WHERE code=:c",
                           {"v": kh, "c": r["code"]})
                write_audit(conn, "ASSET_DEPR", f"{store} {ym.isoformat()}")
                st.success("Đã ghi khấu hao.")
# ============================================================
# PHẦN 5/5 — THU & CHI (CASH / BANK, không theo sản phẩm)
# ============================================================

def page_doanhthu(conn: Connection, user: dict):
    st.markdown("### 💰 Thu & Chi (tiền mặt / chuyển khoản)")
    store = st.session_state.get("store","")

    tab_in, tab_out, tab_rep = st.tabs(["➕ Ghi thu", "➖ Ghi chi", "📊 Báo cáo"])

    # ---------- Ghi thu ----------
    with tab_in:
        col1, col2 = st.columns([1,1])
        with col1:
            d = st.date_input("Ngày thu", value=date.today(), key="rev_in_date")
            method = st.selectbox("Phương thức", ["CASH","BANK"], key="rev_in_method")
            amount = st.number_input("Số tiền (VND)", value=0.0, step=1000.0, min_value=0.0, key="rev_in_amount")
        with col2:
            ref    = st.text_input("Số CT / Mã GD", key="rev_in_ref")
            payer  = st.text_input("Người nộp / Diễn giải ngắn", key="rev_in_payer")
            note   = st.text_area("Ghi chú (tùy chọn)", key="rev_in_note")

        if st.button("💾 Ghi thu", type="primary", use_container_width=True, key="rev_in_btn"):
            if amount <= 0:
                st.error("Nhập số tiền > 0.")
            else:
                run_sql(conn, """
                    INSERT INTO payments(pay_date, store, method, direction, amount, ref, note, actor)
                    VALUES (:d, :s, :m, 'IN', :a, :r, :n, :u)
                """, {"d": d, "s": store, "m": method, "a": float(amount),
                      "r": ref or "", "n": note or (payer or ""), "u": user.get("email","sys")})
                write_audit(conn, "REV_IN", f"{method} {amount}")
                st.success("✅ Đã ghi thu.")
                st.rerun()

    # ---------- Ghi chi ----------
    with tab_out:
        col1, col2 = st.columns([1,1])
        with col1:
            d = st.date_input("Ngày chi", value=date.today(), key="rev_out_date")
            method = st.selectbox("Phương thức", ["CASH","BANK"], key="rev_out_method")
            amount = st.number_input("Số tiền (VND)", value=0.0, step=1000.0, min_value=0.0, key="rev_out_amount")
        with col2:
            ref    = st.text_input("Số CT / Mã GD", key="rev_out_ref")
            payee  = st.text_input("Người nhận / Diễn giải ngắn", key="rev_out_payee")
            note   = st.text_area("Ghi chú (tùy chọn)", key="rev_out_note")

        if st.button("💾 Ghi chi", type="primary", use_container_width=True, key="rev_out_btn"):
            if amount <= 0:
                st.error("Nhập số tiền > 0.")
            else:
                run_sql(conn, """
                    INSERT INTO payments(pay_date, store, method, direction, amount, ref, note, actor)
                    VALUES (:d, :s, :m, 'OUT', :a, :r, :n, :u)
                """, {"d": d, "s": store, "m": method, "a": float(amount),
                      "r": ref or "", "n": note or (payee or ""), "u": user.get("email","sys")})
                write_audit(conn, "REV_OUT", f"{method} {amount}")
                st.success("✅ Đã ghi chi.")
                st.rerun()

    # ---------- Báo cáo ----------
    with tab_rep:
        col1, col2, col3 = st.columns([1,1,1])
        with col1:
            fr = st.date_input("Từ ngày", value=date.today().replace(day=1), key="rev_rep_from")
        with col2:
            to = st.date_input("Đến ngày", value=date.today(), key="rev_rep_to")
        with col3:
            m  = st.selectbox("Phương thức", ["Tất cả","CASH","BANK"], key="rev_rep_method")

        cond = "store=:s AND pay_date BETWEEN :f AND :t"
        prm  = {"s": store, "f": fr, "t": to}
        if m != "Tất cả":
            cond += " AND method=:m"; prm["m"] = m

        df = fetch_df(conn, f"""
            SELECT pay_date AS ngày, method AS phương_thức, direction AS loại, amount AS số_tiền,
                   ref AS chứng_từ, note AS ghi_chú, actor AS người_nhập
            FROM payments
            WHERE {cond}
            ORDER BY pay_date
        """, prm)

        st.dataframe(df, use_container_width=True, hide_index=True, height=350)

        # Tổng hợp
        if df.empty:
            cash_in = bank_in = cash_out = bank_out = 0.0
        else:
            cash_in  = float(df[(df["phương_thức"]=="CASH") & (df["loại"]=="IN")]["số_tiền"].sum())
            bank_in  = float(df[(df["phương_thức"]=="BANK") & (df["loại"]=="IN")]["số_tiền"].sum())
            cash_out = float(df[(df["phương_thức"]=="CASH") & (df["loại"]=="OUT")]["số_tiền"].sum())
            bank_out = float(df[(df["phương_thức"]=="BANK") & (df["loại"]=="OUT")]["số_tiền"].sum())

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Thu CASH", f"{cash_in:,.0f} VND")
        c2.metric("Thu BANK", f"{bank_in:,.0f} VND")
        c3.metric("Chi CASH", f"{cash_out:,.0f} VND")
        c4.metric("Chi BANK", f"{bank_out:,.0f} VND")
        c5.metric("Cân đối", f"{(cash_in+bank_in-cash_out-bank_out):,.0f} VND")

        # Xuất CSV
        st.download_button(
            "⬇️ Xuất CSV",
            (df.to_csv(index=False).encode("utf-8") if not df.empty else "".encode("utf-8")),
            file_name=f"thu_chi_{fr}_{to}.csv",
            mime="text/csv",
            use_container_width=True
        )

# ---------- Cập nhật router để bật 'Doanh thu' ----------
def router():
    _ensure_session_defaults()
    conn = get_conn()
    user = require_login(conn)
    header_top(conn, user)
    menu = sidebar_menu(conn, user)

    if menu == "Dashboard":
        page_dashboard(conn, user)
    elif menu == "Danh mục":
        page_danhmuc(conn, user)
    elif menu == "Cửa hàng":
        page_cuahang(conn, user)
    elif menu == "Người dùng":
        page_nguoidung(conn, user)
    elif menu == "Kho":
        page_kho(conn, user)
    elif menu == "Sản xuất":
        page_sanxuat(conn, user)
    elif menu == "Báo cáo":
        page_baocao(conn, user)
    elif menu == "TSCD":
        page_tscd(conn, user)
    elif menu == "Doanh thu":
        page_doanhthu(conn, user)
    elif menu == "Nhật ký":
        if has_perm(user, "AUDIT_VIEW"):
            df = fetch_df(conn, "SELECT ts,actor,action,detail FROM syslog ORDER BY ts DESC LIMIT 300")
            st.markdown("### 🗒️ Nhật ký hệ thống")
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.warning("Bạn không có quyền xem nhật ký.")

