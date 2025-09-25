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

# ------------------- PLACEHOLDER ROUTES (sẽ viết ở Phần 2–5) -------------------
def route_part2_placeholder(menu: str):
    if menu == "Dashboard":
        st.info("Dashboard sẽ được hoàn thiện ở **Phần 2**.")
    elif menu == "Danh mục":
        st.info("Danh mục (Sản phẩm, Danh mục, Công thức) sẽ có ở **Phần 2**.")
    elif menu == "Cửa hàng":
        st.info("Quản lý cửa hàng (CRUD) sẽ nằm ở **Phần 2**.")
    elif menu == "Người dùng":
        st.info("Quản lý người dùng (CRUD + phân quyền) sẽ nằm ở **Phần 2**.")

def route_part3_placeholder(menu: str):
    if menu == "Kho":
        st.info("Kho (Nhập/Xuất/Kiểm kê) + Tồn số **cốc** → ở **Phần 3**.")
    elif menu == "Sản xuất":
        st.info("Sản xuất **CỐT** (1 bước) & **MỨT** (2 bước) → ở **Phần 3**.")

def route_part4_placeholder(menu: str):
    if menu == "Báo cáo":
        st.info("Báo cáo Tồn kho/Trị giá, Tài chính (BCKQKD, CĐKT, LCTT) → ở **Phần 4**.")
    elif menu == "TSCD":
        st.info("Tài sản cố định (thêm/sửa/xóa, khấu hao, báo cáo) → ở **Phần 4**.")

def route_part5_placeholder(menu: str):
    if menu == "Doanh thu":
        st.info("Doanh thu (CASH/BANK), xuất Excel/PDF, tra cứu… → ở **Phần 5**.")

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
