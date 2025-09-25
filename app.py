
# ============================================================
# app.py — Phần 1/5: Hạ tầng & Giao diện khung (Postgres only)
# ============================================================
import os, re, json, hashlib
from datetime import datetime, date, timedelta
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

# ------------------- CẤU HÌNH TRANG -------------------
st.set_page_config(
    page_title="Fruit Tea ERP v5",
    page_icon="🍵",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ------------------- KẾT NỐI POSTGRES -------------------
_ENGINE = None

def _normalize_pg_url(url: str) -> str:
    # Cho phép cả postgres:// và postgresql:// ; ép dùng psycopg2 + SSL
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif url.startswith("postgresql://") and "+psycopg2" not in url:
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    if "sslmode=" not in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}sslmode=require"
    return url

def get_conn() -> Connection:
    """Kết nối Postgres qua SQLAlchemy (pool_pre_ping)."""
    global _ENGINE
    pg_url = os.getenv("DATABASE_URL", "").strip()
    if not pg_url:
        st.error("❌ DATABASE_URL chưa được cấu hình trong biến môi trường.")
        st.stop()
    if _ENGINE is None:
        _ENGINE = create_engine(_normalize_pg_url(pg_url), pool_pre_ping=True, future=True)
    return _ENGINE.connect()

# ------------------- HELPER SQL -------------------
def _qmark_to_named(sql: str, params):
    """Chuyển ? -> :p1, :p2 khi gọi bằng tuple/list (tương thích code cũ)."""
    idx = 1
    def repl(_):
        nonlocal idx
        s = f":p{idx}"; idx += 1
        return s
    sql_named = re.sub(r"\?", repl, sql)
    named = {f"p{i+1}": v for i, v in enumerate(params)}
    return sql_named, named

def run_sql(conn: Connection, sql: str, params=None):
    """Execute + commit an toàn, hỗ trợ cả ?-params lẫn dict-params."""
    if isinstance(params, (list, tuple)):
        sql, params = _qmark_to_named(sql, params)
    res = conn.execute(text(sql), params or {})
    try: conn.commit()
    except Exception: pass
    return res

def fetch_df(conn: Connection, sql: str, params=None) -> pd.DataFrame:
    """read_sql_query với text() + params chuẩn hóa."""
    if isinstance(params, (list, tuple)):
        sql, params = _qmark_to_named(sql, params)
    return pd.read_sql_query(text(sql), conn, params=params or {})

# ------------------- AUTH & PHÂN QUYỀN -------------------
def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def has_perm(user: dict, perm: str) -> bool:
    """SuperAdmin luôn full quyền; còn lại check 'perms' CSV."""
    if not user: return False
    if user.get("role") == "SuperAdmin": return True
    return perm in (user.get("perms") or "").split(",")

def write_audit(conn: Connection, action: str, detail: str = "", ip: str = ""):
    """Nhật ký hệ thống (bảng audit_log)."""
    try:
        run_sql(conn,
            "INSERT INTO audit_log(ts,actor,action,detail,ip) VALUES (NOW(),:u,:a,:d,:ip)",
            {"u": st.session_state.get("user",{}).get("email","anonymous"),
             "a": action, "d": (detail or "")[:1000], "ip": ip or ""})
    except Exception:
        pass

# ------------------- LOGIN / LOGOUT -------------------
def login_form(conn: Connection):
    st.markdown("### 🔐 Đăng nhập hệ thống")
    email = st.text_input("Email", key="login_email")
    pw = st.text_input("Mật khẩu", type="password", key="login_pw")
    if st.button("Đăng nhập", type="primary", use_container_width=True):
        df = fetch_df(conn,
            "SELECT email,display,password,role,store_code,perms FROM users WHERE email=:e",
            {"e": email})
        if df.empty:
            st.error("Sai tài khoản hoặc mật khẩu."); return
        row = df.iloc[0]
        if row["password"] != sha256(pw):
            st.error("Sai tài khoản hoặc mật khẩu."); return

        user = {
            "email": row["email"],
            "display": row["display"] or row["email"],
            "role": row["role"] or "User",
            "perms": row["perms"] or "",
            "store": row["store_code"] or ""
        }
        st.session_state["user"] = user
        if user["store"]:
            st.session_state["store"] = user["store"]
        write_audit(conn, "LOGIN", user["email"])
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

# ------------------- HEADER (đổi mật khẩu, chọn cửa hàng, logout) -------------------
def _store_selector(conn: Connection, user: dict):
    """Chọn/đổi cửa hàng (SuperAdmin hoặc có quyền STORE_SWITCH)."""
    try:
        df = fetch_df(conn, "SELECT code,name FROM stores WHERE active=TRUE ORDER BY code")
        options = df["code"].tolist() if not df.empty else []
        current = st.session_state.get("store", user.get("store",""))
        if user.get("role")=="SuperAdmin" or has_perm(user,"STORE_SWITCH"):
            sel = st.selectbox("Cửa hàng", options, index=(options.index(current) if current in options else 0))
            if sel and sel != current:
                st.session_state["store"] = sel
                write_audit(conn, "STORE_SWITCH", sel)
                st.rerun()
        else:
            if current:
                st.caption(f"Cửa hàng: **{current}**")
            else:
                st.caption("Chưa gán cửa hàng.")
    except Exception:
        pass

def header_top(conn: Connection, user: dict):
    col1, col2 = st.columns([0.70, 0.30])
    with col1:
        st.markdown("## 🍵 Fruit Tea ERP v5")
        _store_selector(conn, user)
    with col2:
        with st.popover(f"👤 {user.get('display','')}"):
            st.caption(user.get("email",""))
            st.markdown("---")
            st.markdown("**Đổi mật khẩu**")
            with st.form("form_pwd", clear_on_submit=True):
                old = st.text_input("Mật khẩu cũ", type="password")
                new1 = st.text_input("Mật khẩu mới", type="password")
                new2 = st.text_input("Xác nhận", type="password")
                ok = st.form_submit_button("Cập nhật")
            if ok:
                df = fetch_df(conn,"SELECT password FROM users WHERE email=:e",{"e":user["email"]})
                if df.empty or df.iloc[0]["password"]!=sha256(old):
                    st.error("Mật khẩu cũ không đúng.")
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

# ------------------- ROUTER (khung) -------------------
def _call_if_exist(name: str, *args, **kwargs):
    """Gọi hàm router phần dưới nếu đã được định nghĩa."""
    fn = globals().get(name)
    if callable(fn):
        return fn(*args, **kwargs)
    return None

def router():
    conn = get_conn()
    user = require_login(conn)
    header_top(conn, user)

    st.sidebar.markdown("## 📌 Menu")
    menu = st.sidebar.radio(
        "Chọn chức năng",
        [
            "Dashboard",
            "Danh mục",
            "Kho",
            "Sản xuất",
            "Lịch sử lô",
            "Doanh thu",
            "Báo cáo",
            "TSCD",
            "Nhật ký",
            "Cửa hàng",
            "Người dùng",
        ],
        index=0
    )
    st.sidebar.divider()
    st.sidebar.caption("DB: Postgres (Supabase)")

    # Điều hướng: nếu các phần sau đã dán code, sẽ gọi; nếu chưa, hiện placeholder
    if menu == "Dashboard":
        # Giao cho phần 2 nếu có
        if _call_if_exist("route_part2", menu, conn) is None:
            st.info("Dashboard sẽ làm ở Phần 2.")
    elif menu == "Danh mục":
        if _call_if_exist("route_part2", menu, conn) is None:
            st.info("Danh mục sẽ làm ở Phần 2.")
    elif menu in ("Kho", "Sản xuất", "Lịch sử lô"):
        if _call_if_exist("route_part3", menu, conn) is None:
            st.info("Kho/Sản xuất/Lịch sử lô sẽ làm ở Phần 3.")
    elif menu in ("Báo cáo", "TSCD"):
        if _call_if_exist("route_part4", menu, conn) is None:
            st.info("Báo cáo & TSCD sẽ làm ở Phần 4.")
    elif menu in ("Doanh thu", "Nhật ký", "Cửa hàng"):
        if _call_if_exist("route_part5", menu, conn) is None:
            st.info("Doanh thu/Nhật ký/Cửa hàng sẽ làm ở Phần 5.")
    elif menu == "Người dùng":
        if _call_if_exist("route_part2", menu, conn) is None:
            st.info("Người dùng (CRUD & phân quyền) sẽ làm ở Phần 2.")

# ------------------- ENTRY -------------------
if __name__ == "__main__":
    router()
# app.py — PHẦN 2/5: Danh mục (Cửa hàng / Người dùng / Sản phẩm)
# ============================================================

# ------------------- CỬA HÀNG -------------------
def page_cuahang(conn):
    user = st.session_state.get("user", {})
    if not (user and (user.get("role") == "SuperAdmin" or has_perm(user, "STORES"))):
        st.warning("⛔ Bạn không có quyền quản lý Cửa hàng."); return

    st.header("🏬 Cửa hàng")
    box = st.container()
    with box:
        c1, c2 = st.columns([2, 1])
        with c1:
            tukhoa = st.text_input("Tìm kiếm (mã hoặc tên)", key="store_kw")
        with c2:
            st.caption("Tạo / sửa / xóa – theo mã cửa hàng")

        df = fetch_df(conn, "SELECT code, name, COALESCE(address,'') AS address, COALESCE(note,'') AS note FROM stores ORDER BY code")
        if tukhoa:
            k = tukhoa.lower()
            df = df[df.apply(lambda r: k in str(r["code"]).lower() or k in str(r["name"]).lower(), axis=1)]
        st.dataframe(df, use_container_width=True, height=320)

        st.markdown("#### ➕ Thêm mới / ✏️ Sửa")
        with st.form("store_form", clear_on_submit=False):
            cc1, cc2 = st.columns([1, 2])
            with cc1:
                code = st.text_input("Mã cửa hàng*", key="store_code").strip().upper()
                name = st.text_input("Tên cửa hàng*", key="store_name").strip()
            with cc2:
                address = st.text_input("Địa chỉ", key="store_address")
                note = st.text_input("Ghi chú", key="store_note")
            ok = st.form_submit_button("💾 Lưu (Upsert)")
        if ok:
            if not code or not name:
                st.error("⚠️ Mã & Tên là bắt buộc.")
            else:
                run_sql(conn, """
                    INSERT INTO stores(code,name,address,note)
                    VALUES(:c,:n,:a,:no)
                    ON CONFLICT (code) DO UPDATE
                      SET name=EXCLUDED.name, address=EXCLUDED.address, note=EXCLUDED.note
                """, {"c": code, "n": name, "a": address, "no": note})
                write_audit(conn, "CUAHANG_UPSERT", f"{code}")
                st.success("✅ Đã lưu cửa hàng.")
                st.rerun()

        st.markdown("#### 🗑️ Xóa")
        if not df.empty:
            del_code = st.selectbox("Chọn cửa hàng cần xóa", [""] + df["code"].tolist(), key="store_del_pick")
            if st.button("Xóa cửa hàng", key="store_del_btn"):
                if not del_code:
                    st.warning("Chọn mã trước khi xóa.")
                else:
                    try:
                        run_sql(conn, "DELETE FROM stores WHERE code=:c", {"c": del_code})
                        write_audit(conn, "CUAHANG_DELETE", del_code)
                        st.success("Đã xóa.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Không thể xóa (vì có dữ liệu liên quan): {e}")

        st.markdown("#### ✅ Đặt làm cửa hàng đang dùng")
        if not df.empty:
            act = st.selectbox("Chọn cửa hàng", df["code"], key="store_active_pick")
            if st.button("Đặt làm cửa hàng đang dùng", key="store_set_active"):
                st.session_state["store"] = act
                write_audit(conn, "CUAHANG_SET_ACTIVE", act)
                st.success(f"Đang làm việc tại: **{act}**")

        if not df.empty:
            st.download_button(
                "⬇️ Xuất CSV",
                df.to_csv(index=False).encode("utf-8"),
                file_name="cuahang.csv",
                mime="text/csv",
                key="store_export"
            )

# ------------------- NGƯỜI DÙNG & QUYỀN -------------------
def page_nguoidung(conn):
    user = st.session_state.get("user", {})
    if not (user and (user.get("role") == "SuperAdmin" or has_perm(user, "USERS"))):
        st.warning("⛔ Bạn không có quyền quản lý Người dùng."); return

    st.header("👥 Người dùng & Quyền")

    c1, c2 = st.columns([2,1])
    with c1:
        kw = st.text_input("Tìm email / tên hiển thị", key="usr_kw")
    with c2:
        st.caption("Thêm / sửa / xóa, gán quyền chi tiết (CSV)")

    df = fetch_df(conn, "SELECT email, display, role, store_code, COALESCE(perms,'') AS perms FROM users ORDER BY email")
    if kw:
        k = kw.lower()
        df = df[df.apply(lambda r: k in str(r["email"]).lower() or k in str(r["display"]).lower(), axis=1)]
    st.dataframe(df, use_container_width=True, height=320)

    st.markdown("#### ➕ Thêm / ✏️ Sửa (Upsert)")
    stores = fetch_df(conn, "SELECT code FROM stores ORDER BY code")
    store_opts = stores["code"].tolist() if not stores.empty else ["HOSEN"]
    with st.form("user_form", clear_on_submit=False):
        u1, u2, u3 = st.columns([1.5, 1, 1])
        with u1:
            email = st.text_input("Email*", key="usr_email").strip()
            display = st.text_input("Tên hiển thị", key="usr_display").strip()
        with u2:
            role = st.selectbox("Vai trò", ["SuperAdmin", "admin", "user"], key="usr_role", index=2)
            store_code = st.selectbox("Cửa hàng mặc định", store_opts, key="usr_store")
        with u3:
            pwd = st.text_input("Mật khẩu (để trống = giữ nguyên nếu đã tồn tại)", type="password", key="usr_pwd")
            perms = st.text_area("Quyền (CSV)", key="usr_perms",
                                 placeholder="VD: STORES,PRODUCTS,WAREHOUSE,PRODUCTION,REVENUE,REPORTS,ASSETS,USERS,FORMULAS,AUDIT_VIEW",
                                 height=70)
        ok = st.form_submit_button("💾 Lưu người dùng")
    if ok:
        if not email:
            st.error("⚠️ Email bắt buộc.")
        else:
            exists = fetch_df(conn, "SELECT email, password FROM users WHERE email=:e", {"e": email})
            if exists.empty:
                # tạo mới
                if not pwd:
                    st.error("Tạo mới cần nhập mật khẩu."); st.stop()
                run_sql(conn, """
                    INSERT INTO users(email,display,password,role,store_code,perms)
                    VALUES(:e,:d,:p,:r,:s,:m)
                """, {"e": email, "d": (display or email), "p": sha256(pwd), "r": role,
                      "s": store_code, "m": (perms or "")})
            else:
                # cập nhật
                if pwd:
                    run_sql(conn, """
                        UPDATE users SET display=:d, password=:p, role=:r, store_code=:s, perms=:m
                        WHERE email=:e
                    """, {"e": email, "d": (display or email), "p": sha256(pwd), "r": role,
                          "s": store_code, "m": (perms or "")})
                else:
                    run_sql(conn, """
                        UPDATE users SET display=:d, role=:r, store_code=:s, perms=:m
                        WHERE email=:e
                    """, {"e": email, "d": (display or email), "r": role, "s": store_code, "m": (perms or "")})
            write_audit(conn, "USER_UPSERT", email)
            st.success("✅ Đã lưu người dùng.")
            st.rerun()

    st.markdown("#### 🔑 Đổi mật khẩu (quản trị)")
    with st.form("user_pwd_form", clear_on_submit=True):
        target = st.text_input("Email người dùng", key="usr_pwd_email").strip()
        newp = st.text_input("Mật khẩu mới", type="password", key="usr_pwd_new")
        ok2 = st.form_submit_button("Cập nhật mật khẩu")
    if ok2:
        if not target or not newp:
            st.error("Email / Mật khẩu mới không được để trống.")
        else:
            run_sql(conn, "UPDATE users SET password=:p WHERE email=:e", {"p": sha256(newp), "e": target})
            write_audit(conn, "USER_ADMIN_CHANGE_PWD", target)
            st.success("Đã đổi mật khẩu.")

    st.markdown("#### 🗑️ Xóa người dùng")
    if not df.empty:
        del_u = st.selectbox("Chọn email cần xóa", [""] + df["email"].tolist(), key="usr_del_pick")
        if st.button("Xóa người dùng", key="usr_del_btn"):
            if not del_u:
                st.warning("Chọn email trước khi xóa.")
            else:
                run_sql(conn, "DELETE FROM users WHERE email=:e", {"e": del_u})
                write_audit(conn, "USER_DELETE", del_u)
                st.success("Đã xóa.")
                st.rerun()

# ------------------- SẢN PHẨM -------------------
CAT_CHOICES = ["TRAI_CAY", "PHU_GIA", "COT", "MUT", "KHAC"]

def page_sanpham(conn):
    user = st.session_state.get("user", {})
    if not (user and (user.get("role") == "SuperAdmin" or has_perm(user, "PRODUCTS"))):
        st.warning("⛔ Bạn không có quyền quản lý Sản phẩm."); return

    st.header("📦 Sản phẩm")

    f1, f2, f3 = st.columns([1.2, 1, 1.8])
    with f1:
        cat = st.selectbox("Nhóm", ["TẤT CẢ"] + CAT_CHOICES, key="prod_cat")
    with f2:
        kw = st.text_input("Lọc (mã / tên)", key="prod_kw")
    with f3:
        st.caption("COT/MUT có thêm số cốc ở các báo cáo/kho theo công thức.")

    df = fetch_df(conn, "SELECT code, name, uom, cat_code FROM products ORDER BY code")
    if cat != "TẤT CẢ":
        df = df[df["cat_code"] == cat]
    if kw:
        k = kw.lower()
        df = df[df.apply(lambda r: k in str(r["code"]).lower() or k in str(r["name"]).lower(), axis=1)]
    st.dataframe(df, use_container_width=True, height=340)

    st.markdown("#### ➕ Thêm / ✏️ Sửa (Upsert)")
    with st.form("prod_form", clear_on_submit=False):
        p1, p2, p3, p4 = st.columns([1.2, 2, 1, 1])
        with p1:
            code = st.text_input("Mã SP*", key="prod_code").strip().upper()
        with p2:
            name = st.text_input("Tên SP*", key="prod_name").strip()
        with p3:
            uom = st.text_input("ĐVT*", value="kg", key="prod_uom").strip()
        with p4:
            cat2 = st.selectbox("Nhóm*", CAT_CHOICES, key="prod_cat2")
        ok = st.form_submit_button("💾 Lưu SP")
    if ok:
        if not code or not name or not uom:
            st.error("⚠️ Mã / Tên / ĐVT là bắt buộc.")
        else:
            run_sql(conn, """
                INSERT INTO products(code,name,uom,cat_code)
                VALUES(:c,:n,:u,:cat)
                ON CONFLICT (code) DO UPDATE
                  SET name=EXCLUDED.name, uom=EXCLUDED.uom, cat_code=EXCLUDED.cat_code
            """, {"c": code, "n": name, "u": uom, "cat": cat2})
            write_audit(conn, "PRODUCT_UPSERT", code)
            st.success("✅ Đã lưu sản phẩm.")
            st.rerun()

    st.markdown("#### 🗑️ Xóa")
    if not df.empty:
        del_p = st.selectbox("Chọn SP cần xóa", [""] + df["code"].tolist(), key="prod_del_pick")
        if st.button("Xóa SP", key="prod_del_btn"):
            if not del_p:
                st.warning("Chọn mã trước khi xóa.")
            else:
                run_sql(conn, "DELETE FROM products WHERE code=:c", {"c": del_p})
                write_audit(conn, "PRODUCT_DELETE", del_p)
                st.success("Đã xóa.")
                st.rerun()

    if not df.empty:
        st.download_button(
            "⬇️ Xuất CSV",
            df.to_csv(index=False).encode("utf-8"),
            file_name="san_pham.csv",
            mime="text/csv",
            key="prod_export"
        )

# ------------------- DANH MỤC (gộp 3 tab) -------------------
def page_danhmuc(conn):
    st.header("📚 Danh mục")
    tabs = st.tabs(["🏬 Cửa hàng", "👥 Người dùng & Quyền", "📦 Sản phẩm"])
    with tabs[0]: page_cuahang(conn)
    with tabs[1]: page_nguoidung(conn)
    with tabs[2]: page_sanpham(conn)

# ------------------- Router gắn các trang Danh mục -------------------
def route_part2(menu, conn):
    if menu == "Danh mục":
        page_danhmuc(conn)
    elif menu == "Cửa hàng":
        page_cuahang(conn)
    elif menu == "Người dùng":
        page_nguoidung(conn)
# ============================================================
# app.py — PHẦN 3/5: KHO (Nhập/Xuất/Tồn/ Kiểm kê) + SẢN XUẤT + Lịch sử lô
# ============================================================

from datetime import date, datetime, timedelta

# -------------------- Helpers chung (Kho/SX) --------------------
REASONS_OUT = ["BÁN LẺ","BÁN SỈ","MẪU DÙNG THỬ","HỎNG/MẤT","ĐIỀU CHUYỂN","KHÁC…"]

def cups_per_kg_of(conn, output_pcode: str) -> float:
    """Số cốc / 1kg theo công thức có TP = output_pcode (ưu tiên bản mới nhất)."""
    df = fetch_df(conn, """
        SELECT cups_per_kg FROM formulas
        WHERE output_pcode=:o
        ORDER BY code DESC
        LIMIT 1
    """, {"o": output_pcode})
    if df.empty: return 0.0
    try: return float(df.iloc[0]["cups_per_kg"] or 0.0)
    except: return 0.0

def product_picker(conn, key_prefix: str, label="Chọn sản phẩm", cats: list[str]|None=None):
    """Select sản phẩm có ô tìm kiếm; có thể lọc theo nhóm `cats`."""
    kw = st.text_input("Tìm (mã/tên)", key=f"{key_prefix}_kw", placeholder="Gõ vài ký tự…")
    if cats:
        df = fetch_df(conn, "SELECT code,name,uom,cat_code FROM products WHERE cat_code = ANY(:c) ORDER BY code", {"c": cats})
    else:
        df = fetch_df(conn, "SELECT code,name,uom,cat_code FROM products ORDER BY code")
    if kw:
        k = kw.lower()
        df = df[df.apply(lambda r: k in str(r["code"]).lower() or k in str(r["name"]).lower(), axis=1)]
    codes = df["code"].tolist() if not df.empty else []
    def fmt(x):
        if not x or df.empty or x not in df["code"].values: return x or ""
        r = df.set_index("code").loc[x]
        return f"{x} — {r['name']} ({r['uom']}, {r['cat_code']})"
    sel = st.selectbox(label, [""]+codes, format_func=lambda x: "" if not x else fmt(x), key=f"{key_prefix}_pick")
    return sel, (df.set_index("code").loc[sel].to_dict() if sel else None)

def post_ledger(conn, store, pcode, kind, qty, price_in, cups, ref, note):
    run_sql(conn, """
        INSERT INTO inventory_ledger(ts,store,pcode,kind,qty,price_in,cups,ref,note)
        VALUES(NOW(),:s,:p,:k,:q,:pr,:c,:r,:n)
    """, {"s": store, "p": pcode, "k": kind, "q": float(q or 0), "pr": float(price_in or 0),
          "c": float(cups or 0), "r": ref or "", "n": note or ""})

def avg_cost(conn, store, pcode):
    """Giá bình quân di động từ sổ kho."""
    dfc = fetch_df(conn, """
        SELECT kind, qty, price_in FROM inventory_ledger
        WHERE store=:s AND pcode=:p
        ORDER BY ts, id
    """, {"s": store, "p": pcode})
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
    return float(cost)

def onhand(conn, store: str, pcode: str) -> float:
    """Tồn hiện tại của 1 mã ở cửa hàng."""
    df = fetch_df(conn, """
        SELECT COALESCE(SUM(CASE WHEN kind='IN' THEN qty ELSE -qty END),0) AS oh
        FROM inventory_ledger WHERE store=:s AND pcode=:p
    """, {"s": store, "p": pcode})
    return float(df.iloc[0]["oh"] if not df.empty else 0.0)

def ensure_can_out(conn, store: str, items: list[tuple[str, float]]) -> tuple[bool, pd.DataFrame]:
    """
    items: [(pcode, qty_out), ...]  → check chống xuất âm.
    Trả về (ok, df_thieu). ok=False nếu có mã bị thiếu.
    """
    rows = []
    for p, q in items:
        oh = onhand(conn, store, p)
        thieu = max(0.0, float(q or 0) - oh)
        rows.append({"pcode": p, "tồn_hiện_tại": oh, "yêu_cầu_xuất": float(q or 0), "thiếu": thieu})
    df = pd.DataFrame(rows)
    ok = df["thiếu"].max() <= 1e-9 if not df.empty else True
    return ok, df

def stock_snapshot(conn, store, to_date: date):
    """Ảnh tồn đến ngày `to_date` (SL, số cốc, trị giá BQ)."""
    df = fetch_df(conn, """
        WITH m AS (
          SELECT p.code, p.name, p.uom, p.cat_code,
                 COALESCE(SUM(CASE WHEN l.kind='IN' THEN l.qty ELSE -l.qty END),0) AS ton_qty,
                 COALESCE(SUM(l.cups),0) AS ton_cups
          FROM products p
          LEFT JOIN inventory_ledger l
            ON p.code=l.pcode AND l.store=:s AND l.ts::date<=:d
          GROUP BY p.code,p.name,p.uom,p.cat_code
        )
        SELECT * FROM m ORDER BY code
    """, {"s": store, "d": to_date.strftime("%Y-%m-%d")})
    if df.empty: return df
    df["avg_cost"] = df["code"].apply(lambda c: avg_cost(conn, store, c))
    df["value"] = (df["ton_qty"].astype(float) * df["avg_cost"].astype(float)).astype(float)
    return df

def _raws_from_formula(row):
    raws = [x for x in (row.get("fruits_csv") or "").split(",") if x]
    try:
        adds = json.loads(row.get("additives_json") or "{}")
    except:
        adds = {}
    return raws, adds

def _avg_cost_from_list(conn, store, p_list: list[str]) -> float:
    vals = [avg_cost(conn, store, p) for p in (p_list or [])]
    vals = [v for v in vals if v>0]
    return float(sum(vals)/len(vals)) if vals else 0.0

# -------------------- KHO (nhập/xuất/kiểm kê/tồn) --------------------
def page_kho(conn):
    st.header(f"📦 Kho — {st.session_state.get('store','(chưa chọn)')}")
    store = st.session_state.get("store", "")
    tab_in, tab_out, tab_stock, tab_check = st.tabs(["Phiếu nhập", "Phiếu xuất", "Tồn kho", "Kiểm kê nâng cao"])

    # ===== Phiếu nhập =====
    with tab_in:
        st.subheader("Phiếu nhập (bắt buộc có đơn giá)")
        p, info = product_picker(conn, "in", "Sản phẩm nhập")
        c1, c2, c3 = st.columns([1,1,2])
        with c1:
            qty = st.number_input("Số lượng", min_value=0.0, step=0.1, key="in_qty")
        with c2:
            price = st.number_input("Đơn giá nhập (VND/ĐVT)", min_value=0.0, step=100.0, key="in_price")
        with c3:
            note = st.text_input("Ghi chú", key="in_note", placeholder="Số HĐ, NCC…")
        cups_in = 0.0
        if p and info and info.get("cat_code") in ("COT","MUT"):
            cups_in = qty * cups_per_kg_of(conn, p)
            st.caption(f"Số cốc dự kiến ghi: **{cups_in:.0f}**")
        if st.button("💾 Lưu phiếu nhập", key="in_save"):
            if not p or qty<=0 or price<=0:
                st.error("⚠️ Chọn sản phẩm + số lượng > 0 + đơn giá > 0.")
            else:
                post_ledger(conn, store, p, "IN", qty, price, cups_in, ref="NHAP_TAY", note=note)
                write_audit(conn, "KHO_NHAP", f"{p} {qty}@{price}")
                st.success("✅ Đã lưu nhập kho.")
                st.rerun()

    # ===== Phiếu xuất =====
    with tab_out:
        st.subheader("Phiếu xuất (bắt buộc có lý do) — CHẶN XUẤT ÂM")
        p2, info2 = product_picker(conn, "out", "Sản phẩm xuất")
        c1, c2 = st.columns([1,2])
        with c1:
            qty2 = st.number_input("Số lượng", min_value=0.0, step=0.1, key="out_qty")
        with c2:
            reason = st.selectbox("Lý do xuất", REASONS_OUT, key="out_reason")
            reason_note = st.text_input("Ghi chú", key="out_note")
        if st.button("💾 Lưu phiếu xuất", key="out_save"):
            if not p2 or qty2<=0 or not reason:
                st.error("⚠️ Chọn sản phẩm + số lượng > 0 + lý do.")
            else:
                ok, df_chk = ensure_can_out(conn, store, [(p2, qty2)])
                if not ok:
                    st.error("❌ Không đủ tồn để xuất. Vui lòng kiểm tra:")
                    st.dataframe(df_chk, use_container_width=True)
                else:
                    cost = avg_cost(conn, store, p2)
                    cups_out = 0.0
                    if info2 and info2.get("cat_code") in ("COT","MUT"):
                        cups_out = qty2 * cups_per_kg_of(conn, p2)
                    post_ledger(conn, store, p2, "OUT", qty2, cost, cups_out, ref=f"XUAT:{reason}", note=reason_note)
                    write_audit(conn, "KHO_XUAT", f"{p2} {qty2} ({reason})")
                    st.success("✅ Đã lưu xuất kho.")
                    st.rerun()

    # ===== Tồn kho =====
    with tab_stock:
        st.subheader("Báo cáo tồn kho")
        # Chỉ lọc khi bấm nút
        with st.expander("🔎 Bộ lọc (chỉ áp khi bấm **Áp dụng**)", expanded=False):
            to_date = st.date_input("Chốt đến ngày", value=date.today(), key="ton_to")
            catf = st.selectbox("Nhóm", ["TẤT CẢ","TRAI_CÂY","PHU_GIA","COT","MUT","KHAC"], key="ton_cat")
            kwf = st.text_input("Mã/Tên chứa…", key="ton_kw")
            apply = st.button("Áp dụng", key="ton_apply")
        df_ton = stock_snapshot(conn, store, to_date)
        if apply:
            if catf!="TẤT CẢ": df_ton = df_ton[df_ton["cat_code"]==catf]
            if kwf:
                k = kwf.lower()
                df_ton = df_ton[df_ton.apply(lambda r: k in str(r["code"]).lower() or k in str(r["name"]).lower(), axis=1)]
        if not df_ton.empty:
            df_show = df_ton.rename(columns={"code":"Mã","name":"Tên","uom":"ĐVT","cat_code":"Nhóm",
                                             "ton_qty":"Tồn (kg)","ton_cups":"Số cốc",
                                             "avg_cost":"Đơn giá BQ","value":"Giá trị tồn (VND)"})
        else:
            df_show = df_ton
        st.dataframe(df_show, use_container_width=True, height=380)
        if not df_ton.empty:
            c1,c2,c3 = st.columns(3)
            c1.metric("Tổng trị giá", f"{df_ton['value'].sum():,.0f} VND")
            c2.metric("Tổng số lượng", f"{df_ton['ton_qty'].sum():,.2f} kg")
            c3.metric("Tổng số cốc", f"{df_ton['ton_cups'].sum():,.0f}")
            st.download_button("⬇️ Xuất CSV", df_show.to_csv(index=False).encode("utf-8"),
                               file_name=f"ton_kho_{to_date}.csv", mime="text/csv")

    # ===== Kiểm kê nâng cao =====
    with tab_check:
        st.subheader("Kiểm kê kho (nâng cao)")
        # Chọn SP để kiểm kê
        pkk, _ = product_picker(conn, "kk", "Chọn SP kiểm kê")
        if pkk:
            snap = stock_snapshot(conn, store, date.today())
            ton_now = float(snap.set_index("code").loc[pkk,"ton_qty"]) if not snap.empty and pkk in snap["code"].values else 0.0
            st.caption(f"Tồn hệ thống hiện tại: **{ton_now:.2f}**")
            real = st.number_input("Số lượng thực tế", min_value=0.0, step=0.1, key="kk_real")
            note_kk = st.text_input("Ghi chú kiểm kê", key="kk_note")
            if st.button("📋 Xem chênh lệch", key="kk_preview"):
                delta = real - ton_now
                st.info(f"Chênh lệch: **{delta:+.2f}**  (dương → nhập điều chỉnh; âm → xuất điều chỉnh)")
            if st.button("✅ Ghi sổ kiểm kê", key="kk_commit"):
                delta = real - ton_now
                if abs(delta) < 1e-9:
                    st.success("Không có chênh lệch.")
                else:
                    kind = "IN" if delta>0 else "OUT"
                    cost = avg_cost(conn, store, pkk)
                    post_ledger(conn, store, pkk, kind, abs(delta), cost, 0.0, ref="KIEMKE", note=note_kk or "KIEM_KE")
                    write_audit(conn, "KHO_KIEMKE", f"{pkk} diff={delta}")
                    st.success("✅ Đã ghi điều chỉnh kiểm kê.")
                    st.rerun()

# -------------------- SẢN XUẤT (CỐT 1 bước, MỨT 2 bước) --------------------
def page_sanxuat(conn):
    st.header("🏭 Sản xuất")
    store = st.session_state.get("store","")
    t_cot, t_mut_tc, t_mut_ct = st.tabs(["CỐT (1 bước)","MỨT từ Trái cây (2 bước)","MỨT từ Cốt (2 bước)"])

    # === CỐT: 1 bước (HS thu hồi) ===
    with t_cot:
        st.subheader("CỐT (1 bước) — có hệ số thu hồi")
        df_ct = fetch_df(conn, "SELECT * FROM formulas WHERE type='COT' ORDER BY code")
        ct = st.selectbox("Công thức CỐT", df_ct["code"].tolist() if not df_ct.empty else [], key="cot_ct")
        if ct:
            row = df_ct[df_ct["code"]==ct].iloc[0].to_dict()
            outp = row["output_pcode"]; rec = float(row["recovery"] or 1.0); cups_kg = float(row["cups_per_kg"] or 0.0)
            raws, adds = _raws_from_formula(row)
            c1,c2 = st.columns(2)
            with c1:
                kg_in = st.number_input("KG sau sơ chế (đầu vào)", min_value=0.0, step=0.1, key="cot_in")
                lot = st.text_input("Mã lô", value=f"COT-{ct}-{datetime.now():%y%m%d%H%M%S}", key="cot_lot")
            with c2:
                kg_out = st.number_input("KG thành phẩm (tính theo HS, có thể sửa)", value=kg_in*rec, step=0.1, key="cot_out")
                st.caption(f"HS thu hồi: **{rec}** • Số cốc/1kg: **{cups_kg}**")
            if st.button("👀 Preview NVL", key="cot_prev"):
                use = {}
                n = max(1,len(raws))
                for r in raws: use[r] = use.get(r,0.0) + kg_in/n
                for k,v in adds.items(): use[k] = use.get(k,0.0) + float(v or 0)*kg_in
                st.json({"lot": lot, "NVL dùng (kg)": use, "TP nhận (kg)": kg_out, "Số cốc nhận": kg_out*cups_kg})
            if st.button("✅ Tạo lô & ghi sổ", key="cot_commit"):
                # CHẶN XUẤT ÂM
                need = []
                n = max(1,len(raws))
                for r in raws: need.append((r, kg_in/n))
                for k,v in adds.items(): need.append((k, float(v or 0)*kg_in))
                ok, df_chk = ensure_can_out(conn, store, need)
                if not ok:
                    st.error("❌ Không đủ tồn để xuất NVL:")
                    st.dataframe(df_chk, use_container_width=True)
                else:
                    for (pc,q) in need:
                        post_ledger(conn, store, pc, "OUT", q, avg_cost(conn,store,pc), 0.0, ref=f"PRD_COT:{lot}", note=f"CT {ct}")
                    unit_cost = _avg_cost_from_list(conn, store, raws)
                    post_ledger(conn, store, outp, "IN", kg_out, unit_cost, kg_out*cups_kg, ref=f"PRD_COT:{lot}", note=f"CT {ct}")
                    write_audit(conn, "PRD_COT", f"{ct} LOT={lot} OUT={kg_out}kg")
                    st.success("✅ Đã ghi sổ sản xuất CỐT.")
                    st.rerun()

    # === MỨT từ Trái cây: 2 bước (KHÔNG có HS) ===
    with t_mut_tc:
        st.subheader("MỨT từ Trái cây (2 bước) — KHÔNG có hệ số thu hồi")
        df_ct = fetch_df(conn, "SELECT * FROM formulas WHERE type='MUT' AND (note LIKE 'SRC=TRÁI_CÂY%' OR note LIKE 'SRC=TRAI_CAY%') ORDER BY code")
        ct = st.selectbox("Công thức MỨT (TRÁI CÂY)", df_ct["code"].tolist() if not df_ct.empty else [], key="mut_tc_ct")
        if ct:
            row = df_ct[df_ct["code"]==ct].iloc[0].to_dict()
            outp = row["output_pcode"]; cups_kg = float(row["cups_per_kg"] or 0.0)
            raws, adds = _raws_from_formula(row)
            c1,c2 = st.columns(2)
            with c1:
                kg_in = st.number_input("KG sau sơ chế (đầu vào)", min_value=0.0, step=0.1, key="mut_tc_in")
                lot = st.text_input("Mã lô", value=f"MUTTC-{ct}-{datetime.now():%y%m%d%H%M%S}", key="mut_tc_lot")
            with c2:
                kg_out = st.number_input("KG thành phẩm (nhập kho)", min_value=0.0, step=0.1, key="mut_tc_out")
                st.caption(f"Số cốc/1kg TP: **{cups_kg}**")
            if st.button("👀 Preview NVL", key="mut_tc_prev"):
                use={}; n=max(1,len(raws))
                for r in raws: use[r]=use.get(r,0.0)+kg_in/n
                for k,v in adds.items(): use[k]=use.get(k,0.0)+float(v or 0)*kg_in
                st.json({"lot": lot, "NVL dùng (kg)": use, "TP nhận (kg)": kg_out, "Số cốc": kg_out*cups_kg})
            if st.button("✅ Tạo lô & ghi sổ", key="mut_tc_commit"):
                need=[]; n=max(1,len(raws))
                for r in raws: need.append((r, kg_in/n))
                for k,v in adds.items(): need.append((k, float(v or 0)*kg_in))
                ok, df_chk = ensure_can_out(conn, store, need)
                if not ok:
                    st.error("❌ Không đủ tồn để xuất NVL:")
                    st.dataframe(df_chk, use_container_width=True)
                else:
                    for (pc,q) in need:
                        post_ledger(conn, store, pc, "OUT", q, avg_cost(conn,store,pc), 0.0, ref=f"PRD_MUT_TC:{lot}", note=f"CT {ct}")
                    unit_cost = _avg_cost_from_list(conn, store, raws)
                    post_ledger(conn, store, outp, "IN", kg_out, unit_cost, kg_out*cups_kg, ref=f"PRD_MUT_TC:{lot}", note=f"CT {ct}")
                    write_audit(conn, "PRD_MUT_TC", f"{ct} LOT={lot} OUT={kg_out}kg")
                    st.success("✅ Đã ghi sổ MỨT (Trái cây).")
                    st.rerun()

    # === MỨT từ Cốt: 2 bước (KHÔNG có HS) ===
    with t_mut_ct:
        st.subheader("MỨT từ Cốt (2 bước) — KHÔNG có hệ số thu hồi")
        df_ct = fetch_df(conn, "SELECT * FROM formulas WHERE type='MUT' AND (note LIKE 'SRC=CỐT%' OR note LIKE 'SRC=COT%') ORDER BY code")
        ct = st.selectbox("Công thức MỨT (CỐT)", df_ct["code"].tolist() if not df_ct.empty else [], key="mut_ct_ct")
        if ct:
            row = df_ct[df_ct["code"]==ct].iloc[0].to_dict()
            outp = row["output_pcode"]; cups_kg = float(row["cups_per_kg"] or 0.0)
            raws, adds = _raws_from_formula(row)   # raws ở đây là danh mục CỐT
            c1,c2 = st.columns(2)
            with c1:
                kg_in = st.number_input("KG CỐT sử dụng", min_value=0.0, step=0.1, key="mut_ct_in")
                lot = st.text_input("Mã lô", value=f"MUTCT-{ct}-{datetime.now():%y%m%d%H%M%S}", key="mut_ct_lot")
            with c2:
                kg_out = st.number_input("KG thành phẩm (nhập kho)", min_value=0.0, step=0.1, key="mut_ct_out")
                st.caption(f"Số cốc/1kg TP: **{cups_kg}**")
            if st.button("👀 Preview NVL", key="mut_ct_prev"):
                use={}; n=max(1,len(raws))
                for r in raws: use[r]=use.get(r,0.0)+kg_in/n
                for k,v in adds.items(): use[k]=use.get(k,0.0)+float(v or 0)*kg_in
                st.json({"lot": lot, "NVL (CỐT) dùng (kg)": use, "TP nhận (kg)": kg_out, "Số cốc": kg_out*cups_kg})
            if st.button("✅ Tạo lô & ghi sổ", key="mut_ct_commit"):
                need=[]; n=max(1,len(raws))
                for r in raws: need.append((r, kg_in/n))
                for k,v in adds.items(): need.append((k, float(v or 0)*kg_in))
                ok, df_chk = ensure_can_out(conn, store, need)
                if not ok:
                    st.error("❌ Không đủ tồn để xuất NVL:")
                    st.dataframe(df_chk, use_container_width=True)
                else:
                    for (pc,q) in need:
                        post_ledger(conn, store, pc, "OUT", q, avg_cost(conn,store,pc), 0.0, ref=f"PRD_MUT_CT:{lot}", note=f"CT {ct}")
                    unit_cost = _avg_cost_from_list(conn, store, raws)
                    post_ledger(conn, store, outp, "IN", kg_out, unit_cost, kg_out*cups_kg, ref=f"PRD_MUT_CT:{lot}", note=f"CT {ct}")
                    write_audit(conn, "PRD_MUT_CT", f"{ct} LOT={lot} OUT={kg_out}kg")
                    st.success("✅ Đã ghi sổ MỨT (Cốt).")
                    st.rerun()

# -------------------- LỊCH SỬ LÔ --------------------
def page_lichsu_lo(conn):
    st.header("📜 Lịch sử lô sản xuất")
    store = st.session_state.get("store","")
    c1,c2,c3,c4 = st.columns(4)
    with c1:
        fr = st.date_input("Từ ngày", value=date.today()-timedelta(days=7), key="lot_fr")
    with c2:
        to = st.date_input("Đến ngày", value=date.today(), key="lot_to")
    with c3:
        loai = st.selectbox("Loại lô", ["TẤT CẢ","CỐT","MỨT_TC","MỨT_CỐT"], key="lot_type")
    with c4:
        kw = st.text_input("Tìm trong mã lô/CT", key="lot_kw", placeholder="vd: CT001, PRD_COT…")

    pat = {
        "TẤT CẢ": "%",
        "CỐT": "PRD_COT:%",
        "MỨT_TC": "PRD_MUT_TC:%",
        "MỨT_CỐT": "PRD_MUT_CT:%"
    }[loai]

    df_ref = fetch_df(conn, """
        SELECT ref,
               MIN(ts)::timestamp(0) AS started,
               MAX(ts)::timestamp(0) AS finished,
               COUNT(*) AS lines
        FROM inventory_ledger
        WHERE store=:s
          AND ref LIKE :pat
          AND ts::date BETWEEN :fr AND :to
        GROUP BY ref
        ORDER BY started DESC
    """, {"s": store, "pat": pat, "fr": fr.strftime("%Y-%m-%d"), "to": to.strftime("%Y-%m-%d")})

    if kw:
        k = kw.lower()
        df_ref = df_ref[df_ref["ref"].str.lower().str.contains(k)]

    st.dataframe(df_ref, use_container_width=True, height=260)

    if not df_ref.empty:
        st.markdown("### Chi tiết từng lô")
        for _, r in df_ref.iterrows():
            with st.expander(f"🔹 {r['ref']} — {r['started']} → {r['finished']} (dòng: {int(r['lines'])})", expanded=False):
                d = fetch_df(conn, """
                    SELECT ts::timestamp(0) AS ts, pcode, kind, qty, price_in, cups, note
                    FROM inventory_ledger
                    WHERE store=:s AND ref=:r
                    ORDER BY ts, id
                """, {"s": store, "r": r["ref"]})
                st.markdown("**NVL đã xuất**")
                st.dataframe(d[d["kind"]=="OUT"][["ts","pcode","qty","price_in","note"]], use_container_width=True)
                st.markdown("**Thành phẩm đã nhập**")
                st.dataframe(d[d["kind"]=="IN"][["ts","pcode","qty","price_in","cups","note"]], use_container_width=True)

        if st.button("⬇️ Xuất toàn bộ lịch sử lô (CSV)", key="lot_export"):
            all_rows = []
            for _, r in df_ref.iterrows():
                d = fetch_df(conn, """
                    SELECT :ref AS ref, ts::timestamp(0) AS ts, pcode, kind, qty, price_in, cups, note
                    FROM inventory_ledger
                    WHERE store=:s AND ref=:r
                    ORDER BY ts, id
                """, {"s": store, "r": r["ref"], "ref": r["ref"]})
                all_rows.append(d)
            full = pd.concat(all_rows) if all_rows else pd.DataFrame()
            st.download_button("Tải CSV", full.to_csv(index=False).encode("utf-8"),
                               file_name=f"lich_su_lo_{fr}_{to}.csv", mime="text/csv")

# -------------------- Router phần 3 --------------------
def route_part3(menu, conn):
    if menu == "Kho":
        page_kho(conn)
    elif menu == "Sản xuất":
        page_sanxuat(conn)
    elif menu == "Lịch sử lô":
        page_lichsu_lo(conn)
# ============================================================
# app.py — PHẦN 4/5: TÀI CHÍNH (BCTC) + TSCĐ + LƯƠNG
# ============================================================

# ----------------------- Helpers Kế toán -----------------------
GL_TYPES = ["ASSET","LIABILITY","EQUITY","INCOME","EXPENSE"]

def acct_map(conn) -> pd.DataFrame:
    return fetch_df(conn, "SELECT code,name,type FROM gl_accounts ORDER BY code")

def tb_period(conn, store: str, fr: date, to: date) -> pd.DataFrame:
    """
    Trial Balance giai đoạn [fr, to] theo cửa hàng.
    gl_entries(dc='D'/'C') —> sum phát sinh Nợ/Có + số dư đầu kỳ, số dư cuối kỳ.
    """
    # Số dư đầu kỳ (đến fr-1)
    df_open = fetch_df(conn, """
        SELECT acct,
               SUM(CASE WHEN dc='D' THEN amount ELSE -amount END) AS bal
        FROM gl_entries
        WHERE store=:s AND ts::date < :fr
        GROUP BY acct
    """, {"s": store, "fr": fr.strftime("%Y-%m-%d")})
    df_open = df_open.set_index("acct") if not df_open.empty else pd.DataFrame(columns=["bal"]).set_index(pd.Index([]))

    # Phát sinh trong kỳ
    df_mov = fetch_df(conn, """
        SELECT acct,
               SUM(CASE WHEN dc='D' THEN amount ELSE 0 END) AS debit,
               SUM(CASE WHEN dc='C' THEN amount ELSE 0 END) AS credit
        FROM gl_entries
        WHERE store=:s AND ts::date BETWEEN :fr AND :to
        GROUP BY acct
        ORDER BY acct
    """, {"s": store, "fr": fr.strftime("%Y-%m-%d"), "to": to.strftime("%Y-%m-%d")})

    if df_mov.empty:
        # trả khung rỗng
        df_mov = pd.DataFrame(columns=["acct","debit","credit"])

    # Join với danh mục TK
    accts = acct_map(conn)
    df = df_mov.merge(accts, left_on="acct", right_on="code", how="right").fillna({"debit":0.0,"credit":0.0})
    df["opening"] = df["code"].apply(lambda a: float(df_open.loc[a,"bal"]) if a in df_open.index else 0.0)
    df["movement"] = df["debit"] - df["credit"]
    df["closing"] = df["opening"] + df["movement"]
    df = df[["code","name","type","opening","debit","credit","closing"]].sort_values("code")
    return df

def is_bs_type(t): return t in ("ASSET","LIABILITY","EQUITY")
def is_pl_type(t): return t in ("INCOME","EXPENSE")

def bs_statement(tb: pd.DataFrame) -> pd.DataFrame:
    # Cân đối kế toán: lấy số dư cuối kỳ, đảo dấu cho nhóm Có nếu muốn hiển thị thuần dương
    df = tb.copy()
    df["balance"] = df["closing"]
    # Chuẩn: ASSET dương, LIAB/ EQUITY âm -> chuyển thành dương hiển thị
    df.loc[df["type"].isin(["LIABILITY","EQUITY"]), "balance"] *= -1
    out = df.groupby(["type"])[["balance"]].sum().reset_index()
    return out

def pl_statement(tb: pd.DataFrame) -> pd.DataFrame:
    # KQKD: dùng phát sinh kỳ (debit/credit). Thu nhập > 0, chi phí > 0
    df = tb[tb["type"].isin(["INCOME","EXPENSE"])].copy()
    df["amount"] = df.apply(lambda r: (r["credit"]-r["debit"]) if r["type"]=="INCOME" else (r["debit"]-r["credit"]), axis=1)
    grp = df.groupby("type")[["amount"]].sum().reset_index()
    # thêm dòng Lợi nhuận
    profit = (grp.loc[grp["type"]=="INCOME","amount"].sum() - grp.loc[grp["type"]=="EXPENSE","amount"].sum())
    grp = pd.concat([grp, pd.DataFrame([{"type":"PROFIT","amount": profit}])], ignore_index=True)
    return grp

def cf_statement(conn, store: str, fr: date, to: date) -> pd.DataFrame:
    """
    Lưu chuyển tiền tệ (gián tiếp) đơn giản:
    - Dòng tiền HĐKD = Lợi nhuận + khấu hao + thay đổi VLĐ (tồn kho, phải thu, phải trả… nếu bạn dùng TK tương ứng)
    - HĐĐT = Mua/bán TSCĐ (TK TSCD)
    - HĐTC = Vay/Trả (nếu có TK nợ vay)
    Gợi ý mapping nhanh dựa vào gl_accounts.type + mã TK thông dụng.
    """
    tbdf = tb_period(conn, store, fr, to)
    pl = pl_statement(tbdf)
    profit = float(pl.loc[pl["type"]=="PROFIT","amount"].sum()) if not pl.empty else 0.0

    # Khấu hao kỳ này (gom từ gl_entries có memo='DEPR' hoặc acct thuộc EXPENSE khấu hao)
    dep = fetch_df(conn, """
        SELECT COALESCE(SUM(amount),0) AS dep FROM gl_entries
        WHERE store=:s AND ts::date BETWEEN :fr AND :to AND memo LIKE 'DEPR%%' AND dc='D'
    """, {"s": store, "fr": fr.strftime("%Y-%m-%d"), "to": to.strftime("%Y-%m-%d")})
    dep_amt = float(dep.iloc[0]["dep"] if not dep.empty else 0.0)

    # Thay đổi hàng tồn kho (giả định TK 152/155/156 có prefix '1' và type ASSET + có từ 'INVENTORY' trong tên), bạn có thể chuẩn hoá mã TK để chính xác hơn
    inv_accts = fetch_df(conn, "SELECT code FROM gl_accounts WHERE type='ASSET' AND (LOWER(name) LIKE '%inventory%' OR LOWER(name) LIKE '%tồn%')")
    chg_inv = 0.0
    if not inv_accts.empty:
        in_codes = tuple(inv_accts["code"].tolist())
        inv_mov = fetch_df(conn, f"""
            SELECT COALESCE(SUM(CASE WHEN dc='D' THEN amount ELSE -amount END),0) AS mv
            FROM gl_entries WHERE store=:s AND ts::date BETWEEN :fr AND :to AND acct IN :codes
        """, {"s": store, "fr": fr.strftime("%Y-%m-%d"), "to": to.strftime("%Y-%m-%d"), "codes": in_codes})
        chg_inv = float(inv_mov.iloc[0]["mv"] if not inv_mov.empty else 0.0)

    cfo = profit + dep_amt - chg_inv
    cfi = 0.0  # tối giản: thuần mua TSCD sẽ âm, bán TSCD dương — xem ở phần TSCĐ ghi bút toán
    cff = 0.0  # tối giản: không xử lý vay nợ nếu chưa có TK

    df = pd.DataFrame([
        {"section":"HĐKD", "item":"Lợi nhuận trước thuế", "amount": profit},
        {"section":"HĐKD", "item":"Khấu hao", "amount": dep_amt},
        {"section":"HĐKD", "item":"(+) / (–) Thay đổi hàng tồn kho", "amount": -chg_inv},
        {"section":"HĐKD", "item":"Lưu chuyển tiền thuần từ HĐKD", "amount": cfo},
        {"section":"HĐĐT", "item":"Lưu chuyển tiền thuần từ HĐĐT", "amount": cfi},
        {"section":"HĐTC", "item":"Lưu chuyển tiền thuần từ HĐTC", "amount": cff},
        {"section":"TỔNG", "item":"Tăng/giảm tiền thuần", "amount": cfo + cfi + cff},
    ])
    return df

# ----------------------- TSCĐ (nâng cao) -----------------------
def calc_depr(monthly_rate: float, months: int, cost: float) -> float:
    return round(cost * monthly_rate * months, 2)

def page_tscd(conn):
    st.subheader("🏗️ Tài sản cố định")
    user = st.session_state.get("user", {})
    if not (user and (user.get("role")=="SuperAdmin" or has_perm(user,"ASSETS"))):
        st.warning("⛔ Bạn không có quyền TSCD."); return
    store = st.session_state.get("store","")

    tab_reg, tab_dep = st.tabs(["Danh mục TSCD", "Chạy khấu hao"])

    # Danh mục TSCD
    with tab_reg:
        df = fetch_df(conn, """
            SELECT id, code, name, buy_date::date AS buy_date, cost, life_months, monthly_rate, acct_asset, acct_dep_exp, acct_acc_dep, note
            FROM fa_assets WHERE store=:s ORDER BY code
        """, {"s": store})
        st.dataframe(df, use_container_width=True, height=280)

        with st.form("fa_new", clear_on_submit=True):
            c1,c2,c3 = st.columns([1,1,1])
            with c1:
                code = st.text_input("Mã TSCD*")
                name = st.text_input("Tên TSCD*")
            with c2:
                buy_date = st.date_input("Ngày mua", value=date.today())
                cost = st.number_input("Nguyên giá*", min_value=0.0, step=1.0)
            with c3:
                life = st.number_input("Thời gian (tháng)", min_value=1, step=1, value=36)
                rate = st.number_input("Tỷ lệ KH/tháng", min_value=0.0, max_value=1.0, step=0.001, value=1.0/life if life else 0.0278)
            acct = st.text_input("TK tài sản / TK chi phí KH / TK hao mòn lũy kế", value="211/627/214")
            note = st.text_input("Ghi chú")
            ok = st.form_submit_button("💾 Lưu/Upsert")
        if ok:
            acct_asset, acct_dep_exp, acct_acc_dep = (acct.split("/") + ["","",""])[:3]
            run_sql(conn, """
                INSERT INTO fa_assets(store,code,name,buy_date,cost,life_months,monthly_rate,acct_asset,acct_dep_exp,acct_acc_dep,note)
                VALUES(:s,:c,:n,:bd,:cost,:life,:rate,:aa,:ae,:ad,:no)
                ON CONFLICT (store,code) DO UPDATE
                  SET name=EXCLUDED.name, buy_date=EXCLUDED.buy_date, cost=EXCLUDED.cost,
                      life_months=EXCLUDED.life_months, monthly_rate=EXCLUDED.monthly_rate,
                      acct_asset=EXCLUDED.acct_asset, acct_dep_exp=EXCLUDED.acct_dep_exp,
                      acct_acc_dep=EXCLUDED.acct_acc_dep, note=EXCLUDED.note
            """, {"s":store,"c":code,"n":name,"bd":buy_date.strftime("%Y-%m-%d"),"cost":cost,
                  "life":int(life),"rate":float(rate),
                  "aa":acct_asset,"ae":acct_dep_exp,"ad":acct_acc_dep,"no":note})
            write_audit(conn,"FA_UPSERT",code)
            st.success("✅ Đã lưu TSCD.")
            st.rerun()

        if not df.empty:
            del_code = st.selectbox("Chọn TSCD để xóa", [""]+df["code"].tolist(), key="fa_del")
            if st.button("🗑️ Xóa TSCD"):
                run_sql(conn, "DELETE FROM fa_assets WHERE store=:s AND code=:c", {"s":store,"c":del_code})
                write_audit(conn,"FA_DELETE",del_code)
                st.success("Đã xóa TSCD.")
                st.rerun()

    # Khấu hao
    with tab_dep:
        c1,c2 = st.columns([1,1])
        with c1:
            m_from = st.date_input("Khấu hao từ (tháng)", value=date(date.today().year, date.today().month, 1))
        with c2:
            m_to = st.date_input("đến (tháng)", value=date.today())
        if st.button("▶️ Tính & Ghi bút toán khấu hao"):
            assets = fetch_df(conn, "SELECT * FROM fa_assets WHERE store=:s", {"s":store})
            if assets.empty: st.info("Chưa có TSCD."); return
            for _, a in assets.iterrows():
                # tính số tháng trong khoảng
                months = max(0, (m_to.year - m_from.year)*12 + (m_to.month - m_from.month) + 1)
                dep_amt = calc_depr(float(a["monthly_rate"]), months, float(a["cost"]))
                if dep_amt <= 0: continue
                # ghi bút toán: Nợ chi phí khấu hao / Có hao mòn lũy kế
                run_sql(conn, """
                    INSERT INTO gl_entries(ts,store,acct,dc,amount,ref,memo,actor)
                    VALUES (NOW(),:s,:de,'D',:amt,:ref,:memo,:u),
                           (NOW(),:s,:ad,'C',:amt,:ref,:memo,:u)
                """, {"s":store,"de":a["acct_dep_exp"],"ad":a["acct_acc_dep"],"amt":dep_amt,
                      "ref":f"DEPR:{a['code']}","memo":"DEPR","u":st.session_state.get("user",{}).get("email","sys")})
            write_audit(conn,"FA_DEPR",f"{m_from}..{m_to}")
            st.success("✅ Đã ghi khấu hao.")
            st.rerun()

# ----------------------- Lương (Payroll) -----------------------
def page_luong(conn):
    st.subheader("🧾 Lương")
    user = st.session_state.get("user", {})
    if not (user and (user.get("role")=="SuperAdmin" or has_perm(user,"PAYROLL"))):
        st.warning("⛔ Bạn không có quyền Lương."); return
    store = st.session_state.get("store","")

    tab_emp, tab_ts, tab_run = st.tabs(["Nhân viên", "Chấm công", "Tính lương & Ghi sổ"])

    # Nhân viên
    with tab_emp:
        dfe = fetch_df(conn, "SELECT code,name,dept,base_salary,bank_no,active FROM employees WHERE store=:s ORDER BY code", {"s":store})
        st.dataframe(dfe, use_container_width=True, height=260)
        with st.form("emp_form", clear_on_submit=True):
            c1,c2,c3 = st.columns([1,1.2,1])
            with c1:
                code = st.text_input("Mã NV*")
                name = st.text_input("Tên NV*")
            with c2:
                dept = st.text_input("Phòng ban")
                base = st.number_input("Lương cơ bản", min_value=0.0, step=100000.0)
            with c3:
                bank = st.text_input("TK Ngân hàng")
                active = st.checkbox("Đang làm", value=True)
            ok = st.form_submit_button("💾 Lưu/Upsert")
        if ok:
            run_sql(conn, """
                INSERT INTO employees(store,code,name,dept,base_salary,bank_no,active)
                VALUES(:s,:c,:n,:d,:b,:k,:a)
                ON CONFLICT (store,code) DO UPDATE
                  SET name=EXCLUDED.name, dept=EXCLUDED.dept, base_salary=EXCLUDED.base_salary,
                      bank_no=EXCLUDED.bank_no, active=EXCLUDED.active
            """, {"s":store,"c":code,"n":name,"d":dept,"b":base,"k":bank,"a":active})
            write_audit(conn,"PAY_EMP_UPSERT",code); st.success("Đã lưu NV."); st.rerun()

    # Chấm công
    with tab_ts:
        dfts = fetch_df(conn, """
            SELECT id, emp_code, work_date::date AS work_date, hours, note
            FROM timesheets WHERE store=:s ORDER BY work_date DESC, emp_code
        """, {"s":store})
        st.dataframe(dfts, use_container_width=True, height=260)
        with st.form("ts_form", clear_on_submit=True):
            emp = st.text_input("Mã NV*")
            wdate = st.date_input("Ngày làm", value=date.today())
            hours = st.number_input("Số giờ", min_value=0.0, step=0.5)
            note = st.text_input("Ghi chú")
            ok2 = st.form_submit_button("💾 Lưu chấm công")
        if ok2:
            run_sql(conn, """
                INSERT INTO timesheets(store,emp_code,work_date,hours,note)
                VALUES(:s,:e,:d,:h,:n)
            """, {"s":store,"e":emp,"d":wdate.strftime("%Y-%m-%d"),"h":hours,"n":note})
            write_audit(conn,"PAY_TS_ADD",f"{emp} {wdate} {hours}")
            st.success("Đã lưu chấm công."); st.rerun()

    # Tính lương & Ghi sổ
    with tab_run:
        month = st.date_input("Kỳ lương (tháng)", value=date(date.today().year, date.today().month, 1))
        if st.button("▶️ Tính lương tháng"):
            start = month.replace(day=1)
            end = (start + timedelta(days=40)).replace(day=1) - timedelta(days=1)
            dfe = fetch_df(conn, "SELECT code,name,base_salary FROM employees WHERE store=:s AND active=TRUE", {"s":store})
            rows = []
            for _, e in dfe.iterrows():
                ts = fetch_df(conn, """
                    SELECT COALESCE(SUM(hours),0) AS h FROM timesheets
                    WHERE store=:s AND emp_code=:e AND work_date BETWEEN :fr AND :to
                """, {"s":store,"e":e["code"],"fr":start.strftime("%Y-%m-%d"),"to":end.strftime("%Y-%m-%d")})
                hours = float(ts.iloc[0]["h"] if not ts.empty else 0.0)
                gross = float(e["base_salary"] or 0.0)  # tối giản: lương cơ bản/tháng
                bhxh = round(gross*0.105, 0)  # ví dụ
                thue = round(max(0.0, (gross-11000000)*0.05), 0) if gross>11000000 else 0.0
                advance = 0.0
                net = gross - bhxh - thue - advance
                rows.append({"emp":e["code"],"name":e["name"],"hours":hours,"gross":gross,"bhxh":bhxh,"thue":thue,"advance":advance,"net":net})
            dfpay = pd.DataFrame(rows)
            st.dataframe(dfpay, use_container_width=True)

            if not dfpay.empty and st.button("💾 Ghi bút toán lương (kỳ này)"):
                # Ghi chi phí lương / Phải trả NLĐ
                debit_acct = "642"   # chi phí quản lý (ví dụ)
                credit_acct = "334"  # phải trả người lao động
                amt = float(dfpay["net"].sum())
                run_sql(conn, """
                    INSERT INTO gl_entries(ts,store,acct,dc,amount,ref,memo,actor)
                    VALUES (NOW(),:s,:d,'D',:amt,:ref,:memo,:u),
                           (NOW(),:s,:c,'C',:amt,:ref,:memo,:u)
                """, {"s":store,"d":debit_acct,"c":credit_acct,"amt":amt,
                      "ref":f"PAY:{start:%Y-%m}","memo":"PAYROLL","u":st.session_state.get("user",{}).get("email","sys")})
                write_audit(conn,"PAY_GL_BOOK",f"{start:%Y-%m}")
                st.success("✅ Đã ghi bút toán lương.")

# ----------------------- Báo cáo tài chính -----------------------
def page_baocao_taichinh(conn):
    st.subheader("📈 Báo cáo tài chính")
    user = st.session_state.get("user", {})
    if not (user and (user.get("role")=="SuperAdmin" or has_perm(user,"REPORTS"))):
        st.warning("⛔ Bạn không có quyền Báo cáo."); return
    store = st.session_state.get("store","")

    c1,c2 = st.columns(2)
    with c1:
        fr = st.date_input("Từ ngày", value=date(date.today().year, 1, 1))
    with c2:
        to = st.date_input("Đến ngày", value=date.today())

    tb = tb_period(conn, store, fr, to)
    st.markdown("#### 🔢 Trial Balance")
    st.dataframe(tb, use_container_width=True, height=260)

    tabs = st.tabs(["Cân đối kế toán","Kết quả KD","Lưu chuyển tiền tệ"])
    with tabs[0]:
        bs = bs_statement(tb)
        st.dataframe(bs, use_container_width=True)
        st.metric("Tổng Tài sản", f"{bs.loc[bs['type']=='ASSET','balance'].sum():,.0f} VND")
        st.metric("Tổng Nợ + Vốn", f"{(bs.loc[bs['type']=='LIABILITY','balance'].sum()+bs.loc[bs['type']=='EQUITY','balance'].sum()):,.0f} VND")

    with tabs[1]:
        pl = pl_statement(tb)
        st.dataframe(pl, use_container_width=True)
        st.metric("Lợi nhuận kỳ", f"{pl.loc[pl['type']=='PROFIT','amount'].sum():,.0f} VND")

    with tabs[2]:
        cf = cf_statement(conn, store, fr, to)
        st.dataframe(cf, use_container_width=True)

    st.download_button("⬇️ Xuất TB (CSV)", tb.to_csv(index=False).encode("utf-8"), file_name=f"trial_balance_{fr}_{to}.csv", mime="text/csv")

# ----------------------- Tổng trang TÀI CHÍNH -----------------------
def page_finance(conn):
    st.header("💼 Tài chính")
    tabs = st.tabs(["Báo cáo", "Tài sản cố định", "Lương"])
    with tabs[0]: page_baocao_taichinh(conn)
    with tabs[1]: page_tscd(conn)
    with tabs[2]: page_luong(conn)

# ----------------------- Router phần 4 -----------------------
def route_part4(menu, conn):
    if menu == "Báo cáo":
        page_finance(conn)
    elif menu == "TSCD":
        # vẫn gom trong Finance, nhưng nếu bạn tách menu riêng thì cũng OK
        page_finance(conn)
# ============================================================
# app.py — PHẦN 5/5: Doanh thu (Cash/Bank) + Nhật ký + Cửa hàng
# ============================================================

from datetime import date, datetime, timedelta

# ----------------------- DOANH THU (chỉ Tiền mặt / Chuyển khoản) -----------------------
def page_doanhthu(conn):
    st.header("💰 Doanh thu (Chỉ Tiền mặt / Chuyển khoản)")
    store = st.session_state.get("store","")
    user  = st.session_state.get("user",{})

    tab_new, tab_rep = st.tabs(["Ghi nhận thu", "Báo cáo & Xuất file"])

    # ===== Ghi nhận thu =====
    with tab_new:
        c1,c2,c3 = st.columns([1,1,1.2])
        with c1:
            d = st.date_input("Ngày thu", value=date.today(), key="rev_date")
            amount = st.number_input("Số tiền", min_value=0.0, step=1000.0, key="rev_amount")
        with c2:
            method = st.selectbox("Phương thức", ["TIEN_MAT", "CHUYEN_KHOAN"], key="rev_method")
            refno = st.text_input("Số chứng từ / Số giao dịch", key="rev_ref")
        with c3:
            payer = st.text_input("Người nộp / Diễn giải ngắn", key="rev_payer")
        note = st.text_area("Ghi chú", key="rev_note", placeholder="Ví dụ: Thu trong ngày, mã đơn, ...")

        st.caption("⚠️ **Chỉ ghi nhận dòng tiền** – không cần chọn sản phẩm. Báo cáo doanh thu sẽ tổng hợp theo ngày & phương thức.")

        if st.button("💾 Ghi nhận", key="rev_save"):
            if amount <= 0:
                st.error("Nhập số tiền > 0.")
            else:
                run_sql(conn, """
                    INSERT INTO revenues(ts, store, method, amount, refno, payer, note, actor)
                    VALUES (:ts, :s, :m, :a, :r, :p, :n, :u)
                """, {
                    "ts": f"{d} 00:00:00",
                    "s": store, "m": method, "a": float(amount),
                    "r": refno or "", "p": payer or "", "n": note or "",
                    "u": user.get("email", "sys")
                })
                write_audit(conn, "REV_ADD", f"{method} {amount}")
                st.success("✅ Đã ghi nhận doanh thu.")
                st.rerun()

        st.markdown("**Thu gần đây**")
        df_recent = fetch_df(conn, """
            SELECT ts::timestamp(0) AS ts, method, amount, refno, payer, note, actor
            FROM revenues WHERE store=:s ORDER BY ts DESC LIMIT 200
        """, {"s": store})
        st.dataframe(df_recent, use_container_width=True, height=260)

    # ===== Báo cáo & Xuất =====
    with tab_rep:
        c1,c2,c3 = st.columns([1,1,1])
        with c1:
            fr = st.date_input("Từ ngày", value=date.today()-timedelta(days=7), key="rev_fr")
        with c2:
            to = st.date_input("Đến ngày", value=date.today(), key="rev_to")
        with c3:
            m = st.selectbox("Phương thức", ["TẤT CẢ","TIEN_MAT","CHUYEN_KHOAN"], key="rev_mf")

        cond = "store=:s AND ts::date BETWEEN :fr AND :to"
        prm = {"s": store, "fr": fr.strftime("%Y-%m-%d"), "to": to.strftime("%Y-%m-%d")}
        if m != "TẤT CẢ":
            cond += " AND method=:m"; prm["m"] = m

        df = fetch_df(conn, f"""
            SELECT ts::date AS ngay, method, amount, refno, payer, note, actor
            FROM revenues WHERE {cond} ORDER BY ts
        """, prm)

        st.dataframe(df, use_container_width=True, height=320)
        total_tm = float(df[df["method"]=="TIEN_MAT"]["amount"].sum()) if not df.empty else 0.0
        total_ck = float(df[df["method"]=="CHUYEN_KHOAN"]["amount"].sum()) if not df.empty else 0.0
        c1,c2,c3 = st.columns(3)
        c1.metric("Tiền mặt", f"{total_tm:,.0f} VND")
        c2.metric("Chuyển khoản", f"{total_ck:,.0f} VND")
        c3.metric("Tổng thu", f"{(total_tm+total_ck):,.0f} VND")

        st.download_button(
            "⬇️ Xuất CSV",
            (df.to_csv(index=False).encode("utf-8") if not df.empty else "".encode("utf-8")),
            file_name=f"doanh_thu_{fr}_{to}.csv", mime="text/csv"
        )

# ----------------------- NHẬT KÝ HỆ THỐNG -----------------------
def page_nhatky(conn):
    st.header("📝 Nhật ký hệ thống")
    user = st.session_state.get("user",{})
    if not (user and (user.get("role")=="SuperAdmin" or has_perm(user,"AUDIT"))):
        st.warning("⛔ Bạn không có quyền xem Nhật ký."); return
    store = st.session_state.get("store","")

    c1,c2,c3 = st.columns([1,1,1.5])
    with c1:
        fr = st.date_input("Từ ngày", value=date.today()-timedelta(days=7), key="aud_fr")
    with c2:
        to = st.date_input("Đến ngày", value=date.today(), key="aud_to")
    with c3:
        kw = st.text_input("Tìm theo action / detail / actor", key="aud_kw")

    df = fetch_df(conn, """
        SELECT ts::timestamp(0) AS ts, actor, action, detail, ip
        FROM audit_log
        WHERE ts::date BETWEEN :fr AND :to
        ORDER BY ts DESC
    """, {"fr": fr.strftime("%Y-%m-%d"), "to": to.strftime("%Y-%m-%d")})
    if kw:
        k = kw.lower()
        df = df[df.apply(lambda r: k in str(r["action"]).lower() or k in str(r["detail"]).lower() or k in str(r["actor"]).lower(), axis=1)]

    st.dataframe(df, use_container_width=True, height=380)
    st.download_button(
        "⬇️ Xuất CSV",
        (df.to_csv(index=False).encode("utf-8") if not df.empty else "".encode("utf-8")),
        file_name=f"nhat_ky_{fr}_{to}.csv", mime="text/csv"
    )

# ----------------------- CỬA HÀNG (CRUD) -----------------------
def page_cuahang(conn):
    st.header("🏪 Cửa hàng")
    user = st.session_state.get("user",{})
    if not (user and (user.get("role")=="SuperAdmin" or has_perm(user,"STORES"))):
        st.warning("⛔ Bạn không có quyền Cửa hàng."); return

    tab_list, tab_edit = st.tabs(["Danh sách", "Thêm / Sửa / Xóa"])

    with tab_list:
        df = fetch_df(conn, "SELECT code, name, address, phone, active FROM stores ORDER BY code", {})
        st.dataframe(df, use_container_width=True, height=300)

    with tab_edit:
        st.markdown("#### Thêm / Sửa")
        with st.form("store_upsert", clear_on_submit=True):
            c1,c2,c3 = st.columns([1,1.5,1])
            with c1:
                code = st.text_input("Mã cửa hàng*")
                phone = st.text_input("Điện thoại")
            with c2:
                name = st.text_input("Tên cửa hàng*")
                address = st.text_input("Địa chỉ")
            with c3:
                active = st.checkbox("Đang hoạt động", value=True)
            ok = st.form_submit_button("💾 Lưu/Upsert")
        if ok:
            run_sql(conn, """
                INSERT INTO stores(code,name,address,phone,active)
                VALUES(:c,:n,:a,:p,:ac)
                ON CONFLICT (code) DO UPDATE
                  SET name=EXCLUDED.name, address=EXCLUDED.address, phone=EXCLUDED.phone, active=EXCLUDED.active
            """, {"c":code,"n":name,"a":address,"p":phone,"ac":active})
            write_audit(conn, "STORE_UPSERT", code)
            st.success("✅ Đã lưu cửa hàng.")
            st.rerun()

        st.markdown("#### Xóa")
        dfl = fetch_df(conn, "SELECT code,name FROM stores ORDER BY code", {})
        delc = st.selectbox("Chọn cửa hàng", [""] + (dfl["code"].tolist() if not dfl.empty else []), key="store_del")
        if st.button("🗑️ Xóa cửa hàng"):
            if delc:
                run_sql(conn, "DELETE FROM stores WHERE code=:c", {"c": delc})
                write_audit(conn,"STORE_DELETE",delc)
                st.success("Đã xóa.")
                st.rerun()
            else:
                st.error("Chọn cửa hàng để xóa.")

# ----------------------- Router phần 5 -----------------------
def route_part5(menu, conn):
    if menu == "Doanh thu":
        page_doanhthu(conn)
    elif menu == "Nhật ký":
        page_nhatky(conn)
    elif menu == "Cửa hàng":
        page_cuahang(conn)
