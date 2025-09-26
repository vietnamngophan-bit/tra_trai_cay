# ============================================================
# app.py — Fruit Tea ERP v5 (Postgres Only)
# Phần 1/4: Hạ tầng, Kết nối, Đăng nhập, Header, Router
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

# ------------------- HELPER SQL -------------------
def run_sql(conn: Connection, sql: str, params=None):
    if isinstance(params, (list, tuple)):
        idx = 1
        def repl(_):
            nonlocal idx
            s = f":p{idx}"
            idx += 1
            return s
        sql_named = re.sub(r"\?", repl, sql)
        params = {f"p{i+1}": v for i,v in enumerate(params)}
        res = conn.execute(text(sql_named), params)
    else:
        res = conn.execute(text(sql), params or {})
    try: conn.commit()
    except Exception: pass
    return res

def fetch_df(conn: Connection, sql: str, params=None) -> pd.DataFrame:
    if isinstance(params, (list, tuple)):
        idx = 1
        def repl(_):
            nonlocal idx
            s = f":p{idx}"
            idx += 1
            return s
        sql_named = re.sub(r"\?", repl, sql)
        params = {f"p{i+1}": v for i,v in enumerate(params)}
        return pd.read_sql_query(text(sql_named), conn, params=params)
    return pd.read_sql_query(text(sql), conn, params=params or {})

# ------------------- AUTH & PHÂN QUYỀN -------------------
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
             "a": action, "d": detail[:1000]})
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
        user = {
            "email": row["email"],
            "display": row["display"] or row["email"],
            "role": row["role"] or "User",
            "perms": row["perms"] or "",
            "store": row["store_code"] or ""
        }
        st.session_state["user"] = user
        if user["store"]: st.session_state["store"] = user["store"]
        write_audit(conn, "LOGIN", email)
        st.rerun()

def require_login(conn: Connection) -> dict:
    if "user" not in st.session_state: login_form(conn); st.stop()
    return st.session_state["user"]

def logout(conn: Connection):
    u = st.session_state.get("user",{})
    write_audit(conn, "LOGOUT", u.get("email",""))
    st.session_state.clear()
    st.rerun()

# ------------------- HEADER TRÊN CÙNG -------------------
def header_top(conn: Connection, user: dict):
    col1, col2 = st.columns([0.8,0.2])
    with col1:
        st.markdown("## 🍵 Quản Trị Trà Trái Cây Anh Gầy")
        st.caption("Kết nối: Postgres (Supabase)")
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

# ------------------- ROUTER (DUY NHẤT) -------------------
def router():
    conn = get_conn()
    user = require_login(conn)
    header_top(conn, user)

    st.sidebar.markdown("## 📌 Menu")
    menu = st.sidebar.radio(
        "Chọn chức năng",
        ["Dashboard","Danh mục","Cửa hàng","Người dùng","Kho","Sản xuất",
         "Doanh thu","Báo cáo","TSCD","Nhật ký"],
        index=0
    )
    st.sidebar.divider()

    if menu=="Dashboard": page_dashboard(conn,user)
    elif menu=="Danh mục": page_catalog(conn,user)
    elif menu=="Cửa hàng": page_stores(conn,user)
    elif menu=="Người dùng": page_users(conn,user)
    elif menu=="Kho": page_kho(conn,user)
    elif menu=="Sản xuất": page_sanxuat(conn,user)
    elif menu=="Doanh thu": page_doanhthu(conn,user)
    elif menu=="Báo cáo": page_baocao(conn,user)
    elif menu=="TSCD": page_tscd(conn,user)
    elif menu=="Nhật ký": page_audit(conn,user)

# ------------------- ENTRY -------------------
if __name__=="__main__":
    router()
# ============================================================
# app.py — Phần 2/4: Dashboard + Danh mục + Cửa hàng + Người dùng
# (Dán ngay dưới Phần 1)
# ============================================================

# ---------- TIỆN ÍCH GIAO DIỆN NHỎ ----------
def _pill(text, color="#eee"):
    st.markdown(
        f"<span style='padding:4px 10px;border-radius:999px;background:{color};"
        f"font-size:12px;border:1px solid rgba(0,0,0,.06)'>{text}</span>",
        unsafe_allow_html=True
    )

def _select_row(df: pd.DataFrame, label: str, val_col="code", show_col="name", key=None) -> str | None:
    if df.empty:
        st.info("Không có dữ liệu.")
        return None
    opts = [f"{r[val_col]} — {r[show_col]}" for _, r in df.iterrows()]
    pick = st.selectbox(label, ["— Chọn —", *opts], index=0, key=key)
    return None if pick == "— Chọn —" else pick.split(" — ", 1)[0]

# ============================================================
# DASHBOARD
# ============================================================
def page_dashboard(conn: Connection, user: dict):
    st.markdown("### 📊 Dashboard")
    c1, c2, c3, c4 = st.columns(4)

    # Tổng quan nhanh (không đòi bảng doanh thu để tránh crash)
    n_store = fetch_df(conn, "SELECT COUNT(*) n FROM stores").iloc[0]["n"]
    n_prod  = fetch_df(conn, "SELECT COUNT(*) n FROM products").iloc[0]["n"]
    n_user  = fetch_df(conn, "SELECT COUNT(*) n FROM users").iloc[0]["n"]
    n_ct    = fetch_df(conn, "SELECT COUNT(*) n FROM formulas").iloc[0]["n"]

    with c1: st.metric("Cửa hàng", int(n_store))
    with c2: st.metric("Sản phẩm", int(n_prod))
    with c3: st.metric("Người dùng", int(n_user))
    with c4: st.metric("Công thức", int(n_ct))

    st.divider()
    st.markdown("#### Nhật ký gần đây")
    log = fetch_df(conn, "SELECT ts,actor,action,detail FROM syslog ORDER BY ts DESC LIMIT 20")
    st.dataframe(log, use_container_width=True, height=300)

# ============================================================
# DANH MỤC: Danh mục SP, Sản phẩm, Công thức
# ============================================================
def page_catalog(conn: Connection, user: dict):
    st.markdown("### 🧾 Danh mục")
    tabs = st.tabs(["Danh mục SP", "Sản phẩm", "Công thức"])

    # --------- 1) Danh mục SP ---------
    with tabs[0]:
        st.subheader("Danh mục sản phẩm")
        df = fetch_df(conn, "SELECT code,name FROM categories ORDER BY code")
        st.dataframe(df, use_container_width=True, height=260)

        with st.form("fm_cat_add", clear_on_submit=True, border=True):
            st.markdown("**Thêm/Sửa**")
            c1, c2 = st.columns([1, 2])
            with c1: code = st.text_input("Mã", key="cat_code")
            with c2: name = st.text_input("Tên", key="cat_name")
            ok = st.form_submit_button("Lưu", type="primary")
        if ok and code and name:
            run_sql(conn, """
                INSERT INTO categories(code,name) VALUES (:c,:n)
                ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name
            """, {"c": code.strip(), "n": name.strip()})
            write_audit(conn, "CAT_UPSERT", code); st.success("Đã lưu!"); st.rerun()

        del_code = _select_row(df, "Xoá danh mục", key="pick_del_cat")
        if del_code and st.button("🗑️ Xoá", key="btn_del_cat"):
            run_sql(conn, "DELETE FROM categories WHERE code=:c", {"c": del_code})
            write_audit(conn, "CAT_DELETE", del_code); st.success("Đã xoá!"); st.rerun()

    # --------- 2) Sản phẩm ---------
    with tabs[1]:
        st.subheader("Sản phẩm")
        dfp = fetch_df(conn, """
            SELECT code,name,cat_code,uom,cups_per_kg,price_ref
            FROM products ORDER BY name
        """)
        st.dataframe(dfp, use_container_width=True, height=320)

        cats = fetch_df(conn, "SELECT code,name FROM categories ORDER BY code")
        cat_opts = [f"{r['code']} — {r['name']}" for _, r in cats.iterrows()]

        with st.form("fm_prod_add", clear_on_submit=True, border=True):
            st.markdown("**Thêm/Sửa**")
            c1, c2, c3 = st.columns([1, 2, 1])
            with c1: pcode = st.text_input("Mã SP")
            with c2: pname = st.text_input("Tên SP")
            with c3: uom   = st.text_input("ĐVT", value="kg")
            cat_pick = st.selectbox("Nhóm", ["— Chọn —", *cat_opts], index=0)
            cups = st.number_input("Số cốc/kg TP", value=0.0, step=0.1, min_value=0.0)
            pref = st.number_input("Giá tham chiếu", value=0.0, step=1000.0, min_value=0.0)
            okp = st.form_submit_button("Lưu", type="primary")
        if okp and pcode and pname and cat_pick != "— Chọn —":
            cat_code = cat_pick.split(" — ", 1)[0]
            run_sql(conn, """
                INSERT INTO products(code,name,cat_code,uom,cups_per_kg,price_ref)
                VALUES (:c,:n,:g,:u,:k,:p)
                ON CONFLICT (code) DO UPDATE SET
                  name=EXCLUDED.name, cat_code=EXCLUDED.cat_code,
                  uom=EXCLUDED.uom, cups_per_kg=EXCLUDED.cups_per_kg, price_ref=EXCLUDED.price_ref
            """, {"c": pcode.strip(), "n": pname.strip(), "g": cat_code,
                  "u": uom.strip(), "k": float(cups), "p": float(pref)})
            write_audit(conn, "PROD_UPSERT", pcode); st.success("Đã lưu!"); st.rerun()

        del_p = _select_row(dfp, "Xoá sản phẩm", key="pick_del_prod")
        if del_p and st.button("🗑️ Xoá sản phẩm"):
            run_sql(conn, "DELETE FROM products WHERE code=:c", {"c": del_p})
            write_audit(conn, "PROD_DELETE", del_p); st.success("Đã xoá!"); st.rerun()

    # --------- 3) Công thức (CỐT 1 bước | MỨT 2 nguồn) ---------
    with tabs[2]:
        st.subheader("Công thức (CỐT / MỨT)")
        st.caption("• CỐT: 1 bước, có **recovery**. • MỨT: từ **TRÁI_CÂY** *hoặc* **CỐT**, không dùng recovery. \
                   • Mỗi công thức gồm *nguyên liệu chính* (1..n) + *phụ gia* (0..n). \
                   • Lượng là **kg cho 1 kg TP**.")

        df_hdr = fetch_df(conn, """
            SELECT code,name,type,output_pcode,output_uom,recovery,cups_per_kg,note
            FROM formulas ORDER BY type,name
        """)
        st.dataframe(df_hdr, use_container_width=True, height=280)

        st.markdown("#### Thêm công thức")
        with st.form("fm_ct_add", clear_on_submit=True, border=True):
            c1, c2, c3 = st.columns([1, 2, 1])
            with c1: ct_code = st.text_input("Mã CT")
            with c2: ct_name = st.text_input("Tên CT")
            with c3: typ     = st.selectbox("Loại", ["COT","MUT"], index=0)

            # SP đầu ra
            out_cat = "COT" if typ == "COT" else "MUT"
            df_out = fetch_df(conn,
                "SELECT code,name FROM products WHERE cat_code=:c ORDER BY name",
                {"c": out_cat})
            out_pick = _select_row(df_out, "Sản phẩm đầu ra", key="ct_out_pick")
            cups = st.number_input("Số cốc/kg TP", value=0.0, step=0.1, min_value=0.0)

            # Recovery (chỉ CỐT)
            rec = st.number_input("Hệ số thu hồi (chỉ CỐT)", value=1.0, step=0.01, min_value=0.01,
                                  disabled=(typ!="COT"))

            # Nguồn NVL chính (chỉ MỨT)
            if typ == "MUT":
                src_kind = st.radio("Nguồn NVL chính (MỨT)", ["TRAI_CAY","COT"], horizontal=True, index=0)
            else:
                src_kind = "TRAI_CAY"
                _pill("Nguồn NVL chính: TRÁI_CÂY (CỐT)", "#eaf7ff")

            # Danh sách chọn NVL chính
            src_cat = "TRAI_CAY" if src_kind == "TRAI_CAY" else "COT"
            df_src = fetch_df(conn,
                "SELECT code,name FROM products WHERE cat_code=:c ORDER BY name", {"c": src_cat})
            picks_raw = st.multiselect("Chọn NVL chính (1..n)",
                [f"{r['code']} — {r['name']}" for _, r in df_src.iterrows()],
                key="ct_raw_multi")

            # Nhập định lượng cho từng NVL chính
            raw_inputs = {}
            for it in picks_raw:
                c0 = it.split(" — ", 1)[0]
                q0 = st.number_input(f"{it} — kg / 1kg TP", value=0.0, step=0.01, min_value=0.0,
                                     key=f"raw_{c0}")
                if q0 > 0: raw_inputs[c0] = q0

            # Phụ gia
            df_add = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code='PHU_GIA' ORDER BY name")
            picks_add = st.multiselect("Chọn phụ gia (0..n)",
                [f"{r['code']} — {r['name']}" for _, r in df_add.iterrows()], key="ct_add_multi")
            add_inputs = {}
            for it in picks_add:
                c0 = it.split(" — ", 1)[0]
                q0 = st.number_input(f"{it} — kg / 1kg TP", value=0.0, step=0.01, min_value=0.0,
                                     key=f"add_{c0}")
                if q0 > 0: add_inputs[c0] = q0

            ok_ct = st.form_submit_button("Lưu công thức", type="primary")

        if ok_ct:
            if not (ct_code and ct_name and out_pick and raw_inputs):
                st.error("Thiếu mã/tên/SP đầu ra hoặc chưa chọn NVL chính.")
            else:
                out_p = out_pick
                note = "" if typ=="COT" else f"SRC={src_kind}"
                run_sql(conn, """
                    INSERT INTO formulas(code,name,type,output_pcode,output_uom,recovery,cups_per_kg,note)
                    VALUES (:c,:n,:t,:o,'kg',:r,:k,:x)
                    ON CONFLICT (code) DO UPDATE SET
                      name=EXCLUDED.name,type=EXCLUDED.type,output_pcode=EXCLUDED.output_pcode,
                      output_uom=EXCLUDED.output_uom,recovery=EXCLUDED.recovery,
                      cups_per_kg=EXCLUDED.cups_per_kg,note=EXCLUDED.note
                """, {"c": ct_code.strip(), "n": ct_name.strip(), "t": typ,
                      "o": out_p, "r": (float(rec) if typ=="COT" else 1.0),
                      "k": float(cups), "x": note})
                # xoá & chèn lại định mức
                run_sql(conn, "DELETE FROM formula_inputs WHERE formula_code=:c", {"c": ct_code})
                for k, v in raw_inputs.items():
                    run_sql(conn, """
                        INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind)
                        VALUES (:f,:p,:q,:k)
                    """, {"f": ct_code, "p": k, "q": float(v), "k": src_cat})
                for k, v in add_inputs.items():
                    run_sql(conn, """
                        INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind)
                        VALUES (:f,:p,:q,'PHU_GIA')
                    """, {"f": ct_code, "p": k, "q": float(v)})
                write_audit(conn, "FORMULA_UPSERT", ct_code)
                st.success("Đã lưu công thức!"); st.rerun()

        # Xoá công thức
        del_ct = _select_row(df_hdr, "Xoá công thức", key="pick_del_ct")
        if del_ct and st.button("🗑️ Xoá CT"):
            run_sql(conn, "DELETE FROM formula_inputs WHERE formula_code=:c", {"c": del_ct})
            run_sql(conn, "DELETE FROM formulas WHERE code=:c", {"c": del_ct})
            write_audit(conn, "FORMULA_DELETE", del_ct)
            st.success("Đã xoá!"); st.rerun()

# ============================================================
# CỬA HÀNG (CRUD)
# ============================================================
def page_stores(conn: Connection, user: dict):
    st.markdown("### 🏬 Cửa hàng")
    df = fetch_df(conn, "SELECT code,name,addr,note FROM stores ORDER BY name")
    st.dataframe(df, use_container_width=True, height=320)

    with st.form("fm_store_add", clear_on_submit=True, border=True):
        st.markdown("**Thêm/Sửa**")
        c1, c2 = st.columns([1, 2])
        with c1: code = st.text_input("Mã cửa hàng")
        with c2: name = st.text_input("Tên cửa hàng")
        addr = st.text_input("Địa chỉ")
        note = st.text_input("Ghi chú")
        ok = st.form_submit_button("Lưu", type="primary")
    if ok and code and name:
        run_sql(conn, """
            INSERT INTO stores(code,name,addr,note) VALUES (:c,:n,:a,:o)
            ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name, addr=EXCLUDED.addr, note=EXCLUDED.note
        """, {"c": code.strip(), "n": name.strip(), "a": addr.strip(), "o": note.strip()})
        write_audit(conn, "STORE_UPSERT", code); st.success("Đã lưu!"); st.rerun()

    del_code = _select_row(df, "Xoá cửa hàng", key="pick_del_store")
    if del_code and st.button("🗑️ Xoá cửa hàng"):
        run_sql(conn, "DELETE FROM stores WHERE code=:c", {"c": del_code})
        write_audit(conn, "STORE_DELETE", del_code); st.success("Đã xoá!"); st.rerun()

# ============================================================
# NGƯỜI DÙNG (CRUD + Reset/Đổi mật khẩu, phân quyền)
# ============================================================
def page_users(conn: Connection, user: dict):
    st.markdown("### 👥 Người dùng")
    df = fetch_df(conn, "SELECT email,display,role,store_code,perms,created_at FROM users ORDER BY created_at DESC")
    st.dataframe(df, use_container_width=True, height=320)

    stores = fetch_df(conn, "SELECT code,name FROM stores ORDER BY name")
    store_opts = [""] + [f"{r['code']} — {r['name']}" for _, r in stores.iterrows()]

    with st.form("fm_user_add", clear_on_submit=True, border=True):
        st.markdown("**Tạo tài khoản**")
        c1, c2 = st.columns([2, 1])
        with c1:
            email   = st.text_input("Email")
            display = st.text_input("Tên hiển thị")
        with c2:
            role    = st.selectbox("Role", ["User","Admin","SuperAdmin"], index=0)
            store_pick = st.selectbox("Cửa hàng", store_opts, index=0)
        perms = st.multiselect("Quyền", [
            "CAT_EDIT","PROD_EDIT","FORMULA_EDIT",
            "INV_EDIT","PROD_RUN","USER_EDIT","STORE_EDIT",
            "REPORT_VIEW","AUDIT_VIEW","FA_ASSET","FA_DEPR","FINANCE"
        ])
        pw_plain = st.text_input("Mật khẩu", type="password")
        ok = st.form_submit_button("Tạo", type="primary")

    if ok and email and pw_plain:
        store_code = store_pick.split(" — ", 1)[0] if (" — " in store_pick) else ""
        run_sql(conn, """
            INSERT INTO users(email,display,password,role,store_code,perms)
            VALUES (:e,:d,:p,:r,:s,:m)
            ON CONFLICT (email) DO UPDATE SET
              display=EXCLUDED.display, role=EXCLUDED.role, store_code=EXCLUDED.store_code, perms=EXCLUDED.perms
        """, {"e": email.strip(), "d": display.strip() or email.strip(),
              "p": sha256(pw_plain), "r": role, "s": store_code, "m": ",".join(perms)})
        write_audit(conn, "USER_UPSERT", email); st.success("OK!"); st.rerun()

    # Reset/Đổi mật khẩu + Xoá
    pick = _select_row(df, "Sửa/Xoá/Đổi mật khẩu", val_col="email", show_col="email", key="user_pick")
    if pick:
        with st.expander("🛠️ Thao tác tài khoản", expanded=True):
            colx, coly, colz = st.columns([2, 2, 1])
            with colx:
                newpw = st.text_input("Mật khẩu mới", type="password", key="user_newpw")
                if st.button("Đặt lại mật khẩu", key="btn_reset_pw"):
                    run_sql(conn, "UPDATE users SET password=:p WHERE email=:e",
                           {"p": sha256(newpw or "123456"), "e": pick})
                    write_audit(conn, "USER_RESET_PW", pick)
                    st.success("Đã đặt lại mật khẩu.")
            with coly:
                if st.button("🗑️ Xoá tài khoản", key="btn_del_user"):
                    run_sql(conn, "DELETE FROM users WHERE email=:e", {"e": pick})
                    write_audit(conn, "USER_DELETE", pick); st.success("Đã xoá!"); st.rerun()
            with colz:
                _pill("ROLE & QUYỀN sửa tại form Tạo tài khoản", "#fff6e5")
