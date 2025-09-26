# Fruit Tea ERP v5 — Postgres only (Streamlit)
# ==========================================
import os, re, json, hashlib
from datetime import date, datetime, timedelta
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

st.set_page_config(page_title="Fruit Tea ERP v5", page_icon="🍵", layout="wide")

# ====================== DB (Postgres only) ======================
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
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        st.error("❌ Chưa cấu hình biến môi trường DATABASE_URL"); st.stop()
    if _ENGINE is None:
        _ENGINE = create_engine(_normalize_pg_url(url), pool_pre_ping=True, future=True)
    return _ENGINE.connect()

# ====================== SQL helpers ======================
def run_sql(conn: Connection, sql: str, params=None):
    if isinstance(params, (list, tuple)):
        idx = 1
        def repl(_):
            nonlocal idx
            s = f":p{idx}"; idx += 1; return s
        sql2 = re.sub(r"\?", repl, sql)
        params = {f"p{i+1}": v for i, v in enumerate(params)}
        res = conn.execute(text(sql2), params)
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
            s = f":p{idx}"; idx += 1; return s
        sql2 = re.sub(r"\?", repl, sql)
        params = {f"p{i+1}": v for i, v in enumerate(params)}
        return pd.read_sql_query(text(sql2), conn, params=params)
    return pd.read_sql_query(text(sql), conn, params=params or {})

# ====================== Auth & audit ======================
def sha256(s: str) -> str: return hashlib.sha256(s.encode("utf-8")).hexdigest()

def write_audit(conn: Connection, action: str, detail: str=""):
    try:
        run_sql(conn, "INSERT INTO syslog(ts,actor,action,detail) VALUES (NOW(),:a,:b,:c)",
               {"a": st.session_state.get("user",{}).get("email","anonymous"),
                "b": action, "c": detail[:1000]})
    except Exception:
        pass

def login_form(conn: Connection):
    st.markdown("### 🔐 Đăng nhập hệ thống")
    e = st.text_input("Email", key="login_e")
    p = st.text_input("Mật khẩu", type="password", key="login_p")
    if st.button("Đăng nhập", type="primary", use_container_width=True):
        df = fetch_df(conn, "SELECT email,display,password,role,store_code,perms FROM users WHERE email=:e", {"e":e})
        if df.empty: st.error("Sai tài khoản hoặc mật khẩu."); return
        row = df.iloc[0]
        if row["password"] != sha256(p): st.error("Sai tài khoản hoặc mật khẩu."); return
        user = {"email":row["email"], "display":row["display"] or row["email"],
                "role":row["role"] or "User", "perms":row["perms"] or "",
                "store": row["store_code"] or ""}
        st.session_state["user"] = user
        st.session_state["store"] = user["store"]
        write_audit(conn, "LOGIN", e)
        st.rerun()

def require_login(conn: Connection) -> dict:
    if "user" not in st.session_state:
        login_form(conn); st.stop()
    return st.session_state["user"]

def has_perm(user: dict, perm: str) -> bool:
    if not user: return False
    if user.get("role") == "SuperAdmin": return True
    return perm in (user.get("perms") or "").split(",")

def header_top(conn: Connection, user: dict):
    c1, c2 = st.columns([0.8,0.2])
    with c1: st.markdown("## 🍵 Fruit Tea ERP v5")
    with c2:
        with st.popover(f"👤 {user.get('display','')}"):
            st.caption(user.get("email",""))
            st.markdown("**Đổi mật khẩu**")
            with st.form("pwform", clear_on_submit=True):
                old = st.text_input("Mật khẩu cũ", type="password")
                new1 = st.text_input("Mật khẩu mới", type="password")
                new2 = st.text_input("Xác nhận", type="password")
                ok = st.form_submit_button("Cập nhật")
            if ok:
                df = fetch_df(conn,"SELECT password FROM users WHERE email=:e",{"e":user["email"]})
                if df.empty or df.iloc[0]["password"] != sha256(old):
                    st.error("Mật khẩu cũ không đúng.")
                elif not new1 or new1!=new2:
                    st.error("Xác nhận chưa khớp.")
                else:
                    run_sql(conn,"UPDATE users SET password=:p WHERE email=:e",
                        {"p":sha256(new1), "e":user["email"]})
                    write_audit(conn, "CHANGE_PW", user["email"])
                    st.success("Đã đổi mật khẩu. Vui lòng đăng nhập lại.")
                    st.session_state.clear(); st.rerun()
            st.divider()
            if st.button("Đăng xuất", use_container_width=True):
                write_audit(conn, "LOGOUT", user["email"])
                st.session_state.clear(); st.rerun()

# ====================== Common UI utils ======================
def sb_store_selector(conn, user):
    st.sidebar.markdown("### 🏪 Cửa hàng")
    df = fetch_df(conn, "SELECT code,name FROM stores ORDER BY name")
    if df.empty:
        st.sidebar.warning("Chưa có cửa hàng."); st.session_state["store"]=""; return ""
    opts = [(r["code"], f'{r["name"]} ({r["code"]})') for _,r in df.iterrows()]
    codes = [o[0] for o in opts]; labels = [o[1] for o in opts]
    cur = st.session_state.get("store", user.get("store",""))
    if cur not in codes: cur=codes[0]
    pick = st.sidebar.selectbox("Đang thao tác tại", labels, index=codes.index(cur), key="sb_store")
    st.session_state["store"] = codes[labels.index(pick)]
    return st.session_state["store"]

def _money(x):
    try: return f"{float(x):,.0f}"
    except: return "0"

# ====================== Pages ======================
def page_dashboard(conn, user):
    st.markdown("### 📊 Dashboard")
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Người dùng", int(fetch_df(conn,"SELECT COUNT(*) n FROM users")["n"].iloc[0]))
    c2.metric("Cửa hàng", int(fetch_df(conn,"SELECT COUNT(*) n FROM stores")["n"].iloc[0]))
    c3.metric("Sản phẩm", int(fetch_df(conn,"SELECT COUNT(*) n FROM products")["n"].iloc[0]))
    c4.metric("Công thức", int(fetch_df(conn,"SELECT COUNT(*) n FROM formulas")["n"].iloc[0]))

def page_catalog(conn, user):
    import json
    st.markdown("### 🧾 Danh mục")
    tabs = st.tabs(["Danh mục SP", "Sản phẩm", "Công thức"])

    # ================== TAB 1: DANH MỤC ==================
    with tabs[0]:
        st.subheader("Danh mục sản phẩm")
        df_cat = fetch_df(conn, "SELECT code, name FROM categories ORDER BY code")
        st.dataframe(df_cat, use_container_width=True, height=320)

        with st.form("cat_upsert", clear_on_submit=True):
            c1, c2 = st.columns([1, 2])
            with c1:
                cat_code = st.text_input("Mã danh mục", key="cat_code")
            with c2:
                cat_name = st.text_input("Tên danh mục", key="cat_name")
            ok = st.form_submit_button("💾 Lưu / Cập nhật", type="primary")
        if ok:
            if not cat_code or not cat_name:
                st.error("Thiếu mã hoặc tên danh mục.")
            else:
                run_sql(conn, """
                    INSERT INTO categories(code,name) VALUES (:c,:n)
                    ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name
                """, {"c": cat_code.strip(), "n": cat_name.strip()})
                write_audit(conn, "CAT_UPSERT", cat_code.strip())
                st.success("Đã lưu."); st.rerun()

        del_pick = st.selectbox("Chọn danh mục để xoá", ["—"] + df_cat["code"].tolist(), key="cat_del_pick")
        if del_pick != "—" and st.button("🗑️ Xoá danh mục", key="cat_delete_btn"):
            run_sql(conn, "DELETE FROM categories WHERE code=:c", {"c": del_pick})
            write_audit(conn, "CAT_DELETE", del_pick)
            st.success("Đã xoá."); st.rerun()

    # ================== TAB 2: SẢN PHẨM ==================
    with tabs[1]:
        st.subheader("Sản phẩm")
        df_cat2 = fetch_df(conn, "SELECT code, name FROM categories ORDER BY name")
        cat_opts = df_cat2["code"].tolist()

        df_prod = fetch_df(conn, """
            SELECT code, name, cat_code, uom, cups_per_kg, price_ref
            FROM products
            ORDER BY name
        """)
        st.dataframe(df_prod, use_container_width=True, height=360)

        with st.form("prod_upsert", clear_on_submit=True):
            c1, c2, c3 = st.columns([1.2, 2.4, 1.2])
            with c1:
                p_code = st.text_input("Mã SP", key="prod_code")
                uom = st.text_input("ĐVT", value="kg", key="prod_uom")
            with c2:
                p_name = st.text_input("Tên SP", key="prod_name")
                cat = st.selectbox("Nhóm", cat_opts, index=0, key="prod_cat")
            with c3:
                cups = st.number_input("Cốc/kg TP", value=0.0, step=0.1, min_value=0.0, key="prod_cups")
                pref = st.number_input("Giá tham chiếu", value=0.0, step=1000.0, min_value=0.0, key="prod_pref")
            ok = st.form_submit_button("💾 Lưu / Cập nhật", type="primary")
        if ok:
            if not p_code or not p_name:
                st.error("Thiếu mã hoặc tên sản phẩm.")
            else:
                run_sql(conn, """
                    INSERT INTO products(code,name,cat_code,uom,cups_per_kg,price_ref)
                    VALUES (:c,:n,:g,:u,:k,:p)
                    ON CONFLICT (code) DO UPDATE SET
                       name=EXCLUDED.name,
                       cat_code=EXCLUDED.cat_code,
                       uom=EXCLUDED.uom,
                       cups_per_kg=EXCLUDED.cups_per_kg,
                       price_ref=EXCLUDED.price_ref
                """, {
                    "c": p_code.strip(), "n": p_name.strip(), "g": cat,
                    "u": uom.strip(), "k": float(cups), "p": float(pref)
                })
                write_audit(conn, "PROD_UPSERT", p_code.strip())
                st.success("Đã lưu."); st.rerun()

        del_prod = st.selectbox("Chọn SP để xoá", ["—"] + df_prod["code"].tolist(), key="prod_del_pick")
        if del_prod != "—" and st.button("🗑️ Xoá sản phẩm", key="prod_delete_btn"):
            run_sql(conn, "DELETE FROM products WHERE code=:c", {"c": del_prod})
            write_audit(conn, "PROD_DELETE", del_prod)
            st.success("Đã xoá."); st.rerun()

    # ================== TAB 3: CÔNG THỨC ==================
    with tabs[2]:
        st.subheader("Công thức (CỐT / MỨT)")
        st.caption("""
- **CỐT**: 1 bước. Có *hệ số thu hồi (recovery)* > 0. Có thể dùng 1 hoặc nhiều trái cây (tỷ lệ phần khối lượng), phụ gia là mã → tỷ lệ.
- **MỨT**: không dùng recovery. Chọn **nguồn NVL** là *TRÁI CÂY* hoặc *CỐT*. Cho phép nhiều trái cây/cốt theo tỷ lệ. Phụ gia dạng mã → tỷ lệ.
- Lưu **trái cây/cốt** trong `fruits_csv` theo dạng `CODE:TYLE` cách nhau dấu phẩy. **Phụ gia** lưu `additives_json` dạng JSON.
        """)

        # Danh mục sản phẩm theo nhóm để build lựa chọn
        df_trai_cay = fetch_df(conn, "SELECT code, name FROM products WHERE cat_code='TRAI_CAY' ORDER BY name")
        df_cot      = fetch_df(conn, "SELECT code, name FROM products WHERE cat_code='COT' ORDER BY name")
        df_mut      = fetch_df(conn, "SELECT code, name FROM products WHERE cat_code='MUT' ORDER BY name")
        df_phu_gia  = fetch_df(conn, "SELECT code, name FROM products WHERE cat_code='PHU_GIA' ORDER BY name")

        # Bảng công thức hiện có
        df_ct = fetch_df(conn, """
            SELECT code, name, type, output_pcode, output_uom, recovery, cups_per_kg,
                   fruits_csv, additives_json, note
            FROM formulas
            ORDER BY type, name
        """)
        st.dataframe(df_ct, use_container_width=True, height=280)

        # ==== Pick CT để sửa hoặc tạo mới ====
        left, right = st.columns([1.2, 2.0])

        with left:
            all_codes = ["— Tạo mới —"] + df_ct["code"].tolist()
            pick_code = st.selectbox("Chọn CT để sửa", all_codes, key="ct_pick")

            # Load dữ liệu khi chọn
            init = {
                "code": "", "name": "", "type": "COT",
                "output_pcode": "", "output_uom": "kg",
                "recovery": 1.0, "cups_per_kg": 0.0,
                "src_kind": "TRAI_CAY",  # mặc định cho MỨT
                "fruits_rows": [], "adds_rows": []
            }
            if pick_code != "— Tạo mới —":
                row = df_ct[df_ct["code"] == pick_code].iloc[0].to_dict()
                init["code"] = row["code"]
                init["name"] = row["name"]
                init["type"] = row["type"]
                init["output_pcode"] = row["output_pcode"]
                init["output_uom"] = row.get("output_uom") or "kg"
                init["recovery"] = float(row.get("recovery") or 1.0)
                init["cups_per_kg"] = float(row.get("cups_per_kg") or 0.0)
                # note có thể chứa SRC=...
                if (row.get("note") or "").startswith("SRC="):
                    init["src_kind"] = (row["note"].split("=",1)[1] or "TRAI_CAY").strip()
                # fruits_csv: CODE:TYLE, CODE:TYLE
                fruits_rows = []
                for tok in (row.get("fruits_csv") or "").split(","):
                    tok = tok.strip()
                    if not tok: continue
                    if ":" in tok:
                        c, r = tok.split(":", 1)
                        try:
                            fruits_rows.append({"code": c.strip(), "ratio": float(r)})
                        except:
                            fruits_rows.append({"code": c.strip(), "ratio": 0.0})
                    else:
                        fruits_rows.append({"code": tok, "ratio": 1.0})
                init["fruits_rows"] = fruits_rows
                # additives_json
                adds_rows = []
                try:
                    m = json.loads(row.get("additives_json") or "{}")
                    for k, v in m.items():
                        adds_rows.append({"code": k, "ratio": float(v)})
                except Exception:
                    pass
                init["adds_rows"] = adds_rows

        with right:
            # ------- FORM UPSERT -------
            with st.form("ct_upsert", clear_on_submit=False):
                c1, c2, c3 = st.columns([1.1, 2.0, 1.2])
                with c1:
                    f_code = st.text_input("Mã CT", value=init["code"], key="ct_code")
                with c2:
                    f_name = st.text_input("Tên CT", value=init["name"], key="ct_name")
                with c3:
                    f_type = st.selectbox("Loại", ["COT", "MUT"],
                                          index=(0 if init["type"] != "MUT" else 1),
                                          key="ct_type")

                # Output product theo loại
                if f_type == "COT":
                    out_opts = ["— Chọn —"] + [f"{r['code']} — {r['name']}" for _, r in df_cot.iterrows()]
                    default_out = init["output_pcode"]
                else:
                    out_opts = ["— Chọn —"] + [f"{r['code']} — {r['name']}" for _, r in df_mut.iterrows()]
                    default_out = init["output_pcode"]
                try:
                    idx_out = 0 if not default_out else 1 + [o.split(" — ",1)[0] for o in out_opts[1:]].index(default_out)
                except ValueError:
                    idx_out = 0
                out_pick = st.selectbox("Sản phẩm đầu ra", out_opts, index=idx_out, key="ct_outpick")
                output_pcode = "" if out_pick == "— Chọn —" else out_pick.split(" — ", 1)[0]

                c4, c5, c6 = st.columns([1, 1, 1])
                with c4:
                    output_uom = st.text_input("ĐVT TP", value=init["output_uom"], key="ct_uom", disabled=True)
                with c5:
                    cups_per_kg = st.number_input("Cốc/kg TP", value=float(init["cups_per_kg"]),
                                                  step=0.1, min_value=0.0, key="ct_cups")
                with c6:
                    if f_type == "COT":
                        recovery = st.number_input("Hệ số thu hồi (CỐT)",
                                                   value=max(0.01, float(init["recovery"])),
                                                   step=0.01, min_value=0.01, key="ct_recovery")
                    else:
                        recovery = 1.0
                        st.text_input("Hệ số thu hồi (MỨT)", value="—", disabled=True, key="ct_recovery_fake")

                # Nguồn NVL cho MỨT
                if f_type == "MUT":
                    src_kind = st.radio("Nguồn NVL cho MỨT", ["TRAI_CAY", "COT"],
                                        index=(0 if init["src_kind"] != "COT" else 1),
                                        horizontal=True, key="ct_src_kind")
                else:
                    src_kind = "TRAI_CAY"

                # ====== Bảng thành phần ======
                st.markdown("**Thành phần chính (trái cây/cốt) – tỷ lệ**")
                if f_type == "COT" or (f_type == "MUT" and src_kind == "TRAI_CAY"):
                    base_df = df_trai_cay
                else:
                    base_df = df_cot
                base_codes = base_df["code"].tolist()
                if not init["fruits_rows"]:
                    init_rows = [{"code": "", "ratio": 1.0}]
                else:
                    init_rows = init["fruits_rows"]
                fruits_rows = st.data_editor(
                    pd.DataFrame(init_rows),
                    column_config={
                        "code": st.column_config.SelectboxColumn(
                            "Mã NVL",
                            options=[""] + base_codes,
                            width="medium"
                        ),
                        "ratio": st.column_config.NumberColumn(
                            "Tỷ lệ",
                            step=0.01, min_value=0.0, max_value=9999.0
                        ),
                    },
                    num_rows="dynamic",
                    use_container_width=True,
                    key="ct_fruits_rows"
                )

                st.markdown("**Phụ gia (mã → tỷ lệ)**")
                pg_codes = df_phu_gia["code"].tolist()
                if not init["adds_rows"]:
                    init_adds = [{"code": "", "ratio": 0.0}]
                else:
                    init_adds = init["adds_rows"]
                adds_rows = st.data_editor(
                    pd.DataFrame(init_adds),
                    column_config={
                        "code": st.column_config.SelectboxColumn(
                            "Mã phụ gia",
                            options=[""] + pg_codes,
                            width="medium"
                        ),
                        "ratio": st.column_config.NumberColumn(
                            "Tỷ lệ",
                            step=0.01, min_value=0.0, max_value=9999.0
                        ),
                    },
                    num_rows="dynamic",
                    use_container_width=True,
                    key="ct_adds_rows"
                )

                submit = st.form_submit_button("💾 Lưu / Cập nhật CT", type="primary")

            # ====== Xử lý lưu ======
            if submit:
                if not f_code or not f_name or not output_pcode:
                    st.error("Thiếu **Mã CT / Tên CT / Sản phẩm đầu ra**.")
                else:
                    # Validate thành phần
                    clean_fruits = []
                    for _, r in fruits_rows.dropna().iterrows():
                        c = (r.get("code") or "").strip()
                        try:
                            ratio = float(r.get("ratio") or 0.0)
                        except:
                            ratio = 0.0
                        if c and ratio > 0:
                            if c not in base_codes:
                                st.error(f"NVL `{c}` không hợp lệ cho loại nguồn đã chọn.")
                                st.stop()
                            clean_fruits.append((c, ratio))
                    if not clean_fruits:
                        st.error("Chưa khai báo thành phần chính.")
                        st.stop()
                    fruits_csv = ",".join([f"{c}:{ratio}" for c, ratio in clean_fruits])

                    # Phụ gia
                    adds_map = {}
                    for _, r in adds_rows.dropna().iterrows():
                        c = (r.get("code") or "").strip()
                        try:
                            ratio = float(r.get("ratio") or 0.0)
                        except:
                            ratio = 0.0
                        if c and ratio > 0:
                            if c not in pg_codes:
                                st.error(f"Phụ gia `{c}` không hợp lệ.")
                                st.stop()
                            adds_map[c] = ratio
                    adds_json = json.dumps(adds_map, ensure_ascii=False)

                    note = ("SRC=" + src_kind) if f_type == "MUT" else ""

                    run_sql(conn, """
                        INSERT INTO formulas(
                            code, name, type, output_pcode, output_uom,
                            recovery, cups_per_kg, fruits_csv, additives_json, note
                        )
                        VALUES (:c,:n,:t,:o,:u,:r,:k,:f,:a,:x)
                        ON CONFLICT (code) DO UPDATE SET
                            name=EXCLUDED.name,
                            type=EXCLUDED.type,
                            output_pcode=EXCLUDED.output_pcode,
                            output_uom=EXCLUDED.output_uom,
                            recovery=EXCLUDED.recovery,
                            cups_per_kg=EXCLUDED.cups_per_kg,
                            fruits_csv=EXCLUDED.fruits_csv,
                            additives_json=EXCLUDED.additives_json,
                            note=EXCLUDED.note
                    """, {
                        "c": f_code.strip(),
                        "n": f_name.strip(),
                        "t": f_type,
                        "o": output_pcode,
                        "u": output_uom,
                        "r": float(recovery),
                        "k": float(cups_per_kg),
                        "f": fruits_csv,
                        "a": adds_json,
                        "x": note
                    })
                    write_audit(conn, "FORMULA_UPSERT", f_code.strip())
                    st.success("Đã lưu công thức.")
                    st.rerun()

        # ==== Xoá CT ====
        st.markdown("---")
        del_ct = st.selectbox("Chọn CT để xoá", ["—"] + df_ct["code"].tolist(), key="ct_del_pick")
        if del_ct != "—" and st.button("🗑️ Xoá công thức", key="ct_delete_btn"):
            run_sql(conn, "DELETE FROM formulas WHERE code=:c", {"c": del_ct})
            write_audit(conn, "FORMULA_DELETE", del_ct)
            st.success("Đã xoá.")
            st.rerun()# --- formulas (PRO; dùng formulas + formula_inputs) ---
    with tabs[2]:
        st.info(
            "CỐT = 1 bước (có hệ số thu hồi). "
            "MỨT = 2 bước (không có hệ số). "
            "Công thức hỗ trợ nhiều NVL chính + nhiều phụ gia. "
            "Định lượng nhập theo **kg NVL / 1kg TP**."
        )

        df_hdr = fetch_df(conn, """
            SELECT code,name,type,output_pcode,output_uom,recovery,cups_per_kg,note
            FROM formulas
            ORDER BY type,name
        """)
        st.dataframe(df_hdr, use_container_width=True, height=280)

        mode = st.radio("Chế độ", ["Tạo mới", "Sửa/Xóa"], horizontal=True)

    # ======== TẠO MỚI ========
    if mode == "Tạo mới":
        with st.form("fm_ct_new", clear_on_submit=True):
            colA, colB = st.columns(2)
            with colA:
                code = st.text_input("Mã công thức")
                name = st.text_input("Tên công thức")
                typ  = st.selectbox("Loại", ["COT","MUT"])
            with colB:
                cups = st.number_input("Số cốc/kg TP", value=0.0, step=0.1, min_value=0.0)
                if typ == "COT":
                    recovery = st.number_input("Hệ số thu hồi (chỉ CỐT)", value=1.10, step=0.01, min_value=0.01)
                else:
                    st.caption("MỨT: không có hệ số thu hồi (mặc định 1.0)")
                    recovery = 1.0

            # Sản phẩm đầu ra theo loại
            out_cat = "COT" if typ=="COT" else "MUT"
            df_out = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code=:c ORDER BY name", {"c": out_cat})
            out_options = ["— Chọn —"] + [f"{r['code']} — {r['name']}" for _,r in df_out.iterrows()]
            out_pick = st.selectbox("Sản phẩm đầu ra", out_options, index=0)
            output_pcode = "" if out_pick=="— Chọn —" else out_pick.split(" — ",1)[0]

            # Nguồn NVL chính
            if typ == "COT":
                src_kind = "TRAI_CAY"   # NVL chính của CỐT = trái cây
                st.caption("Nguồn NVL chính: Trái cây")
            else:
                src_kind = st.radio("Nguồn NVL chính (chỉ MỨT)", ["TRAI_CAY","COT"], horizontal=True, index=0)

            # Chọn NVL chính (nhiều)
            st.markdown("#### Nguyên liệu chính")
            src_cat = "TRAI_CAY" if src_kind=="TRAI_CAY" else "COT"
            df_src = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code=:c ORDER BY name",
                              {"c": src_cat})
            src_multi = st.multiselect(
                "Chọn NVL chính",
                [f"{r['code']} — {r['name']}" for _,r in df_src.iterrows()],
                key="src_multi_new"
            )
            raw_inputs = {}
            for item in src_multi:
                c0 = item.split(" — ",1)[0]
                q0 = st.number_input(f"{item} — kg / 1kg TP", value=0.0, step=0.01, min_value=0.0, key=f"raw_new_{c0}")
                if q0>0: raw_inputs[c0] = q0

            # Chọn phụ gia (nhiều)
            st.markdown("#### Phụ gia")
            df_add = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code='PHU_GIA' ORDER BY name")
            add_multi = st.multiselect(
                "Chọn phụ gia",
                [f"{r['code']} — {r['name']}" for _,r in df_add.iterrows()],
                key="add_multi_new"
            )
            add_inputs = {}
            for item in add_multi:
                c0 = item.split(" — ",1)[0]
                q0 = st.number_input(f"{item} — kg / 1kg TP", value=0.0, step=0.01, min_value=0.0, key=f"add_new_{c0}")
                if q0>0: add_inputs[c0] = q0

            ok = st.form_submit_button("💾 Lưu công thức", type="primary")
            if ok:
                if not code or not name or not output_pcode or (typ=="COT" and not raw_inputs):
                    st.error("Thiếu mã/tên/SP đầu ra/NVL."); 
                else:
                    note = "" if typ=="COT" else (f"SRC={'TRAI_CAY' if src_kind=='TRAI_CAY' else 'COT'}")
                    # Header
                    run_sql(conn, """
                      INSERT INTO formulas(code,name,type,output_pcode,output_uom,recovery,cups_per_kg,note)
                      VALUES (:c,:n,:t,:o,'kg',:r,:k,:x)
                      ON CONFLICT (code) DO UPDATE SET
                        name=EXCLUDED.name, type=EXCLUDED.type, output_pcode=EXCLUDED.output_pcode,
                        output_uom=EXCLUDED.output_uom, recovery=EXCLUDED.recovery,
                        cups_per_kg=EXCLUDED.cups_per_kg, note=EXCLUDED.note
                    """, {"c": code.strip(), "n": name.strip(), "t": typ, "o": output_pcode,
                          "r": float(recovery), "k": float(cups), "x": note})
                    # Detail
                    run_sql(conn, "DELETE FROM formula_inputs WHERE formula_code=:c", {"c": code.strip()})
                    for k,v in raw_inputs.items():
                        run_sql(conn, """
                          INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind)
                          VALUES (:f,:p,:q,:k)
                        """, {"f": code.strip(), "p": k, "q": float(v),
                              "k": ("TRAI_CAY" if src_cat=="TRAI_CAY" else "COT")})
                    for k,v in add_inputs.items():
                        run_sql(conn, """
                          INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind)
                          VALUES (:f,:p,:q,'PHU_GIA')
                        """, {"f": code.strip(), "p": k, "q": float(v)})
                    write_audit(conn, "FORMULA_UPSERT", code)
                    st.success("Đã lưu/cập nhật công thức."); st.rerun()

    # ======== SỬA / XÓA ========
    else:
        if df_hdr.empty:
            st.info("Chưa có công thức."); 
        else:
            pick = st.selectbox("Chọn CT", [f"{r['code']} — {r['name']}" for _,r in df_hdr.iterrows()], key="ct_pick_edit")
            ct_code = pick.split(" — ",1)[0]
            hdr = fetch_df(conn, "SELECT * FROM formulas WHERE code=:c", {"c": ct_code}).iloc[0].to_dict()
            det = fetch_df(conn, "SELECT * FROM formula_inputs WHERE formula_code=:c ORDER BY kind", {"c": ct_code})

            with st.form("fm_ct_edit", clear_on_submit=True):
                colA, colB = st.columns(2)
                with colA:
                    name = st.text_input("Tên công thức", value=hdr["name"] or "")
                    typ  = st.selectbox("Loại", ["COT","MUT"], index=(0 if hdr["type"]=="COT" else 1))
                with colB:
                    cups = st.number_input("Số cốc/kg TP", value=float(hdr.get("cups_per_kg") or 0.0), step=0.1, min_value=0.0)
                    recovery = st.number_input("Hệ số thu hồi (chỉ CỐT)",
                                               value=float(hdr.get("recovery") or 1.0), step=0.01, min_value=0.01,
                                               disabled=(typ!="COT"), key="rec_edit")

                # Đầu ra theo loại hiện chọn
                out_cat = "COT" if typ=="COT" else "MUT"
                df_out = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code=:c ORDER BY name", {"c": out_cat})
                lbls = [f"{r['code']} — {r['name']}"] + []  # placeholder để gợi ý
                cur_out = hdr["output_pcode"]
                out_options = [f"{cur_out} — (hiện tại)"] + [f"{r['code']} — {r['name']}" for _,r in df_out.iterrows() if r["code"]!=cur_out]
                out_pick = st.selectbox("Sản phẩm đầu ra", out_options, index=0)
                output_pcode = cur_out if " (hiện tại)" in out_pick else out_pick.split(" — ",1)[0]

                # Nguồn NVL chính (chỉ MỨT)
                if typ=="MUT":
                    src_kind = "TRAI_CAY"
                    if (hdr.get("note") or "").startswith("SRC="):
                        src_kind = (hdr["note"].split("=",1)[1] or "TRAI_CAY")
                    src_kind = st.radio("Nguồn NVL chính (chỉ MỨT)", ["TRAI_CAY","COT"],
                                        index=(0 if src_kind=="TRAI_CAY" else 1), horizontal=True, key="mut_src_edit")
                else:
                    src_kind = "TRAI_CAY"
                    st.caption("Nguồn NVL chính: Trái cây")

                # Tách det cũ để set default
                raw_old = det[det["kind"].isin(["TRAI_CAY","COT"])].copy()
                add_old = det[det["kind"]=="PHU_GIA"].copy()

                # NVL chính
                st.markdown("#### Nguyên liệu chính")
                src_cat = "TRAI_CAY" if src_kind=="TRAI_CAY" else "COT"
                df_src = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code=:c ORDER BY name", {"c": src_cat})
                choices_raw = [f"{r['code']} — {r['name']}" for _,r in df_src.iterrows()]
                defaults_raw = []
                raw_map = {r["pcode"]: float(r["qty_per_kg"]) for _,r in raw_old.iterrows()}
                for r in df_src.itertuples():
                    key = f"{r.code} — {r.name}"
                    if r.code in raw_map: defaults_raw.append(key)
                picked_raw = st.multiselect("Chọn NVL chính", choices_raw, default=defaults_raw, key="src_multi_edit")

                raw_inputs = {}
                for item in picked_raw:
                    c0 = item.split(" — ",1)[0]
                    q0 = st.number_input(f"{item} — kg / 1kg TP",
                                         value=float(raw_map.get(c0,0.0)), step=0.01, min_value=0.0,
                                         key=f"raw_edit_{c0}")
                    if q0>0: raw_inputs[c0] = q0

                # Phụ gia
                st.markdown("#### Phụ gia")
                df_add = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code='PHU_GIA' ORDER BY name")
                choices_add = [f"{r['code']} — {r['name']}" for _,r in df_add.iterrows()]
                add_map = {r["pcode"]: float(r["qty_per_kg"]) for _,r in add_old.iterrows()}
                defaults_add = []
                for r in df_add.itertuples():
                    key = f"{r.code} — {r.name}"
                    if r.code in add_map: defaults_add.append(key)
                picked_add = st.multiselect("Chọn phụ gia", choices_add, default=defaults_add, key="add_multi_edit")

                add_inputs = {}
                for item in picked_add:
                    c0 = item.split(" — ",1)[0]
                    q0 = st.number_input(f"{item} — kg / 1kg TP",
                                         value=float(add_map.get(c0,0.0)), step=0.01, min_value=0.0,
                                         key=f"add_edit_{c0}")
                    if q0>0: add_inputs[c0] = q0

                colX, colY = st.columns(2)
                with colX:
                    if st.form_submit_button("💾 Cập nhật", type="primary"):
                        if not name or not output_pcode or (typ=="COT" and not raw_inputs):
                            st.error("Thiếu tên/SP đầu ra/NVL."); 
                        else:
                            note = "" if typ=="COT" else (f"SRC={'TRAI_CAY' if src_kind=='TRAI_CAY' else 'COT'}")
                            run_sql(conn, """
                              UPDATE formulas
                              SET name=:n, type=:t, output_pcode=:o, output_uom='kg',
                                  recovery=:r, cups_per_kg=:k, note=:x
                              WHERE code=:c
                            """, {"n": name.strip(), "t": typ, "o": output_pcode,
                                  "r": (float(recovery) if typ=="COT" else 1.0),
                                  "k": float(cups), "x": note, "c": ct_code})
                            run_sql(conn, "DELETE FROM formula_inputs WHERE formula_code=:c", {"c": ct_code})
                            for k,v in raw_inputs.items():
                                run_sql(conn, """
                                  INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind)
                                  VALUES (:f,:p,:q,:k)
                                """, {"f": ct_code, "p": k, "q": float(v),
                                      "k": ("TRAI_CAY" if src_cat=="TRAI_CAY" else "COT")})
                            for k,v in add_inputs.items():
                                run_sql(conn, """
                                  INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind)
                                  VALUES (:f,:p,:q,'PHU_GIA')
                                """, {"f": ct_code, "p": k, "q": float(v)})
                            write_audit(conn, "FORMULA_UPDATE", ct_code)
                            st.success("Đã cập nhật."); st.rerun()
                with colY:
                    if st.form_submit_button("🗑️ Xóa công thức"):
                        run_sql(conn, "DELETE FROM formulas WHERE code=:c", {"c": ct_code})
                        write_audit(conn, "FORMULA_DELETE", ct_code)
                        st.success("Đã xóa."); st.rerun()

    # ======== SỬA / XÓA ========
    else:
        if df_hdr.empty:
            st.info("Chưa có công thức."); 
        else:
            pick = st.selectbox("Chọn CT", [f"{r['code']} — {r['name']}" for _,r in df_hdr.iterrows()], key="ct_pick_edit")
            ct_code = pick.split(" — ",1)[0]
            hdr = fetch_df(conn, "SELECT * FROM formulas WHERE code=:c", {"c": ct_code}).iloc[0].to_dict()
            det = fetch_df(conn, "SELECT * FROM formula_inputs WHERE formula_code=:c ORDER BY kind", {"c": ct_code})

            with st.form("fm_ct_edit", clear_on_submit=True):
                colA, colB = st.columns(2)
                with colA:
                    name = st.text_input("Tên công thức", value=hdr["name"] or "")
                    typ  = st.selectbox("Loại", ["COT","MUT"], index=(0 if hdr["type"]=="COT" else 1))
                with colB:
                    cups = st.number_input("Số cốc/kg TP", value=float(hdr.get("cups_per_kg") or 0.0), step=0.1, min_value=0.0)
                    recovery = st.number_input("Hệ số thu hồi (chỉ CỐT)",
                                               value=float(hdr.get("recovery") or 1.0), step=0.01, min_value=0.01,
                                               disabled=(typ!="COT"), key="rec_edit")

                # Đầu ra theo loại hiện chọn
                out_cat = "COT" if typ=="COT" else "MUT"
                df_out = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code=:c ORDER BY name", {"c": out_cat})
                lbls = [f"{r['code']} — {r['name']}"] + []  # placeholder để gợi ý
                cur_out = hdr["output_pcode"]
                out_options = [f"{cur_out} — (hiện tại)"] + [f"{r['code']} — {r['name']}" for _,r in df_out.iterrows() if r["code"]!=cur_out]
                out_pick = st.selectbox("Sản phẩm đầu ra", out_options, index=0)
                output_pcode = cur_out if " (hiện tại)" in out_pick else out_pick.split(" — ",1)[0]

                # Nguồn NVL chính (chỉ MỨT)
                if typ=="MUT":
                    src_kind = "TRAI_CAY"
                    if (hdr.get("note") or "").startswith("SRC="):
                        src_kind = (hdr["note"].split("=",1)[1] or "TRAI_CAY")
                    src_kind = st.radio("Nguồn NVL chính (chỉ MỨT)", ["TRAI_CAY","COT"],
                                        index=(0 if src_kind=="TRAI_CAY" else 1), horizontal=True, key="mut_src_edit")
                else:
                    src_kind = "TRAI_CAY"
                    st.caption("Nguồn NVL chính: Trái cây")

                # Tách det cũ để set default
                raw_old = det[det["kind"].isin(["TRAI_CAY","COT"])].copy()
                add_old = det[det["kind"]=="PHU_GIA"].copy()

                # NVL chính
                st.markdown("#### Nguyên liệu chính")
                src_cat = "TRAI_CAY" if src_kind=="TRAI_CAY" else "COT"
                df_src = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code=:c ORDER BY name", {"c": src_cat})
                choices_raw = [f"{r['code']} — {r['name']}" for _,r in df_src.iterrows()]
                defaults_raw = []
                raw_map = {r["pcode"]: float(r["qty_per_kg"]) for _,r in raw_old.iterrows()}
                for r in df_src.itertuples():
                    key = f"{r.code} — {r.name}"
                    if r.code in raw_map: defaults_raw.append(key)
                picked_raw = st.multiselect("Chọn NVL chính", choices_raw, default=defaults_raw, key="src_multi_edit")

                raw_inputs = {}
                for item in picked_raw:
                    c0 = item.split(" — ",1)[0]
                    q0 = st.number_input(f"{item} — kg / 1kg TP",
                                         value=float(raw_map.get(c0,0.0)), step=0.01, min_value=0.0,
                                         key=f"raw_edit_{c0}")
                    if q0>0: raw_inputs[c0] = q0

                # Phụ gia
                st.markdown("#### Phụ gia")
                df_add = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code='PHU_GIA' ORDER BY name")
                choices_add = [f"{r['code']} — {r['name']}" for _,r in df_add.iterrows()]
                add_map = {r["pcode"]: float(r["qty_per_kg"]) for _,r in add_old.iterrows()}
                defaults_add = []
                for r in df_add.itertuples():
                    key = f"{r.code} — {r.name}"
                    if r.code in add_map: defaults_add.append(key)
                picked_add = st.multiselect("Chọn phụ gia", choices_add, default=defaults_add, key="add_multi_edit")

                add_inputs = {}
                for item in picked_add:
                    c0 = item.split(" — ",1)[0]
                    q0 = st.number_input(f"{item} — kg / 1kg TP",
                                         value=float(add_map.get(c0,0.0)), step=0.01, min_value=0.0,
                                         key=f"add_edit_{c0}")
                    if q0>0: add_inputs[c0] = q0

                colX, colY = st.columns(2)
                with colX:
                    if st.form_submit_button("💾 Cập nhật", type="primary"):
                        if not name or not output_pcode or (typ=="COT" and not raw_inputs):
                            st.error("Thiếu tên/SP đầu ra/NVL."); 
                        else:
                            note = "" if typ=="COT" else (f"SRC={'TRAI_CAY' if src_kind=='TRAI_CAY' else 'COT'}")
                            run_sql(conn, """
                              UPDATE formulas
                              SET name=:n, type=:t, output_pcode=:o, output_uom='kg',
                                  recovery=:r, cups_per_kg=:k, note=:x
                              WHERE code=:c
                            """, {"n": name.strip(), "t": typ, "o": output_pcode,
                                  "r": (float(recovery) if typ=="COT" else 1.0),
                                  "k": float(cups), "x": note, "c": ct_code})
                            run_sql(conn, "DELETE FROM formula_inputs WHERE formula_code=:c", {"c": ct_code})
                            for k,v in raw_inputs.items():
                                run_sql(conn, """
                                  INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind)
                                  VALUES (:f,:p,:q,:k)
                                """, {"f": ct_code, "p": k, "q": float(v),
                                      "k": ("TRAI_CAY" if src_cat=="TRAI_CAY" else "COT")})
                            for k,v in add_inputs.items():
                                run_sql(conn, """
                                  INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind)
                                  VALUES (:f,:p,:q,'PHU_GIA')
                                """, {"f": ct_code, "p": k, "q": float(v)})
                            write_audit(conn, "FORMULA_UPDATE", ct_code)
                            st.success("Đã cập nhật."); st.rerun()
                with colY:
                    if st.form_submit_button("🗑️ Xóa công thức"):
                        run_sql(conn, "DELETE FROM formulas WHERE code=:c", {"c": ct_code})
                        write_audit(conn, "FORMULA_DELETE", ct_code)
                        st.success("Đã xóa."); st.rerun()

def page_kho(conn, user):
    st.markdown("### 🏬 Kho")
    store = st.session_state.get("store","")
    with st.expander("🔎 Lọc (chỉ áp khi bấm)", expanded=False):
        c1,c2,c3 = st.columns(3)
        pcode = c1.text_input("Mã SP chứa", value="", key="inv_pcode")
        from_d = c2.date_input("Từ ngày", value=date.today()-timedelta(days=30), key="inv_fr")
        to_d   = c3.date_input("Đến ngày", value=date.today(), key="inv_to")
        go = st.button("Áp dụng lọc")
    q = """
       SELECT ts::timestamp, store, pcode, kind, qty, price_in, lot_id, reason
       FROM inventory_ledger
       WHERE store = :s
    """
    par = {"s": store}
    if 'go' in locals() and go:
        if pcode: 
            q += " AND pcode ILIKE :p"
            par["p"] = f"%{pcode}%"
        q += " AND ts::date BETWEEN :f AND :t"
        par["f"] = from_d; par["t"] = to_d
    q += " ORDER BY ts DESC LIMIT 1000"
    df = fetch_df(conn, q, par)
    st.dataframe(df, use_container_width=True, height=360)

    # tồn kho + cốc
    st.markdown("#### Tồn kho hiện tại")
    snap = fetch_df(conn, """
      WITH mv AS (
        SELECT pcode,
               SUM(CASE WHEN kind='IN' THEN qty ELSE -qty END) AS qty
        FROM inventory_ledger WHERE store=:s GROUP BY pcode
      )
      SELECT m.pcode, pr.name, m.qty,
             pr.cups_per_kg * m.qty AS cups_est
      FROM mv m JOIN products pr ON pr.code=m.pcode
      WHERE m.qty <> 0
      ORDER BY pr.name
    """, {"s": store})
    st.dataframe(snap.rename(columns={"pcode":"Mã","name":"Tên","qty":"Tồn (kg)","cups_est":"Số cốc ước tính"}),
                 use_container_width=True, height=360)

    st.markdown("#### Nhập / Xuất kho nhanh")
    col1,col2,col3,col4 = st.columns([1,2,1,1])
    with col1:
        kind = st.selectbox("Loại", ["IN","OUT"], key="inv_kind")
    with col2:
        sp = fetch_df(conn, "SELECT code,name FROM products ORDER BY name")
        sp_lbl = ["— Chọn —"] + [f"{r['code']} — {r['name']}" for _,r in sp.iterrows()]
        sp_pick = st.selectbox("Sản phẩm", sp_lbl, index=0, key="inv_sp")
        sp_code = "" if sp_pick=="— Chọn —" else sp_pick.split(" — ",1)[0]
    with col3:
        qty = st.number_input("Số lượng (kg)", value=0.0, step=0.1, min_value=0.0, key="inv_qty")
    with col4:
        price = st.number_input("Đơn giá nhập (nếu IN)", value=0.0, step=1000.0, min_value=0.0, key="inv_price")
    reason = st.text_input("Lý do (nếu OUT)", value="", key="inv_reason")
    if st.button("Ghi sổ kho", type="primary"):
        if not sp_code or qty<=0:
            st.error("Thiếu SP/số lượng."); 
        else:
            if kind=="OUT":
                # chặn xuất âm
                cur = fetch_df(conn, """
                  SELECT COALESCE(SUM(CASE WHEN kind='IN' THEN qty ELSE -qty END),0) AS stock
                  FROM inventory_ledger WHERE store=:s AND pcode=:p
                """, {"s":store,"p":sp_code})["stock"].iloc[0]
                if float(cur) - float(qty) < -1e-9:
                    st.error("Không cho phép xuất âm."); st.stop()
            run_sql(conn, """
              INSERT INTO inventory_ledger(ts,store,pcode,kind,qty,price_in,reason)
              VALUES (NOW(),:s,:p,:k,:q,:pr,:r)
            """, {"s":store,"p":sp_code,"k":kind,"q":float(qty),
                  "pr": (float(price) if kind=="IN" else None),
                  "r": reason.strip() or None})
            write_audit(conn,"INV_"+kind, f"{sp_code} {qty}")
            st.success("OK"); st.rerun()

def _cost_avg(conn, store, pcode):
    df = fetch_df(conn, """
      SELECT kind, qty, COALESCE(price_in,0) price_in FROM inventory_ledger
      WHERE store=:s AND pcode=:p ORDER BY ts
    """, {"s":store,"p":pcode})
    stock=0.0; cost=0.0
    for _,r in df.iterrows():
        if r["kind"]=="IN":
            q=float(r["qty"] or 0); p=float(r["price_in"] or 0)
            if q>0:
                total=cost*stock + p*q
                stock+=q; cost=(total/stock) if stock>0 else 0.0
        else:
            stock-=float(r["qty"] or 0)
            if stock<0: stock=0.0
    return cost

def page_sanxuat(conn, user):
    st.markdown("### 🛠️ Sản xuất")
    store = st.session_state.get("store","")
    tab1, tab2 = st.tabs(["Tạo lô & Xuất NVL","Hoàn thành lô & Nhập TP"])

    # --- Tạo lô: chọn CT, nhập sản lượng kế hoạch → xuất NVL ngay ---
    with tab1:
        df_ct = fetch_df(conn, "SELECT code,name,type,output_pcode,recovery,fruits_csv,additives_json FROM formulas ORDER BY name")
        if df_ct.empty:
            st.warning("Chưa có công thức. Vào Danh mục → Công thức để tạo."); return
        pick = st.selectbox("Chọn công thức", [f"{r['code']} — {r['name']} ({r['type']})" for _,r in df_ct.iterrows()])
        ct = df_ct[df_ct["code"]==pick.split(" — ",1)[0]].iloc[0].to_dict()
        qty_plan = st.number_input("Sản lượng kế hoạch (kg TP)", value=0.0, step=0.1, min_value=0.0)

        # Preview NVL: nguồn chính 1 mã + phụ gia theo % TP
        src = ct["fruits_csv"] or ""
        adds = {}
        try: adds = json.loads(ct["additives_json"] or "{}")
        except: adds = {}
        st.caption("**Preview NVL**")
        rows = []
        if src:
            rows.append({"pcode": src, "qty": qty_plan * (1.0 if ct["type"]=="MUT" else (1.0/ct["recovery"] if ct["recovery"] else 1.0)), "note":"Nguồn chính"})
        for k,v in adds.items():
            rows.append({"pcode": k, "qty": qty_plan * float(v), "note":"Phụ gia"})
        st.dataframe(pd.DataFrame(rows), use_container_width=True)

        lot_id = st.text_input("Mã lô (để trống sẽ tự sinh)")
        if st.button("➕ Tạo lô & Xuất NVL", type="primary"):
            if qty_plan<=0: st.error("Nhập sản lượng."); st.stop()
            if not lot_id:
                lot_id = f"LOT{datetime.now().strftime('%Y%m%d%H%M%S')}"
            # ghi lô
            run_sql(conn, "INSERT INTO lots(lot_id, store, formula_code, qty_plan, status, created_at) VALUES (:i,:s,:f,:q,'WIP',NOW())",
                   {"i":lot_id,"s":store,"f":ct["code"],"q":qty_plan})
            # xuất NVL
            for r in rows:
                # chặn xuất âm
                cur = fetch_df(conn, """
                  SELECT COALESCE(SUM(CASE WHEN kind='IN' THEN qty ELSE -qty END),0) stock
                  FROM inventory_ledger WHERE store=:s AND pcode=:p
                """, {"s":store,"p":r["pcode"]})["stock"].iloc[0]
                if float(cur) - float(r["qty"]) < -1e-9:
                    st.error(f"Không đủ tồn để xuất: {r['pcode']}"); st.stop()
                run_sql(conn, """
                  INSERT INTO inventory_ledger(ts,store,pcode,kind,qty,reason,lot_id)
                  VALUES (NOW(),:s,:p,'OUT',:q,'SX xuất NVL',:l)
                """, {"s":store,"p":r["pcode"],"q":float(r["qty"]),"l":lot_id})
            write_audit(conn,"MFG_START",lot_id)
            st.success(f"Đã tạo lô {lot_id} và xuất NVL."); st.rerun()

    # --- Hoàn thành lô: nhập TP về kho theo giá vốn bình quân của NVL ---
    with tab2:
        df_wip = fetch_df(conn, """
           SELECT l.lot_id, l.store, l.formula_code, f.output_pcode, f.type, l.qty_plan, l.status, l.created_at
           FROM lots l JOIN formulas f ON f.code=l.formula_code
           WHERE l.store=:s AND l.status='WIP' ORDER BY l.created_at DESC
        """, {"s":store})
        if df_wip.empty:
            st.info("Không có lô đang WIP.")
            return
        pick = st.selectbox("Chọn lô WIP", [f"{r['lot_id']} — {r['formula_code']} — plan {r['qty_plan']}kg" for _,r in df_wip.iterrows()], key="wip_pick")
        lot = df_wip[df_wip["lot_id"]==pick.split(" — ",1)[0]].iloc[0].to_dict()
        qty_ok = st.number_input("Sản lượng nhập kho (kg)", value=float(lot["qty_plan"]), step=0.1, min_value=0.0, key="lot_qty_ok")

        # tính giá vốn TP: tổng chi phí NVL đã OUT trong lô / qty_ok
        df_cost = fetch_df(conn, "SELECT pcode, SUM(qty) q FROM inventory_ledger WHERE lot_id=:l AND kind='OUT' GROUP BY pcode", {"l":lot["lot_id"]})
        total_cost = 0.0
        for _,r in df_cost.iterrows():
            # dùng giá vốn bình quân hiện tại của từng NVL
            avg = _cost_avg(conn, store, r["pcode"])
            total_cost += float(avg)*float(r["q"])
        unit_cost = (total_cost/qty_ok) if qty_ok>0 else 0.0
        st.write(f"Giá vốn ước tính: {_money(unit_cost)} / kg TP")

        if st.button("✅ Hoàn thành & Nhập TP", type="primary"):
            if qty_ok<=0: st.error("Sản lượng > 0"); st.stop()
            # nhập kho TP
            run_sql(conn, """
              INSERT INTO inventory_ledger(ts,store,pcode,kind,qty,price_in,reason,lot_id)
              VALUES (NOW(),:s,:p,'IN',:q,:pr,'SX hoàn thành',:l)
            """, {"s":store,"p":lot["output_pcode"],"q":float(qty_ok),"pr":float(unit_cost),"l":lot["lot_id"]})
            # đóng lô
            run_sql(conn, "UPDATE lots SET status='DONE', qty_ok=:q, finished_at=NOW() WHERE lot_id=:l",
                   {"q":float(qty_ok),"l":lot["lot_id"]})
            write_audit(conn,"MFG_DONE", lot["lot_id"])
            st.success("Đã nhập TP & đóng lô."); st.rerun()

def page_doanhthu(conn, user):
    st.markdown("### 💵 Doanh thu (CASH/BANK)")
    store = st.session_state.get("store","")
    c1,c2,c3,c4 = st.columns([1,1,1,2])
    with c1: ts = st.date_input("Ngày", value=date.today())
    with c2: pay = st.selectbox("Hình thức", ["CASH","BANK"])
    with c3: amt = st.number_input("Số tiền", value=0.0, step=1000.0, min_value=0.0)
    with c4: note = st.text_input("Ghi chú", value="")
    if st.button("Ghi thu", type="primary"):
        run_sql(conn, "INSERT INTO revenue(ts,store,pay,amount,note,actor) VALUES (:t,:s,:p,:m,:n,:a)",
               {"t": datetime.combine(ts, datetime.min.time()),
                "s": store, "p": pay, "m": float(amt), "n": note, "a": user["email"]})
        write_audit(conn,"REV_ADD", f"{pay}:{amt}")
        st.success("OK")
    st.divider()
    f1,f2 = st.columns(2)
    with f1: fr = st.date_input("Từ ngày", value=date.today()-timedelta(days=30))
    with f2: to = st.date_input("Đến ngày", value=date.today())
    df = fetch_df(conn, """
      SELECT date_trunc('day', ts) d, pay, SUM(amount) total
      FROM revenue WHERE store=:s AND ts::date BETWEEN :f AND :t
      GROUP BY d, pay ORDER BY d
    """, {"s":store,"f":fr,"t":to})
    st.dataframe(df, use_container_width=True)
    if not df.empty:
        pvt = df.pivot_table(index="d", columns="pay", values="total", aggfunc="sum").fillna(0.0)
        pvt["NET"]=pvt.sum(axis=1)
        st.line_chart(pvt)

def page_baocao(conn, user):
    st.markdown("### 📈 Báo cáo (tổng hợp)")
    store = st.session_state.get("store","")
    fr = st.date_input("Từ ngày", value=date.today()-timedelta(days=30), key="r_fr")
    to = st.date_input("Đến ngày", value=date.today(), key="r_to")
    df_rev = fetch_df(conn, """
      SELECT pay, SUM(amount) total FROM revenue
      WHERE store=:s AND ts::date BETWEEN :f AND :t GROUP BY pay
    """, {"s":store,"f":fr,"t":to})
    cash = float(df_rev.loc[df_rev["pay"]=="CASH","total"].sum() or 0)
    bank = float(df_rev.loc[df_rev["pay"]=="BANK","total"].sum() or 0)
    c1,c2,c3 = st.columns(3)
    c1.metric("Thu CASH", _money(cash))
    c2.metric("Thu BANK", _money(bank))
    c3.metric("Tổng thu", _money(cash+bank))

def page_tscd(conn, user):
    st.markdown("### 💼 Tài sản cố định (rút gọn)")
    df = fetch_df(conn, "SELECT asset_code,name,start_date::date,cost,salvage,life_months,method,location,active FROM assets ORDER BY asset_code")
    st.dataframe(df, use_container_width=True, height=360)
    with st.form("fa_form", clear_on_submit=True):
        code = st.text_input("Mã TS")
        name = st.text_input("Tên")
        start= st.date_input("Ngày bắt đầu", value=date.today())
        cost = st.number_input("Nguyên giá", value=0.0, step=100000.0, min_value=0.0)
        salv = st.number_input("Giá trị còn lại", value=0.0, step=100000.0, min_value=0.0)
        life = st.number_input("Thời gian KH (tháng)", value=12, step=1, min_value=1)
        meth = st.selectbox("Phương pháp", ["SL"])
        loc  = st.text_input("Vị trí")
        actv = st.checkbox("Đang dùng", value=True)
        if st.form_submit_button("Lưu", type="primary"):
            run_sql(conn, """
              INSERT INTO assets(asset_code,name,start_date,cost,salvage,life_months,method,location,active)
              VALUES (:c,:n,:sd,:co,:sa,:li,:m,:l,:a)
              ON CONFLICT (asset_code) DO UPDATE SET name=EXCLUDED.name,start_date=EXCLUDED.start_date,
                cost=EXCLUDED.cost,salvage=EXCLUDED.salvage,life_months=EXCLUDED.life_months,method=EXCLUDED.method,
                location=EXCLUDED.location,active=EXCLUDED.active
            """, {"c":code,"n":name,"sd":start,"co":cost,"sa":salv,"li":life,"m":meth,"l":loc,"a":actv})
            st.success("OK"); st.rerun()
    pick = st.selectbox("Xoá TS", ["—"]+[r["asset_code"] for _,r in df.iterrows()], index=0, key="del_fa")
    if pick!="—" and st.button("Xoá TSCD"):
        run_sql(conn, "DELETE FROM assets WHERE asset_code=:c", {"c":pick}); st.rerun()

def page_stores(conn, user):
    st.markdown("### 🏪 Cửa hàng")
    df = fetch_df(conn, "SELECT code,name,addr,note FROM stores ORDER BY name")
    st.dataframe(df, use_container_width=True, height=360)
    with st.form("store_form", clear_on_submit=True):
        code = st.text_input("Mã")
        name = st.text_input("Tên")
        addr = st.text_input("Địa chỉ")
        note = st.text_input("Ghi chú")
        if st.form_submit_button("Lưu", type="primary"):
            run_sql(conn, """
              INSERT INTO stores(code,name,addr,note) VALUES (:c,:n,:a,:o)
              ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name, addr=EXCLUDED.addr, note=EXCLUDED.note
            """, {"c":code,"n":name,"a":addr,"o":note})
            st.success("OK"); st.rerun()
    pick = st.selectbox("Xoá", ["—"]+[r["code"] for _,r in df.iterrows()], index=0, key="del_st")
    if pick!="—" and st.button("Xoá cửa hàng"):
        run_sql(conn,"DELETE FROM stores WHERE code=:c",{"c":pick}); st.rerun()

def page_users(conn, user):
    st.markdown("### 👥 Người dùng")
    df = fetch_df(conn, "SELECT email,display,role,store_code,perms,created_at FROM users ORDER BY created_at DESC")
    st.dataframe(df, use_container_width=True, height=360)
    with st.form("user_form", clear_on_submit=True):
        email = st.text_input("Email")
        display = st.text_input("Tên hiển thị")
        pw = st.text_input("Mật khẩu", type="password")
        role = st.selectbox("Vai trò", ["User","Admin","SuperAdmin"])
        store = st.text_input("Cửa hàng mặc định")
        perms = st.text_input("Quyền (CSV)")
        if st.form_submit_button("Lưu", type="primary"):
            if not email or not pw: st.error("Thiếu email/mật khẩu."); 
            else:
                run_sql(conn, """
                  INSERT INTO users(email,display,password,role,store_code,perms)
                  VALUES (:e,:d,:p,:r,:s,:m)
                  ON CONFLICT (email) DO UPDATE SET display=EXCLUDED.display, role=EXCLUDED.role,
                    store_code=EXCLUDED.store_code, perms=EXCLUDED.perms
                """, {"e":email,"d":display,"p":sha256(pw),"r":role,"s":store,"m":perms})
                st.success("OK"); st.rerun()
    pick = st.selectbox("Xoá", ["—"]+[r["email"] for _,r in df.iterrows()], index=0, key="del_user")
    if pick!="—" and st.button("Xoá người dùng"):
        run_sql(conn,"DELETE FROM users WHERE email=:e",{"e":pick}); st.rerun()

def page_audit(conn, user):
    st.markdown("### 📜 Nhật ký hệ thống")
    df = fetch_df(conn, "SELECT ts,actor,action,detail FROM syslog ORDER BY ts DESC LIMIT 300")
    st.dataframe(df, use_container_width=True, height=420)

# ====================== Router cố định ======================
_MENU = [
    ("Dashboard","page_dashboard"),
    ("Danh mục","page_catalog"),
    ("Kho","page_kho"),
    ("Sản xuất","page_sanxuat"),
    ("Doanh thu","page_doanhthu"),
    ("Báo cáo","page_baocao"),
    ("TSCD","page_tscd"),
    ("Cửa hàng","page_stores"),
    ("Người dùng","page_users"),
    ("Nhật ký","page_audit"),
]

def router(conn, user):
    # chọn cửa hàng trên sidebar
    sb_store_selector(conn, user)
    st.sidebar.markdown("## 📌 Chức năng")
    # chỉ hiện những page đã có hàm
    visible = [(lbl, fn) for (lbl, fn) in _MENU if fn in globals() and callable(globals()[fn])]
    labels = [lbl for (lbl,_) in visible]
    choice = st.sidebar.radio("", labels, index=0, label_visibility="collapsed")
    # header top
    header_top(conn, user)
    for lbl, fn in visible:
        if lbl == choice:
            globals()[fn](conn, user)
            break

# ====================== ENTRY ======================
if __name__ == "__main__":
    conn = get_conn()
    user = require_login(conn)
    router(conn, user)
