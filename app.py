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
    st.markdown("### 🧾 Danh mục")
    tabs = st.tabs(["Danh mục SP","Sản phẩm","Công thức (Quản lý)"])
    # --- categories ---
    with tabs[0]:
        df = fetch_df(conn, "SELECT code,name FROM categories ORDER BY code")
        st.dataframe(df, use_container_width=True)
        with st.form("fm_cat", clear_on_submit=True):
            code = st.text_input("Mã", key="cat_code")
            name = st.text_input("Tên", key="cat_name")
            if st.form_submit_button("Lưu", type="primary"):
                if code and name:
                    run_sql(conn, """
                        INSERT INTO categories(code,name) VALUES (:c,:n)
                        ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name
                    """, {"c":code.strip(),"n":name.strip()})
                    st.success("Đã lưu"); st.rerun()
        pick = st.selectbox("Xoá mã", ["—"]+[r["code"] for _,r in df.iterrows()], index=0)
        if pick!="—" and st.button("Xoá danh mục"):
            run_sql(conn,"DELETE FROM categories WHERE code=:c",{"c":pick}); st.rerun()

    # --- products ---
    with tabs[1]:
        df = fetch_df(conn, "SELECT code,name,cat_code,uom,cups_per_kg,price_ref FROM products ORDER BY name")
        st.dataframe(df, use_container_width=True, height=360)
        with st.form("fm_prod", clear_on_submit=True):
            code = st.text_input("Mã SP", key="prod_code")
            name = st.text_input("Tên SP", key="prod_name")
            cat = st.selectbox("Nhóm", ["TRAI_CAY","COT","MUT","PHU_GIA","TP_KHAC"], key="prod_cat")
            uom = st.text_input("ĐVT", value="kg", key="prod_uom")
            cups = st.number_input("Cốc/kg TP", value=0.0, step=0.1, min_value=0.0, key="prod_cups")
            pref = st.number_input("Giá tham chiếu (nếu có)", value=0.0, step=1000.0, min_value=0.0, key="prod_pref")
            if st.form_submit_button("Lưu", type="primary"):
                if code and name:
                    run_sql(conn, """
                        INSERT INTO products(code,name,cat_code,uom,cups_per_kg,price_ref)
                        VALUES (:c,:n,:g,:u,:k,:p)
                        ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name,cat_code=EXCLUDED.cat_code,
                          uom=EXCLUDED.uom,cups_per_kg=EXCLUDED.cups_per_kg,price_ref=EXCLUDED.price_ref
                    """, {"c":code.strip(),"n":name.strip(),"g":cat,"u":uom.strip(),"k":float(cups),"p":float(pref)})
                    st.success("Đã lưu"); st.rerun()
        pick = st.selectbox("Xoá SP", ["—"]+[r["code"] for _,r in df.iterrows()], index=0, key="del_sp")
        if pick!="—" and st.button("Xoá sản phẩm"):
            run_sql(conn,"DELETE FROM products WHERE code=:c",{"c":pick}); st.rerun()

    # --- formulas (PRO; mới UI theo yêu cầu) ---
    with tabs[2]:
        st.info(
            "Quản lý công thức — Giao diện trực quan. "
            "NVL chính: chọn từ TRÁI_CÂY hoặc CỐT (checkbox). "
            "Phụ gia: nhập định lượng **per 1 kg sau sơ chế**."
        )

        df_hdr = fetch_df(conn, """
            SELECT code,name,type,output_pcode,output_uom,recovery,cups_per_kg,note
            FROM formulas
            ORDER BY type,name
        """)
        st.dataframe(df_hdr, use_container_width=True, height=220)

        mode = st.radio("Chọn hành động", ["Tạo mới","Sửa/Xóa"], horizontal=True)

        # --- Tạo mới ---
        if mode=="Tạo mới":
            with st.form("fm_ct_new", clear_on_submit=True):
                c1,c2 = st.columns(2)
                with c1:
                    code = st.text_input("Mã công thức", key="new_ct_code")
                    name = st.text_input("Tên công thức", key="new_ct_name")
                    typ  = st.selectbox("Loại công thức", ["COT","MUT"], key="new_ct_type")
                with c2:
                    cups = st.number_input("Số cốc / kg thành phẩm", value=0.0, step=0.1, key="new_ct_cups")
                    if typ=="COT":
                        recovery = st.number_input("Hệ số thu hồi (CỐT)", value=1.10, step=0.01, min_value=0.01, key="new_ct_recovery")
                    else:
                        st.caption("MỨT: không có hệ số thu hồi (mặc định 1.0)")
                        recovery = 1.0

                # Sản phẩm đầu ra
                out_df = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code IN ('COT','MUT') ORDER BY name")
                out_opts = ["— Chọn —"] + [f"{r['code']} — {r['name']}" for _,r in out_df.iterrows()]
                out_pick = st.selectbox("Sản phẩm đầu ra (chỉ chọn SP có loại phù hợp)", out_opts, index=0, key="new_ct_out")
                output_pcode = "" if out_pick=="— Chọn —" else out_pick.split(" — ",1)[0]

                st.markdown("#### NVL chính (chỉ tick, không nhập tỷ lệ)")
                # nguồn NVL chính tùy loại: COT mặc định TRÁI_CÂY; MUT có thể TRÁI_CÂY hoặc COT
                if typ=="COT":
                    src_cat = "TRAI_CAY"
                    st.caption("Nguồn NVL chính: TRÁI_CÂY (chỉ CỐT)")
                else:
                    src_src_choice = st.radio("Nguồn NVL chính cho MỨT", ["TRAI_CAY","COT"], index=0, horizontal=True, key="new_ct_mut_src")
                    src_cat = src_src_choice

                df_src = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code=:c ORDER BY name", {"c": src_cat})
                src_choices = [f\"{r['code']} — {r['name']}\" for _,r in df_src.iterrows()]
                picked_src = st.multiselect("Chọn NVL chính (tick các mã cần có trong công thức)", src_choices, key="new_ct_srcs")

                st.markdown("#### Phụ gia (định lượng per 1 kg sau sơ chế)")
                df_add = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code='PHU_GIA' ORDER BY name")
                add_choices = [f\"{r['code']} — {r['name']}\" for _,r in df_add.iterrows()]
                picked_add = st.multiselect("Chọn phụ gia", add_choices, key="new_ct_adds")
                add_map = {}
                for item in picked_add:
                    pcode = item.split(" — ",1)[0]
                    q = st.number_input(f"{item} — kg / 1kg sau sơ chế", value=0.0, step=0.01, min_value=0.0, key=f"new_add_{pcode}")
                    if q>0: add_map[pcode]=q

                note = st.text_input("Ghi chú (tuỳ chọn)", key="new_ct_note")

                if st.form_submit_button("💾 Lưu công thức", type="primary"):
                    if not code or not name or not output_pcode:
                        st.error("Vui lòng nhập Mã, Tên và chọn Sản phẩm đầu ra."); st.stop()
                    if typ=="COT" and len(picked_src)==0:
                        st.error("CỐT cần ít nhất 1 NVL chính (trái cây)."); st.stop()
                    # upsert header
                    run_sql(conn, """
                      INSERT INTO formulas(code,name,type,output_pcode,output_uom,recovery,cups_per_kg,note,fruits_csv,additives_json)
                      VALUES (:c,:n,:t,:o,'kg',:r,:k,:x,:f,:a)
                      ON CONFLICT (code) DO UPDATE SET
                        name=EXCLUDED.name, type=EXCLUDED.type, output_pcode=EXCLUDED.output_pcode,
                        output_uom=EXCLUDED.output_uom, recovery=EXCLUDED.recovery,
                        cups_per_kg=EXCLUDED.cups_per_kg, note=EXCLUDED.note,
                        fruits_csv=EXCLUDED.fruits_csv, additives_json=EXCLUDED.additives_json
                    """, {"c": code.strip(), "n": name.strip(), "t": typ, "o": output_pcode,
                          "r": float(recovery), "k": float(cups), "x": note or "", "f": ",".join([i.split(" — ",1)[0] for i in picked_src]),
                          "a": json.dumps(add_map)})
                    # update formula_inputs table (clear then insert)
                    run_sql(conn, "DELETE FROM formula_inputs WHERE formula_code=:c", {"c": code.strip()})
                    for p in picked_src:
                        pcode = p.split(" — ",1)[0]
                        run_sql(conn, """
                          INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind)
                          VALUES (:f,:p,0.0,:k)
                        """, {"f": code.strip(), "p": pcode, "k": ("TRAI_CAY" if src_cat=="TRAI_CAY" else "COT")})
                    for pcode,q in add_map.items():
                        run_sql(conn, """
                          INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind)
                          VALUES (:f,:p,:q,'PHU_GIA')
                        """, {"f": code.strip(), "p": pcode, "q": float(q)})
                    write_audit(conn, "FORMULA_UPSERT", code)
                    st.success("Đã lưu/cập nhật công thức."); st.rerun()

        # --- Sửa / Xóa ---
        else:
            if df_hdr.empty:
                st.info("Chưa có công thức."); 
            else:
                pick = st.selectbox("Chọn công thức để sửa/xóa", [f\"{r['code']} — {r['name']} ({r['type']})\" for _,r in df_hdr.iterrows()], key="edit_ct_pick")
                ct_code = pick.split(" — ",1)[0]
                hdr = fetch_df(conn, "SELECT * FROM formulas WHERE code=:c", {"c": ct_code}).iloc[0].to_dict()
                det = fetch_df(conn, "SELECT * FROM formula_inputs WHERE formula_code=:c ORDER BY kind", {"c": ct_code})

                with st.form("fm_ct_edit", clear_on_submit=True):
                    c1,c2 = st.columns(2)
                    with c1:
                        name = st.text_input("Tên công thức", value=hdr.get("name",""), key="edit_name")
                        typ  = st.selectbox("Loại", ["COT","MUT"], index=(0 if hdr.get("type","COT")=="COT" else 1), key="edit_type")
                    with c2:
                        cups = st.number_input("Số cốc / kg thành phẩm", value=float(hdr.get("cups_per_kg") or 0.0), step=0.1, key="edit_cups")
                        recovery = st.number_input("Hệ số thu hồi (CỐT)", value=float(hdr.get("recovery") or 1.0), step=0.01, min_value=0.01, disabled=(typ!="COT"), key="edit_recovery")

                    out_df = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code IN ('COT','MUT') ORDER BY name")
                    cur_out = hdr.get("output_pcode","")
                    out_options = [f\"{cur_out} — (hiện tại)\"] + [f\"{r['code']} — {r['name']}\" for _,r in out_df.iterrows() if r['code']!=cur_out]
                    out_pick = st.selectbox("Sản phẩm đầu ra", out_options, index=0, key="edit_out")
                    output_pcode = cur_out if "(hiện tại)" in out_pick else out_pick.split(" — ",1)[0]

                    # NVL chính
                    st.markdown("#### NVL chính (tick chọn mã, không nhập tỷ lệ)")
                    src_kind = "TRAI_CAY"
                    if typ=="MUT" and (hdr.get("note") or "").startswith("SRC="):
                        src_kind = (hdr["note"].split("=",1)[1] or "TRAI_CAY")
                    src_cat = "TRAI_CAY" if src_kind=="TRAI_CAY" else "COT"
                    df_src = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code=:c ORDER BY name", {"c": src_cat})
                    src_choices = [f\"{r['code']} — {r['name']}\" for _,r in df_src.iterrows()]

                    old_srcs = (hdr.get("fruits_csv") or "")
                    defaults = [f\"{c} — {next((x['name'] for _,x in df_src.iterrows() if x['code']==c), '')}\" for c in old_srcs.split(",") if c]
                    picked_src = st.multiselect("Chọn NVL chính", src_choices, default=defaults, key="edit_srcs")

                    # Phụ gia
                    st.markdown("#### Phụ gia (per 1kg sau sơ chế)")
                    df_add = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code='PHU_GIA' ORDER BY name")
                    add_choices = [f\"{r['code']} — {r['name']}\" for _,r in df_add.iterrows()]
                    # build defaults from formula_inputs
                    add_old = det[det["kind"]=="PHU_GIA"].copy()
                    add_map = {}
                    defaults_add = []
                    for _,r in add_old.iterrows():
                        k=r["pcode"]; v=float(r.get("qty_per_kg") or 0)
                        defaults_add.append(f\"{k} — {next((x['name'] for _,x in df_add.iterrows() if x['code']==k), '')}\")
                        add_map[k]=v
                    picked_add = st.multiselect("Chọn phụ gia", add_choices, default=defaults_add, key="edit_adds")
                    new_add_map={}
                    for item in picked_add:
                        pcode = item.split(" — ",1)[0]
                        q = st.number_input(f"{item} — kg / 1kg sau sơ chế", value=float(add_map.get(pcode,0.0)), step=0.01, min_value=0.0, key=f"edit_add_{pcode}")
                        if q>0: new_add_map[pcode]=q

                    note = st.text_input("Ghi chú", value=hdr.get("note",""), key="edit_note")
                    colA,colB = st.columns(2)
                    with colA:
                        if st.form_submit_button("💾 Cập nhật", type="primary"):
                            if not name or not output_pcode:
                                st.error("Thiếu tên hoặc SP đầu ra."); st.stop()
                            run_sql(conn, """
                              UPDATE formulas
                              SET name=:n, type=:t, output_pcode=:o, output_uom='kg',
                                  recovery=:r, cups_per_kg=:k, note=:x, fruits_csv=:f, additives_json=:a
                              WHERE code=:c
                            """, {"n": name.strip(), "t": typ, "o": output_pcode,
                                  "r": (float(recovery) if typ=="COT" else 1.0),
                                  "k": float(cups), "x": note or "", "f": ",".join([i.split(" — ",1)[0] for i in picked_src]), "a": json.dumps(new_add_map), "c": ct_code})
                            # refresh formula_inputs
                            run_sql(conn, "DELETE FROM formula_inputs WHERE formula_code=:c", {"c": ct_code})
                            for p in picked_src:
                                pcode = p.split(" — ",1)[0]
                                run_sql(conn, """
                                  INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind)
                                  VALUES (:f,:p,0.0,:k)
                                """, {"f": ct_code, "p": pcode, "k": ("TRAI_CAY" if src_cat=="TRAI_CAY" else "COT")})
                            for p,q in new_add_map.items():
                                run_sql(conn, """
                                  INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind)
                                  VALUES (:f,:p,:q,'PHU_GIA')
                                """, {"f": ct_code, "p": p, "q": float(q)})
                            write_audit(conn, "FORMULA_UPDATE", ct_code)
                            st.success("Đã cập nhật"); st.rerun()
                    with colB:
                        if st.form_submit_button("🗑️ Xóa công thức"):
                            run_sql(conn, "DELETE FROM formulas WHERE code=:c", {"c": ct_code})
                            run_sql(conn, "DELETE FROM formula_inputs WHERE formula_code=:c", {"c": ct_code})
                            write_audit(conn, "FORMULA_DELETE", ct_code)
                            st.success("Đã xóa"); st.rerun()


def page_kho:
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
    st.markdown("### 🛠️ Sản xuất — Tạo lô, Xuất NVL, Hoàn thành")
    store = st.session_state.get("store","")
    tabs = st.tabs(["Tạo lô & Xuất NVL (Cốt/Mứt)","Hoàn thành lô & Nhập TP"])
    # ---------- Tab 1: Tạo lô & Xuất NVL ----------
    with tabs[0]:
        st.markdown("#### Bước 1 — Khởi tạo lô / Sản xuất (CỐT: nhập kho ngay; MỨT: tạo lô tạm)")
        df_ct = fetch_df(conn, "SELECT code,name,type,output_pcode,recovery,fruits_csv,additives_json,cups_per_kg FROM formulas ORDER BY name")
        if df_ct.empty:
            st.warning("Chưa có công thức. Vào Danh mục → Công thức để tạo."); return
        pick = st.selectbox("Chọn công thức", [f\"{r['code']} — {r['name']} ({r['type']})\" for _,r in df_ct.iterrows()], key="mfg_ct_pick")
        ct = df_ct[df_ct["code"]==pick.split(" — ",1)[0]].iloc[0].to_dict()

        st.markdown("##### Danh sách NVL chính theo công thức")
        # build list of NVL main items
        fruits = (ct.get("fruits_csv") or "")
        fruit_list = [s for s in fruits.split(",") if s]
        if not fruit_list:
            st.warning("Công thức chưa định nghĩa NVL chính.")
        # For each raw NVL show dropdown (product) and input KG thô
        raw_rows = []
        cols = st.columns(2)
        for pcode in fruit_list:
            prod = fetch_df(conn, "SELECT code,name FROM products WHERE code=:c", {"c":pcode})
            label = f\"{pcode} — {prod.iloc[0]['name'] if not prod.empty else 'Unknown'}\"
            kg_tho = st.number_input(f"{label} — KG thô", value=0.0, step=0.01, min_value=0.0, key=f"mfg_raw_{pcode}")
            raw_rows.append({"pcode": pcode, "kg_tho": float(kg_tho)})

        kg_sau = st.number_input("Tổng KG sau sơ chế (kg)", value=0.0, step=0.01, min_value=0.0, key="mfg_kg_sau")
        # preview additives calc
        adds = {}
        try:
            adds = json.loads(ct.get("additives_json") or "{}")
        except:
            adds = {}
        st.markdown("##### Preview: Phụ gia cần dùng")
        add_rows = []
        for k,v in adds.items():
            need = float(v) * float(kg_sau)
            prod = fetch_df(conn, "SELECT code,name FROM products WHERE code=:c", {"c":k})
            name = prod.iloc[0]["name"] if not prod.empty else k
            add_rows.append({"pcode":k, "name":name, "qty_needed": need})
        st.dataframe(pd.DataFrame(add_rows), use_container_width=True)

        st.divider()
        lot_id = st.text_input("Mã lô (để trống sẽ tự sinh)", key="mfg_lot")
        if st.button("➕ Thực hiện (Xuất NVL / Tạo lô nếu MỨT)", type="primary"):
            # validation
            if kg_sau<=0:
                st.error("Vui lòng nhập KG sau sơ chế."); st.stop()
            total_tho = sum([r["kg_tho"] for r in raw_rows])
            if total_tho<=0:
                st.error("Nhập ít nhất 1 loại KG thô > 0."); st.stop()
            # prepare lot id
            if not lot_id:
                lot_id = f\"LOT{datetime.now().strftime('%Y%m%d%H%M%S')}\"
            # compute NVL outs: each raw kg_tho, plus additives qty
            outs = []
            for r in raw_rows:
                if r["kg_tho"]>0:
                    outs.append({"pcode": r["pcode"], "qty": r["kg_tho"], "note":"NVL thô"})
            for a in add_rows:
                if a["qty_needed"]>0:
                    outs.append({"pcode": a["pcode"], "qty": a["qty_needed"], "note":"PHU_GIA"})
            # check stock and perform OUTs
            for it in outs:
                cur = fetch_df(conn, """
                  SELECT COALESCE(SUM(CASE WHEN kind='IN' THEN qty ELSE -qty END),0) AS stock
                  FROM inventory_ledger WHERE store=:s AND pcode=:p
                """, {"s":store,"p":it["pcode"]})["stock"].iloc[0]
                if float(cur) - float(it["qty"]) < -1e-9:
                    st.error(f"Không đủ tồn {it['pcode']} để xuất ({it['qty']} kg)."); st.stop()
            # create lot record (WIP for MUT, DONE for COT)
            status = "WIP" if ct["type"]=="MUT" else "DONE"
            run_sql(conn, "INSERT INTO lots(lot_id, store, formula_code, qty_plan, status, created_at) VALUES (:i,:s,:f,:q,:st, NOW())",
                    {"i":lot_id, "s":store, "f":ct["code"], "q":float(kg_sau), "st": status})
            # execute OUTs and record
            for it in outs:
                run_sql(conn, """
                  INSERT INTO inventory_ledger(ts,store,pcode,kind,qty,reason,lot_id)
                  VALUES (NOW(),:s,:p,'OUT',:q,:r,:l)
                """, {"s":store, "p": it["pcode"], "q": float(it["qty"]), "r": f"SX xuất NVL ({lot_id})", "l": lot_id})
            # compute wip cost (sum avg_price * qty at time)
            wip_cost = 0.0
            for it in outs:
                avg = _cost_avg(conn, store, it["pcode"])
                wip_cost += float(avg) * float(it["qty"])
            # If COT: compute KG TP, cups, unit price, and IN to inventory immediately
            if ct["type"]=="COT":
                kg_tp = float(kg_sau) * float(ct.get("recovery") or 1.0)
                cups = float(ct.get("cups_per_kg") or 0.0) * kg_tp
                unit_price = (wip_cost / kg_tp) if kg_tp>0 else 0.0
                # insert IN record for TP (CỐT)
                run_sql(conn, """
                  INSERT INTO inventory_ledger(ts,store,pcode,kind,qty,price_in,reason,lot_id)
                  VALUES (NOW(),:s,:p,'IN',:q,:pr,:r,:l)
                """, {"s":store, "p": ct["output_pcode"], "q": float(kg_tp), "pr": float(unit_price),
                      "r": f"SX CỐT hoàn thành — cups:{int(round(cups))}", "l": lot_id})
                # update lot as DONE
                run_sql(conn, "UPDATE lots SET status='DONE', qty_ok=:q, finished_at=NOW() WHERE lot_id=:l",
                        {"q": float(kg_tp), "l": lot_id})
                write_audit(conn, "MFG_COT_DONE", f"{lot_id} kg_tp={kg_tp} unit_pr={unit_price}")
                st.success(f"Hoàn thành CỐT — Đã nhập kho {kg_tp} kg với đơn giá {_money(unit_price)} /kg. Số cốc: {int(round(cups))}")
                st.rerun()
            else:
                # For MUT: store wip cost as a syslog entry so we can trace (or future DB migration)
                write_audit(conn, "MFG_WIP_CREATED", f"{lot_id} wip_cost={wip_cost}")
                st.success(f"Đã tạo lô tạm {lot_id}. Tổng chi phí NVL (ước): {_money(wip_cost)} — Hoàn thành lô sau để nhập kho TP.")
                st.rerun()

    # ---------- Tab 2: Hoàn thành lô ----------
    with tabs[1]:
        st.markdown("#### Bước 2 — Hoàn thành lô MỨT và nhập kho")
        df_wip = fetch_df(conn, """
           SELECT l.lot_id, l.store, l.formula_code, f.output_pcode, f.type, l.qty_plan, l.status, l.created_at
           FROM lots l JOIN formulas f ON f.code=l.formula_code
           WHERE l.store=:s AND l.status='WIP' ORDER BY l.created_at DESC
        """, {"s":store})
        if df_wip.empty:
            st.info("Không có lô WIP đang chờ hoàn thành.")
        else:
            pick = st.selectbox("Chọn lô WIP", [f\"{r['lot_id']} — {r['formula_code']} — plan {r['qty_plan']}kg\" for _,r in df_wip.iterrows()], key="wip_pick2")
            lot_id = pick.split(" — ",1)[0]
            lot = df_wip[df_wip["lot_id"]==lot_id].iloc[0].to_dict()
            qty_ok = st.number_input("Sản lượng thực tế nhập kho (kg)", value=float(lot["qty_plan"]), step=0.01, min_value=0.0, key="wip_qty_ok")
            # compute total NVL OUT in this lot
            df_cost = fetch_df(conn, "SELECT pcode, SUM(qty) q FROM inventory_ledger WHERE lot_id=:l AND kind='OUT' GROUP BY pcode", {"l":lot_id})
            total_cost = 0.0
            for _,r in df_cost.iterrows():
                avg = _cost_avg(conn, store, r["pcode"])
                total_cost += float(avg) * float(r["q"])
            unit_cost = (total_cost / qty_ok) if qty_ok>0 else 0.0
            cups_per_kg = float(lot.get("cups_per_kg") or fetch_df(conn, "SELECT cups_per_kg FROM formulas WHERE code=:c", {"c": lot["formula_code"]})["cups_per_kg"].iloc[0])
            cups = qty_ok * cups_per_kg
            st.write(f"Giá vốn ước tính: {_money(unit_cost)} / kg TP — Số cốc: {int(round(cups))}")
            if st.button("✅ Hoàn thành & Nhập TP", type="primary"):
                if qty_ok<=0:
                    st.error("Sản lượng > 0"); st.stop()
                # nhập kho TP
                run_sql(conn, """
                  INSERT INTO inventory_ledger(ts,store,pcode,kind,qty,price_in,reason,lot_id)
                  VALUES (NOW(),:s,:p,'IN',:q,:pr,:r,:l)
                """, {"s":store, "p": lot["output_pcode"], "q": float(qty_ok), "pr": float(unit_cost),
                      "r": f"SX MỨT hoàn thành — cups:{int(round(cups))}", "l": lot_id})
                # update lot
                run_sql(conn, "UPDATE lots SET status='DONE', qty_ok=:q, finished_at=NOW() WHERE lot_id=:l",
                        {"q": float(qty_ok), "l": lot_id})
                write_audit(conn, "MFG_WIP_DONE", f"{lot_id} qty_ok={qty_ok} unit_pr={unit_cost}")
                st.success("Đã nhập TP & đóng lô."); st.rerun()


def page_doanhthu st.markdown("### 💵 Doanh thu (CASH/BANK)")
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
