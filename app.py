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
    tabs = st.tabs(["Danh mục SP","Sản phẩm","Công thức"])
    # --- categories ---
    with tabs[0]:
        df = fetch_df(conn, "SELECT code,name FROM categories ORDER BY code")
        st.dataframe(df, use_container_width=True)
        with st.form("fm_cat", clear_on_submit=True):
            code = st.text_input("Mã")
            name = st.text_input("Tên")
            if st.form_submit_button("Lưu", type="primary"):
                if code and name:
                    run_sql(conn, """
                        INSERT INTO categories(code,name) VALUES (:c,:n)
                        ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name
                    """, {"c":code.strip(),"n":name.strip()})
                    st.success("OK"); st.rerun()
        pick = st.selectbox("Xoá mã", ["—"]+[r["code"] for _,r in df.iterrows()], index=0)
        if pick!="—" and st.button("Xoá danh mục"):
            run_sql(conn,"DELETE FROM categories WHERE code=:c",{"c":pick}); st.rerun()
    # --- products ---
    with tabs[1]:
        df = fetch_df(conn, "SELECT code,name,cat_code,uom,cups_per_kg,price_ref FROM products ORDER BY name")
        st.dataframe(df, use_container_width=True, height=360)
        with st.form("fm_prod", clear_on_submit=True):
            code = st.text_input("Mã SP")
            name = st.text_input("Tên SP")
            cat = st.selectbox("Nhóm", ["TRAI_CAY","COT","MUT","PHU_GIA","TP_KHAC"])
            uom = st.text_input("ĐVT", value="kg")
            cups = st.number_input("Cốc/kg TP", value=0.0, step=0.1, min_value=0.0)
            pref = st.number_input("Giá tham chiếu", value=0.0, step=1000.0, min_value=0.0)
            if st.form_submit_button("Lưu", type="primary"):
                if code and name:
                    run_sql(conn, """
                        INSERT INTO products(code,name,cat_code,uom,cups_per_kg,price_ref)
                        VALUES (:c,:n,:g,:u,:k,:p)
                        ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name,cat_code=EXCLUDED.cat_code,
                          uom=EXCLUDED.uom,cups_per_kg=EXCLUDED.cups_per_kg,price_ref=EXCLUDED.price_ref
                    """, {"c":code.strip(),"n":name.strip(),"g":cat,"u":uom.strip(),"k":float(cups),"p":float(pref)})
                    st.success("OK"); st.rerun()
        pick = st.selectbox("Xoá SP", ["—"]+[r["code"] for _,r in df.iterrows()], index=0, key="del_sp")
        if pick!="—" and st.button("Xoá sản phẩm"):
            run_sql(conn,"DELETE FROM products WHERE code=:c",{"c":pick}); st.rerun()
    # --- formulas ---
    with tabs[2]:
        st.info("CỐT 1 bước; MỨT không có hệ số; MỨT từ trái cây hoặc từ cốt. Nguồn chính lưu ở fruits_csv; phụ gia (mã→tỷ lệ) ở additives_json.")
        df = fetch_df(conn, "SELECT code,name,type,output_pcode,output_uom,recovery,cups_per_kg,fruits_csv,additives_json,note FROM formulas ORDER BY type,name")
        st.dataframe(df, use_container_width=True, height=360)
        with st.form("fm_ct", clear_on_submit=True):
            code = st.text_input("Mã CT")
            name = st.text_input("Tên CT")
            typ  = st.selectbox("Loại", ["COT","MUT"])
            # output
            out_cat = ["COT"] if typ=="COT" else ["MUT"]
            df_out = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code = ANY(:cats) ORDER BY name", {"cats":out_cat})
            out_lbls = ["— Chọn —"] + [f"{r['code']} — {r['name']}" for _,r in df_out.iterrows()]
            out_pick = st.selectbox("SP đầu ra", out_lbls, index=0)
            output_pcode = "" if out_pick=="— Chọn —" else out_pick.split(" — ",1)[0]
            uom = st.text_input("ĐVT TP", value="kg", disabled=True)
            rec = st.number_input("Hệ số thu hồi (CỐT)", value=1.0, step=0.01, min_value=0.0, disabled=(typ!="COT"))
            cups = st.number_input("Cốc/kg TP", value=0.0, step=0.1, min_value=0.0)
            src_kind = st.radio("Nguồn NVL (MỨT)", ["TRAI_CAY","COT"], horizontal=True, index=0, key="mut_src")
            src_cat = ["TRAI_CAY"] if typ=="COT" else ([src_kind])
            df_src = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code = ANY(:cats) ORDER BY name", {"cats":src_cat})
            src_lbls = ["— Chọn —"] + [f"{r['code']} — {r['name']}" for _,r in df_src.iterrows()]
            src_pick = st.selectbox("Nguồn chính", src_lbls, index=0)
            src_code = "" if src_pick=="— Chọn —" else src_pick.split(" — ",1)[0]
            adds = st.text_area("Phụ gia JSON (mã → tỷ lệ, ví dụ {\"DUONG\":0.05})", value="{}", height=80)
            note = ("SRC="+src_kind) if typ=="MUT" else ""
            if st.form_submit_button("Lưu CT", type="primary"):
                if not code or not name or not output_pcode:
                    st.error("Thiếu mã/tên/SP đầu ra."); 
                else:
                    run_sql(conn, """
                      INSERT INTO formulas(code,name,type,output_pcode,output_uom,recovery,cups_per_kg,fruits_csv,additives_json,note)
                      VALUES (:c,:n,:t,:o,'kg',:r,:k,:f,:a,:x)
                      ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name, type=EXCLUDED.type,
                        output_pcode=EXCLUDED.output_pcode, output_uom=EXCLUDED.output_uom,
                        recovery=EXCLUDED.recovery, cups_per_kg=EXCLUDED.cups_per_kg,
                        fruits_csv=EXCLUDED.fruits_csv, additives_json=EXCLUDED.additives_json, note=EXCLUDED.note
                    """, {"c":code.strip(),"n":name.strip(),"t":typ,"o":output_pcode,
                          "r": (float(rec) if typ=="COT" else 1.0), "k":float(cups),
                          "f": src_code, "a": adds.strip(), "x": note})
                    write_audit(conn,"FORMULA_UPSERT",code); st.success("OK"); st.rerun()
        pick = st.selectbox("Xoá CT", ["—"]+[r["code"] for _,r in df.iterrows()], index=0, key="del_ct")
        if pick!="—" and st.button("Xoá công thức"):
            run_sql(conn, "DELETE FROM formulas WHERE code=:c", {"c":pick}); st.rerun()

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
