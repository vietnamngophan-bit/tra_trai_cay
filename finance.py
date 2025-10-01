# finance.py
from datetime import datetime, date
import math
import streamlit as st
import pandas as pd
from core import fetch_df, run_sql, write_audit

# =========================
# Helpers: tồn kho & giá trị
# =========================
def onhand_qty(conn, store, pcode, to_ts=None):
    params = {"s": store, "p": pcode}
    where_ts = ""
    if to_ts:
        where_ts = " AND ts <= :t "
        params["t"] = to_ts
    df = fetch_df(conn, f"""
        SELECT
          COALESCE(SUM(CASE WHEN type='IN'  THEN qty ELSE 0 END),0) -
          COALESCE(SUM(CASE WHEN type='OUT' THEN qty ELSE 0 END),0) AS onhand
        FROM transactions
        WHERE store_code=:s AND pcode=:p {where_ts}
    """, params)
    return 0.0 if df.empty else float(df.iloc[0]["onhand"] or 0.0)

def avg_cost(conn, store, pcode, to_ts=None):
    """Bình quân gia quyền theo các dòng IN đến thời điểm to_ts (nếu có)."""
    params = {"s": store, "p": pcode}
    where_ts = ""
    if to_ts:
        where_ts = " AND ts <= :t "
        params["t"] = to_ts
    df = fetch_df(conn, f"""
        SELECT SUM(qty*price_in) AS cost, SUM(qty) AS qty
        FROM transactions
        WHERE store_code=:s AND pcode=:p AND type='IN'
              AND price_in IS NOT NULL AND price_in>0 {where_ts}
    """, params)
    if df.empty: 
        pr = fetch_df(conn, "SELECT price_ref FROM products WHERE code=:p", {"p": pcode})
        return float(pr.iloc[0]["price_ref"] or 0.0) if not pr.empty else 0.0
    cost = float(df.iloc[0]["cost"] or 0.0); qty = float(df.iloc[0]["qty"] or 0.0)
    if qty > 0:
        return cost/qty
    pr = fetch_df(conn, "SELECT price_ref FROM products WHERE code=:p", {"p": pcode})
    return float(pr.iloc[0]["price_ref"] or 0.0) if not pr.empty else 0.0

def inv_valuation(conn, store, to_ts=None):
    """Trả về DF: pcode, name, cat_code, onhand, avg_cost, value, cups (nếu có)."""
    # lấy danh sách sản phẩm có phát sinh hoặc có onhand > 0
    params = {"s": store}
    where_ts = "" if not to_ts else " AND ts <= :t "
    if to_ts: params["t"] = to_ts

    df_codes = fetch_df(conn, f"""
        SELECT DISTINCT pcode
        FROM transactions
        WHERE store_code=:s {where_ts}
    """, params)

    if df_codes.empty:
        return pd.DataFrame(columns=["code","name","cat_code","onhand","avg_cost","value","cups"])

    pcodes = tuple(df_codes["pcode"].tolist())
    df_p = fetch_df(conn, f"""
        SELECT code,name,cat_code,uom,cups_per_kg
        FROM products
        WHERE code = ANY(:codes)
        ORDER BY name
    """, {"codes": list(pcodes)})

    rows = []
    for r in df_p.itertuples():
        q = onhand_qty(conn, store, r.code, to_ts=to_ts)
        if abs(q) < 1e-9:
            continue
        c = avg_cost(conn, store, r.code, to_ts=to_ts)
        v = q * c
        cups = (q * float(r.cups_per_kg or 0.0)) if (r.cat_code in ["COT","MUT"]) else 0
        rows.append({"code": r.code, "name": r.name, "cat_code": r.cat_code,
                     "onhand": q, "avg_cost": c, "value": v, "cups": cups})
    df = pd.DataFrame(rows)
    if not df.empty:
        df["value"] = df["value"].round(0)
        df["avg_cost"] = df["avg_cost"].round(0)
    return df

# =========================
# Doanh thu (Sổ quỹ)
# =========================
def tab_revenue(conn, user):
    st.markdown("### 💰 Doanh thu (Sổ quỹ)")
    st.caption("Chỉ thu **Tiền mặt** hoặc **Chuyển khoản**. Không gắn doanh số theo sản phẩm.")

    d1, d2 = st.columns(2)
    with d1:
        dt_from = st.date_input("Từ ngày", value=date.today().replace(day=1))
    with d2:
        dt_to   = st.date_input("Đến ngày", value=date.today())

    method = st.radio("Kênh thu", ["Tất cả","Tiền mặt","Chuyển khoản"], horizontal=True)
    store = st.session_state.get("store", "")
    st.caption(f"Cửa hàng: **{store or '— Tất cả —'}**")

    where = " WHERE 1=1 "
    params = {}
    if store:
        where += " AND store_code=:s "
        params["s"] = store
    if dt_from: 
        where += " AND ts >= :f "
        params["f"] = datetime.combine(dt_from, datetime.min.time())
    if dt_to:
        where += " AND ts <= :t "
        params["t"] = datetime.combine(dt_to, datetime.max.time())
    if method != "Tất cả":
        where += " AND method=:m "
        params["m"] = ("CASH" if method=="Tiền mặt" else "BANK")

    df = fetch_df(conn, f"""
        SELECT id, ts, store_code, method, io, amount, note, actor
        FROM cashbook
        {where}
        ORDER BY ts DESC
    """, params)
    st.dataframe(df, use_container_width=True, height=320)

    st.markdown("#### ➕ Thêm / sửa")
    with st.form("fm_rev", clear_on_submit=True):
        c1,c2,c3 = st.columns([1,1,2])
        with c1:
            ts = st.date_input("Ngày", value=date.today())
            kieu = st.selectbox("Thu/Chi", ["Thu","Chi"])
        with c2:
            method2 = st.selectbox("Kênh", ["Tiền mặt","Chuyển khoản"])
            amt = st.number_input("Số tiền", min_value=0.0, step=1000.0)
        with c3:
            note = st.text_input("Ghi chú")
        ok = st.form_submit_button("Lưu", type="primary")
    if ok:
        io = "IN" if kieu=="Thu" else "OUT"
        run_sql(conn, """
            INSERT INTO cashbook(ts, store_code, method, io, amount, note, actor)
            VALUES (:ts, :s, :m, :io, :a, :n, :u)
        """, {
            "ts": datetime.combine(ts, datetime.min.time()),
            "s": store or None,
            "m": ("CASH" if method2=="Tiền mặt" else "BANK"),
            "io": io, "a": float(amt), "n": note.strip(),
            "u": user["email"]
        })
        write_audit(conn, "CASHBOOK_INSERT", f"{kieu}-{method2}-{amt}")
        st.success("Đã ghi."); st.rerun()

    st.markdown("#### 🗑️ Xoá bản ghi")
    del_id = st.selectbox("Chọn ID", ["—"] + df["id"].astype(str).tolist() if not df.empty else ["—"])
    if del_id != "—" and st.button("Xoá"):
        run_sql(conn, "DELETE FROM cashbook WHERE id=:i", {"i": int(del_id)})
        write_audit(conn, "CASHBOOK_DELETE", str(del_id))
        st.success("Đã xoá."); st.rerun()

    st.markdown("#### 📊 Tổng hợp kỳ")
    if not df.empty:
        s_in  = float(df[df["io"]=="IN"]["amount"].sum() or 0.0)
        s_out = float(df[df["io"]=="OUT"]["amount"].sum() or 0.0)
        st.info(f"**Thu:** {s_in:,.0f} — **Chi:** {s_out:,.0f} — **Chênh:** {(s_in-s_out):,.0f}")

# =========================
# Báo cáo tài chính
# =========================
def tab_reports(conn, user):
    st.markdown("### 📈 Báo cáo")
    sub = st.tabs(["Tồn kho (có giá trị)","Cân đối kế toán","Lưu chuyển tiền tệ"])

    # ----- Tồn kho có giá trị -----
    with sub[0]:
        to_date = st.date_input("Tính đến ngày", value=date.today())
        to_ts = datetime.combine(to_date, datetime.max.time())
        store = st.session_state.get("store","")
        st.caption(f"Cửa hàng: **{store or '— Tất cả —'}** (báo cáo theo cửa hàng hiện chọn)")
        if not store:
            st.warning("Chọn 1 cửa hàng ở sidebar để tính tồn giá trị."); return
        df = inv_valuation(conn, store, to_ts=to_ts)
        if df.empty:
            st.info("Không có tồn."); 
        else:
            df_show = df.rename(columns={"code":"Mã","name":"Tên","cat_code":"Nhóm",
                                         "onhand":"SL tồn","avg_cost":"Giá vốn","value":"Giá trị","cups":"Số cốc (ước)"})
            st.dataframe(df_show, use_container_width=True)
            st.success(f"**Tổng giá trị tồn**: {df['value'].sum():,.0f}")

    # ----- Cân đối kế toán -----
    with sub[1]:
        to_date = st.date_input("Tính đến ngày (BS)", value=date.today(), key="bs_date")
        to_ts = datetime.combine(to_date, datetime.max.time())
        store = st.session_state.get("store","")

        # Tiền (cashbook)
        params = {"t": to_ts}
        wh = " WHERE ts<=:t "
        if store:
            wh += " AND store_code=:s "; params["s"] = store
        df_cash = fetch_df(conn, f"""
            SELECT
              COALESCE(SUM(CASE WHEN io='IN'  THEN amount ELSE 0 END),0) -
              COALESCE(SUM(CASE WHEN io='OUT' THEN amount ELSE 0 END),0) AS bal
            FROM cashbook
            {wh}
        """, params)
        cash_bal = 0.0 if df_cash.empty else float(df_cash.iloc[0]["bal"] or 0.0)

        # Hàng tồn kho
        inv_val = 0.0
        if store:
            df_val = inv_valuation(conn, store, to_ts=to_ts)
            inv_val = 0.0 if df_val.empty else float(df_val["value"].sum())

        # TSCĐ (nguyên giá & KH lũy kế đến ngày)
        df_assets = fetch_df(conn, """
            SELECT id, name, cost, start_date, life_months, salvage, method
            FROM assets
            WHERE (:s IS NULL OR store_code=:s)
        """, {"s": store if store else None})
        gross = float(df_assets["cost"].sum() or 0.0) if not df_assets.empty else 0.0
        dep = 0.0
        if not df_assets.empty:
            for _,a in df_assets.iterrows():
                dep += _accum_dep_till(a, to_ts)

        tscd_net = max(gross - dep, 0.0)

        assets_total = cash_bal + inv_val + tscd_net
        equity = assets_total  # chưa xét nợ phải trả → vốn CSH = tổng TS

        st.subheader("Cân đối")
        st.markdown(f"""
        **Tài sản:**
        - Tiền: **{cash_bal:,.0f}**
        - Hàng tồn kho (giá vốn): **{inv_val:,.0f}**
        - TSCĐ (nguyên giá): **{gross:,.0f}**
        - Khấu hao lũy kế: **{dep:,.0f}**
        - TSCĐ thuần: **{tscd_net:,.0f}**

        **Tổng tài sản:** **{assets_total:,.0f}**

        **Nguồn vốn:**
        - Vốn CSH (tạm tính): **{equity:,.0f}**
        """)

    # ----- Lưu chuyển tiền tệ -----
    with sub[2]:
        d1, d2 = st.columns(2)
        with d1: from_date = st.date_input("Từ ngày (CF)", value=date.today().replace(day=1))
        with d2: to_date   = st.date_input("Đến ngày (CF)", value=date.today())
        store = st.session_state.get("store","")
        params = {"f": datetime.combine(from_date, datetime.min.time()),
                  "t": datetime.combine(to_date, datetime.max.time())}
        wh = " WHERE ts BETWEEN :f AND :t "
        if store:
            wh += " AND store_code=:s "; params["s"] = store
        df = fetch_df(conn, f"""
            SELECT DATE_TRUNC('day', ts) AS d, io, SUM(amount) AS amt
            FROM cashbook
            {wh}
            GROUP BY 1,2
            ORDER BY 1
        """, params)
        if df.empty:
            st.info("Không có phát sinh."); return
        pv = df.pivot_table(index="d", columns="io", values="amt", aggfunc="sum").fillna(0)
        pv["NET"] = pv.get("IN",0) - pv.get("OUT",0)
        st.dataframe(pv, use_container_width=True)
        st.success(f"**Tổng Thu:** {pv.get('IN',pd.Series([0])).sum():,.0f} — "
                   f"**Tổng Chi:** {pv.get('OUT',pd.Series([0])).sum():,.0f} — "
                   f"**Dòng tiền thuần:** {pv['NET'].sum():,.0f}")

# =========================
# TSCD
# =========================
def _accum_dep_till(asset_row, to_ts):
    """KH lũy kế đến ngày to_ts (đường thẳng)."""
    method = (asset_row.get("method") or "SL").upper()
    cost = float(asset_row.get("cost") or 0.0)
    life = int(asset_row.get("life_months") or 0)
    salvage = float(asset_row.get("salvage") or 0.0)
    start = asset_row.get("start_date")
    if not start or life <= 0 or cost <= 0:
        return 0.0
    if isinstance(start, str):
        start = pd.to_datetime(start)
    months_passed = (to_ts.year - start.year) * 12 + (to_ts.month - start.month) + 1
    months_passed = max(0, min(months_passed, life))
    if method == "SL":
        dep_per_month = max((cost - salvage) / life, 0.0)
        return dep_per_month * months_passed
    # dự phòng cho phương pháp khác
    dep_per_month = max((cost - salvage) / life, 0.0)
    return dep_per_month * months_passed

def tab_assets(conn, user):
    st.markdown("### 🏭 Tài sản cố định (TSCD)")
    df = fetch_df(conn, """
        SELECT id, code, name, cost, start_date, life_months, salvage, method, store_code, note
        FROM assets
        WHERE (:s IS NULL OR store_code=:s)
        ORDER BY start_date DESC
    """, {"s": st.session_state.get("store", None)})
    st.dataframe(df, use_container_width=True, height=300)

    st.markdown("#### ➕ Thêm TSCD")
    with st.form("fm_asset", clear_on_submit=True):
        c1,c2,c3 = st.columns(3)
        with c1:
            code = st.text_input("Mã TS")
            name = st.text_input("Tên TS")
            cost = st.number_input("Nguyên giá", min_value=0.0, step=1_000_000.0)
        with c2:
            start = st.date_input("Ngày bắt đầu sử dụng", value=date.today())
            life  = st.number_input("Thời gian KH (tháng)", min_value=1, step=1)
            salvage = st.number_input("Giá trị còn lại (nếu có)", min_value=0.0, step=1_000_000.0, value=0.0)
        with c3:
            method = st.selectbox("Phương pháp", ["SL"])  # Straight-Line
            note   = st.text_input("Ghi chú")
        ok = st.form_submit_button("Lưu TS", type="primary")
    if ok:
        run_sql(conn, """
            INSERT INTO assets(code,name,cost,start_date,life_months,salvage,method,store_code,note)
            VALUES (:c,:n,:cost,:d,:life,:sal,:m,:s,:note)
        """, {"c": code.strip(), "n": name.strip(), "cost": cost,
              "d": datetime.combine(start, datetime.min.time()),
              "life": int(life), "sal": salvage, "m": method,
              "s": st.session_state.get("store", None), "note": note.strip()})
        write_audit(conn, "ASSET_INSERT", code)
        st.success("Đã thêm TSCD."); st.rerun()

    st.markdown("#### 🗑️ Xoá TSCD")
    del_id = st.selectbox("Chọn ID TS", ["—"] + (df["id"].astype(str).tolist() if not df.empty else []))
    if del_id != "—" and st.button("Xoá TS"):
        run_sql(conn, "DELETE FROM assets WHERE id=:i", {"i": int(del_id)})
        write_audit(conn, "ASSET_DELETE", str(del_id))
        st.success("Đã xoá."); st.rerun()

    st.markdown("#### 📉 Khấu hao lũy kế tới hôm nay")
    if not df.empty:
        rows = []
        to_ts = datetime.combine(date.today(), datetime.max.time())
        for _,a in df.iterrows():
            dep = _accum_dep_till(a, to_ts)
            rows.append({
                "id": a["id"], "code": a["code"], "name": a["name"],
                "cost": float(a["cost"] or 0.0), "accum_dep": dep,
                "net": max(float(a["cost"] or 0.0) - dep, 0.0)
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True)

# =========================
# Lương (đơn giản, ghi vào quỹ)
# =========================
def tab_payroll(conn, user):
    st.markdown("### 👥 Lương nhân viên (đơn giản)")
    d1, d2 = st.columns(2)
    with d1: dt_from = st.date_input("Từ ngày", value=date.today().replace(day=1))
    with d2: dt_to   = st.date_input("Đến ngày", value=date.today())

    store = st.session_state.get("store", "")
    params = {"f": datetime.combine(dt_from, datetime.min.time()),
              "t": datetime.combine(dt_to, datetime.max.time()),
              "s": store if store else None}
    df = fetch_df(conn, """
        SELECT id, ts, store_code, staff, amount, note, actor
        FROM payroll
        WHERE ts BETWEEN :f AND :t
          AND (:s IS NULL OR store_code=:s)
        ORDER BY ts DESC
    """, params)
    st.dataframe(df, use_container_width=True, height=280)

    st.markdown("#### ➕ Trả lương")
    with st.form("fm_pay", clear_on_submit=True):
        c1,c2,c3 = st.columns([1,1,2])
        with c1:
            ts = st.date_input("Ngày", value=date.today())
            staff = st.text_input("Nhân viên")
        with c2:
            amount = st.number_input("Số tiền", min_value=0.0, step=100_000.0)
            method = st.selectbox("Kênh chi", ["Tiền mặt","Chuyển khoản"])
        with c3:
            note = st.text_input("Ghi chú")
        ok = st.form_submit_button("Ghi lương + sổ quỹ", type="primary")
    if ok:
        # ghi payroll
        run_sql(conn, """
            INSERT INTO payroll(ts, store_code, staff, amount, note, actor)
            VALUES (:ts, :s, :st, :a, :n, :u)
        """, {"ts": datetime.combine(ts, datetime.min.time()),
              "s": store if store else None, "st": staff.strip(),
              "a": float(amount), "n": note.strip(), "u": user["email"]})
        # ghi quỹ (chi)
        run_sql(conn, """
            INSERT INTO cashbook(ts, store_code, method, io, amount, note, actor)
            VALUES (:ts, :s, :m, 'OUT', :a, :n, :u)
        """, {"ts": datetime.combine(ts, datetime.min.time()),
              "s": store if store else None,
              "m": ("CASH" if method=="Tiền mặt" else "BANK"),
              "a": float(amount), "n": f"Chi lương {staff}: {note}", "u": user["email"]})
        write_audit(conn, "PAYROLL_AND_CASH_OUT", f"{staff}-{amount}")
        st.success("Đã ghi."); st.rerun()

    st.markdown("#### 🗑️ Xoá")
    del_id = st.selectbox("Chọn ID lương", ["—"] + (df["id"].astype(str).tolist() if not df.empty else []))
    if del_id != "—" and st.button("Xoá bản ghi lương"):
        run_sql(conn, "DELETE FROM payroll WHERE id=:i", {"i": int(del_id)})
        write_audit(conn, "PAYROLL_DELETE", str(del_id))
        st.success("Đã xoá."); st.rerun()

# =========================
# ENTRY PAGE FINANCE
# =========================
def page_finance(conn, user):
    st.markdown("## 💼 Tài chính")
    tabs = st.tabs(["Doanh thu", "Báo cáo", "TSCD", "Lương"])
    with tabs[0]:
        tab_revenue(conn, user)
    with tabs[1]:
        tab_reports(conn, user)
    with tabs[2]:
        tab_assets(conn, user)
    with tabs[3]:
        tab_payroll(conn, user)
