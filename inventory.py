# inventory.py
from datetime import datetime, date
import streamlit as st
import pandas as pd
from core import fetch_df, run_sql, write_audit
from finance import avg_cost, inv_valuation, onhand_qty

# ===============================
# Nhập kho
# ===============================
def tab_in(conn, user):
    st.subheader("📥 Nhập kho")
    store = st.session_state.get("store","")
    dfp = fetch_df(conn, "SELECT code,name,cat_code,uom,cups_per_kg FROM products ORDER BY name")
    opts = ["— Chọn —"] + [f"{r['code']} — {r['name']}" for _,r in dfp.iterrows()]
    pick = st.selectbox("Sản phẩm nhập", opts, key="in_pick")
    if pick=="— Chọn —": return
    pcode = pick.split(" — ",1)[0]
    row = dfp[dfp["code"]==pcode].iloc[0].to_dict()

    qty  = st.number_input(f"Số lượng ({row['uom']})", min_value=0.0, step=0.1)
    price= st.number_input("Đơn giá nhập", min_value=0.0, step=1000.0)
    note = st.text_input("Ghi chú")

    if st.button("💾 Ghi nhập", type="primary"):
        run_sql(conn, """
            INSERT INTO transactions(store_code,pcode,qty,type,price_in,note,ts)
            VALUES (:s,:p,:q,'IN',:pr,:n,NOW())
        """, {"s": store, "p": pcode, "q": qty, "pr": price, "n": note})
        write_audit(conn, "INVENTORY_IN", f"{pcode}-{qty}-{price}")
        st.success("Đã nhập kho"); st.rerun()

# ===============================
# Xuất kho
# ===============================
def tab_out(conn, user):
    st.subheader("📤 Xuất kho")
    store = st.session_state.get("store","")
    dfp = fetch_df(conn, "SELECT code,name,cat_code,uom FROM products ORDER BY name")
    opts = ["— Chọn —"] + [f"{r['code']} — {r['name']}" for _,r in dfp.iterrows()]
    pick = st.selectbox("Sản phẩm xuất", opts, key="out_pick")
    if pick=="— Chọn —": return
    pcode = pick.split(" — ",1)[0]
    row = dfp[dfp["code"]==pcode].iloc[0].to_dict()

    qty  = st.number_input(f"Số lượng ({row['uom']})", min_value=0.0, step=0.1)
    note = st.text_input("Lý do xuất")

    if st.button("💾 Ghi xuất", type="primary"):
        onhand = onhand_qty(conn, store, pcode)
        if qty > onhand:
            st.error(f"Tồn hiện tại {onhand}, không đủ xuất!")
            return
        run_sql(conn, """
            INSERT INTO transactions(store_code,pcode,qty,type,note,ts)
            VALUES (:s,:p,:q,'OUT',:n,NOW())
        """, {"s": store, "p": pcode, "q": qty, "n": note})
        write_audit(conn, "INVENTORY_OUT", f"{pcode}-{qty}")
        st.success("Đã xuất kho"); st.rerun()

# ===============================
# Tồn kho
# ===============================
def tab_stock(conn, user):
    st.subheader("📊 Báo cáo tồn kho")
    store = st.session_state.get("store","")
    to_date = st.date_input("Tính đến ngày", value=date.today())
    to_ts = datetime.combine(to_date, datetime.max.time())

    if not store:
        st.warning("Chọn cửa hàng ở sidebar trước khi xem tồn")
        return

    df = inv_valuation(conn, store, to_ts=to_ts)
    if df.empty:
        st.info("Không có tồn")
        return

    # Hiển thị thêm số cốc nếu là CỐT hoặc MỨT
    df_show = df.rename(columns={"code":"Mã SP","name":"Tên SP","cat_code":"Nhóm",
                                 "onhand":"Số lượng","avg_cost":"Giá vốn",
                                 "value":"Thành tiền","cups":"Số cốc"})
    st.dataframe(df_show, use_container_width=True, height=400)
    st.success(f"Tổng giá trị tồn: {df['value'].sum():,.0f}")

# ===============================
# Kiểm kê
# ===============================
def tab_audit(conn, user):
    st.subheader("📋 Kiểm kê kho")
    store = st.session_state.get("store","")
    dfp = fetch_df(conn, "SELECT code,name,uom FROM products ORDER BY name")
    opts = ["— Chọn —"] + [f"{r['code']} — {r['name']}" for _,r in dfp.iterrows()]
    pick = st.selectbox("Chọn sản phẩm kiểm kê", opts, key="kk_pick")
    if pick=="— Chọn —": return
    pcode = pick.split(" — ",1)[0]
    row = dfp[dfp["code"]==pcode].iloc[0].to_dict()
    system = onhand_qty(conn, store, pcode)

    st.info(f"Tồn hệ thống hiện tại: **{system} {row['uom']}**")
    actual = st.number_input("Số lượng thực tế kiểm kê", min_value=0.0, step=0.1)
    diff = actual - system
    if st.button("⚖️ Cập nhật chênh lệch", type="primary"):
        if abs(diff) < 1e-9:
            st.info("Không chênh lệch.")
            return
        if diff > 0:
            run_sql(conn, """
                INSERT INTO transactions(store_code,pcode,qty,type,note,ts)
                VALUES (:s,:p,:q,'IN','Điều chỉnh kiểm kê',NOW())
            """, {"s": store, "p": pcode, "q": diff})
        else:
            run_sql(conn, """
                INSERT INTO transactions(store_code,pcode,qty,type,note,ts)
                VALUES (:s,:p,:q,'OUT','Điều chỉnh kiểm kê',NOW())
            """, {"s": store, "p": pcode, "q": -diff})
        write_audit(conn, "STOCK_AUDIT", f"{pcode} {diff}")
        st.success("Đã điều chỉnh."); st.rerun()

# ===============================
# ENTRY PAGE KHO
# ===============================
def page_inventory(conn, user):
    st.markdown("## 🏪 Kho")
    tabs = st.tabs(["Nhập kho","Xuất kho","Tồn kho","Kiểm kê"])
    with tabs[0]:
        tab_in(conn, user)
    with tabs[1]:
        tab_out(conn, user)
    with tabs[2]:
        tab_stock(conn, user)
    with tabs[3]:
        tab_audit(conn, user)
