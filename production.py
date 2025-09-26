# production.py — Module 3: Sản xuất
import streamlit as st
from core import run_sql, fetch_df, write_audit

# ========== Tab 1: Sản xuất CỐT ==========
def tab_cot(conn, user):
    st.markdown("### 🏭 Sản xuất CỐT (1 bước)")

    # Chọn công thức loại COT
    df_ct = fetch_df(conn, "SELECT code,name,output_pcode,recovery FROM formulas WHERE type='COT' ORDER BY name")
    if df_ct.empty:
        st.warning("⚠️ Chưa có công thức CỐT trong danh mục.")
        return

    pick = st.selectbox("Công thức CỐT", [f"{r['code']} — {r['name']}" for _,r in df_ct.iterrows()], index=0)
    ct_code = pick.split(" — ",1)[0]
    ct = df_ct[df_ct["code"]==ct_code].iloc[0]

    # Lượng đầu vào & sơ chế
    kg_tho = st.number_input("Kg trái cây thô (xuất kho)", min_value=0.0, step=0.1, value=0.0)
    kg_ss  = st.number_input("Kg sau sơ chế", min_value=0.0, step=0.1, value=0.0)

    if st.button("➡️ Bắt đầu sản xuất CỐT", type="primary"):
        if kg_tho<=0 or kg_ss<=0:
            st.error("Nhập đủ kg thô và kg sơ chế.")
            return
        # Tạo mã lô
        lot_code = f"COT_{ct_code}_{st.session_state['user']['store']}_{st.session_state['user']['email']}_{st.session_state['ts']}"
        run_sql(conn, """
            INSERT INTO batches(lot_code,type,formula_code,output_pcode,store_code,status)
            VALUES (:lot,'COT',:f,:o,:s,'WIP')
        """, {"lot":lot_code,"f":ct_code,"o":ct["output_pcode"],"s":user["store"]})
        # Ghi xuất kho trái cây thô
        run_sql(conn, """
            INSERT INTO batch_inputs(lot_code,pcode,qty,kind) VALUES (:lot,:p,:q,'TRAI_CAY')
        """, {"lot":lot_code,"p":"(FRUIT_RAW)", "q":kg_tho})   # chú thích giả, bạn map thực tế
        write_audit(conn,"BATCH_COT_START",lot_code)
        st.success(f"Đã tạo lô {lot_code}, chờ hoàn tất.")
        st.rerun()

    # Hoàn tất lô WIP
    df_wip = fetch_df(conn,"SELECT lot_code,formula_code FROM batches WHERE type='COT' AND status='WIP'")
    if not df_wip.empty:
        st.markdown("#### ✅ Hoàn tất lô CỐT")
        pick2 = st.selectbox("Chọn lô WIP", df_wip["lot_code"].tolist(), index=0, key="lot_cot_done")
        lot = df_wip[df_wip["lot_code"]==pick2].iloc[0]
        qty_out = st.number_input("Số kg thành phẩm CỐT", min_value=0.0, step=0.1, value=0.0, key="cot_qty_out")
        if st.button("Hoàn tất lô CỐT"):
            run_sql(conn,"UPDATE batches SET status='DONE', finished_at=NOW() WHERE lot_code=:lot",{"lot":pick2})
            run_sql(conn, """
                INSERT INTO stocks(store_code,ts,pcode,qty,price,reason,lot_code)
                VALUES (:s,NOW(),:p,:q,0,'NHAP_TP_COT',:lot)
            """, {"s":user["store"],"p":lot["formula_code"],"q":qty_out,"lot":pick2})
            write_audit(conn,"BATCH_COT_DONE",pick2)
            st.success(f"Đã nhập kho thành phẩm từ lô {pick2}.")


# ========== Tab 2: MỨT từ Trái Cây ==========
def tab_mut_tc(conn, user):
    st.markdown("### 🍯 Mứt từ Trái Cây (2 bước)")

    df_ct = fetch_df(conn,"SELECT code,name,output_pcode FROM formulas WHERE type='MUT' AND note LIKE 'SRC=TRAI_CAY%'")
    if df_ct.empty:
        st.warning("⚠️ Chưa có công thức MỨT từ trái cây.")
        return
    pick = st.selectbox("Công thức MUT-TC", [f"{r['code']} — {r['name']}" for _,r in df_ct.iterrows()])
    ct_code = pick.split(" — ",1)[0]
    ct = df_ct[df_ct["code"]==ct_code].iloc[0]

    kg_tho = st.number_input("Kg trái cây thô", min_value=0.0, step=0.1)
    kg_ss  = st.number_input("Kg sau sơ chế", min_value=0.0, step=0.1)

    if st.button("➡️ Bắt đầu lô MỨT-TC"):
        if kg_tho<=0 or kg_ss<=0:
            st.error("Thiếu số liệu.")
            return
        lot_code = f"MUTTC_{ct_code}_{user['store']}"
        run_sql(conn,"INSERT INTO batches(lot_code,type,formula_code,output_pcode,store_code,status) VALUES (:l,'MUT_TC',:f,:o,:s,'WIP')",
               {"l":lot_code,"f":ct_code,"o":ct["output_pcode"],"s":user["store"]})
        run_sql(conn,"INSERT INTO batch_inputs(lot_code,pcode,qty,kind) VALUES (:l,:p,:q,'TRAI_CAY')",
               {"l":lot_code,"p":"(FRUIT_RAW)","q":kg_tho})
        write_audit(conn,"BATCH_MUT_TC_START",lot_code)
        st.success(f"Đã tạo lô {lot_code}."); st.rerun()

    df_wip = fetch_df(conn,"SELECT lot_code FROM batches WHERE type='MUT_TC' AND status='WIP'")
    if not df_wip.empty:
        st.markdown("#### ✅ Hoàn tất MỨT-TC")
        pick2 = st.selectbox("Chọn lô WIP", df_wip["lot_code"].tolist())
        qty_out = st.number_input("Số kg TP MỨT", min_value=0.0, step=0.1, value=0.0)
        if st.button("Hoàn tất lô MỨT-TC"):
            run_sql(conn,"UPDATE batches SET status='DONE', finished_at=NOW() WHERE lot_code=:lot",{"lot":pick2})
            run_sql(conn,"INSERT INTO stocks(store_code,ts,pcode,qty,price,reason,lot_code) VALUES (:s,NOW(),:p,:q,0,'NHAP_TP_MUT_TC',:lot)",
                   {"s":user["store"],"p":ct["output_pcode"],"q":qty_out,"lot":pick2})
            write_audit(conn,"BATCH_MUT_TC_DONE",pick2)
            st.success("Đã nhập kho TP.")


# ========== Tab 3: MỨT từ CỐT ==========
def tab_mut_ct(conn, user):
    st.markdown("### 🍯 Mứt từ CỐT (2 bước)")

    df_ct = fetch_df(conn,"SELECT code,name,output_pcode FROM formulas WHERE type='MUT' AND note LIKE 'SRC=COT%'")
    if df_ct.empty:
        st.warning("⚠️ Chưa có công thức MỨT từ CỐT.")
        return
    pick = st.selectbox("Công thức MUT-CT", [f"{r['code']} — {r['name']}" for _,r in df_ct.iterrows()])
    ct_code = pick.split(" — ",1)[0]
    ct = df_ct[df_ct["code"]==ct_code].iloc[0]

    kg_cot = st.number_input("Kg CỐT xuất kho", min_value=0.0, step=0.1)
    if st.button("➡️ Bắt đầu lô MỨT-CT"):
        if kg_cot<=0:
            st.error("Thiếu số liệu.")
            return
        lot_code = f"MUTCT_{ct_code}_{user['store']}"
        run_sql(conn,"INSERT INTO batches(lot_code,type,formula_code,output_pcode,store_code,status) VALUES (:l,'MUT_CT',:f,:o,:s,'WIP')",
               {"l":lot_code,"f":ct_code,"o":ct["output_pcode"],"s":user["store"]})
        run_sql(conn,"INSERT INTO batch_inputs(lot_code,pcode,qty,kind) VALUES (:l,:p,:q,'COT')",
               {"l":lot_code,"p":"(COT_RAW)","q":kg_cot})
        write_audit(conn,"BATCH_MUT_CT_START",lot_code)
        st.success(f"Đã tạo lô {lot_code}."); st.rerun()

    df_wip = fetch_df(conn,"SELECT lot_code FROM batches WHERE type='MUT_CT' AND status='WIP'")
    if not df_wip.empty:
        st.markdown("#### ✅ Hoàn tất MỨT-CT")
        pick2 = st.selectbox("Chọn lô WIP", df_wip["lot_code"].tolist())
        qty_out = st.number_input("Số kg TP MỨT", min_value=0.0, step=0.1, value=0.0)
        if st.button("Hoàn tất lô MỨT-CT"):
            run_sql(conn,"UPDATE batches SET status='DONE', finished_at=NOW() WHERE lot_code=:lot",{"lot":pick2})
            run_sql(conn,"INSERT INTO stocks(store_code,ts,pcode,qty,price,reason,lot_code) VALUES (:s,NOW(),:p,:q,0,'NHAP_TP_MUT_CT',:lot)",
                   {"s":user["store"],"p":ct["output_pcode"],"q":qty_out,"lot":pick2})
            write_audit(conn,"BATCH_MUT_CT_DONE",pick2)
            st.success("Đã nhập kho TP.")


# ========== PAGE SẢN XUẤT ==========
def page_production(conn, user):
    tabs = st.tabs(["CỐT", "MỨT từ TRÁI CÂY", "MỨT từ CỐT"])
    with tabs[0]: tab_cot(conn, user)
    with tabs[1]: tab_mut_tc(conn, user)
    with tabs[2]: tab_mut_ct(conn, user)
