# production.py — Module 3: Sản xuất (CỐT 1 bước; MỨT 2 nguồn)
import time
import pandas as pd
import streamlit as st
from core import run_sql, fetch_df, write_audit

def _new_lot(prefix: str, store: str) -> str:
    # lot_code ngắn gọn, duy nhất theo mili-giây
    return f"{prefix}_{store}_{int(time.time()*1000)}"

def _additives_of_formula(conn, formula_code: str) -> pd.DataFrame:
    # Lấy phụ gia (định mức / 1kg SƠ CHẾ)
    df = fetch_df(conn, """
        SELECT pcode, qty_per_kg
        FROM formula_inputs
        WHERE formula_code = :c AND kind = 'PHU_GIA'
        ORDER BY pcode
    """, {"c": formula_code})
    if df.empty:
        df = pd.DataFrame({"pcode":[], "qty_per_kg":[]})
    return df

def _raw_candidates(conn, src_kind: str) -> pd.DataFrame:
    # Liệt kê NVL chính để người dùng chọn và nhập số kg thực xuất
    cat = "TRAI_CAY" if src_kind == "TRAI_CAY" else "COT"
    return fetch_df(conn, """
        SELECT code AS pcode, name
        FROM products
        WHERE cat_code = :cat
        ORDER BY name
    """, {"cat": cat})

def _show_raw_input_picker(df_raw: pd.DataFrame, key_prefix: str):
    st.markdown("##### Nguyên liệu chính (nhập số lượng **thực xuất**)")
    opts = [f"{r.pcode} — {r.name}" for r in df_raw.itertuples()]
    picked = st.multiselect("Chọn NVL chính", opts, key=f"{key_prefix}_raw_pick")
    rows = []
    for item in picked:
        pcode = item.split(" — ", 1)[0]
        q = st.number_input(f"{item} — số kg xuất", min_value=0.0, step=0.1, value=0.0,
                            key=f"{key_prefix}_raw_qty_{pcode}")
        if q > 0:
            rows.append({"pcode": pcode, "qty": float(q)})
    return pd.DataFrame(rows)

def _preview_additives(df_add: pd.DataFrame, kg_ss: float) -> pd.DataFrame:
    if df_add.empty or kg_ss <= 0:
        return pd.DataFrame(columns=["pcode", "qty"])
    out = df_add.copy()
    out["qty"] = out["qty_per_kg"].astype(float) * float(kg_ss)
    return out[["pcode", "qty"]]

# ==================== TAB: CỐT (1 bước) ====================
def tab_cot(conn, user):
    st.markdown("### 🏭 Sản xuất CỐT (1 bước)")
    df_ct = fetch_df(conn, """
        SELECT code, name, output_pcode, recovery
        FROM formulas
        WHERE type='COT'
        ORDER BY name
    """)
    if df_ct.empty:
        st.warning("Chưa có công thức CỐT.")
        return

    pick = st.selectbox("Công thức CỐT", [f"{r['code']} — {r['name']}" for _, r in df_ct.iterrows()],
                        index=0, key="cot_pick")
    ct_code = pick.split(" — ", 1)[0]
    ct = df_ct[df_ct["code"] == ct_code].iloc[0]
    df_add = _additives_of_formula(conn, ct_code)

    with st.form("fm_cot_start", clear_on_submit=True):
        c1, c2, c3 = st.columns([1,1,1])
        with c1:
            kg_tho = st.number_input("Kg trái cây **thô** (xuất kho)", min_value=0.0, step=0.1, value=0.0)
        with c2:
            kg_ss  = st.number_input("Kg **sau sơ chế**", min_value=0.0, step=0.1, value=0.0)
        with c3:
            rec    = float(ct.get("recovery") or 1.0)
            g_suggest = round(kg_ss * rec, 3)
            st.metric("Gợi ý TP (= kg sơ chế × hệ số)", g_suggest)
        df_raw = _raw_candidates(conn, "TRAI_CAY")
        df_raw_pick = _show_raw_input_picker(df_raw, "cot")
        df_add_need = _preview_additives(df_add, kg_ss)
        st.markdown("##### Phụ gia dự tính (theo **kg sau sơ chế**)")
        st.dataframe(df_add_need, use_container_width=True, hide_index=True)
        ok = st.form_submit_button("➡️ Tạo lô CỐT (WIP)", type="primary")

    if ok:
        if kg_tho <= 0 or kg_ss <= 0:
            st.error("Nhập đủ kg thô và kg sau sơ chế.")
            return
        if df_raw_pick.empty:
            st.error("Chọn ít nhất 1 NVL chính và nhập số kg xuất.")
            return
        lot = _new_lot("COT", user["store"])
        run_sql(conn, """
            INSERT INTO batches(lot_code, type, formula_code, output_pcode, store_code, status, planned_wip_kg)
            VALUES (:lot,'COT',:f,:o,:s,'WIP',:w)
        """, {"lot": lot, "f": ct_code, "o": ct["output_pcode"], "s": user["store"], "w": float(kg_ss)})
        # Lưu inputs + ghi sổ kho âm
        for r in df_raw_pick.itertuples():
            run_sql(conn, "INSERT INTO batch_inputs(lot_code,pcode,qty,kind) VALUES (:l,:p,:q,'TRAI_CAY')",
                    {"l": lot, "p": r.pcode, "q": float(r.qty)})
            run_sql(conn, """
                INSERT INTO stocks(store_code, ts, pcode, qty, price, reason, lot_code)
                VALUES (:s, NOW(), :p, :q, 0, 'SX_XUAT_TRAI_CAY', :l)
            """, {"s": user["store"], "p": r.pcode, "q": -float(r.qty), "l": lot})
        for r in df_add_need.itertuples():
            run_sql(conn, "INSERT INTO batch_inputs(lot_code,pcode,qty,kind) VALUES (:l,:p,:q,'PHU_GIA')",
                    {"l": lot, "p": r.pcode, "q": float(r.qty)})
            run_sql(conn, """
                INSERT INTO stocks(store_code, ts, pcode, qty, price, reason, lot_code)
                VALUES (:s, NOW(), :p, :q, 0, 'SX_XUAT_PHU_GIA', :l)
            """, {"s": user["store"], "p": r.pcode, "q": -float(r.qty), "l": lot})
        write_audit(conn, "BATCH_COT_START", lot)
        st.success(f"Đã tạo lô {lot}. Vào mục 'Hoàn tất' bên dưới để nhập TP.")

    # Hoàn tất
    st.markdown("#### ✅ Hoàn tất lô CỐT")
    df_wip = fetch_df(conn, "SELECT lot_code, formula_code, planned_wip_kg FROM batches WHERE type='COT' AND status='WIP' ORDER BY created_at")
    if df_wip.empty:
        st.caption("Không có lô WIP.")
        return
    lot_pick = st.selectbox("Chọn lô", df_wip["lot_code"].tolist(), key="cot_lot_done")
    lot_row = df_wip[df_wip["lot_code"] == lot_pick].iloc[0]
    # công thức của lô
    f_row = fetch_df(conn, "SELECT output_pcode, recovery FROM formulas WHERE code=:c", {"c": lot_row["formula_code"]}).iloc[0]
    g_suggest = (float(lot_row["planned_wip_kg"] or 0) * float(f_row.get("recovery") or 1.0))
    with st.form("fm_cot_finish", clear_on_submit=True):
        qty_out = st.number_input("Kg thành phẩm CỐT nhập kho", min_value=0.0, step=0.1, value=float(g_suggest))
        ok2 = st.form_submit_button("Hoàn tất lô CỐT", type="primary")
    if ok2:
        run_sql(conn, "UPDATE batches SET status='DONE', finished_at=NOW() WHERE lot_code=:l", {"l": lot_pick})
        run_sql(conn, """
            INSERT INTO stocks(store_code, ts, pcode, qty, price, reason, lot_code)
            VALUES (:s, NOW(), :p, :q, 0, 'SX_NHAP_TP_COT', :l)
        """, {"s": user["store"], "p": f_row["output_pcode"], "q": float(qty_out), "l": lot_pick})
        write_audit(conn, "BATCH_COT_DONE", lot_pick)
        st.success("Đã nhập kho thành phẩm.")

# ==================== TAB: MỨT từ TRÁI CÂY ====================
def tab_mut_tc(conn, user):
    st.markdown("### 🍯 Mứt từ Trái Cây (2 bước)")
    df_ct = fetch_df(conn, """
        SELECT code, name, output_pcode
        FROM formulas
        WHERE type='MUT' AND (note LIKE 'SRC=TRAI_CAY%' OR note IS NULL OR note='')
        ORDER BY name
    """)
    if df_ct.empty:
        st.warning("Chưa có công thức MỨT (SRC=TRAI_CAY).")
        return

    pick = st.selectbox("Công thức", [f"{r['code']} — {r['name']}" for _, r in df_ct.iterrows()], key="mut_tc_pick")
    ct_code = pick.split(" — ", 1)[0]
    ct = df_ct[df_ct["code"] == ct_code].iloc[0]
    df_add = _additives_of_formula(conn, ct_code)

    with st.form("fm_mut_tc_start", clear_on_submit=True):
        c1, c2 = st.columns(2)
        with c1:
            kg_tho = st.number_input("Kg trái cây **thô** (xuất kho)", min_value=0.0, step=0.1, value=0.0)
        with c2:
            kg_ss  = st.number_input("Kg **sau sơ chế**", min_value=0.0, step=0.1, value=0.0)
        df_raw = _raw_candidates(conn, "TRAI_CAY")
        df_raw_pick = _show_raw_input_picker(df_raw, "mut_tc")
        df_add_need = _preview_additives(df_add, kg_ss)
        st.markdown("##### Phụ gia dự tính (theo **kg sau sơ chế**)")
        st.dataframe(df_add_need, use_container_width=True, hide_index=True)
        ok = st.form_submit_button("➡️ Tạo lô MỨT-TC (WIP)", type="primary")

    if ok:
        if kg_tho <= 0 or kg_ss <= 0:
            st.error("Nhập đủ kg thô và kg sau sơ chế.")
            return
        if df_raw_pick.empty:
            st.error("Chọn ít nhất 1 NVL chính và nhập số kg xuất.")
            return
        lot = _new_lot("MUTTC", user["store"])
        run_sql(conn, """
            INSERT INTO batches(lot_code, type, formula_code, output_pcode, store_code, status, planned_wip_kg)
            VALUES (:lot,'MUT_TC',:f,:o,:s,'WIP',:w)
        """, {"lot": lot, "f": ct_code, "o": ct["output_pcode"], "s": user["store"], "w": float(kg_ss)})
        for r in df_raw_pick.itertuples():
            run_sql(conn, "INSERT INTO batch_inputs(lot_code,pcode,qty,kind) VALUES (:l,:p,:q,'TRAI_CAY')",
                    {"l": lot, "p": r.pcode, "q": float(r.qty)})
            run_sql(conn, "INSERT INTO stocks(store_code, ts, pcode, qty, price, reason, lot_code) VALUES (:s,NOW(),:p,:q,0,'SX_XUAT_TRAI_CAY',:l)",
                    {"s": user["store"], "p": r.pcode, "q": -float(r.qty), "l": lot})
        for r in df_add_need.itertuples():
            run_sql(conn, "INSERT INTO batch_inputs(lot_code,pcode,qty,kind) VALUES (:l,:p,:q,'PHU_GIA')",
                    {"l": lot, "p": r.pcode, "q": float(r.qty)})
            run_sql(conn, "INSERT INTO stocks(store_code, ts, pcode, qty, price, reason, lot_code) VALUES (:s,NOW(),:p,:q,0,'SX_XUAT_PHU_GIA',:l)",
                    {"s": user["store"], "p": r.pcode, "q": -float(r.qty), "l": lot})
        write_audit(conn, "BATCH_MUT_TC_START", lot)
        st.success(f"Đã tạo lô {lot}. Vào mục 'Hoàn tất' bên dưới để nhập TP.")

    st.markdown("#### ✅ Hoàn tất lô MỨT-TC")
    df_wip = fetch_df(conn, "SELECT lot_code FROM batches WHERE type='MUT_TC' AND status='WIP' ORDER BY created_at")
    if df_wip.empty:
        st.caption("Không có lô WIP.")
        return
    lot_pick = st.selectbox("Chọn lô", df_wip["lot_code"].tolist(), key="mut_tc_lot_done")
    with st.form("fm_mut_tc_finish", clear_on_submit=True):
        qty_out = st.number_input("Kg TP MỨT nhập kho", min_value=0.0, step=0.1, value=0.0)
        ok2 = st.form_submit_button("Hoàn tất lô MỨT-TC", type="primary")
    if ok2:
        # Lấy output_pcode của lô
        row = fetch_df(conn, "SELECT output_pcode FROM batches WHERE lot_code=:l", {"l": lot_pick}).iloc[0]
        run_sql(conn, "UPDATE batches SET status='DONE', finished_at=NOW() WHERE lot_code=:l", {"l": lot_pick})
        run_sql(conn, "INSERT INTO stocks(store_code, ts, pcode, qty, price, reason, lot_code) VALUES (:s,NOW(),:p,:q,0,'SX_NHAP_TP_MUT_TC',:l)",
                {"s": user["store"], "p": row["output_pcode"], "q": float(qty_out), "l": lot_pick})
        write_audit(conn, "BATCH_MUT_TC_DONE", lot_pick)
        st.success("Đã nhập kho thành phẩm.")

# ==================== TAB: MỨT từ CỐT ====================
def tab_mut_ct(conn, user):
    st.markdown("### 🍯 Mứt từ CỐT (2 bước)")
    df_ct = fetch_df(conn, """
        SELECT code, name, output_pcode
        FROM formulas
        WHERE type='MUT' AND note LIKE 'SRC=COT%'
        ORDER BY name
    """)
    if df_ct.empty:
        st.warning("Chưa có công thức MỨT (SRC=COT).")
        return

    pick = st.selectbox("Công thức", [f"{r['code']} — {r['name']}" for _, r in df_ct.iterrows()], key="mut_ct_pick")
    ct_code = pick.split(" — ", 1)[0]
    ct = df_ct[df_ct["code"] == ct_code].iloc[0]
    df_add = _additives_of_formula(conn, ct_code)

    with st.form("fm_mut_ct_start", clear_on_submit=True):
        kg_cot = st.number_input("Kg CỐT **xuất kho**", min_value=0.0, step=0.1, value=0.0)
        # có thể vẫn cần kg sơ chế để tính phụ gia nếu công thức định nghĩa theo kg sơ chế
        kg_ss   = st.number_input("Kg **sau sơ chế** (để tính phụ gia)", min_value=0.0, step=0.1, value=0.0)
        df_raw  = _raw_candidates(conn, "COT")
        df_raw_pick = _show_raw_input_picker(df_raw, "mut_ct")  # cho phép chọn nhiều mã CỐT nếu cần
        df_add_need = _preview_additives(df_add, kg_ss)
        st.markdown("##### Phụ gia dự tính (theo **kg sau sơ chế**)")
        st.dataframe(df_add_need, use_container_width=True, hide_index=True)
        ok = st.form_submit_button("➡️ Tạo lô MỨT-CT (WIP)", type="primary")

    if ok:
        if kg_cot <= 0 and df_raw_pick.empty:
            st.error("Nhập kg CỐT xuất kho (hoặc chọn mã CỐT và nhập số kg).")
            return
        lot = _new_lot("MUTCT", user["store"])
        run_sql(conn, """
            INSERT INTO batches(lot_code, type, formula_code, output_pcode, store_code, status, planned_wip_kg)
            VALUES (:lot,'MUT_CT',:f,:o,:s,'WIP',:w)
        """, {"lot": lot, "f": ct_code, "o": ct["output_pcode"], "s": user["store"], "w": float(kg_ss)})

        # nếu nhập trực tiếp kg_cot, ghi 1 dòng; nếu chọn chi tiết ở picker, sẽ ghi theo picker
        if kg_cot > 0:
            run_sql(conn, "INSERT INTO batch_inputs(lot_code,pcode,qty,kind) VALUES (:l,:p,:q,'COT')",
                    {"l": lot, "p": ct["output_pcode"], "q": float(kg_cot)})
            run_sql(conn, "INSERT INTO stocks(store_code, ts, pcode, qty, price, reason, lot_code) VALUES (:s,NOW(),:p,:q,0,'SX_XUAT_COT',:l)",
                    {"s": user["store"], "p": ct["output_pcode"], "q": -float(kg_cot), "l": lot})
        for r in df_raw_pick.itertuples():
            run_sql(conn, "INSERT INTO batch_inputs(lot_code,pcode,qty,kind) VALUES (:l,:p,:q,'COT')",
                    {"l": lot, "p": r.pcode, "q": float(r.qty)})
            run_sql(conn, "INSERT INTO stocks(store_code, ts, pcode, qty, price, reason, lot_code) VALUES (:s,NOW(),:p,:q,0,'SX_XUAT_COT',:l)",
                    {"s": user["store"], "p": r.pcode, "q": -float(r.qty), "l": lot})
        for r in df_add_need.itertuples():
            run_sql(conn, "INSERT INTO batch_inputs(lot_code,pcode,qty,kind) VALUES (:l,:p,:q,'PHU_GIA')",
                    {"l": lot, "p": r.pcode, "q": float(r.qty)})
            run_sql(conn, "INSERT INTO stocks(store_code, ts, pcode, qty, price, reason, lot_code) VALUES (:s,NOW(),:p,:q,0,'SX_XUAT_PHU_GIA',:l)",
                    {"s": user["store"], "p": r.pcode, "q": -float(r.qty), "l": lot})
        write_audit(conn, "BATCH_MUT_CT_START", lot)
        st.success(f"Đã tạo lô {lot}. Vào mục 'Hoàn tất' bên dưới để nhập TP.")

    st.markdown("#### ✅ Hoàn tất lô MỨT-CT")
    df_wip = fetch_df(conn, "SELECT lot_code FROM batches WHERE type='MUT_CT' AND status='WIP' ORDER BY created_at")
    if df_wip.empty:
        st.caption("Không có lô WIP.")
        return
    lot_pick = st.selectbox("Chọn lô", df_wip["lot_code"].tolist(), key="mut_ct_lot_done")
    with st.form("fm_mut_ct_finish", clear_on_submit=True):
        qty_out = st.number_input("Kg TP MỨT nhập kho", min_value=0.0, step=0.1, value=0.0)
        ok2 = st.form_submit_button("Hoàn tất lô MỨT-CT", type="primary")
    if ok2:
        row = fetch_df(conn, "SELECT output_pcode FROM batches WHERE lot_code=:l", {"l": lot_pick}).iloc[0]
        run_sql(conn, "UPDATE batches SET status='DONE', finished_at=NOW() WHERE lot_code=:l", {"l": lot_pick})
        run_sql(conn, "INSERT INTO stocks(store_code, ts, pcode, qty, price, reason, lot_code) VALUES (:s,NOW(),:p,:q,0,'SX_NHAP_TP_MUT_CT',:l)",
                {"s": user["store"], "p": row["output_pcode"], "q": float(qty_out), "l": lot_pick})
        write_audit(conn, "BATCH_MUT_CT_DONE", lot_pick)
        st.success("Đã nhập kho thành phẩm.")

# ==================== PAGE SẢN XUẤT ====================
def page_production(conn, user):
    tabs = st.tabs(["CỐT", "MỨT từ TRÁI CÂY", "MỨT từ CỐT"])
    with tabs[0]: tab_cot(conn, user)
    with tabs[1]: tab_mut_tc(conn, user)
    with tabs[2]: tab_mut_ct(conn, user)
