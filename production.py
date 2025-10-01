# production.py
import time, json
from datetime import datetime
import streamlit as st
from core import fetch_df, run_sql, write_audit

# ===================== TỒN & GIÁ VỐN =====================
def stock_of(conn, store, pcode) -> float:
    df = fetch_df(conn, """
        SELECT COALESCE(SUM(CASE WHEN type='IN'  THEN qty ELSE 0 END),0) -
               COALESCE(SUM(CASE WHEN type='OUT' THEN qty ELSE 0 END),0) AS onhand
        FROM transactions
        WHERE store_code=:s AND pcode=:p
    """, {"s": store, "p": pcode})
    return 0.0 if df.empty else float(df.iloc[0]["onhand"] or 0.0)

def avg_cost_of(conn, store, pcode) -> float:
    df = fetch_df(conn, """
        SELECT SUM(qty*price_in) AS cost, SUM(qty) AS qty
        FROM transactions
        WHERE store_code=:s AND pcode=:p AND type='IN' AND price_in IS NOT NULL AND price_in>0
    """, {"s": store, "p": pcode})
    if df.empty:  # fallback price_ref
        pr = fetch_df(conn, "SELECT price_ref FROM products WHERE code=:p", {"p": pcode})
        return float(pr.iloc[0]["price_ref"] or 0.0) if not pr.empty else 0.0
    cost = float(df.iloc[0]["cost"] or 0.0); qty = float(df.iloc[0]["qty"] or 0.0)
    return (cost/qty) if qty>0 else (fetch_df(conn, "SELECT price_ref FROM products WHERE code=:p", {"p": pcode}).iloc[0]["price_ref"] or 0.0)

def must_have_stock(conn, store, items):
    lacks = []
    for it in items:
        on = stock_of(conn, store, it["pcode"])
        if on + 1e-9 < it["need"]:
            lacks.append(f"- {it['label']}: cần {it['need']}, tồn {on}")
    if lacks:
        st.error("❌ Không đủ tồn để xuất:\n" + "\n".join(lacks))
        return False
    return True

def sum_cost_for_out(conn, store, items) -> float:
    total = 0.0
    for it in items:
        total += avg_cost_of(conn, store, it["pcode"]) * it["need"]
    return total

def batch_id_from(ct_code: str) -> str:
    return f"{ct_code}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

# ===================== ĐỌC CÔNG THỨC =====================
def _load_header(conn, ct_code):
    df = fetch_df(conn, """
        SELECT code,name,type,output_pcode,output_uom,recovery,cups_per_kg,COALESCE(note,'') AS note
        FROM formulas WHERE code=:c
    """, {"c": ct_code})
    return None if df.empty else df.iloc[0].to_dict()

def _load_sources_and_other(conn, ct_code):
    """
    Catalog đang lưu NVL chính với kind='SRC' (không định lượng), phụ gia/khác với kind='OTHER' (có qty_per_kg).
    Ở đây tách SRC thành 2 nhóm dựa theo category của product: TRAI_CAY vs COT.
    """
    df_src = fetch_df(conn, """
        SELECT fi.pcode, p.name, p.cat_code
        FROM formula_inputs fi
        JOIN products p ON p.code=fi.pcode
        WHERE fi.formula_code=:c AND fi.kind='SRC'
        ORDER BY p.name
    """, {"c": ct_code})
    df_other = fetch_df(conn, """
        SELECT fi.pcode, p.name, p.uom, fi.qty_per_kg
        FROM formula_inputs fi
        JOIN products p ON p.code=fi.pcode
        WHERE fi.formula_code=:c AND fi.kind='OTHER'
        ORDER BY p.name
    """, {"c": ct_code})
    src_fruits = df_src[df_src["cat_code"]=="TRAI_CAY"].copy()
    src_cots   = df_src[df_src["cat_code"]=="COT"].copy()
    return src_fruits, src_cots, df_other

def show_preview(out_rows, in_rows, total_cost=None, price_tp=None):
    st.markdown("#### 👀 Preview")
    if out_rows:
        st.markdown("**Xuất kho (OUT):**")
        st.dataframe(out_rows, use_container_width=True, hide_index=True)
    if in_rows:
        st.markdown("**Nhập kho (IN):**")
        st.dataframe(in_rows, use_container_width=True, hide_index=True)
    c1,c2 = st.columns(2)
    with c1:
        if total_cost is not None: st.info(f"**Tổng chi phí OUT (ước tính):** {total_cost:,.0f}")
    with c2:
        if price_tp is not None:  st.info(f"**Giá nhập TP dự kiến:** {price_tp:,.0f} / {in_rows[0]['ĐVT']}")

# ===================== CỐT (1 bước) =====================
def tab_cot(conn, user):
    st.markdown("### 🏭 Sản xuất CỐT (1 bước)")
    df_ct = fetch_df(conn, "SELECT code,name FROM formulas WHERE type='COT' ORDER BY name")
    pick = st.selectbox("Công thức CỐT", ["— Chọn —"]+[f"{r['code']} — {r['name']}" for _,r in df_ct.iterrows()])
    if pick == "— Chọn —": return

    ct_code = pick.split(" — ",1)[0]
    hdr = _load_header(conn, ct_code)
    if not hdr: st.error("Không thấy công thức."); return
    src_fruits, _src_cots, df_other = _load_sources_and_other(conn, ct_code)

    st.caption(f"SP đầu ra: `{hdr['output_pcode']}` • HSTH: {float(hdr['recovery'] or 1.0)} • Cốc/kg TP: {float(hdr['cups_per_kg'] or 0.0)}")

    st.markdown("**1) Nhập kg thô cho trái cây:**")
    fruit_rows = []
    for _,r in src_fruits.iterrows():
        q = st.number_input(f"{r['name']} ({r['pcode']}) — kg thô", min_value=0.0, step=0.1, value=0.0, key=f"cot_tho_{r['pcode']}")
        fruit_rows.append({"pcode": r["pcode"], "name": r["name"], "kg_tho": q})

    st.markdown("**2) Tổng kg sau sơ chế:**")
    kg_soche = st.number_input("kg sau sơ chế", min_value=0.0, step=0.1, value=0.0)

    st.markdown("**3) NVL khác (theo ĐVT gốc / 1kg sơ chế):**")
    other_need = []
    for _,r in df_other.iterrows():
        need = float(r["qty_per_kg"] or 0) * kg_soche
        adj  = st.number_input(f"{r['name']} ({r['uom']}) — xuất", min_value=0.0, step=0.01, value=need, key=f"cot_other_{r['pcode']}")
        other_need.append({"pcode": r["pcode"], "label": f"{r['name']} ({r['uom']})", "need": adj})

    kg_tp = kg_soche * float(hdr["recovery"] or 1.0)
    cups  = kg_tp * float(hdr["cups_per_kg"] or 0.0)

    out_rows = []
    for r in fruit_rows:
        if r["kg_tho"]>0: out_rows.append({"pcode": r["pcode"], "diễn giải": r["name"], "SL xuất": r["kg_tho"], "ĐVT": "kg"})
    for it in other_need:
        if it["need"]>0:
            out_rows.append({"pcode": it["pcode"], "diễn giải": it["label"], "SL xuất": it["need"], "ĐVT": it["label"].split("(")[-1].rstrip(")")})
    in_rows  = [{"pcode": hdr["output_pcode"], "diễn giải":"TP CỐT", "SL nhập": kg_tp, "ĐVT":"kg", "≈ cốc": int(round(cups))}]

    total_cost = sum_cost_for_out(conn, user["store"], [{"pcode": r["pcode"], "need": r["SL xuất"], "label": r["diễn giải"]} for r in out_rows])
    price_tp   = (total_cost/kg_tp) if kg_tp>0 else None
    show_preview(out_rows, in_rows, total_cost, price_tp)

    if st.button("✅ Ghi nhận (xuất NVL & nhập TP CỐT)", type="primary"):
        if not must_have_stock(conn, user["store"], [{"pcode": r["pcode"], "need": r["SL xuất"], "label": r["diễn giải"]} for r in out_rows]): return
        bid = batch_id_from(ct_code)

        for r in fruit_rows:
            if r["kg_tho"]>0:
                run_sql(conn, """
                    INSERT INTO transactions(store_code,pcode,qty,type,note)
                    VALUES (:s,:p,:q,'OUT',:n)
                """, {"s": user["store"], "p": r["pcode"], "q": r["kg_tho"], "n": f"COT {ct_code} {bid} THO"})
        for it in other_need:
            if it["need"]>0:
                run_sql(conn, """
                    INSERT INTO transactions(store_code,pcode,qty,type,note)
                    VALUES (:s,:p,:q,'OUT',:n)
                """, {"s": user["store"], "p": it["pcode"], "q": it["need"], "n": f"COT {ct_code} {bid} OTHER"})

        price_in = (total_cost/kg_tp) if kg_tp>0 else 0.0
        run_sql(conn, """
            INSERT INTO transactions(store_code,pcode,qty,type,price_in,note)
            VALUES (:s,:p,:q,'IN',:pr,:n)
        """, {"s": user["store"], "p": hdr["output_pcode"], "q": kg_tp, "pr": price_in, "n": f"COT {ct_code} {bid} TP"})

        run_sql(conn, """
            INSERT INTO production(batch_id,ct_code,store_code,kind,status,kg_tho,kg_soche,kg_tp,out_pcode,actor,ts_create,ts_done)
            VALUES (:b,:c,:s,'COT','DONE',:a,:k,:t,:o,:u,NOW(),NOW())
        """, {"b": bid, "c": ct_code, "s": user["store"], "a": sum([r["kg_tho"] for r in fruit_rows]),
              "k": kg_soche, "t": kg_tp, "o": hdr["output_pcode"], "u": user["email"]})
        write_audit(conn, "PROD_COT_DONE", bid)
        st.success(f"Đã ghi lô {bid}."); time.sleep(0.6); st.rerun()

# ===================== MỨT – DÙNG CHUNG =====================
def _mut_step1(conn, user, ct_code, src_label):
    hdr = _load_header(conn, ct_code)
    if not hdr: st.error("Không thấy công thức."); return

    src_fruits, src_cots, df_other = _load_sources_and_other(conn, ct_code)
    st.caption(f"SP đầu ra: `{hdr['output_pcode']}` • (MỨT không dùng HSTH) • Cốc/kg TP: {float(hdr['cups_per_kg'] or 0.0)}")

    st.markdown(f"**1) Nguồn {src_label} — nhập kg thô:**")
    src_df = src_fruits if src_label=="TRÁI CÂY" else src_cots
    if src_df is None or src_df.empty:
        st.warning(f"Công thức chưa khai nguồn {src_label}."); src_df = src_df.iloc[0:0]  # empty DF

    src_rows = []
    for _,r in src_df.iterrows():
        q = st.number_input(f"{r['name']} ({r['pcode']}) — kg thô", min_value=0.0, step=0.1, value=0.0, key=f"mut_tho_{ct_code}_{r['pcode']}")
        src_rows.append({"pcode": r["pcode"], "name": r["name"], "kg_tho": q})

    st.markdown("**2) Tổng kg sau sơ chế:**")
    kg_soche = st.number_input("kg sau sơ chế", min_value=0.0, step=0.1, value=0.0, key=f"soche_{ct_code}")

    st.markdown("**3) NVL khác (ĐVT gốc / 1kg sơ chế):**")
    other_need = []
    for _,r in df_other.iterrows():
        need = float(r["qty_per_kg"] or 0) * kg_soche
        adj  = st.number_input(f"{r['name']} ({r['uom']}) — xuất", min_value=0.0, step=0.01, value=need, key=f"mut_other_{ct_code}_{r['pcode']}")
        other_need.append({"pcode": r["pcode"], "label": f"{r['name']} ({r['uom']})", "need": adj})

    out_rows = []
    for r in src_rows:
        if r["kg_tho"]>0: out_rows.append({"pcode": r["pcode"], "diễn giải": r["name"], "SL xuất": r["kg_tho"], "ĐVT":"kg"})
    for it in other_need:
        if it["need"]>0:
            out_rows.append({"pcode": it["pcode"], "diễn giải": it["label"], "SL xuất": it["need"], "ĐVT": it["label"].split("(")[-1].rstrip(")")})

    total_cost = sum_cost_for_out(conn, user["store"], [{"pcode": r["pcode"], "need": r["SL xuất"], "label": r["diễn giải"]} for r in out_rows])
    show_preview(out_rows, [], total_cost, None)

    if st.button("🧺 Tạo lô & ghi Bước 1 (WIP)", type="primary", key=f"btn_b1_{ct_code}_{src_label}"):
        if not must_have_stock(conn, user["store"], [{"pcode": r["pcode"], "need": r["SL xuất"], "label": r["diễn giải"]} for r in out_rows]): return
        bid = batch_id_from(ct_code)

        for r in src_rows:
            if r["kg_tho"]>0:
                run_sql(conn, """
                    INSERT INTO transactions(store_code,pcode,qty,type,note)
                    VALUES (:s,:p,:q,'OUT',:n)
                """, {"s": user["store"], "p": r["pcode"], "q": r["kg_tho"], "n": f"MUT {ct_code} {bid} RAW"})
        for it in other_need:
            if it["need"]>0:
                run_sql(conn, """
                    INSERT INTO transactions(store_code,pcode,qty,type,note)
                    VALUES (:s,:p,:q,'OUT',:n)
                """, {"s": user["store"], "p": it["pcode"], "q": it["need"], "n": f"MUT {ct_code} {bid} OTHER"})

        run_sql(conn, """
            INSERT INTO production(batch_id,ct_code,store_code,kind,status,kg_tho,kg_soche,kg_tp,out_pcode,actor,ts_create)
            VALUES (:b,:c,:s,:k,'WIP',:a,:kg,0,:o,:u,NOW())
        """, {"b": bid, "c": ct_code, "s": user["store"],
              "k": ('MUT_TC' if src_label=='TRÁI CÂY' else 'MUT_CT'),
              "a": sum([r["kg_tho"] for r in src_rows]), "kg": kg_soche,
              "o": hdr["output_pcode"], "u": user["email"]})

        run_sql(conn, """
            INSERT INTO wip_cost(batch_id,cost_total,qty_tp)
            VALUES (:b,:cost,NULL)
            ON CONFLICT (batch_id) DO UPDATE SET cost_total=EXCLUDED.cost_total
        """, {"b": bid, "cost": total_cost})

        write_audit(conn, "PROD_MUT_WIP", bid)
        st.success(f"Đã tạo lô {bid}. Vào tab 'Hoàn thành lô' để nhập TP khi xong.")
        time.sleep(0.6); st.rerun()

def _mut_step2_finish(conn, user):
    st.markdown("### ✅ Hoàn thành lô MỨT (Bước 2)")
    df_wip = fetch_df(conn, """
        SELECT batch_id,ct_code,kind,store_code,kg_soche,out_pcode,ts_create
        FROM production
        WHERE status='WIP' AND store_code=:s
        ORDER BY ts_create DESC
    """, {"s": user["store"]})
    if df_wip.empty:
        st.info("Chưa có lô WIP tại cửa hàng."); return

    opts = [f"{r['batch_id']} — {r['ct_code']} — {r['kind']} — {r['ts_create']}" for _,r in df_wip.iterrows()]
    pick = st.selectbox("Chọn lô WIP", opts)
    if not pick: return
    bid = pick.split(" — ",1)[0]
    row = df_wip[df_wip["batch_id"]==bid].iloc[0].to_dict()

    df_cost = fetch_df(conn, "SELECT cost_total FROM wip_cost WHERE batch_id=:b", {"b": bid})
    cost_total = float(df_cost.iloc[0]["cost_total"] or 0.0) if not df_cost.empty else 0.0

    kg_tp  = st.number_input("Kg thành phẩm MỨT (nhập tay)", min_value=0.0, step=0.1, value=0.0)
    cups_pk = fetch_df(conn, "SELECT cups_per_kg FROM formulas WHERE code=:c", {"c": row["ct_code"]})
    cups = kg_tp * float(cups_pk.iloc[0]["cups_per_kg"] or 0.0) if not cups_pk.empty else 0.0

    price_in = (cost_total/kg_tp) if kg_tp>0 else 0.0
    show_preview([], [{"pcode": row["out_pcode"], "diễn giải":"TP MỨT", "SL nhập": kg_tp, "ĐVT":"kg", "≈ cốc": int(round(cups))}],
                 cost_total, price_in)

    if st.button("✔️ Nhập TP & đóng lô", type="primary"):
        run_sql(conn, """
            INSERT INTO transactions(store_code,pcode,qty,type,price_in,note)
            VALUES (:s,:p,:q,'IN',:pr,:n)
        """, {"s": row["store_code"], "p": row["out_pcode"], "q": kg_tp, "pr": price_in, "n": f"{bid} TP MUT"})
        run_sql(conn, "UPDATE production SET status='DONE', kg_tp=:q, ts_done=NOW() WHERE batch_id=:b", {"q": kg_tp, "b": bid})
        run_sql(conn, "UPDATE wip_cost SET qty_tp=:q WHERE batch_id=:b", {"q": kg_tp, "b": bid})
        write_audit(conn, "PROD_MUT_DONE", bid)
        st.success(f"Đã nhập TP & đóng lô {bid}."); time.sleep(0.6); st.rerun()

# ===================== LỊCH SỬ LÔ =====================
def tab_history(conn, user):
    st.markdown("### 📜 Lịch sử lô gần đây")
    df = fetch_df(conn, """
        SELECT ts_create, batch_id, ct_code, kind, status, kg_tho, kg_soche, kg_tp, out_pcode, ts_done
        FROM production
        WHERE store_code=:s
        ORDER BY ts_create DESC
        LIMIT 200
    """, {"s": user["store"]})
    st.dataframe(df, use_container_width=True)

# ===================== ENTRY PAGE =====================
def page_production(conn, user):
    st.markdown("## 🧯 Sản xuất")
    tabs = st.tabs(["CỐT (1 bước)", "MỨT từ TRÁI CÂY", "MỨT từ CỐT", "Lịch sử lô"])
    with tabs[0]: tab_cot(conn, user)
    with tabs[1]: _mut_step1(conn, user, _pick_ct(conn, 'MUT', want='TC'), "TRÁI CÂY") if True else None
    with tabs[2]: _mut_step1(conn, user, _pick_ct(conn, 'MUT', want='CT'), "CỐT")    if True else None
    with tabs[3]: tab_history(conn, user)

# Helper chọn CT cho 2 tab mứt (lọc theo SRC trong inputs)
def _pick_ct(conn, ct_type, want='TC'):
    df = fetch_df(conn, "SELECT code,name FROM formulas WHERE type=:t ORDER BY name", {"t": ct_type})
    opts = ["— Chọn —"]+[f"{r['code']} — {r['name']}" for _,r in df.iterrows()]
    pick = st.selectbox("Công thức", opts, key=f"ct_{ct_type}_{want}")
    if pick=="— Chọn —": st.stop()
    ct_code = pick.split(" — ",1)[0]

    # xác nhận đúng loại nguồn mong muốn
    src_fruits, src_cots, _ = _load_sources_and_other(conn, ct_code)
    if want=='TC' and (src_fruits is None or src_fruits.empty):
        st.error("CT này không có nguồn TRÁI CÂY. Chọn CT khác."); st.stop()
    if want=='CT' and (src_cots is None or src_cots.empty):
        st.error("CT này không có nguồn CỐT. Chọn CT khác."); st.stop()
    return ct_code
