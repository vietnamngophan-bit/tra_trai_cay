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
    with tabs[2]: tab_mut_ct(conn, user)-- batches: 1 header cho mỗi lô
CREATE TABLE IF NOT EXISTS batches (
  lot_code     TEXT PRIMARY KEY,
  type         TEXT NOT NULL,              -- 'COT' | 'MUT_TC' | 'MUT_CT'
  formula_code TEXT NOT NULL,
  output_pcode TEXT NOT NULL,
  store_code   TEXT NOT NULL,
  status       TEXT NOT NULL DEFAULT 'WIP',-- 'WIP' | 'DONE'
  planned_wip_kg NUMERIC,                  -- kg sau sơ chế (nếu có)
  created_at   TIMESTAMP DEFAULT NOW(),
  finished_at  TIMESTAMP
);

-- batch_inputs: chi tiết đầu vào của lô (NVL chính & phụ gia)
CREATE TABLE IF NOT EXISTS batch_inputs (
  id           BIGSERIAL PRIMARY KEY,
  lot_code     TEXT NOT NULL REFERENCES batches(lot_code) ON DELETE CASCADE,
  pcode        TEXT NOT NULL,              -- mã NVL
  qty          NUMERIC NOT NULL,           -- số lượng (kg hoặc lít tùy NVL)
  kind         TEXT NOT NULL               -- 'TRAI_CAY' | 'COT' | 'PHU_GIA'
);

-- stocks: sổ kho (âm = xuất, dương = nhập)
CREATE TABLE IF NOT EXISTS stocks (
  id         BIGSERIAL PRIMARY KEY,
  store_code TEXT NOT NULL,
  ts         TIMESTAMP NOT NULL DEFAULT NOW(),
  pcode      TEXT NOT NULL,
  qty        NUMERIC NOT NULL,             -- âm xuất, dương nhập
  price      NUMERIC NOT NULL DEFAULT 0,
  reason     TEXT NOT NULL,                -- 'SX_XUAT_*' | 'SX_NHAP_*'
  lot_code   TEXT
);# production.py
import time
from datetime import datetime
import streamlit as st
from core import fetch_df, run_sql, write_audit

# ===================== Helper tính tồn & giá vốn =====================
def stock_of(conn, store, pcode) -> float:
    """Tồn = IN - OUT theo qty (đúng ĐVT gốc của sản phẩm)"""
    df = fetch_df(conn, """
        SELECT COALESCE(SUM(CASE WHEN type='IN'  THEN qty ELSE 0 END),0) AS in_qty,
               COALESCE(SUM(CASE WHEN type='OUT' THEN qty ELSE 0 END),0) AS out_qty
        FROM transactions
        WHERE store_code=:s AND pcode=:p
    """, {"s": store, "p": pcode})
    if df.empty: return 0.0
    return float(df.iloc[0]["in_qty"] - df.iloc[0]["out_qty"])

def avg_cost_of(conn, store, pcode) -> float:
    """
    Giá vốn bình quân di động theo lịch sử IN.
    Nếu chưa có IN -> fallback price_ref của products (nếu có) -> 0.
    """
    df_in = fetch_df(conn, """
        SELECT qty, price_in
        FROM transactions
        WHERE store_code=:s AND pcode=:p AND type='IN' AND price_in IS NOT NULL AND price_in>0
        ORDER BY ts
    """, {"s": store, "p": pcode})
    total_qty, total_cost = 0.0, 0.0
    for _, r in df_in.iterrows():
        q = float(r["qty"] or 0); c = float(r["price_in"] or 0)
        total_qty += q
        total_cost += q * c
    if total_qty > 0:
        return total_cost / total_qty
    # fallback price_ref
    df_pref = fetch_df(conn, "SELECT price_ref FROM products WHERE code=:p", {"p": pcode})
    if not df_pref.empty:
        return float(df_pref.iloc[0]["price_ref"] or 0.0)
    return 0.0

def must_have_stock(conn, store, items):
    """
    items: list[{"pcode":..., "need": float, "label": str}]
    Raise st.error nếu thiếu tồn.
    """
    errs = []
    for it in items:
        onhand = stock_of(conn, store, it["pcode"])
        if onhand + 1e-9 < it["need"]:
            errs.append(f"- {it['label']}: cần {it['need']}, tồn {onhand}")
    if errs:
        st.error("❌ Không đủ tồn để xuất:\n" + "\n".join(errs))
        return False
    return True

def sum_cost_for_out(conn, store, items) -> float:
    """Tổng chi phí cho các dòng OUT, tính theo avg_cost_of * qty."""
    total = 0.0
    for it in items:
        c = avg_cost_of(conn, store, it["pcode"])
        total += c * it["need"]
    return total

def batch_id_from(ct_code: str) -> str:
    return f"{ct_code}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

# ===================== UI tiện ích =====================
def show_khung(title: str):
    st.markdown(f"### {title}")
    st.caption("— Tất cả mã hàng phải chọn từ danh mục; không nhập tay.")

def show_preview(out_rows, in_rows, total_cost=None, price_tp=None):
    st.markdown("#### 👀 Preview hạch toán")
    if out_rows:
        st.markdown("**Xuất kho (OUT):**")
        st.dataframe(out_rows, use_container_width=True, hide_index=True)
    if in_rows:
        st.markdown("**Nhập kho (IN):**")
        st.dataframe(in_rows, use_container_width=True, hide_index=True)
    cols = st.columns(2)
    with cols[0]:
        if total_cost is not None:
            st.info(f"**Tổng chi phí OUT** ước tính: **{total_cost:,.0f}**")
    with cols[1]:
        if price_tp is not None:
            st.info(f"**Giá nhập TP** dự kiến: **{price_tp:,.0f}** / đơn vị TP")

# ===================== Trích công thức =====================
def load_formula(conn, ct_code: str):
    hdr = fetch_df(conn, """
        SELECT code,name,type,output_pcode,recovery,cups_per_kg,COALESCE(note,'') AS note
        FROM formulas WHERE code=:c
    """, {"c": ct_code})
    if hdr.empty: return None, None, None, None, None, "TRAI_CAY"
    h = hdr.iloc[0].to_dict()
    src_kind = "TRAI_CAY"
    note = (h.get("note") or "").strip()
    if note:
        # có thể là JSON hoặc chuỗi "SRC=..."
        try:
            import json
            j = json.loads(note)
            src_kind = (j.get("src") or "TRAI_CAY")
        except Exception:
            if note.startswith("SRC="):
                src_kind = note.split("=",1)[1] or "TRAI_CAY"
    # nguồn được phép
    df_src_fruit = fetch_df(conn, """
        SELECT fi.pcode, p.name FROM formula_inputs fi
        JOIN products p ON p.code=fi.pcode
        WHERE fi.formula_code=:c AND fi.kind='SRC_FRUIT'
        ORDER BY p.name
    """, {"c": ct_code})
    df_src_cot = fetch_df(conn, """
        SELECT fi.pcode, p.name FROM formula_inputs fi
        JOIN products p ON p.code=fi.pcode
        WHERE fi.formula_code=:c AND fi.kind='SRC_COT'
        ORDER BY p.name
    """, {"c": ct_code})
    # NVL khác (định mức theo uom gốc / 1kg sơ chế)
    df_other = fetch_df(conn, """
        SELECT fi.pcode, p.name, p.uom, fi.qty_per_kg
        FROM formula_inputs fi
        JOIN products p ON p.code=fi.pcode
        WHERE fi.formula_code=:c AND fi.kind='OTHER'
        ORDER BY p.name
    """, {"c": ct_code})
    return h, df_src_fruit, df_src_cot, df_other, src_kind

# ===================== Tab CỐT (1 bước) =====================
def tab_cot(conn, user):
    show_khung("🏭 Sản xuất CỐT (1 bước)")
    df_ct = fetch_df(conn, "SELECT code,name FROM formulas WHERE type='COT' ORDER BY name")
    opts = ["— Chọn —"] + [f"{r['code']} — {r['name']}" for _,r in df_ct.iterrows()]
    pick = st.selectbox("Công thức CỐT", opts)
    if pick == "— Chọn —": return
    ct_code = pick.split(" — ",1)[0]
    hdr, df_fruits, _, df_other, _src = load_formula(conn, ct_code)
    if hdr is None:
        st.error("Không tải được công thức."); return

    st.markdown(f"**SP đầu ra:** `{hdr['output_pcode']}` • **HSTH:** {float(hdr['recovery'] or 1.0)} • **Cốc/kg:** {float(hdr['cups_per_kg'] or 0.0)}")

    # Nhập kg thô cho từng trái cây được phép
    st.markdown("#### 1) Nguyên liệu trái cây (chọn & nhập số lượng)")
    fruit_rows = []
    for _, r in df_fruits.iterrows():
        c = r["pcode"]; name = r["name"]
        q_tho = st.number_input(f"{name} ({c}) — **kg thô xuất**", min_value=0.0, step=0.1, value=0.0, key=f"cot_tho_{c}")
        fruit_rows.append({"pcode": c, "name": name, "kg_tho": q_tho})

    # Kg sau sơ chế (tổng), làm cơ sở tính NVL khác & TP cốt
    st.markdown("#### 2) Khối lượng sau sơ chế")
    kg_soche = st.number_input("Tổng **kg sau sơ chế**", min_value=0.0, step=0.1, value=0.0)

    # NVL khác cần xuất theo uom gốc
    st.markdown("#### 3) NVL khác (tự tính theo ĐVT gốc / 1kg sơ chế)")
    other_need = []
    for _, r in df_other.iterrows():
        need = float(r["qty_per_kg"] or 0) * float(kg_soche or 0)
        adj = st.number_input(f"{r['name']} ({r['pcode']}, {r['uom']}) — **xuất**", min_value=0.0, step=0.01, value=need, key=f"cot_other_{r['pcode']}")
        other_need.append({"pcode": r["pcode"], "label": f"{r['name']} ({r['uom']})", "need": adj})

    # Thành phẩm
    kg_tp = float(kg_soche) * float(hdr["recovery"] or 1.0)
    cups  = kg_tp * float(hdr["cups_per_kg"] or 0.0)

    # Preview
    out_rows = []
    for row in fruit_rows:
        if row["kg_tho"] > 0:
            out_rows.append({"pcode": row["pcode"], "diễn giải": row["name"], "SL xuất": row["kg_tho"], "ĐVT": "kg"})
    for it in other_need:
        if it["need"] > 0:
            out_rows.append({"pcode": it["pcode"], "diễn giải": it["label"], "SL xuất": it["need"], "ĐVT": it["label"].split("(")[-1].rstrip(")")})
    in_rows = [{"pcode": hdr["output_pcode"], "diễn giải": "Thành phẩm CỐT", "SL nhập": kg_tp, "ĐVT": "kg", "≈ cốc": int(round(cups))}]
    total_cost = sum_cost_for_out(conn, user["store"], [{"pcode": r["pcode"], "need": r["SL xuất"], "label": r["diễn giải"]} for r in out_rows])
    price_tp = (total_cost / kg_tp) if kg_tp > 0 else None
    show_preview(out_rows, in_rows, total_cost, price_tp)

    if st.button("✅ Ghi nhận (xuất NVL & nhập TP CỐT)", type="primary"):
        # chống xuất âm
        need_items = [{"pcode": r["pcode"], "need": r["SL xuất"], "label": r["diễn giải"]} for r in out_rows]
        if not must_have_stock(conn, user["store"], need_items): return

        bid = batch_id_from(ct_code)
        # OUT trái cây thô
        for r in fruit_rows:
            if r["kg_tho"] > 0:
                run_sql(conn, """
                    INSERT INTO transactions(store_code,pcode,qty,type,note)
                    VALUES (:s,:p,:q,'OUT',:n)
                """, {"s": user["store"], "p": r["pcode"], "q": r["kg_tho"], "n": f"COT {ct_code} {bid} THO"})
        # OUT NVL khác
        for it in other_need:
            if it["need"] > 0:
                run_sql(conn, """
                    INSERT INTO transactions(store_code,pcode,qty,type,note)
                    VALUES (:s,:p,:q,'OUT',:n)
                """, {"s": user["store"], "p": it["pcode"], "q": it["need"], "n": f"COT {ct_code} {bid} OTHER"})

        # IN thành phẩm cốt (giá bình quân từ tổng chi phí OUT)
        price_in = (total_cost / kg_tp) if kg_tp > 0 else 0.0
        run_sql(conn, """
            INSERT INTO transactions(store_code,pcode,qty,type,price_in,note)
            VALUES (:s,:p,:q,'IN',:pr,:n)
        """, {"s": user["store"], "p": hdr["output_pcode"], "q": kg_tp, "pr": price_in, "n": f"COT {ct_code} {bid} TP"})

        # Lưu production (DONE)
        run_sql(conn, """
            INSERT INTO production(batch_id, ct_code, store_code, kind, status,
                                   kg_tho, kg_soche, kg_tp, out_pcode, actor, ts_create, ts_done)
            VALUES (:b,:c,:s,'COT','DONE',:a,:bkg,:t,:o,:u,NOW(),NOW())
        """, {"b": bid, "c": ct_code, "s": user["store"], "a": sum([r["kg_tho"] for r in fruit_rows]),
              "bkg": kg_soche, "t": kg_tp, "o": hdr["output_pcode"], "u": user["email"]})

        write_audit(conn, "PROD_COT_DONE", f"{bid}")
        st.success(f"Đã ghi lô {bid}.")
        time.sleep(0.6)
        st.rerun()

# ===================== Khối dùng chung cho MỨT (B1/B2) =====================
def mut_step1(conn, user, ct_code, src_kind_label):
    hdr, df_fruits, df_cots, df_other, _src = load_formula(conn, ct_code)
    if hdr is None:
        st.error("Không tải được công thức."); return

    st.markdown(f"**SP đầu ra:** `{hdr['output_pcode']}` • (MỨT không dùng HSTH) • **Cốc/kg:** {float(hdr['cups_per_kg'] or 0.0)}")

    # Nguồn được phép
    st.markdown(f"#### 1) Nguồn {src_kind_label} (chọn & nhập kg thô)")
    source_rows = []
    if src_kind_label == "TRÁI CÂY":
        src_df = df_fruits
        uom_src = "kg"
    else:
        src_df = df_cots
        uom_src = "kg"
    if src_df is None or src_df.empty:
        st.warning(f"Công thức chưa khai nguồn {src_kind_label} được phép.")
        src_df = fetch_df(conn, "SELECT code,name FROM products WHERE 1=0")  # rỗng
    for _, r in src_df.iterrows():
        c = r["pcode"]; name = r["name"]
        q_tho = st.number_input(f"{name} ({c}) — **kg thô xuất**", min_value=0.0, step=0.1, value=0.0, key=f"mut_tho_{ct_code}_{c}")
        source_rows.append({"pcode": c, "name": name, "kg_tho": q_tho, "uom": uom_src})

    # Kg sau sơ chế
    st.markdown("#### 2) Khối lượng sau sơ chế")
    kg_soche = st.number_input("Tổng **kg sau sơ chế**", min_value=0.0, step=0.1, value=0.0, key=f"soche_{ct_code}")

    # NVL khác (theo uom gốc)
    st.markdown("#### 3) NVL khác (tự tính theo ĐVT gốc / 1kg sơ chế)")
    other_need = []
    for _, r in df_other.iterrows():
        need = float(r["qty_per_kg"] or 0) * float(kg_soche or 0)
        adj = st.number_input(f"{r['name']} ({r['pcode']}, {r['uom']}) — **xuất**",
                              min_value=0.0, step=0.01, value=need, key=f"mut_other_{ct_code}_{r['pcode']}")
        other_need.append({"pcode": r["pcode"], "label": f"{r['name']} ({r['uom']})", "need": adj})

    # Preview B1
    out_rows = []
    for r in source_rows:
        if r["kg_tho"] > 0:
            out_rows.append({"pcode": r["pcode"], "diễn giải": r["name"], "SL xuất": r["kg_tho"], "ĐVT": r["uom"]})
    for it in other_need:
        if it["need"] > 0:
            out_rows.append({"pcode": it["pcode"], "diễn giải": it["label"], "SL xuất": it["need"], "ĐVT": it["label"].split("(")[-1].rstrip(")")})

    total_cost = sum_cost_for_out(conn, user["store"], [{"pcode": r["pcode"], "need": r["SL xuất"], "label": r["diễn giải"]} for r in out_rows])
    show_preview(out_rows, in_rows=[], total_cost=total_cost, price_tp=None)

    if st.button("🧺 Tạo lô & ghi Bước 1 (WIP)", type="primary", key=f"btn_b1_{ct_code}"):
        need_items = [{"pcode": r["pcode"], "need": r["SL xuất"], "label": r["diễn giải"]} for r in out_rows]
        if not must_have_stock(conn, user["store"], need_items): return

        bid = batch_id_from(ct_code)
        # OUT nguồn (trái cây/cốt) + OTHER
        for r in source_rows:
            if r["kg_tho"] > 0:
                run_sql(conn, """
                    INSERT INTO transactions(store_code,pcode,qty,type,note)
                    VALUES (:s,:p,:q,'OUT',:n)
                """, {"s": user["store"], "p": r["pcode"], "q": r["kg_tho"], "n": f"MUT {ct_code} {bid} RAW"})
        for it in other_need:
            if it["need"] > 0:
                run_sql(conn, """
                    INSERT INTO transactions(store_code,pcode,qty,type,note)
                    VALUES (:s,:p,:q,'OUT',:n)
                """, {"s": user["store"], "p": it["pcode"], "q": it["need"], "n": f"MUT {ct_code} {bid} OTHER"})

        # production WIP + wip_cost
        run_sql(conn, """
            INSERT INTO production(batch_id, ct_code, store_code, kind, status,
                                   kg_tho, kg_soche, kg_tp, out_pcode, actor, ts_create)
            VALUES (:b,:c,:s,:k,'WIP',:a,:kg,0,:o,:u,NOW())
        """, {"b": bid, "c": ct_code, "s": user["store"],
              "k": ("MUT_TC" if src_kind_label=="TRÁI CÂY" else "MUT_CT"),
              "a": sum([r["kg_tho"] for r in source_rows]), "kg": kg_soche,
              "o": hdr["output_pcode"], "u": user["email"]})

        run_sql(conn, """
            INSERT INTO wip_cost(batch_id, cost_total, qty_tp)
            VALUES (:b,:cost,NULL)
            ON CONFLICT (batch_id) DO UPDATE SET cost_total=EXCLUDED.cost_total
        """, {"b": bid, "cost": total_cost})

        write_audit(conn, "PROD_MUT_WIP", bid)
        st.success(f"Đã tạo lô {bid}. Vào tab 'Hoàn thành lô' để nhập TP khi xong.")
        time.sleep(0.6); st.rerun()

def mut_step2_finish(conn, user):
    st.markdown("#### ✅ Hoàn thành lô MỨT (Bước 2)")
    df_wip = fetch_df(conn, """
        SELECT batch_id, ct_code, kind, store_code, kg_soche, out_pcode, ts_create
        FROM production
        WHERE status='WIP' AND store_code=:s
        ORDER BY ts_create DESC
    """, {"s": user["store"]})
    if df_wip.empty:
        st.info("Chưa có lô WIP nào tại cửa hàng này.")
        return
    opts = [f"{r['batch_id']} — {r['ct_code']} — {r['kind']} — {r['ts_create']}" for _,r in df_wip.iterrows()]
    pick = st.selectbox("Chọn lô WIP", opts)
    bid = pick.split(" — ",1)[0]
    row = df_wip[df_wip["batch_id"]==bid].iloc[0].to_dict()

    df_cost = fetch_df(conn, "SELECT cost_total FROM wip_cost WHERE batch_id=:b", {"b": bid})
    cost_total = float(df_cost.iloc[0]["cost_total"] or 0.0) if not df_cost.empty else 0.0

    kg_tp = st.number_input("Kg thành phẩm MỨT (nhập tay)", min_value=0.0, step=0.1, value=0.0)
    cups  = kg_tp * float(fetch_df(conn, "SELECT cups_per_kg FROM formulas WHERE code=:c", {"c": row["ct_code"]}).iloc[0]["cups_per_kg"] or 0.0)

    price_in = (cost_total / kg_tp) if kg_tp > 0 else 0.0
    show_preview(out_rows=[], in_rows=[{"pcode": row["out_pcode"], "diễn giải":"TP MỨT", "SL nhập": kg_tp, "ĐVT":"kg", "≈ cốc": int(round(cups))}],
                 total_cost=cost_total, price_tp=price_in)

    if st.button("✔️ Nhập TP & Đóng lô", type="primary"):
        # IN thành phẩm mứt
        run_sql(conn, """
            INSERT INTO transactions(store_code,pcode,qty,type,price_in,note)
            VALUES (:s,:p,:q,'IN',:pr,:n)
        """, {"s": row["store_code"], "p": row["out_pcode"], "q": kg_tp, "pr": price_in, "n": f"{bid} TP MUT"})

        # cập nhật production & wip_cost
        run_sql(conn, "UPDATE production SET status='DONE', kg_tp=:q, ts_done=NOW() WHERE batch_id=:b",
                {"q": kg_tp, "b": bid})
        run_sql(conn, "UPDATE wip_cost SET qty_tp=:q WHERE batch_id=:b", {"q": kg_tp, "b": bid})

        write_audit(conn, "PROD_MUT_DONE", bid)
        st.success(f"Đã nhập TP và đóng lô {bid}.")
        time.sleep(0.6); st.rerun()

# ===================== Tab MỨT từ TRÁI CÂY =====================
def tab_mut_tc(conn, user):
    show_khung("🍊 MỨT từ TRÁI CÂY (2 bước)")
    # chọn CT mứt có SRC trái cây
    df_ct = fetch_df(conn, """
        SELECT f.code, f.name
        FROM formulas f
        WHERE f.type='MUT'
        ORDER BY f.name
    """)
    opts = ["— Chọn —"] + [f"{r['code']} — {r['name']}" for _,r in df_ct.iterrows()]
    pick = st.selectbox("Công thức MỨT", opts, key="mut_tc_ct")
    if pick=="— Chọn —": 
        st.divider()
        mut_step2_finish(conn, user)  # vẫn cho hoàn thành lô khi chưa chọn B1
        return

    ct_code = pick.split(" — ",1)[0]
    # kiểm tra note/src là trái cây
    hdr, _, _, _, src_kind = load_formula(conn, ct_code)
    if src_kind != "TRAI_CAY":
        st.error("Công thức này không phải MỨT từ TRÁI CÂY (SRC=TRAI_CAY)."); 
        st.stop()

    mut_step1(conn, user, ct_code, "TRÁI CÂY")
    st.divider()
    mut_step2_finish(conn, user)

# ===================== Tab MỨT từ CỐT =====================
def tab_mut_ct(conn, user):
    show_khung("🥤 MỨT từ CỐT (2 bước)")
    # chọn CT mứt có SRC cốt
    df_ct = fetch_df(conn, """
        SELECT f.code, f.name, f.note
        FROM formulas f
        WHERE f.type='MUT'
        ORDER BY f.name
    """)
    # filter SRC=COT
    rows = []
    for _, r in df_ct.iterrows():
        note = (r.get("note") or "")
        is_ct = False
        try:
            import json
            j = json.loads(note) if note else {}
            is_ct = (j.get("src") == "COT")
        except Exception:
            if note.startswith("SRC=") and note.split("=",1)[1] == "COT":
                is_ct = True
        if is_ct:
            rows.append(r)
    if not rows:
        st.info("Chưa có CT mứt từ CỐT (SRC=COT).")
        mut_step2_finish(conn, user)
        return
    opts = ["— Chọn —"] + [f"{r['code']} — {r['name']}" for r in rows]
    pick = st.selectbox("Công thức MỨT từ CỐT", opts, key="mut_ct_ct")
    if pick=="— Chọn —":
        st.divider()
        mut_step2_finish(conn, user)
        return

    ct_code = pick.split(" — ",1)[0]
    mut_step1(conn, user, ct_code, "CỐT")
    st.divider()
    mut_step2_finish(conn, user)

# ===================== ENTRY cho Module Sản xuất =====================
def page_production(conn, user):
    st.markdown("## 🧯 Sản xuất")
    tabs = st.tabs(["CỐT (1 bước)", "MỨT từ TRÁI CÂY", "MỨT từ CỐT"])
    with tabs[0]:
        tab_cot(conn, user)
    with tabs[1]:
        tab_mut_tc(conn, user)
    with tabs[2]:
        tab_mut_ct(conn, user)
