# catalog.py  —  Module 2: Danh mục (Categories / Products / Formulas)

import streamlit as st
from typing import Dict, Any
from core import fetch_df, run_sql, write_audit

# -------------------------- UI Helpers --------------------------
def _pill(title: str, emoji: str):
    st.markdown(f"### {emoji} {title}")

def _note(msg: str):
    st.caption(msg)

# -------------------------- PAGE --------------------------
def page_catalog(conn, user: Dict[str, Any]):
    _pill("Danh mục", "🧾")
    tabs = st.tabs(["Danh mục SP", "Sản phẩm", "Công thức"])

    # ===== TAB 1: DANH MỤC (categories) =====
    with tabs[0]:
        st.subheader("Danh mục sản phẩm")
        df = fetch_df(conn, "SELECT code,name FROM categories ORDER BY code")
        st.dataframe(df, use_container_width=True, height=260)

        with st.form("fm_cat_add", clear_on_submit=True):
            c1, c2 = st.columns([1,2])
            with c1: code = st.text_input("Mã")
            with c2: name = st.text_input("Tên")
            ok = st.form_submit_button("Lưu", type="primary")
        if ok:
            if not code or not name:
                st.error("Thiếu mã hoặc tên.")
            else:
                run_sql(conn, """
                    INSERT INTO categories(code,name) VALUES (:c,:n)
                    ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name
                """, {"c": code.strip(), "n": name.strip()})
                write_audit(conn, "CAT_UPSERT", code)
                st.success("Đã lưu."); st.rerun()

        pick = st.selectbox("Xoá danh mục", ["—"] + df["code"].tolist(), index=0)
        if pick != "—" and st.button("Xoá"):
            run_sql(conn, "DELETE FROM categories WHERE code=:c", {"c": pick})
            write_audit(conn, "CAT_DELETE", pick); st.rerun()

    # ===== TAB 2: SẢN PHẨM (products) =====
    with tabs[1]:
        st.subheader("Sản phẩm")
        dfp = fetch_df(conn, """
            SELECT code,name,cat_code,uom,cups_per_kg,price_ref
            FROM products ORDER BY name
        """)
        st.dataframe(dfp, use_container_width=True, height=300)

        with st.form("fm_prod_add", clear_on_submit=True):
            c1, c2 = st.columns([1,2])
            with c1:
                pcode = st.text_input("Mã SP")
                uom   = st.text_input("ĐVT", value="kg")
            with c2:
                name  = st.text_input("Tên SP")
                cat   = st.selectbox("Nhóm", ["TRAI_CAY","COT","MUT","PHU_GIA","TP_KHAC"], key="prod_cat_add")

            c3, c4 = st.columns(2)
            with c3:
                # Nếu là MỨT: nhập g/cốc -> chuyển cups_per_kg
                if cat == "MUT":
                    g_per_cup = st.number_input("g/cốc (MỨT)", min_value=0.0, step=1.0, value=0.0, key="gpc_prod")
                    cups_per_kg = (1000.0 / g_per_cup) if g_per_cup > 0 else 0.0
                else:
                    cups_per_kg = st.number_input("Cốc/kg TP", min_value=0.0, step=0.1, value=0.0, key="cpc_prod")
            with c4:
                price_ref = st.number_input("Giá tham chiếu", min_value=0.0, step=1000.0, value=0.0)

            ok_p = st.form_submit_button("Lưu SP", type="primary")
        if ok_p:
            if not pcode or not name:
                st.error("Thiếu mã hoặc tên.")
            else:
                run_sql(conn, """
                    INSERT INTO products(code,name,cat_code,uom,cups_per_kg,price_ref)
                    VALUES (:c,:n,:g,:u,:k,:p)
                    ON CONFLICT (code) DO UPDATE SET
                      name=EXCLUDED.name, cat_code=EXCLUDED.cat_code,
                      uom=EXCLUDED.uom, cups_per_kg=EXCLUDED.cups_per_kg,
                      price_ref=EXCLUDED.price_ref
                """, {"c": pcode.strip(), "n": name.strip(), "g": cat,
                      "u": uom.strip(), "k": float(cups_per_kg), "p": float(price_ref)})
                write_audit(conn, "PROD_UPSERT", pcode); st.success("Đã lưu."); st.rerun()

        delp = st.selectbox("Xoá SP", ["—"] + dfp["code"].tolist(), index=0, key="del_sp")
        if delp != "—" and st.button("Xoá sản phẩm"):
            run_sql(conn, "DELETE FROM products WHERE code=:c", {"c": delp})
            write_audit(conn, "PROD_DELETE", delp); st.rerun()

    # ===== TAB 3: CÔNG THỨC (formulas per 1kg SƠ CHẾ) =====
    with tabs[2]:
        st.subheader("Công thức (định mức **/ 1kg SƠ CHẾ**)")
        _note("• CỐT: có **hệ số thu hồi** (kg TP / 1kg sơ chế).  • MỨT: **không** có hệ số; nhập **g/cốc**.")

        df_ct = fetch_df(conn, """
            SELECT code,name,type,output_pcode,output_uom,recovery,cups_per_kg,note
            FROM formulas ORDER BY type,name
        """)
        st.dataframe(df_ct, use_container_width=True, height=280)

        st.markdown("##### ➕ Thêm / Sửa")
        # nhớ & reload khi đổi Loại
        def _change_type():
            st.session_state["__ct_type__"] = st.session_state.get("__type_pick__", "COT")
            st.rerun()

        typ_default = st.session_state.get("__ct_type__", "COT")
        typ = st.selectbox("Loại", ["COT","MUT"],
                           index=(0 if typ_default=="COT" else 1),
                           key="__type_pick__", on_change=_change_type)

        # Sản phẩm đầu ra theo Loại
        out_cat = "COT" if typ=="COT" else "MUT"
        df_out = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code=:c ORDER BY name", {"c": out_cat})
        out_opts = ["— Chọn —"] + [f"{r['code']} — {r['name']}" for _, r in df_out.iterrows()]
        out_pick = st.selectbox("SP đầu ra", out_opts, index=0, key="__out_pick__")
        output_pcode = "" if out_pick=="— Chọn —" else out_pick.split(" — ",1)[0]

        c1, c2, c3 = st.columns([1.4, 1, 1])
        with c1:
            code = st.text_input("Mã CT")
            name = st.text_input("Tên CT")
        with c2:
            if typ == "MUT":
                g_per_cup = st.number_input("g/cốc (MỨT)", min_value=0.0, step=1.0, value=0.0, key="__gpc__")
                cups_per_kg = (1000.0 / g_per_cup) if g_per_cup > 0 else 0.0
            else:
                cups_per_kg = st.number_input("Cốc/kg TP (CỐT)", min_value=0.0, step=0.1, value=0.0, key="__cpc__")
        with c3:
            if typ == "COT":
                recovery = st.number_input("Hệ số thu hồi (kg TP / 1kg sơ chế)", min_value=0.01, step=0.01, value=1.00)
            else:
                recovery = 1.0
                st.caption("MỨT: không dùng hệ số thu hồi.")

        # Nguyên liệu chính (trái cây / cốt) — kg / 1kg sơ chế
        st.markdown("###### Nguyên liệu chính (kg / 1kg sơ chế)")
        df_fruit = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code='TRAI_CAY' ORDER BY name")
        df_cot   = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code='COT' ORDER BY name")

        fruit_choices = [f"{r['code']} — {r['name']}" for _, r in df_fruit.iterrows()]
        cot_choices   = [f"{r['code']} — {r['name']}" for _, r in df_cot.iterrows()]

        picked_fruit = st.multiselect("Trái cây", fruit_choices, key="__raw_fruit__")
        picked_cot   = st.multiselect("CỐT", cot_choices, key="__raw_cot__")

        raw_inputs = {}  # pcode -> qty_per_kg
        for item in picked_fruit:
            p = item.split(" — ",1)[0]
            q = st.number_input(f"{item} — kg / 1kg sơ chế", min_value=0.0, step=0.01, value=0.0, key=f"q_f_{p}")
            if q > 0: raw_inputs[p] = ("TRAI_CAY", q)
        for item in picked_cot:
            p = item.split(" — ",1)[0]
            q = st.number_input(f"{item} — kg / 1kg sơ chế", min_value=0.0, step=0.01, value=0.0, key=f"q_c_{p}")
            if q > 0: raw_inputs[p] = ("COT", q)

        # Phụ gia — kg / 1kg sơ chế
        st.markdown("###### Phụ gia (kg / 1kg sơ chế)")
        df_add = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code='PHU_GIA' ORDER BY name")
        add_choices = [f"{r['code']} — {r['name']}" for _, r in df_add.iterrows()]
        picked_add = st.multiselect("Chọn phụ gia", add_choices, key="__adds__")

        add_inputs = {}  # pcode -> qty_per_kg
        for item in picked_add:
            p = item.split(" — ",1)[0]
            q = st.number_input(f"{item} — kg / 1kg sơ chế", min_value=0.0, step=0.01, value=0.0, key=f"q_a_{p}")
            if q > 0: add_inputs[p] = q

        # Lưu (upsert)
        if st.button("💾 Lưu công thức", type="primary"):
            if not code or not name or not output_pcode:
                st.error("Thiếu mã/tên/SP đầu ra.")
            elif len(raw_inputs)==0 and len(add_inputs)==0:
                st.error("Chưa khai nguyên liệu.")
            else:
                note = "SRC=COT" if any(k=="COT" for k,_ in raw_inputs.values()) else "SRC=TRAI_CAY"
                run_sql(conn, """
                    INSERT INTO formulas(code,name,type,output_pcode,output_uom,recovery,cups_per_kg,note)
                    VALUES (:c,:n,:t,:o,'kg',:r,:k,:x)
                    ON CONFLICT (code) DO UPDATE SET
                      name=EXCLUDED.name, type=EXCLUDED.type,
                      output_pcode=EXCLUDED.output_pcode, output_uom=EXCLUDED.output_uom,
                      recovery=EXCLUDED.recovery, cups_per_kg=EXCLUDED.cups_per_kg, note=EXCLUDED.note
                """, {"c": code.strip(), "n": name.strip(), "t": typ, "o": output_pcode,
                      "r": float(recovery), "k": float(cups_per_kg), "x": note})
                # detail
                run_sql(conn, "DELETE FROM formula_inputs WHERE formula_code=:c", {"c": code.strip()})
                for p,(kind,q) in raw_inputs.items():
                    run_sql(conn, """
                        INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind)
                        VALUES (:f,:p,:q,:k)
                    """, {"f": code.strip(), "p": p, "q": float(q), "k": kind})
                for p,q in add_inputs.items():
                    run_sql(conn, """
                        INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind)
                        VALUES (:f,:p,:q,'PHU_GIA')
                    """, {"f": code.strip(), "p": p, "q": float(q)})
                write_audit(conn, "FORMULA_UPSERT", code); st.success("Đã lưu."); st.rerun()

        # Xoá
        del_ct = st.selectbox("🗑️ Xoá CT", ["—"] + df_ct["code"].tolist(), index=0, key="__del_ct__")
        if del_ct != "—" and st.button("Xoá CT"):
            run_sql(conn, "DELETE FROM formula_inputs WHERE formula_code=:c", {"c": del_ct})
            run_sql(conn, "DELETE FROM formulas WHERE code=:c", {"c": del_ct})
            write_audit(conn, "FORMULA_DELETE", del_ct); st.success("Đã xoá."); st.rerun()
