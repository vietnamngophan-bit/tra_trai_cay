# catalog.py — Module 2: Danh mục (Categories / Products / Formulas)
# Yêu cầu: đã có các helper trong core.py: fetch_df(conn, sql, params), run_sql(conn, sql, params), write_audit(conn, action, detail)

from __future__ import annotations
import streamlit as st

# ===========================
#   DANH MỤC / SẢN PHẨM / CÔNG THỨC
# ===========================

def page_catalog(conn, user):
    st.header("🧾 Danh mục")
    tabs = st.tabs(["Danh mục SP", "Sản phẩm", "Công thức"])

    # ---------------------------------------------------------------------
    # TAB 1 — DANH MỤC
    # ---------------------------------------------------------------------
    with tabs[0]:
        st.subheader("Danh mục sản phẩm")
        df_cat = fetch_df(conn, "SELECT code, name FROM categories ORDER BY code")
        st.dataframe(df_cat, use_container_width=True, height=280)

        st.markdown("##### Thêm / Sửa danh mục")
        with st.form("fm_cat_upsert", clear_on_submit=True):
            c1, c2 = st.columns([1, 2])
            with c1: code = st.text_input("Mã nhóm")
            with c2: name = st.text_input("Tên nhóm")
            ok = st.form_submit_button("💾 Lưu", type="primary")
        if ok:
            if not code or not name:
                st.error("Thiếu mã hoặc tên.")
            else:
                run_sql(conn, """
                    INSERT INTO categories(code, name) VALUES (:c, :n)
                    ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name
                """, {"c": code.strip(), "n": name.strip()})
                write_audit(conn, "CAT_UPSERT", code)
                st.success("Đã lưu danh mục."); st.rerun()

        st.markdown("##### Xoá danh mục")
        pick = st.selectbox("Chọn mã cần xoá", ["—"] + df_cat["code"].tolist(), index=0, key="cat_del")
        if pick != "—" and st.button("🗑️ Xoá danh mục"):
            run_sql(conn, "DELETE FROM categories WHERE code=:c", {"c": pick})
            write_audit(conn, "CAT_DELETE", pick)
            st.success("Đã xoá."); st.rerun()

    # ---------------------------------------------------------------------
    # TAB 2 — SẢN PHẨM
    # ---------------------------------------------------------------------
    with tabs[1]:
        st.subheader("Sản phẩm")
        df_prod = fetch_df(conn, """
            SELECT code, name, cat_code, uom, cups_per_kg, price_ref
            FROM products ORDER BY name
        """)
        st.dataframe(df_prod, use_container_width=True, height=320)

        st.markdown("##### Thêm / Sửa sản phẩm")
        with st.form("fm_prod_upsert", clear_on_submit=True):
            c1, c2 = st.columns([1,2])
            with c1:
                pcode = st.text_input("Mã SP")
                uom   = st.text_input("ĐVT", value="kg")
            with c2:
                name  = st.text_input("Tên SP")
                cat   = st.selectbox("Nhóm", ["TRAI_CAY", "COT", "MUT", "PHU_GIA", "TP_KHAC"], index=0)

            c3, c4 = st.columns(2)
            with c3:
                if cat == "MUT":
                    g_per_cup = st.number_input("g/cốc (MỨT)", min_value=0.0, step=1.0, value=0.0, key="prod_gpc")
                    cups_per_kg = (1000.0 / g_per_cup) if g_per_cup > 0 else 0.0
                else:
                    cups_per_kg = st.number_input("Cốc/kg TP", min_value=0.0, step=0.1, value=0.0, key="prod_cpk")
            with c4:
                price_ref = st.number_input("Giá tham chiếu", min_value=0.0, step=1000.0, value=0.0)

            okp = st.form_submit_button("💾 Lưu SP", type="primary")

        if okp:
            if not pcode or not name:
                st.error("Thiếu mã hoặc tên.")
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
                """, {"c": pcode.strip(), "n": name.strip(), "g": cat, "u": uom.strip(),
                      "k": float(cups_per_kg), "p": float(price_ref)})
                write_audit(conn, "PROD_UPSERT", pcode)
                st.success("Đã lưu sản phẩm."); st.rerun()

        st.markdown("##### Xoá sản phẩm")
        delp = st.selectbox("Chọn SP cần xoá", ["—"] + df_prod["code"].tolist(), index=0, key="prod_del")
        if delp != "—" and st.button("🗑️ Xoá SP"):
            run_sql(conn, "DELETE FROM products WHERE code=:c", {"c": delp})
            write_audit(conn, "PROD_DELETE", delp)
            st.success("Đã xoá."); st.rerun()

    # ---------------------------------------------------------------------
    # TAB 3 — CÔNG THỨC (per 1kg SƠ CHẾ)
    # ---------------------------------------------------------------------
    with tabs[2]:
        st.subheader("Công thức (định mức / 1kg SƠ CHẾ)")
        st.caption("• Trái cây: chỉ CHỌN danh sách được phép (không định lượng).  "
                   "• Đường/Phụ gia & Sinh tố: nhập **kg / 1kg sơ chế**.  "
                   "• CỐT có **hệ số thu hồi**. • MỨT **không có hệ số**, nhập **g/cốc** để quy đổi.")

        df_ct = fetch_df(conn, """
            SELECT code,name,type,output_pcode,output_uom,recovery,cups_per_kg,note
            FROM formulas ORDER BY type,name
        """)
        st.dataframe(df_ct, use_container_width=True, height=260)

        st.markdown("##### ➕ Thêm / Sửa công thức")

        # ——— thông tin chung
        typ = st.selectbox("Loại", ["COT", "MUT"], index=0, key="ct_type_pick")

        out_cat = "COT" if typ == "COT" else "MUT"
        df_out = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code=:c ORDER BY name", {"c": out_cat})
        out_opts = ["— Chọn —"] + [f"{r['code']} — {r['name']}" for _, r in df_out.iterrows()]
        out_pick = st.selectbox("Sản phẩm đầu ra", out_opts, index=0, key="ct_out_pick")
        output_pcode = "" if out_pick == "— Chọn —" else out_pick.split(" — ", 1)[0]

        c1, c2, c3 = st.columns([1.5, 1, 1])
        with c1:
            code = st.text_input("Mã CT")
            name = st.text_input("Tên CT")
        with c2:
            if typ == "MUT":
                g_per_cup = st.number_input("g/cốc (MỨT)", min_value=0.0, step=1.0, value=0.0, key="ct_gpc")
                cups_per_kg = (1000.0 / g_per_cup) if g_per_cup > 0 else 0.0
            else:
                cups_per_kg = st.number_input("Cốc/kg TP (CỐT)", min_value=0.0, step=0.1, value=0.0, key="ct_cpk")
        with c3:
            if typ == "COT":
                recovery = st.number_input("Hệ số thu hồi (kg TP / 1kg sơ chế)", min_value=0.01, step=0.01, value=1.00)
            else:
                recovery = 1.0
                st.caption("MỨT: thành phẩm nhập tay khi sản xuất.")

        # ——— Trái cây được phép (không định lượng)
        st.markdown("###### Trái cây được phép (không nhập định lượng)")
        df_fruit = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code='TRAI_CAY' ORDER BY name")
        fruit_choices = [f"{r['code']} — {r['name']}" for _, r in df_fruit.iterrows()]
        allow_fruits = st.multiselect("Chọn trái cây", fruit_choices, key="ct_allow_fruits")

        # ——— Đường / Phụ gia (PHU_GIA) — định mức kg / 1kg sơ chế
        st.markdown("###### Đường & Phụ gia (kg / 1kg sơ chế)")
        df_sugar = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code='PHU_GIA' ORDER BY name")
        sugar_choices = [f"{r['code']} — {r['name']}" for _, r in df_sugar.iterrows()]
        picked_sugar = st.multiselect("Chọn Đường/Phụ gia", sugar_choices, key="ct_sugar_pick")
        sugar_inputs = {}
        for item in picked_sugar:
            p = item.split(" — ", 1)[0]
            q = st.number_input(f"{item} — kg / 1kg sơ chế", min_value=0.0, step=0.01, value=0.0,
                                key=f"ct_q_sugar_{p}")
            if q > 0: sugar_inputs[p] = q

        # ——— Sinh tố (TP_KHAC) — định mức kg / 1kg sơ chế
        st.markdown("###### Sinh tố (kg / 1kg sơ chế)")
        df_puree = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code='TP_KHAC' ORDER BY name")
        puree_choices = [f"{r['code']} — {r['name']}" for _, r in df_puree.iterrows()]
        picked_puree = st.multiselect("Chọn Sinh tố", puree_choices, key="ct_puree_pick")
        puree_inputs = {}
        for item in picked_puree:
            p = item.split(" — ", 1)[0]
            q = st.number_input(f"{item} — kg / 1kg sơ chế", min_value=0.0, step=0.01, value=0.0,
                                key=f"ct_q_puree_{p}")
            if q > 0: puree_inputs[p] = q

        # ——— Lưu / upsert CT
        if st.button("💾 Lưu công thức", type="primary", key="ct_save"):
            if not code or not name or not output_pcode:
                st.error("Thiếu mã/tên/SP đầu ra.")
            else:
                # header
                run_sql(conn, """
                    INSERT INTO formulas(code,name,type,output_pcode,output_uom,recovery,cups_per_kg,note)
                    VALUES (:c,:n,:t,:o,'kg',:r,:k,:x)
                    ON CONFLICT (code) DO UPDATE SET
                      name=EXCLUDED.name, type=EXCLUDED.type,
                      output_pcode=EXCLUDED.output_pcode, output_uom=EXCLUDED.output_uom,
                      recovery=EXCLUDED.recovery, cups_per_kg=EXCLUDED.cups_per_kg, note=EXCLUDED.note
                """, {
                    "c": code.strip(),
                    "n": name.strip(),
                    "t": typ,
                    "o": output_pcode,
                    "r": float(recovery),
                    "k": float(cups_per_kg),
                    # cờ đánh dấu: nguồn chính vẫn là trái cây; sinh tố/phụ gia là định mức kèm theo
                    "x": "SRC=TRAI_CAY"
                })
                # detail: xoá cũ, ghi mới
                run_sql(conn, "DELETE FROM formula_inputs WHERE formula_code=:c", {"c": code.strip()})

                # 1) Trái cây được phép (flag) — qty_per_kg = 0
                for item in allow_fruits:
                    p = item.split(" — ", 1)[0]
                    run_sql(conn, """
                        INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind)
                        VALUES (:f,:p,0,'TRAI_CAY_ALLOW')
                    """, {"f": code.strip(), "p": p})

                # 2) Phụ gia
                for p, q in sugar_inputs.items():
                    run_sql(conn, """
                        INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind)
                        VALUES (:f,:p,:q,'PHU_GIA')
                    """, {"f": code.strip(), "p": p, "q": float(q)})

                # 3) Sinh tố
                for p, q in puree_inputs.items():
                    run_sql(conn, """
                        INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind)
                        VALUES (:f,:p,:q,'SINH_TO')
                    """, {"f": code.strip(), "p": p, "q": float(q)})

                write_audit(conn, "FORMULA_UPSERT", code)
                st.success("Đã lưu công thức."); st.rerun()

        # ——— Xoá CT
        st.markdown("##### Xoá công thức")
        del_ct = st.selectbox("Chọn CT cần xoá", ["—"] + df_ct["code"].tolist(), index=0, key="ct_del_pick")
        if del_ct != "—" and st.button("🗑️ Xoá CT"):
            run_sql(conn, "DELETE FROM formula_inputs WHERE formula_code=:c", {"c": del_ct})
            run_sql(conn, "DELETE FROM formulas WHERE code=:c", {"c": del_ct})
            write_audit(conn, "FORMULA_DELETE", del_ct)
            st.success("Đã xoá."); st.rerun()
