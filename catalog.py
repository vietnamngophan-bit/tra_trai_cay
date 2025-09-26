# catalog.py
import streamlit as st
from core import fetch_df, run_sql, write_audit

def page_catalog(conn, user):
    st.markdown("### 🧾 Danh mục")
    tabs = st.tabs(["Danh mục SP", "Sản phẩm", "Công thức"])

    # ==========================================================
    # TAB 1: DANH MỤC
    # ==========================================================
    with tabs[0]:
        df_cat = fetch_df(conn, "SELECT code,name FROM categories ORDER BY code")
        st.dataframe(df_cat, use_container_width=True, height=280)

        with st.form("fm_cat_add", clear_on_submit=True):
            c1, c2 = st.columns([1,2])
            with c1: code = st.text_input("Mã")
            with c2: name = st.text_input("Tên")
            if st.form_submit_button("💾 Lưu", type="primary"):
                if code and name:
                    run_sql(conn, """
                        INSERT INTO categories(code,name) VALUES (:c,:n)
                        ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name
                    """, {"c": code.strip(), "n": name.strip()})
                    write_audit(conn, "CAT_UPSERT", code); st.rerun()

        pick = st.selectbox("🗑️ Xoá mã", ["—"] + df_cat["code"].tolist(), index=0)
        if pick != "—" and st.button("Xoá danh mục"):
            run_sql(conn, "DELETE FROM categories WHERE code=:c", {"c": pick})
            write_audit(conn, "CAT_DELETE", pick); st.rerun()

    # ==========================================================
    # TAB 2: SẢN PHẨM
    # ==========================================================
    with tabs[1]:
        df_prod = fetch_df(conn, """
            SELECT code,name,cat_code,uom,cups_per_kg,price_ref
            FROM products ORDER BY name
        """)
        st.dataframe(df_prod, use_container_width=True, height=300)

        with st.form("fm_prod_add", clear_on_submit=True):
            c1, c2 = st.columns([1,2])
            with c1:
                pcode = st.text_input("Mã SP")
                uom   = st.text_input("ĐVT", value="kg")
            with c2:
                name  = st.text_input("Tên SP")
                cat   = st.selectbox("Nhóm", ["TRAI_CAY","COT","MUT","PHU_GIA","TP_KHAC"])
            c3, c4 = st.columns(2)
            with c3:
                if cat == "MUT":
                    g_per_cup = st.number_input("g/cốc (MỨT)", min_value=0.0, step=1.0, value=0.0)
                    cups_per_kg = (1000.0 / g_per_cup) if g_per_cup > 0 else 0.0
                else:
                    cups_per_kg = st.number_input("Cốc/kg TP", min_value=0.0, step=0.1, value=0.0)
            with c4:
                price_ref = st.number_input("Giá tham chiếu", min_value=0.0, step=1000.0, value=0.0)

            if st.form_submit_button("💾 Lưu SP", type="primary"):
                if pcode and name:
                    run_sql(conn, """
                        INSERT INTO products(code,name,cat_code,uom,cups_per_kg,price_ref)
                        VALUES (:c,:n,:g,:u,:k,:p)
                        ON CONFLICT (code) DO UPDATE SET
                          name=EXCLUDED.name, cat_code=EXCLUDED.cat_code,
                          uom=EXCLUDED.uom, cups_per_kg=EXCLUDED.cups_per_kg,
                          price_ref=EXCLUDED.price_ref
                    """, {"c": pcode.strip(), "n": name.strip(), "g": cat,
                          "u": uom.strip(), "k": float(cups_per_kg), "p": float(price_ref)})
                    write_audit(conn, "PROD_UPSERT", pcode); st.rerun()

        delp = st.selectbox("🗑️ Xoá SP", ["—"] + df_prod["code"].tolist(), index=0, key="del_sp")
        if delp != "—" and st.button("Xoá sản phẩm"):
            run_sql(conn, "DELETE FROM products WHERE code=:c", {"c": delp})
            write_audit(conn, "PROD_DELETE", delp); st.rerun()

    # ==========================================================
    # TAB 3: CÔNG THỨC
    # ==========================================================
    with tabs[2]:
        st.markdown("#### 🧪 Công thức (định mức / 1kg SƠ CHẾ)")
        st.caption("- CỐT: có hệ số thu hồi (kg TP / 1kg sơ chế)")
        st.caption("- MỨT: không có hệ số; nhập g/cốc")

        df_ct = fetch_df(conn, """
            SELECT code,name,type,output_pcode,output_uom,recovery,cups_per_kg,note
            FROM formulas ORDER BY type,name
        """)
        st.dataframe(df_ct, use_container_width=True, height=260)

        # Thêm / sửa công thức
        st.markdown("##### ➕ Thêm / Sửa công thức")

        typ = st.selectbox("Loại", ["COT","MUT"], key="ct_type")

        # Sản phẩm đầu ra
        out_cat = "COT" if typ == "COT" else "MUT"
        df_out = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code=:c ORDER BY name", {"c": out_cat})
        out_opts = ["— Chọn —"] + [f"{r['code']} — {r['name']}" for _, r in df_out.iterrows()]
        out_pick = st.selectbox("SP đầu ra", out_opts, index=0)
        output_pcode = "" if out_pick == "— Chọn —" else out_pick.split(" — ", 1)[0]

        c1, c2, c3 = st.columns([1.5,1,1])
        with c1:
            code = st.text_input("Mã CT")
            name = st.text_input("Tên CT")
        with c2:
            if typ == "MUT":
                g_per_cup = st.number_input("g/cốc (MỨT)", min_value=0.0, step=1.0, value=0.0)
                cups_per_kg = (1000.0 / g_per_cup) if g_per_cup > 0 else 0.0
            else:
                cups_per_kg = st.number_input("Cốc/kg TP (CỐT)", min_value=0.0, step=0.1, value=0.0)
        with c3:
            if typ == "COT":
                recovery = st.number_input("Hệ số thu hồi (CỐT)", min_value=0.01, step=0.01, value=1.00)
            else:
                recovery = 1.0
                st.caption("MỨT: không có hệ số")

        # Trái cây chỉ để chọn, không set định lượng
        st.markdown("##### Trái cây (chỉ chọn, không nhập số kg)")
        df_fruits = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code='TRAI_CAY' ORDER BY name")
        fruit_opts = [f"{r['code']} — {r['name']}" for _, r in df_fruits.iterrows()]
        picked_fruits = st.multiselect("Chọn trái cây", fruit_opts)

        # Phụ gia và TP khác: phải set định lượng /kg sơ chế
        st.markdown("##### Nguyên liệu khác (/kg sơ chế)")
        df_others = fetch_df(conn, """
            SELECT code,name FROM products WHERE cat_code IN ('PHU_GIA','TP_KHAC') ORDER BY name
        """)
        other_opts = [f"{r['code']} — {r['name']}" for _, r in df_others.iterrows()]
        picked_others = st.multiselect("Chọn NVL", other_opts)

        other_inputs = {}
        for item in picked_others:
            p = item.split(" — ", 1)[0]
            q = st.number_input(f"{item} — kg / 1kg sơ chế", min_value=0.0, step=0.01, value=0.0, key=f"q_other_{p}")
            if q > 0: other_inputs[p] = q

        if st.button("💾 Lưu CT", type="primary"):
            if not code or not name or not output_pcode:
                st.error("Thiếu mã/tên/SP đầu ra.")
            else:
                run_sql(conn, """
                    INSERT INTO formulas(code,name,type,output_pcode,output_uom,recovery,cups_per_kg,note)
                    VALUES (:c,:n,:t,:o,'kg',:r,:k,:x)
                    ON CONFLICT (code) DO UPDATE SET
                      name=EXCLUDED.name, type=EXCLUDED.type,
                      output_pcode=EXCLUDED.output_pcode, output_uom=EXCLUDED.output_uom,
                      recovery=EXCLUDED.recovery, cups_per_kg=EXCLUDED.cups_per_kg, note=EXCLUDED.note
                """, {"c": code.strip(), "n": name.strip(), "t": typ, "o": output_pcode,
                      "r": float(recovery), "k": float(cups_per_kg),
                      "x": "FRUITS=" + ",".join([f.split(" — ")[0] for f in picked_fruits])})
                run_sql(conn, "DELETE FROM formula_inputs WHERE formula_code=:c", {"c": code.strip()})
                for p, q in other_inputs.items():
                    run_sql(conn, """
                        INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind)
                        VALUES (:f,:p,:q,'OTHER')
                    """, {"f": code.strip(), "p": p, "q": float(q)})
                write_audit(conn, "FORMULA_UPSERT", code); st.success("Đã lưu."); st.rerun()

        del_ct = st.selectbox("🗑️ Xoá CT", ["—"] + df_ct["code"].tolist(), index=0)
        if del_ct != "—" and st.button("Xoá CT"):
            run_sql(conn, "DELETE FROM formula_inputs WHERE formula_code=:c", {"c": del_ct})
            run_sql(conn, "DELETE FROM formulas WHERE code=:c", {"c": del_ct})
            write_audit(conn, "FORMULA_DELETE", del_ct); st.success("Đã xoá."); st.rerun()
