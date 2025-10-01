# catalog.py
import streamlit as st
from core import fetch_df, run_sql, write_audit

def page_catalog(conn, user):
    st.markdown("## 🧾 Danh mục")
    tabs = st.tabs(["Danh mục SP", "Sản phẩm", "Công thức"])

    # ---------------- TAB 1: DANH MỤC ----------------
    with tabs[0]:
        df_cat = fetch_df(conn, "SELECT code,name FROM categories ORDER BY code")
        st.dataframe(df_cat, use_container_width=True, height=250)

        with st.form("fm_cat", clear_on_submit=True):
            c1, c2 = st.columns([1,3])
            with c1: code = st.text_input("Mã nhóm")
            with c2: name = st.text_input("Tên nhóm")
            if st.form_submit_button("Lưu", type="primary"):
                if code and name:
                    run_sql(conn, """
                        INSERT INTO categories(code,name) VALUES (:c,:n)
                        ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name
                    """, {"c": code.strip(), "n": name.strip()})
                    write_audit(conn, "CAT_UPSERT", code); st.rerun()
        del_code = st.selectbox("Xoá nhóm", ["—"]+[r["code"] for _,r in df_cat.iterrows()], index=0)
        if del_code != "—" and st.button("Xoá nhóm"):
            run_sql(conn, "DELETE FROM categories WHERE code=:c", {"c": del_code})
            write_audit(conn, "CAT_DELETE", del_code); st.rerun()

    # ---------------- TAB 2: SẢN PHẨM ----------------
    with tabs[1]:
        df_prod = fetch_df(conn, """
            SELECT code,name,cat_code,uom,cups_per_kg,price_ref
            FROM products ORDER BY name
        """)
        st.dataframe(df_prod, use_container_width=True, height=280)

        with st.form("fm_prod", clear_on_submit=True):
            c1, c2 = st.columns([1,3])
            with c1: code = st.text_input("Mã SP")
            with c2: name = st.text_input("Tên SP")
            cat = st.selectbox("Nhóm", ["TRAI_CAY","COT","MUT","PHU_GIA","SINH_TO","TP_KHAC"])
            uom = st.text_input("ĐVT", value="kg")

            c3, c4 = st.columns(2)
            with c3:
                if cat == "MUT":
                    gpc = st.number_input("g/cốc (MỨT)", min_value=0.0, step=1.0)
                    cups_per_kg = (1000.0 / gpc) if gpc>0 else 0.0
                else:
                    cups_per_kg = st.number_input("Cốc/kg TP", min_value=0.0, step=0.1)
            with c4:
                price_ref = st.number_input("Giá tham chiếu", min_value=0.0, step=1000.0)

            if st.form_submit_button("Lưu SP", type="primary"):
                if code and name:
                    run_sql(conn, """
                        INSERT INTO products(code,name,cat_code,uom,cups_per_kg,price_ref)
                        VALUES (:c,:n,:g,:u,:k,:p)
                        ON CONFLICT (code) DO UPDATE SET
                          name=EXCLUDED.name, cat_code=EXCLUDED.cat_code,
                          uom=EXCLUDED.uom, cups_per_kg=EXCLUDED.cups_per_kg,
                          price_ref=EXCLUDED.price_ref
                    """, {"c": code.strip(), "n": name.strip(), "g": cat,
                          "u": uom.strip(), "k": cups_per_kg, "p": price_ref})
                    write_audit(conn, "PROD_UPSERT", code); st.rerun()
        del_prod = st.selectbox("Xoá SP", ["—"]+[r["code"] for _,r in df_prod.iterrows()], index=0, key="del_prod")
        if del_prod != "—" and st.button("Xoá SP"):
            run_sql(conn, "DELETE FROM products WHERE code=:c", {"c": del_prod})
            write_audit(conn, "PROD_DELETE", del_prod); st.rerun()

    # ---------------- TAB 3: CÔNG THỨC ----------------
    with tabs[2]:
        st.caption("⚙️ Công thức định mức (per 1kg SƠ CHẾ). "
                   "• CỐT: có hệ số thu hồi, cốc/kg TP. "
                   "• MỨT: không có hệ số, nhập g/cốc.")

        df_ct = fetch_df(conn, "SELECT code,name,type,output_pcode,recovery,cups_per_kg,note FROM formulas ORDER BY name")
        st.dataframe(df_ct, use_container_width=True, height=260)

        with st.form("fm_ct", clear_on_submit=True):
            code = st.text_input("Mã CT")
            name = st.text_input("Tên CT")
            typ = st.selectbox("Loại", ["COT","MUT"])

            # SP đầu ra theo loại
            df_out = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code=:t ORDER BY name",
                              {"t": typ})
            out_opts = ["— Chọn —"]+[f"{r['code']} — {r['name']}" for _,r in df_out.iterrows()]
            out_pick = st.selectbox("SP đầu ra", out_opts)
            output_pcode = "" if out_pick=="— Chọn —" else out_pick.split(" — ",1)[0]

            c1, c2 = st.columns(2)
            with c1:
                if typ=="COT":
                    recovery = st.number_input("Hệ số thu hồi (kg TP/1kg sơ chế)", min_value=0.01, step=0.01, value=1.0)
                else:
                    recovery = 1.0
                cups = st.number_input("Cốc/kg TP" if typ=="COT" else "g/cốc (MỨT)", min_value=0.0, step=0.1)
                cups_per_kg = (1000/cups) if typ=="MUT" and cups>0 else cups
            with c2:
                note = st.text_area("Ghi chú / SRC")

            st.markdown("**Chọn NVL chính (Trái cây hoặc Cốt cho Mứt)**")
            raw_inputs, add_inputs = {}, {}

            df_fruit = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code='TRAI_CAY' ORDER BY name")
            fruits = st.multiselect("Trái cây", [f"{r['code']} — {r['name']}" for _,r in df_fruit.iterrows()])
            for f in fruits:
                raw_inputs[f.split(" — ",1)[0]] = 0.0  # chỉ để xuất kho, không định lượng ở công thức

            if typ=="MUT":
                df_cot = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code='COT' ORDER BY name")
                cots = st.multiselect("Cốt (cho Mứt từ cốt)", [f"{r['code']} — {r['name']}" for _,r in df_cot.iterrows()])
                for c in cots:
                    raw_inputs[c.split(" — ",1)[0]] = 0.0

            st.markdown("**Phụ gia / Nguyên liệu khác (kg hoặc ml / 1kg sơ chế)**")
            df_other = fetch_df(conn, "SELECT code,name,uom FROM products WHERE cat_code IN ('PHU_GIA','SINH_TO') ORDER BY name")
            for _,r in df_other.iterrows():
                q = st.number_input(f"{r['name']} ({r['uom']})", min_value=0.0, step=0.01)
                if q>0: add_inputs[r["code"]] = q

            if st.form_submit_button("Lưu CT", type="primary"):
                if not code or not name or not output_pcode:
                    st.error("Thiếu thông tin bắt buộc.")
                else:
                    run_sql(conn, """
                        INSERT INTO formulas(code,name,type,output_pcode,output_uom,recovery,cups_per_kg,note)
                        VALUES (:c,:n,:t,:o,'kg',:r,:k,:x)
                        ON CONFLICT (code) DO UPDATE SET
                          name=EXCLUDED.name, type=EXCLUDED.type,
                          output_pcode=EXCLUDED.output_pcode, recovery=EXCLUDED.recovery,
                          cups_per_kg=EXCLUDED.cups_per_kg, note=EXCLUDED.note
                    """, {"c": code.strip(), "n": name.strip(), "t": typ, "o": output_pcode,
                          "r": recovery, "k": cups_per_kg, "x": note.strip()})
                    run_sql(conn, "DELETE FROM formula_inputs WHERE formula_code=:c", {"c": code.strip()})
                    for p in raw_inputs:
                        run_sql(conn, """
                            INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind)
                            VALUES (:f,:p,0,:k)
                        """, {"f": code.strip(), "p": p, "k": "SRC"})
                    for p,q in add_inputs.items():
                        run_sql(conn, """
                            INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind)
                            VALUES (:f,:p,:q,'OTHER')
                        """, {"f": code.strip(), "p": p, "q": q})
                    write_audit(conn,"FORMULA_UPSERT",code); st.success("Đã lưu."); st.rerun()

        del_ct = st.selectbox("Xoá CT", ["—"]+df_ct["code"].tolist(), index=0)
        if del_ct!="—" and st.button("Xoá CT"):
            run_sql(conn, "DELETE FROM formula_inputs WHERE formula_code=:c", {"c": del_ct})
            run_sql(conn, "DELETE FROM formulas WHERE code=:c", {"c": del_ct})
            write_audit(conn,"FORMULA_DELETE",del_ct); st.success("Đã xoá."); st.rerun()
