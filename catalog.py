import streamlit as st
import pandas as pd
from utils import fetch_df, run_sql, write_audit

def page_catalog(conn, user):
    st.markdown("## 📂 Danh mục & Công thức")

    tabs = st.tabs(["📑 Danh mục SP", "📦 Sản phẩm", "🧪 Công thức"])

    # ========== TAB 1: DANH MỤC ==========
    with tabs[0]:
        st.subheader("Danh mục sản phẩm")
        df = fetch_df(conn, "SELECT code,name FROM categories ORDER BY code")
        st.dataframe(df, use_container_width=True)

        with st.form("form_cat", clear_on_submit=True):
            code = st.text_input("Mã danh mục")
            name = st.text_input("Tên danh mục")
            ok = st.form_submit_button("💾 Lưu")
            if ok and code and name:
                run_sql(conn, """
                    INSERT INTO categories(code,name) VALUES (:c,:n)
                    ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name
                """, {"c": code.strip(), "n": name.strip()})
                write_audit(conn, "CAT_UPSERT", code)
                st.success("Đã lưu danh mục."); st.rerun()

        pick = st.selectbox("Xoá danh mục", ["—"]+[r["code"] for _,r in df.iterrows()], index=0)
        if pick!="—" and st.button("🗑️ Xoá danh mục"):
            run_sql(conn, "DELETE FROM categories WHERE code=:c", {"c": pick})
            write_audit(conn, "CAT_DELETE", pick)
            st.success("Đã xoá."); st.rerun()

    # ========== TAB 2: SẢN PHẨM ==========
    with tabs[1]:
        st.subheader("Sản phẩm")
        df = fetch_df(conn, """
            SELECT code,name,cat_code,uom,cups_per_kg,price_ref
            FROM products ORDER BY name
        """)
        st.dataframe(df, use_container_width=True)

        with st.form("form_prod", clear_on_submit=True):
            code = st.text_input("Mã SP")
            name = st.text_input("Tên SP")
            cat = st.selectbox("Nhóm", ["TRAI_CAY","COT","MUT","PHU_GIA","TP_KHAC"])
            uom = st.text_input("ĐVT", value="kg")
            cups = st.number_input("Cốc/kg TP", value=0.0, step=0.1, min_value=0.0)
            pref = st.number_input("Giá tham chiếu", value=0.0, step=1000.0, min_value=0.0)
            ok = st.form_submit_button("💾 Lưu")
            if ok and code and name:
                run_sql(conn, """
                    INSERT INTO products(code,name,cat_code,uom,cups_per_kg,price_ref)
                    VALUES (:c,:n,:g,:u,:k,:p)
                    ON CONFLICT (code) DO UPDATE SET 
                      name=EXCLUDED.name, cat_code=EXCLUDED.cat_code,
                      uom=EXCLUDED.uom, cups_per_kg=EXCLUDED.cups_per_kg,
                      price_ref=EXCLUDED.price_ref
                """, {"c":code.strip(),"n":name.strip(),"g":cat,
                      "u":uom.strip(),"k":float(cups),"p":float(pref)})
                write_audit(conn, "PROD_UPSERT", code)
                st.success("Đã lưu sản phẩm."); st.rerun()

        pick = st.selectbox("Xoá SP", ["—"]+[r["code"] for _,r in df.iterrows()], index=0)
        if pick!="—" and st.button("🗑️ Xoá sản phẩm"):
            run_sql(conn, "DELETE FROM products WHERE code=:c", {"c": pick})
            write_audit(conn, "PROD_DELETE", pick)
            st.success("Đã xoá."); st.rerun()

    # ========== TAB 3: CÔNG THỨC ==========
    with tabs[2]:
        st.subheader("Công thức sản xuất")

        df = fetch_df(conn, """
            SELECT code,name,type,output_pcode,output_uom,
                   recovery,cups_per_kg,note
            FROM formulas ORDER BY type,name
        """)
        st.dataframe(df, use_container_width=True)

        with st.form("form_formula", clear_on_submit=True):
            code = st.text_input("Mã CT")
            name = st.text_input("Tên CT")
            typ = st.selectbox("Loại CT", ["COT","MUT"])

            # output product
            out_cat = "COT" if typ=="COT" else "MUT"
            df_out = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code=:c ORDER BY name", {"c": out_cat})
            out_opts = ["— chọn —"]+[f"{r['code']} — {r['name']}" for _,r in df_out.iterrows()]
            out_pick = st.selectbox("SP đầu ra", out_opts, index=0)
            output_pcode = "" if out_pick.startswith("—") else out_pick.split(" — ",1)[0]

            if typ=="COT":
                recovery = st.number_input("Hệ số thu hồi (CỐT)", value=1.0, step=0.01, min_value=0.01)
                cups = st.number_input("Số cốc / kg TP", value=0.0, step=0.1, min_value=0.0)
            else:  # MỨT
                recovery = 1.0
                cups = st.number_input("g / cốc (MỨT)", value=0.0, step=1.0, min_value=0.0)

            ok = st.form_submit_button("💾 Lưu công thức")
            if ok and code and name and output_pcode:
                run_sql(conn, """
                    INSERT INTO formulas(code,name,type,output_pcode,output_uom,recovery,cups_per_kg,note)
                    VALUES (:c,:n,:t,:o,'kg',:r,:k,'')
                    ON CONFLICT (code) DO UPDATE SET
                      name=EXCLUDED.name, type=EXCLUDED.type,
                      output_pcode=EXCLUDED.output_pcode, output_uom=EXCLUDED.output_uom,
                      recovery=EXCLUDED.recovery, cups_per_kg=EXCLUDED.cups_per_kg, note=EXCLUDED.note
                """, {"c": code.strip(),"n": name.strip(),"t": typ,
                      "o": output_pcode,"r": float(recovery),"k": float(cups)})
                write_audit(conn, "FORMULA_UPSERT", code)
                st.success("Đã lưu công thức."); st.rerun()

        pick = st.selectbox("Xoá CT", ["—"]+[r["code"] for _,r in df.iterrows()], index=0, key="del_ct")
        if pick!="—" and st.button("🗑️ Xoá công thức"):
            run_sql(conn, "DELETE FROM formulas WHERE code=:c", {"c": pick})
            write_audit(conn, "FORMULA_DELETE", pick)
            st.success("Đã xoá."); st.rerun()
