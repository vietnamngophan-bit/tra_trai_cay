# catalog.py
import streamlit as st
from core import fetch_df, run_sql, write_audit

def page_catalog(conn, user):
    st.markdown("### 🧾 Danh mục")
    tabs = st.tabs(["Danh mục SP", "Sản phẩm", "Công thức"])

    # ----------------- TAB 1: DANH MỤC -----------------
    with tabs[0]:
        df = fetch_df(conn, "SELECT code,name FROM categories ORDER BY code")
        st.dataframe(df, use_container_width=True, height=280)

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
        pick = st.selectbox("🗑️ Xoá danh mục", ["—"] + df["code"].tolist(), index=0)
        if pick != "—" and st.button("Xoá DM"):
            run_sql(conn, "DELETE FROM categories WHERE code=:c", {"c": pick})
            write_audit(conn, "CAT_DELETE", pick); st.rerun()

    # ----------------- TAB 2: SẢN PHẨM -----------------
    with tabs[1]:
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
                cat   = st.selectbox("Nhóm", ["TRAI_CAY","COT","MUT","PHU_GIA","TP_KHAC"])
            c3, c4 = st.columns(2)
            with c3:
                cups_per_kg = st.number_input("Cốc/kg TP (nếu áp dụng)", min_value=0.0, step=0.1, value=0.0)
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
                    """, {"c": pcode.strip(), "n": name.strip(), "g": cat, "u": uom.strip(),
                          "k": float(cups_per_kg), "p": float(price_ref)})
                    write_audit(conn, "PROD_UPSERT", pcode); st.rerun()

        delp = st.selectbox("🗑️ Xoá SP", ["—"] + dfp["code"].tolist(), index=0, key="del_sp")
        if delp != "—" and st.button("Xoá SP"):
            run_sql(conn, "DELETE FROM products WHERE code=:c", {"c": delp})
            write_audit(conn, "PROD_DELETE", delp); st.rerun()

    # ----------------- TAB 3: CÔNG THỨC -----------------
    with tabs[2]:
        st.markdown("#### 🧪 Công thức sản xuất (định mức trên **1kg sơ chế**)")

        df_ct = fetch_df(conn, """
            SELECT code,name,type,output_pcode,recovery,cups_per_kg,note
            FROM formulas ORDER BY type,name
        """)
        st.dataframe(df_ct, use_container_width=True, height=260)

        with st.form("fm_ct_add", clear_on_submit=True):
            typ = st.selectbox("Loại CT", ["COT","MUT"], key="ct_type")

            code = st.text_input("Mã CT")
            name = st.text_input("Tên CT")

            # sản phẩm đầu ra
            out_cat = "COT" if typ=="COT" else "MUT"
            df_out = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code=:c ORDER BY name", {"c": out_cat})
            out_opts = ["—"] + [f"{r['code']} — {r['name']}" for _, r in df_out.iterrows()]
            out_pick = st.selectbox("Sản phẩm đầu ra", out_opts)
            output_pcode = "" if out_pick=="—" else out_pick.split(" — ",1)[0]

            c1, c2 = st.columns(2)
            with c1:
                if typ=="COT":
                    recovery = st.number_input("Hệ số thu hồi (CỐT)", min_value=0.01, step=0.01, value=1.0)
                else:
                    recovery = 1.0
                    st.caption("MỨT: thành phẩm nhập tay khi SX.")
            with c2:
                if typ=="MUT":
                    g_per_cup = st.number_input("g/cốc (MỨT)", min_value=0.0, step=1.0, value=0.0)
                    cups_per_kg = (1000.0/g_per_cup) if g_per_cup>0 else 0.0
                else:
                    cups_per_kg = st.number_input("Cốc/kg TP (CỐT)", min_value=0.0, step=0.1, value=0.0)

            # NVL chính: chỉ chọn danh sách trái cây hoặc cốt, không nhập định mức
            st.markdown("##### NVL chính (chỉ chọn, định lượng điền ở SX)")
            if typ=="COT":
                df_fruit = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code='TRAI_CAY' ORDER BY name")
                fruit_opts = [f"{r['code']} — {r['name']}" for _,r in df_fruit.iterrows()]
                picked_fruits = st.multiselect("Chọn trái cây", fruit_opts)
                raw_inputs = {i.split(" — ",1)[0]:"TRAI_CAY" for i in picked_fruits}
            else:
                # Mứt có thể từ trái cây hoặc cốt
                src_kind = st.radio("Nguồn NVL chính (MỨT)", ["TRAI_CAY","COT"], horizontal=True)
                df_src = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code=:c ORDER BY name", {"c": src_kind})
                src_opts = [f"{r['code']} — {r['name']}" for _,r in df_src.iterrows()]
                picked_src = st.multiselect("Chọn NVL chính", src_opts)
                raw_inputs = {i.split(" — ",1)[0]:src_kind for i in picked_src}

            # NVL khác: nhập định mức theo đúng uom gốc / 1kg sơ chế
            st.markdown("##### NVL khác (định mức theo ĐVT gốc / 1kg sơ chế)")
            df_other = fetch_df(conn, "SELECT code,name,uom FROM products WHERE cat_code IN ('PHU_GIA','TP_KHAC') ORDER BY name")
            other_opts = [f"{r['code']} — {r['name']} ({r['uom']})" for _,r in df_other.iterrows()]
            picked_other = st.multiselect("Chọn NVL khác", other_opts)

            other_inputs = {}
            for item in picked_other:
                pcode = item.split(" — ",1)[0]
                uom = df_other.loc[df_other["code"]==pcode,"uom"].iloc[0]
                q = st.number_input(f"{item} — {uom}/1kg sơ chế", min_value=0.0, step=0.01, value=0.0, key=f"q_{pcode}")
                if q>0: other_inputs[pcode] = q

            if st.form_submit_button("💾 Lưu CT", type="primary"):
                if not code or not name or not output_pcode:
                    st.error("Thiếu thông tin.")
                else:
                    run_sql(conn, """
                        INSERT INTO formulas(code,name,type,output_pcode,output_uom,recovery,cups_per_kg,note)
                        VALUES (:c,:n,:t,:o,'kg',:r,:k,:x)
                        ON CONFLICT (code) DO UPDATE SET
                          name=EXCLUDED.name,type=EXCLUDED.type,
                          output_pcode=EXCLUDED.output_pcode,output_uom=EXCLUDED.output_uom,
                          recovery=EXCLUDED.recovery,cups_per_kg=EXCLUDED.cups_per_kg,note=EXCLUDED.note
                    """, {"c":code.strip(),"n":name.strip(),"t":typ,"o":output_pcode,
                          "r":float(recovery),"k":float(cups_per_kg),"x":("SRC=COT" if any(v=="COT" for v in raw_inputs.values()) else "SRC=TRAI_CAY")})
                    run_sql(conn,"DELETE FROM formula_inputs WHERE formula_code=:c",{"c":code.strip()})
                    for p,k in raw_inputs.items():
                        run_sql(conn,"INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind) VALUES (:f,:p,0,:k)",
                                {"f":code.strip(),"p":p,"k":k})
                    for p,q in other_inputs.items():
                        run_sql(conn,"INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind) VALUES (:f,:p,:q,'OTHER')",
                                {"f":code.strip(),"p":p,"q":float(q)})
                    write_audit(conn,"FORMULA_UPSERT",code); st.success("Đã lưu."); st.rerun()

        # Xoá công thức
        del_ct = st.selectbox("🗑️ Xoá CT", ["—"]+df_ct["code"].tolist(), index=0, key="del_ct")
        if del_ct!="—" and st.button("Xoá CT"):
            run_sql(conn,"DELETE FROM formula_inputs WHERE formula_code=:c",{"c":del_ct})
            run_sql(conn,"DELETE FROM formulas WHERE code=:c",{"c":del_ct})
            write_audit(conn,"FORMULA_DELETE",del_ct); st.success("Đã xoá."); st.rerun()
