# catalog.py
import json
import streamlit as st
from core import fetch_df, run_sql, write_audit

# ============= Helpers for reactive state =============
def _set(key, value=None):
    # callback: ghi lại lựa chọn rồi rerun
    if value is not None:
        st.session_state[key] = value
    st.rerun()

def _get(key, default):
    if key not in st.session_state:
        st.session_state[key] = default
    return st.session_state[key]

# ======================================================
def page_catalog(conn, user):
    st.markdown("### 🧾 Danh mục")
    tabs = st.tabs(["Danh mục SP", "Sản phẩm", "Công thức"])

    # ----------------- TAB 1: DANH MỤC -----------------
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

        del_code = st.selectbox("🗑️ Xoá danh mục", ["—"] + df_cat["code"].tolist(), index=0)
        if del_code != "—" and st.button("Xoá DM"):
            run_sql(conn, "DELETE FROM categories WHERE code=:c", {"c": del_code})
            write_audit(conn, "CAT_DELETE", del_code); st.rerun()

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
                # Mứt: cho nhập g/cốc -> tự quy đổi cups/kg; các loại khác nhập cốc/kg nếu cần
                if cat == "MUT":
                    gpc = st.number_input("g/cốc (MỨT)", min_value=0.0, step=1.0, value=0.0)
                    cups_per_kg = (1000.0/gpc) if gpc > 0 else 0.0
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
                    """, {"c": pcode.strip(), "n": name.strip(), "g": cat, "u": uom.strip(),
                          "k": float(cups_per_kg), "p": float(price_ref)})
                    write_audit(conn, "PROD_UPSERT", pcode); st.rerun()

        delp = st.selectbox("🗑️ Xoá SP", ["—"] + dfp["code"].tolist(), index=0, key="del_sp")
        if delp != "—" and st.button("Xoá SP"):
            run_sql(conn, "DELETE FROM products WHERE code=:c", {"c": delp})
            write_audit(conn, "PROD_DELETE", delp); st.rerun()

    # ----------------- TAB 3: CÔNG THỨC -----------------
    with tabs[2]:
        st.markdown("#### 🧪 Công thức (định mức theo **1kg SƠ CHẾ**)")
        st.caption("• **CỐT**: bắt buộc chọn TRÁI CÂY được phép + hệ số thu hồi. "
                   "• **MỨT**: 2 loại (từ TRÁI CÂY hoặc từ CỐT), **không có hệ số**; nhập **g/cốc**. "
                   "• NVL khác (đường/siro/sinh tố…): nhập **định mức theo ĐVT gốc / 1kg sơ chế**.")

        df_ct = fetch_df(conn, """
            SELECT code,name,type,output_pcode,output_uom,recovery,cups_per_kg,note
            FROM formulas ORDER BY type,name
        """)
        st.dataframe(df_ct, use_container_width=True, height=280)

        # ====== 1) VÙNG REACTIVE: chọn loại, nguồn, SP đầu ra (ngoài form) ======
        ct_type = _get("ct_type", "COT")
        ct_type = st.selectbox(
            "Loại công thức",
            ["COT", "MUT"],
            index=(0 if ct_type=="COT" else 1),
            key="ct_type",
            on_change=_set, args=("ct_type",)
        )

        mut_src = _get("mut_src", "TRAI_CAY")
        if ct_type == "MUT":
            mut_src = st.radio(
                "Nguồn NVL MỨT",
                ["TRAI_CAY","COT"],
                index=(0 if mut_src=="TRAI_CAY" else 1),
                horizontal=True,
                key="mut_src",
                on_change=_set, args=("mut_src",)
            )

        # SP đầu ra theo loại
        out_cat = "COT" if ct_type=="COT" else "MUT"
        df_out = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code=:c ORDER BY name", {"c": out_cat})
        out_labels = ["— Chọn —"] + [f"{r['code']} — {r['name']}" for _,r in df_out.iterrows()]
        out_pick = st.selectbox(
            "Sản phẩm đầu ra",
            out_labels,
            index=_get("ct_out_idx", 0),
            key="ct_out_lbl",
            on_change=_set, args=("ct_out_idx",)
        )
        output_pcode = "" if out_pick=="— Chọn —" else out_pick.split(" — ",1)[0]

        # Nguồn được phép (reactive)
        if ct_type == "COT" or (ct_type=="MUT" and mut_src=="TRAI_CAY"):
            # trái cây
            df_src = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code='TRAI_CAY' ORDER BY name")
            src_choices = [f"{r['code']} — {r['name']}" for _,r in df_src.iterrows()]
            allow_fruits = st.multiselect("Chọn TRÁI CÂY được phép", src_choices, key="allow_fruits")
            allow_cots = []
        else:
            # cốt
            df_src = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code='COT' ORDER BY name")
            src_choices = [f"{r['code']} — {r['name']}" for _,r in df_src.iterrows()]
            allow_cots = st.multiselect("Chọn CỐT được phép", src_choices, key="allow_cots")
            allow_fruits = []

        # ====== 2) FORM LƯU (ít reload) ======
        st.markdown("##### ➕ Thêm / Sửa công thức")
        with st.form("fm_ct_upsert", clear_on_submit=True):
            c1, c2, c3 = st.columns([1.5,1,1])
            with c1:
                code = st.text_input("Mã CT")
                name = st.text_input("Tên CT")
            with c2:
                if ct_type=="MUT":
                    gpc = st.number_input("g/cốc (MỨT)", min_value=0.0, step=1.0, value=0.0)
                    cups_per_kg = (1000.0/gpc) if gpc>0 else 0.0
                else:
                    cups_per_kg = st.number_input("Cốc/kg TP (CỐT)", min_value=0.0, step=0.1, value=0.0)
            with c3:
                if ct_type=="COT":
                    recovery = st.number_input("Hệ số thu hồi (kg TP / 1kg sơ chế)", min_value=0.01, step=0.01, value=1.00)
                else:
                    recovery = 1.0
                    st.caption("MỨT: không dùng hệ số.")

            # NVL khác theo ĐVT gốc
            st.markdown("###### NVL khác (ĐVT gốc / 1kg sơ chế)")
            df_other = fetch_df(conn, """
                SELECT code,name,uom FROM products
                WHERE cat_code IN ('PHU_GIA','TP_KHAC') ORDER BY name
            """)
            other_labels = [f"{r['code']} — {r['name']} ({r['uom']})" for _,r in df_other.iterrows()]
            picked_other = st.multiselect("Chọn NVL khác", other_labels, key="other_pick")

            other_inputs = {}
            for item in picked_other:
                pcode = item.split(" — ",1)[0]
                uom = df_other.loc[df_other["code"]==pcode, "uom"].iloc[0]
                q = st.number_input(f"{item} — {uom}/1kg sơ chế",
                                    min_value=0.0, step=0.01, value=0.0, key=f"q_other_{pcode}")
                if q > 0:
                    other_inputs[pcode] = q

            ok = st.form_submit_button("💾 Lưu công thức", type="primary")

        if ok:
            # Validate
            if not code or not name or not output_pcode:
                st.error("Thiếu mã/tên/SP đầu ra.")
                st.stop()
            if ct_type=="COT" and len(allow_fruits)==0:
                st.error("CỐT phải chọn ít nhất 1 TRÁI CÂY được phép.")
                st.stop()
            if ct_type=="MUT" and (len(allow_fruits)==0 and len(allow_cots)==0):
                st.error("MỨT phải chọn danh sách TRÁI CÂY hoặc CỐT được phép.")
                st.stop()

            # Header (formulas)
            run_sql(conn, """
                INSERT INTO formulas(code,name,type,output_pcode,output_uom,recovery,cups_per_kg,note)
                VALUES (:c,:n,:t,:o,'kg',:r,:k,:x)
                ON CONFLICT (code) DO UPDATE SET
                  name=EXCLUDED.name, type=EXCLUDED.type,
                  output_pcode=EXCLUDED.output_pcode, output_uom=EXCLUDED.output_uom,
                  recovery=EXCLUDED.recovery, cups_per_kg=EXCLUDED.cups_per_kg, note=EXCLUDED.note
            """, {
                "c": code.strip(), "n": name.strip(), "t": ct_type, "o": output_pcode,
                "r": float(recovery), "k": float(cups_per_kg),
                "x": json.dumps({"src": ("COT" if ct_type=="MUT" and len(allow_cots)>0 else "TRAI_CAY")}, ensure_ascii=False)
            })

            # Details (formula_inputs)
            run_sql(conn, "DELETE FROM formula_inputs WHERE formula_code=:c", {"c": code.strip()})
            for item in allow_fruits:
                p = item.split(" — ",1)[0]
                run_sql(conn, """
                    INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind)
                    VALUES (:f,:p,0,'SRC_FRUIT')
                """, {"f": code.strip(), "p": p})
            for item in allow_cots:
                p = item.split(" — ",1)[0]
                run_sql(conn, """
                    INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind)
                    VALUES (:f,:p,0,'SRC_COT')
                """, {"f": code.strip(), "p": p})
            for p, q in other_inputs.items():
                run_sql(conn, """
                    INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind)
                    VALUES (:f,:p,:q,'OTHER')
                """, {"f": code.strip(), "p": p, "q": float(q)})

            write_audit(conn, "FORMULA_UPSERT", code)
            st.success("Đã lưu công thức."); st.rerun()

        # Xoá công thức
        del_ct = st.selectbox("🗑️ Xoá CT", ["—"] + df_ct["code"].tolist(), index=0, key="del_ct")
        if del_ct != "—" and st.button("Xoá CT"):
            run_sql(conn, "DELETE FROM formula_inputs WHERE formula_code=:c", {"c": del_ct})
            run_sql(conn, "DELETE FROM formulas WHERE code=:c", {"c": del_ct})
            write_audit(conn, "FORMULA_DELETE", del_ct); st.success("Đã xoá."); st.rerun()
