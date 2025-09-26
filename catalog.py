# catalog.py
from __future__ import annotations
import streamlit as st
import pandas as pd

# App đã có các helper này ở file chính:
#   fetch_df(conn, sql, params=None)
#   run_sql(conn, sql, params=None)
#   write_audit(conn, action, detail="")
# Hàm dưới nhận (conn, user) từ router.

def page_catalog(conn, user):
    st.markdown("### 🧾 Danh mục")
    tabs = st.tabs(["Danh mục SP", "Sản phẩm", "Công thức"])

    # ---------------- TAB 1: DANH MỤC ----------------
    with tabs[0]:
        df_cat = fetch_df(conn, "SELECT code,name FROM categories ORDER BY code")
        st.dataframe(df_cat, use_container_width=True, height=280)

        with st.form("fm_cat_add", clear_on_submit=True):
            c1, c2 = st.columns([1, 2])
            with c1: code = st.text_input("Mã")
            with c2: name = st.text_input("Tên")
            if st.form_submit_button("Lưu", type="primary"):
                if code and name:
                    run_sql(conn, """
                        INSERT INTO categories(code,name) VALUES (:c,:n)
                        ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name
                    """, {"c": code.strip(), "n": name.strip()})
                    write_audit(conn, "CAT_UPSERT", code); st.success("OK"); st.rerun()
                else:
                    st.error("Thiếu mã/tên.")

        del_cat = st.selectbox("🗑️ Xoá mã", ["—"] + df_cat["code"].tolist(), index=0)
        if del_cat != "—" and st.button("Xoá danh mục"):
            run_sql(conn, "DELETE FROM categories WHERE code=:c", {"c": del_cat})
            write_audit(conn, "CAT_DELETE", del_cat); st.success("Đã xoá"); st.rerun()

    # ---------------- TAB 2: SẢN PHẨM ----------------
    with tabs[1]:
        dfp = fetch_df(conn, """
            SELECT code,name,cat_code,uom,cups_per_kg,price_ref
            FROM products ORDER BY name
        """)
        st.dataframe(dfp, use_container_width=True, height=300)

        with st.form("fm_prod_add", clear_on_submit=True):
            c1, c2 = st.columns([1, 2])
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
                    st.caption(f"Cốc/kg TP tính ra: {cups_per_kg:,.2f}")
                else:
                    cups_per_kg = st.number_input("Cốc/kg TP", min_value=0.0, step=0.1, value=0.0)
            with c4:
                price_ref = st.number_input("Giá tham chiếu", min_value=0.0, step=1000.0, value=0.0)

            if st.form_submit_button("Lưu SP", type="primary"):
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
                    write_audit(conn, "PROD_UPSERT", pcode); st.success("OK"); st.rerun()
                else:
                    st.error("Thiếu dữ liệu bắt buộc.")

        delp = st.selectbox("🗑️ Xoá SP", ["—"] + dfp["code"].tolist(), index=0, key="del_sp")
        if delp != "—" and st.button("Xoá sản phẩm"):
            run_sql(conn, "DELETE FROM products WHERE code=:c", {"c": delp})
            write_audit(conn, "PROD_DELETE", delp); st.success("Đã xoá"); st.rerun()

    # ---------------- TAB 3: CÔNG THỨC ----------------
    with tabs[2]:
        st.markdown("#### 🧪 Công thức (định mức **/ 1kg SƠ CHẾ**)")
        df_ct = fetch_df(conn, """
            SELECT code,name,type,output_pcode,output_uom,recovery,cups_per_kg,note
            FROM formulas ORDER BY type,name
        """)
        st.dataframe(df_ct, use_container_width=True, height=260)

        # --- helper: reload khi đổi loại ---
        def _ct_on_change_type():
            st.session_state["ct_type_current"] = st.session_state.get("ct_type_pick", "COT")
            st.rerun()

        # ===== Thêm / Sửa =====
        with st.form("fm_formula_addedit", clear_on_submit=True):
            st.markdown("##### ➕ Thêm / Sửa")

            # Chọn chế độ: thêm mới hay sửa
            mode = st.radio("Chế độ", ["Thêm mới", "Sửa công thức"], horizontal=True)

            # Nếu sửa → chọn CT để nạp sẵn
            hdr = None
            det = pd.DataFrame()
            if mode == "Sửa công thức" and not df_ct.empty:
                pick_ct = st.selectbox("Chọn CT", [f"{r['code']} — {r['name']}" for _, r in df_ct.iterrows()])
                pick_code = pick_ct.split(" — ", 1)[0]
                hdr = fetch_df(conn, "SELECT * FROM formulas WHERE code=:c", {"c": pick_code})
                det = fetch_df(conn, "SELECT * FROM formula_inputs WHERE formula_code=:c ORDER BY kind,pcode", {"c": pick_code})
                hdr = (None if hdr.empty else hdr.iloc[0].to_dict())

            # Loại CT (COT/MUT) + reload
            default_type = (hdr["type"] if hdr else st.session_state.get("ct_type_current", "COT"))
            typ = st.selectbox("Loại", ["COT","MUT"],
                               index=(0 if default_type=="COT" else 1),
                               key="ct_type_pick", on_change=_ct_on_change_type)

            # output theo loại
            out_cat = "COT" if typ == "COT" else "MUT"
            df_out = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code=:c ORDER BY name", {"c": out_cat})
            out_opts = ["— Chọn —"] + [f"{r['code']} — {r['name']}" for _, r in df_out.iterrows()]
            cur_out = (hdr["output_pcode"] if hdr else "")
            out_index = 0
            if cur_out:
                try:
                    out_index = 1 + [o.split(" — ",1)[0] for o in out_opts[1:]].index(cur_out)
                except ValueError:
                    out_index = 0
            out_pick = st.selectbox("SP đầu ra", out_opts, index=out_index, key="ct_out_pick")
            output_pcode = "" if out_pick=="— Chọn —" else out_pick.split(" — ",1)[0]

            # Thông tin chung
            c1, c2, c3 = st.columns([1.5,1,1])
            with c1:
                code = st.text_input("Mã CT", value=(hdr["code"] if hdr else ""))
                name = st.text_input("Tên CT", value=(hdr["name"] if hdr else ""))
            with c2:
                if typ == "MUT":
                    # nhập g/cốc → tính cups/kg
                    g_per_cup_default = 0.0
                    if hdr and float(hdr.get("cups_per_kg") or 0) > 0:
                        g_per_cup_default = 1000.0 / float(hdr["cups_per_kg"])
                    g_per_cup = st.number_input("g/cốc (MỨT)", min_value=0.0, step=1.0, value=g_per_cup_default)
                    cups_per_kg = (1000.0 / g_per_cup) if g_per_cup > 0 else 0.0
                    st.caption(f"Cốc/kg TP: {cups_per_kg:,.2f}")
                else:
                    cups_per_kg = st.number_input("Cốc/kg TP (CỐT)", min_value=0.0, step=0.1,
                                                  value=float(hdr["cups_per_kg"]) if hdr else 0.0)
            with c3:
                if typ == "COT":
                    recovery = st.number_input("Hệ số thu hồi (kg TP / 1kg sơ chế)",
                                               min_value=0.01, step=0.01,
                                               value=float(hdr["recovery"]) if hdr else 1.00)
                else:
                    recovery = 1.0
                    st.caption("MỨT: **không dùng** hệ số thu hồi (thành phẩm nhập tay khi sản xuất).")

            # NVL chính (per 1kg sơ chế)
            st.markdown("##### Nguyên liệu chính (kg / 1kg sơ chế)")
            df_fruit = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code='TRAI_CAY' ORDER BY name")
            df_cot   = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code='COT' ORDER BY name")

            # default từ det cũ
            old_map_kind = {}
            if not det.empty:
                for r in det.itertuples():
                    if r.kind in ("TRAI_CAY", "COT"):
                        old_map_kind[r.pcode] = (r.kind, float(r.qty_per_kg))

            fruit_choices = [f"{r['code']} — {r['name']}" for _, r in df_fruit.iterrows()]
            cot_choices   = [f"{r['code']} — {r['name']}" for _, r in df_cot.iterrows()]

            default_fruit = [f"{c} — {df_fruit.loc[df_fruit['code']==c, 'name'].iloc[0]}"
                             for c,(k,_) in old_map_kind.items() if k=="TRAI_CAY" and (c in df_fruit['code'].values)]
            default_cot   = [f"{c} — {df_cot.loc[df_cot['code']==c, 'name'].iloc[0]}"
                             for c,(k,_) in old_map_kind.items() if k=="COT" and (c in df_cot['code'].values)]

            picked_fruit = st.multiselect("Trái cây", fruit_choices, default=default_fruit, key="ms_fruit")
            picked_cot   = st.multiselect("CỐT",       cot_choices,   default=default_cot,   key="ms_cot")

            raw_inputs = {}
            for item in picked_fruit:
                p = item.split(" — ",1)[0]
                default_q = old_map_kind.get(p, ("TRAI_CAY", 0.0))[1] if hdr else 0.0
                q = st.number_input(f"{item} — kg / 1kg sơ chế", min_value=0.0, step=0.01, value=float(default_q),
                                    key=f"q_fruit_{p}")
                if q > 0: raw_inputs[p] = ("TRAI_CAY", q)

            for item in picked_cot:
                p = item.split(" — ",1)[0]
                default_q = old_map_kind.get(p, ("COT", 0.0))[1] if hdr else 0.0
                q = st.number_input(f"{item} — kg / 1kg sơ chế", min_value=0.0, step=0.01, value=float(default_q),
                                    key=f"q_cot_{p}")
                if q > 0: raw_inputs[p] = ("COT", q)

            # Phụ gia
            st.markdown("##### Phụ gia (kg / 1kg sơ chế)")
            df_add = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code='PHU_GIA' ORDER BY name")
            add_choices = [f"{r['code']} — {r['name']}" for _, r in df_add.iterrows()]

            add_old = {}
            if not det.empty:
                for r in det.itertuples():
                    if r.kind == "PHU_GIA":
                        add_old[r.pcode] = float(r.qty_per_kg)

            default_add = [f"{c} — {df_add.loc[df_add['code']==c,'name'].iloc[0]}"
                           for c in add_old.keys() if c in df_add['code'].values]

            picked_add = st.multiselect("Phụ gia", add_choices, default=default_add, key="ms_add")
            add_inputs = {}
            for item in picked_add:
                p = item.split(" — ",1)[0]
                q = st.number_input(f"{item} — kg / 1kg sơ chế",
                                    min_value=0.0, step=0.01,
                                    value=float(add_old.get(p, 0.0)),
                                    key=f"q_add_{p}")
                if q > 0: add_inputs[p] = q

            submitted = st.form_submit_button("💾 Lưu công thức", type="primary")
            if submitted:
                if not code or not name or not output_pcode:
                    st.error("Thiếu mã/tên/SP đầu ra.")
                elif len(raw_inputs) == 0 and len(add_inputs) == 0:
                    st.error("Chưa khai nguyên liệu/phụ gia.")
                else:
                    note = ("SRC=COT" if any(k=="COT" for k,_ in raw_inputs.values()) else "SRC=TRAI_CAY")
                    run_sql(conn, """
                        INSERT INTO formulas(code,name,type,output_pcode,output_uom,recovery,cups_per_kg,note)
                        VALUES (:c,:n,:t,:o,'kg',:r,:k,:x)
                        ON CONFLICT (code) DO UPDATE SET
                          name=EXCLUDED.name, type=EXCLUDED.type, output_pcode=EXCLUDED.output_pcode,
                          output_uom=EXCLUDED.output_uom, recovery=EXCLUDED.recovery,
                          cups_per_kg=EXCLUDED.cups_per_kg, note=EXCLUDED.note
                    """, {
                        "c": code.strip(), "n": name.strip(), "t": typ, "o": output_pcode,
                        "r": float(recovery), "k": float(cups_per_kg), "x": note
                    })
                    run_sql(conn, "DELETE FROM formula_inputs WHERE formula_code=:c", {"c": code.strip()})
                    for p,(knd,q) in raw_inputs.items():
                        run_sql(conn, """
                            INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind)
                            VALUES (:f,:p,:q,:k)
                        """, {"f": code.strip(), "p": p, "q": float(q), "k": knd})
                    for p,q in add_inputs.items():
                        run_sql(conn, """
                            INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind)
                            VALUES (:f,:p,:q,'PHU_GIA')
                        """, {"f": code.strip(), "p": p, "q": float(q)})

                    write_audit(conn, "FORMULA_UPSERT", code); st.success("Đã lưu"); st.rerun()

        # ===== XÓA NHANH =====
        del_ct = st.selectbox("🗑️ Xoá công thức", ["—"] + df_ct["code"].tolist(), index=0, key="del_ct_fast")
        if del_ct != "—" and st.button("Xoá CT"):
            run_sql(conn, "DELETE FROM formula_inputs WHERE formula_code=:c", {"c": del_ct})
            run_sql(conn, "DELETE FROM formulas WHERE code=:c", {"c": del_ct})
            write_audit(conn, "FORMULA_DELETE", del_ct); st.success("Đã xoá"); st.rerun()
