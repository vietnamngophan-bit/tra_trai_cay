# catalog.py — Module 2: Cửa hàng, Người dùng, Danh mục / Sản phẩm / Công thức
import streamlit as st
from core import fetch_df, run_sql, write_audit, sha256, has_perm

# ===================== CỬA HÀNG =====================
def page_stores(conn, user):
    st.header("🏬 Cửa hàng (CRUD)")
    if not has_perm(user, "STORE_EDIT"):
        st.warning("Bạn không có quyền thao tác mục này.")
        return

    df = fetch_df(conn, "SELECT code,name,addr,note FROM stores ORDER BY name")
    st.dataframe(df, use_container_width=True, height=320)

    st.markdown("#### Thêm / sửa cửa hàng")
    with st.form("fm_store", clear_on_submit=True):
        c1, c2 = st.columns(2)
        with c1:
            code = st.text_input("Mã cửa hàng")
            name = st.text_input("Tên cửa hàng")
        with c2:
            addr = st.text_input("Địa chỉ")
            note = st.text_input("Ghi chú")
        ok = st.form_submit_button("Lưu", type="primary")
    if ok:
        if not code or not name:
            st.error("Thiếu mã / tên.")
        else:
            run_sql(conn, """
                INSERT INTO stores(code,name,addr,note)
                VALUES (:c,:n,:a,:t)
                ON CONFLICT (code) DO UPDATE
                  SET name=EXCLUDED.name, addr=EXCLUDED.addr, note=EXCLUDED.note
            """, {"c":code.strip(),"n":name.strip(),"a":addr.strip(),"t":note.strip()})
            write_audit(conn,"STORE_UPSERT",code)
            st.success("Đã lưu."); st.rerun()

    st.markdown("#### Xóa cửa hàng")
    if not df.empty:
        pick = st.selectbox("Chọn", ["—"]+[f"{r['code']} — {r['name']}" for _,r in df.iterrows()], index=0)
        if pick!="—" and st.button("🗑️ Xóa", use_container_width=False):
            code_del = pick.split(" — ",1)[0]
            run_sql(conn, "DELETE FROM stores WHERE code=:c", {"c":code_del})
            write_audit(conn,"STORE_DELETE",code_del)
            st.success("Đã xóa."); st.rerun()

# ===================== NGƯỜI DÙNG =====================
_PERM_CHOICES = [
    "CAT_EDIT","PROD_EDIT","CT_EDIT",
    "INV_VIEW","INV_EDIT",
    "MFG_RUN",
    "SALES_EDIT","REPORT_VIEW",
    "STORE_EDIT","USER_EDIT","AUDIT_VIEW"
]

def page_users(conn, user):
    st.header("👥 Người dùng (CRUD & phân quyền)")
    if not has_perm(user, "USER_EDIT"):
        st.warning("Bạn không có quyền thao tác mục này.")
        return

    df = fetch_df(conn, "SELECT email,display,role,store_code,perms FROM users ORDER BY email")
    st.dataframe(df, use_container_width=True, height=320)

    stores = fetch_df(conn, "SELECT code,name FROM stores ORDER BY name")
    store_opts = [r["code"] for _,r in stores.iterrows()] or ["DEFAULT"]

    st.markdown("#### Thêm / sửa người dùng")
    with st.form("fm_user", clear_on_submit=True):
        c1,c2,c3 = st.columns(3)
        with c1:
            email   = st.text_input("Email")
            display = st.text_input("Tên hiển thị")
        with c2:
            role    = st.selectbox("Vai trò", ["User","Manager","SuperAdmin"], index=0)
            store   = st.selectbox("Cửa hàng mặc định", store_opts,
                                   index=(store_opts.index(user.get("store","")) if user.get("store") in store_opts else 0))
        with c3:
            pw      = st.text_input("Mật khẩu (đặt mới / đặt lại)", type="password")
            perms   = st.multiselect("Quyền chi tiết", _PERM_CHOICES, default=[])

        ok = st.form_submit_button("Lưu", type="primary")
    if ok:
        if not email:
            st.error("Thiếu email.")
        else:
            if pw:
                run_sql(conn, """
                    INSERT INTO users(email,display,password,role,store_code,perms)
                    VALUES (:e,:d,:p,:r,:s,:m)
                    ON CONFLICT (email) DO UPDATE SET
                      display=EXCLUDED.display, password=EXCLUDED.password,
                      role=EXCLUDED.role, store_code=EXCLUDED.store_code, perms=EXCLUDED.perms
                """, {"e":email.strip(),"d":(display or email).strip(),"p":sha256(pw),
                      "r":role,"s":store,"m":",".join(perms)})
            else:
                run_sql(conn, """
                    INSERT INTO users(email,display,password,role,store_code,perms)
                    VALUES (:e,:d,'',:r,:s,:m)
                    ON CONFLICT (email) DO UPDATE SET
                      display=EXCLUDED.display, role=EXCLUDED.role,
                      store_code=EXCLUDED.store_code, perms=EXCLUDED.perms
                """, {"e":email.strip(),"d":(display or email).strip(),
                      "r":role,"s":store,"m":",".join(perms)})
            write_audit(conn,"USER_UPSERT",email)
            st.success("Đã lưu."); st.rerun()

    st.markdown("#### Xóa người dùng")
    if not df.empty:
        pick = st.selectbox("Chọn email", ["—"]+[r["email"] for _,r in df.iterrows()], index=0, key="del_user")
        if pick!="—" and st.button("🗑️ Xóa", key="btn_del_user"):
            run_sql(conn, "DELETE FROM users WHERE email=:e", {"e": pick})
            write_audit(conn,"USER_DELETE",pick)
            st.success("Đã xóa."); st.rerun()

# ===================== DANH MỤC / SẢN PHẨM / CÔNG THỨC =====================
def _ensure_type_state(key="ct_type"):
    """Giữ state loại công thức để rerun khi đổi lựa chọn."""
    if key not in st.session_state:
        st.session_state[key] = "COT"

def page_catalog(conn, user):
    st.header("🧾 Danh mục")
    tabs = st.tabs(["Danh mục SP","Sản phẩm","Công thức"])

    # ---------- Danh mục SP ----------
    with tabs[0]:
        if not has_perm(user, "CAT_EDIT"):
            st.warning("Bạn không có quyền.")
        else:
            df = fetch_df(conn, "SELECT code,name FROM categories ORDER BY code")
            st.dataframe(df, use_container_width=True, height=260)

            with st.form("fm_cat", clear_on_submit=True):
                c1,c2 = st.columns(2)
                with c1: code = st.text_input("Mã")
                with c2: name = st.text_input("Tên")
                ok = st.form_submit_button("Lưu", type="primary")
            if ok and code and name:
                run_sql(conn, """
                    INSERT INTO categories(code,name) VALUES (:c,:n)
                    ON CONFLICT (code) DO UPDATE SET name=EXCLUDED.name
                """, {"c":code.strip(),"n":name.strip()})
                write_audit(conn,"CAT_UPSERT",code); st.rerun()

            pick = st.selectbox("Xoá danh mục", ["—"]+[r["code"] for _,r in df.iterrows()], index=0)
            if pick!="—" and st.button("🗑️ Xóa danh mục"):
                run_sql(conn,"DELETE FROM categories WHERE code=:c",{"c":pick})
                write_audit(conn,"CAT_DELETE",pick); st.rerun()

    # ---------- Sản phẩm ----------
    with tabs[1]:
        if not has_perm(user, "PROD_EDIT"):
            st.warning("Bạn không có quyền.")
        else:
            df = fetch_df(conn, "SELECT code,name,cat_code,uom,cups_per_kg,price_ref FROM products ORDER BY name")
            st.dataframe(df, use_container_width=True, height=300)

            with st.form("fm_prod", clear_on_submit=True):
                c1,c2,c3 = st.columns(3)
                with c1:
                    code = st.text_input("Mã SP")
                    name = st.text_input("Tên SP")
                with c2:
                    cat  = st.selectbox("Nhóm", ["TRAI_CAY","COT","MUT","PHU_GIA","TP_KHAC"])
                    uom  = st.text_input("ĐVT", value="kg")
                with c3:
                    cups = st.number_input("Cốc/kg TP (áp dụng cho CỐT)", value=0.0, step=0.1, min_value=0.0)
                    pref = st.number_input("Giá tham chiếu", value=0.0, step=1000.0, min_value=0.0)
                ok = st.form_submit_button("Lưu", type="primary")
            if ok and code and name:
                run_sql(conn, """
                    INSERT INTO products(code,name,cat_code,uom,cups_per_kg,price_ref)
                    VALUES (:c,:n,:g,:u,:k,:p)
                    ON CONFLICT (code) DO UPDATE SET
                      name=EXCLUDED.name, cat_code=EXCLUDED.cat_code, uom=EXCLUDED.uom,
                      cups_per_kg=EXCLUDED.cups_per_kg, price_ref=EXCLUDED.price_ref
                """, {"c":code.strip(),"n":name.strip(),"g":cat,"u":uom.strip(),
                      "k":float(cups),"p":float(pref)})
                write_audit(conn,"PROD_UPSERT",code); st.rerun()

            pick = st.selectbox("Xoá SP", ["—"]+[r["code"] for _,r in df.iterrows()], index=0, key="del_sp")
            if pick!="—" and st.button("🗑️ Xóa sản phẩm"):
                run_sql(conn,"DELETE FROM products WHERE code=:c",{"c":pick})
                write_audit(conn,"PROD_DELETE",pick); st.rerun()

    # ---------- Công thức ----------
    with tabs[2]:
        if not has_perm(user, "CT_EDIT"):
            st.warning("Bạn không có quyền.")
            return

        st.info(
            "• **CỐT**: có **hệ số thu hồi**; `cups_per_kg` = **cốc/1kg TP**.\n"
            "• **MỨT**: **không** có hệ số thu hồi; `cups_per_kg` = **Gram/cốc (MỨT)**.\n"
            "• NVL chính theo nguồn: **CỐT → Trái cây**; **MỨT → Trái cây/Cốt**.\n"
            "• Phụ gia thuộc nhóm **PHU_GIA**; định lượng đều là **kg / 1kg TP**."
        )

        df_hdr = fetch_df(conn, """
            SELECT code,name,type,output_pcode,output_uom,recovery,cups_per_kg,note
            FROM formulas ORDER BY type,name
        """)
        st.dataframe(df_hdr, use_container_width=True, height=280)

        # ===== Thêm mới =====
        with st.expander("➕ Thêm công thức", expanded=False):
            _ensure_type_state("ct_type_add")
            # chọn loại và ép rerun khi đổi
            typ = st.selectbox("Loại", ["COT","MUT"], key="ct_type_add")
            if "last_ct_type_add" not in st.session_state or st.session_state["last_ct_type_add"] != typ:
                st.session_state["last_ct_type_add"] = typ
                st.experimental_rerun()

            with st.form("fm_ct_add", clear_on_submit=True):
                colA, colB = st.columns(2)
                with colA:
                    code = st.text_input("Mã CT")
                    name = st.text_input("Tên CT")
                with colB:
                    if typ=="COT":
                        cups = st.number_input("Cốc/kg TP (CỐT)", value=0.0, step=0.1, min_value=0.0, key="cups_cot_add")
                        recovery = st.number_input("Hệ số thu hồi (CỐT)", value=1.0, step=0.01, min_value=0.01, key="rec_cot_add")
                    else:
                        cups = st.number_input("Gram/cốc (MỨT)", value=0.0, step=1.0, min_value=0.0, key="cups_mut_add")
                        recovery = 1.0  # ẩn/khóa

                out_cat = "COT" if typ=="COT" else "MUT"
                df_out = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code=:c ORDER BY name", {"c":out_cat})
                out_pick = st.selectbox("Sản phẩm đầu ra",
                                        [f"{r['code']} — {r['name']}" for _,r in df_out.iterrows()] or ["—"],
                                        key="ct_out_add")
                output_pcode = "" if out_pick=="—" else out_pick.split(" — ",1)[0]

                # nguồn NVL chính
                if typ=="COT":
                    src_kind = "TRAI_CAY"
                    st.caption("Nguồn NVL chính: Trái cây")
                else:
                    src_kind = st.radio("Nguồn NVL (MỨT)", ["TRAI_CAY","COT"], horizontal=True, index=0, key="mut_src_add")

                df_src = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code=:c ORDER BY name", {"c": src_kind})
                picked_raw = st.multiselect("Chọn NVL chính",
                                            [f"{r['code']} — {r['name']}" for _, r in df_src.iterrows()],
                                            key="raw_multi_add")
                raw_inputs = {}
                for item in picked_raw:
                    c0 = item.split(" — ",1)[0]
                    q0 = st.number_input(f"{item} — kg / 1kg TP", 0.0, step=0.01, min_value=0.0, key=f"raw_add_{c0}")
                    if q0>0: raw_inputs[c0] = q0

                df_add = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code='PHU_GIA' ORDER BY name")
                picked_add = st.multiselect("Chọn phụ gia",
                                            [f"{r['code']} — {r['name']}" for _, r in df_add.iterrows()],
                                            key="add_multi_add")
                add_inputs = {}
                for item in picked_add:
                    c0 = item.split(" — ",1)[0]
                    q0 = st.number_input(f"{item} — kg / 1kg TP", 0.0, step=0.01, min_value=0.0, key=f"add_add_{c0}")
                    if q0>0: add_inputs[c0] = q0

                if st.form_submit_button("💾 Lưu CT", type="primary"):
                    if not code or not name or not output_pcode or (typ=="COT" and not raw_inputs):
                        st.error("Thiếu thông tin bắt buộc.")
                    else:
                        note = "" if typ=="COT" else f"SRC={src_kind}"
                        run_sql(conn, """
                            INSERT INTO formulas(code,name,type,output_pcode,output_uom,recovery,cups_per_kg,note)
                            VALUES (:c,:n,:t,:o,'kg',:r,:k,:x)
                            ON CONFLICT (code) DO UPDATE
                              SET name=EXCLUDED.name, type=EXCLUDED.type,
                                  output_pcode=EXCLUDED.output_pcode, output_uom=EXCLUDED.output_uom,
                                  recovery=EXCLUDED.recovery, cups_per_kg=EXCLUDED.cups_per_kg, note=EXCLUDED.note
                        """, {"c":code,"n":name,"t":typ,"o":output_pcode,
                              "r": float(recovery), "k": float(cups), "x": note})
                        run_sql(conn, "DELETE FROM formula_inputs WHERE formula_code=:c", {"c":code})
                        for k,v in raw_inputs.items():
                            run_sql(conn, """
                                INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind)
                                VALUES (:f,:p,:q,:k)
                            """, {"f":code,"p":k,"q":float(v),"k":src_kind})
                        for k,v in add_inputs.items():
                            run_sql(conn, """
                                INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind)
                                VALUES (:f,:p,:q,'PHU_GIA')
                            """, {"f":code,"p":k,"q":float(v)})
                        write_audit(conn,"FORMULA_UPSERT",code)
                        st.success("Đã lưu công thức."); st.rerun()

        # ===== Sửa / Xóa =====
        with st.expander("✏️ Sửa / Xóa công thức", expanded=False):
            if df_hdr.empty:
                st.info("Chưa có công thức.")
            else:
                _ensure_type_state("ct_type_edit_holder")
                pick = st.selectbox("Chọn CT", [f"{r['code']} — {r['name']}" for _,r in df_hdr.iterrows()], key="ct_pick_edit")
                ct_code = pick.split(" — ",1)[0]
                hdr = fetch_df(conn, "SELECT * FROM formulas WHERE code=:c", {"c": ct_code}).iloc[0].to_dict()
                det = fetch_df(conn, "SELECT * FROM formula_inputs WHERE formula_code=:c ORDER BY kind", {"c": ct_code})

                # đồng bộ loại hiện tại vào state để auto-rerun khi người dùng đổi
                default_type = "COT" if hdr["type"]=="COT" else "MUT"
                if "ct_type_edit" not in st.session_state:
                    st.session_state["ct_type_edit"] = default_type

                typ = st.selectbox("Loại", ["COT","MUT"], index=(0 if default_type=="COT" else 1), key="ct_type_edit")
                if "last_ct_type_edit" not in st.session_state or st.session_state["last_ct_type_edit"] != typ:
                    st.session_state["last_ct_type_edit"] = typ
                    st.experimental_rerun()

                with st.form("fm_ct_edit", clear_on_submit=True):
                    colA, colB = st.columns(2)
                    with colA:
                        name = st.text_input("Tên CT", value=hdr["name"] or "", key="ct_name_edit")
                    with colB:
                        if typ=="COT":
                            cups = st.number_input("Cốc/kg TP (CỐT)",
                                                   value=float(hdr.get("cups_per_kg") or 0.0),
                                                   step=0.1, min_value=0.0, key="cups_cot_edit")
                            recovery = st.number_input("Hệ số thu hồi (CỐT)",
                                                       value=float(hdr.get("recovery") or 1.0),
                                                       step=0.01, min_value=0.01, key="rec_cot_edit")
                        else:
                            cups = st.number_input("Gram/cốc (MỨT)",
                                                   value=float(hdr.get("cups_per_kg") or 0.0),
                                                   step=1.0, min_value=0.0, key="cups_mut_edit")
                            recovery = 1.0  # ẩn

                    out_cat = "COT" if typ=="COT" else "MUT"
                    df_out = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code=:c ORDER BY name", {"c": out_cat})
                    cur_out = hdr["output_pcode"]
                    options = ([f"{cur_out} — (hiện tại)"] +
                               [f"{r['code']} — {r['name']}" for _,r in df_out.iterrows() if r["code"]!=cur_out]) or ["—"]
                    out_pick = st.selectbox("Sản phẩm đầu ra", options, index=0, key="ct_out_edit")
                    output_pcode = cur_out if " (hiện tại)" in out_pick else out_pick.split(" — ",1)[0]

                    # nguồn NVL
                    if typ=="MUT":
                        src_kind = "TRAI_CAY"
                        if (hdr.get("note") or "").startswith("SRC="):
                            src_kind = (hdr["note"].split("=",1)[1] or "TRAI_CAY")
                        src_kind = st.radio("Nguồn NVL (MỨT)",
                                            ["TRAI_CAY","COT"],
                                            index=(0 if src_kind=="TRAI_CAY" else 1),
                                            horizontal=True, key="mut_src_edit")
                    else:
                        src_kind = "TRAI_CAY"
                        st.caption("Nguồn NVL chính: Trái cây")

                    raws_old = {r["pcode"]: float(r["qty_per_kg"]) for _,r in det.iterrows() if r["kind"] in ["TRAI_CAY","COT"]}
                    adds_old = {r["pcode"]: float(r["qty_per_kg"]) for _,r in det.iterrows() if r["kind"]=="PHU_GIA"}

                    df_src = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code=:c ORDER BY name",
                                      {"c": ("TRAI_CAY" if (typ=="COT" or src_kind=="TRAI_CAY") else "COT")})
                    choices_raw = [f"{r['code']} — {r['name']}" for _,r in df_src.iterrows()]
                    defaults_raw = [f"{c} — ..." for c in raws_old.keys() if c in [r["code"] for _,r in df_src.iterrows()]]
                    picked_raw = st.multiselect("Chọn NVL chính", choices_raw, default=defaults_raw, key="src_multi_edit")
                    raw_inputs = {}
                    for item in picked_raw:
                        c0 = item.split(" — ",1)[0]
                        q0 = st.number_input(f"{item} — kg / 1kg TP",
                                             value=float(raws_old.get(c0,0.0)),
                                             step=0.01, min_value=0.0, key=f"raw_edit_{c0}")
                        if q0>0: raw_inputs[c0] = q0

                    df_add = fetch_df(conn, "SELECT code,name FROM products WHERE cat_code='PHU_GIA' ORDER BY name")
                    choices_add = [f"{r['code']} — {r['name']}" for _,r in df_add.iterrows()]
                    defaults_add = [f"{c} — ..." for c in adds_old.keys()]
                    picked_add = st.multiselect("Chọn phụ gia", choices_add, default=defaults_add, key="add_multi_edit")
                    add_inputs = {}
                    for item in picked_add:
                        c0 = item.split(" — ",1)[0]
                        q0 = st.number_input(f"{item} — kg / 1kg TP",
                                             value=float(adds_old.get(c0,0.0)),
                                             step=0.01, min_value=0.0, key=f"add_edit_{c0}")
                        if q0>0: add_inputs[c0] = q0

                    colX, colY = st.columns(2)
                    with colX:
                        if st.form_submit_button("💾 Cập nhật", type="primary"):
                            note = "" if typ=="COT" else f"SRC={'TRAI_CAY' if src_kind=='TRAI_CAY' else 'COT'}"
                            run_sql(conn, """
                                UPDATE formulas
                                SET name=:n, type=:t, output_pcode=:o, output_uom='kg',
                                    recovery=:r, cups_per_kg=:k, note=:x
                                WHERE code=:c
                            """, {"n":name.strip(),"t":typ,"o":output_pcode,
                                  "r": float(recovery), "k": float(cups), "x": note, "c": ct_code})
                            run_sql(conn, "DELETE FROM formula_inputs WHERE formula_code=:c", {"c": ct_code})
                            for k,v in raw_inputs.items():
                                run_sql(conn, """
                                    INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind)
                                    VALUES (:f,:p,:q,:k)
                                """, {"f": ct_code, "p": k, "q": float(v),
                                      "k": ("TRAI_CAY" if (typ=="COT" or src_kind=="TRAI_CAY") else "COT")})
                            for k,v in add_inputs.items():
                                run_sql(conn, """
                                    INSERT INTO formula_inputs(formula_code,pcode,qty_per_kg,kind)
                                    VALUES (:f,:p,:q,'PHU_GIA')
                                """, {"f": ct_code, "p": k, "q": float(v)})
                            write_audit(conn,"FORMULA_UPDATE",ct_code)
                            st.success("Đã cập nhật."); st.rerun()
                    with colY:
                        if st.form_submit_button("🗑️ Xóa công thức"):
                            run_sql(conn, "DELETE FROM formula_inputs WHERE formula_code=:c", {"c": ct_code})
                            run_sql(conn, "DELETE FROM formulas WHERE code=:c", {"c": ct_code})
                            write_audit(conn,"FORMULA_DELETE",ct_code)
                            st.success("Đã xóa."); st.rerun()
