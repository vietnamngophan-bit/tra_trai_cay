# core.py  —  HẠ TẦNG + CHỌN CỬA HÀNG (Postgres only)

import os, re, hashlib
from datetime import datetime
from typing import Optional, Dict, Any

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine


# =============== CẤU HÌNH TRANG CƠ BẢN (đặt 1 lần ở app.py cũng được) ===============
st.set_page_config(
    page_title="Quản Trị Trà Trái Cây",
    page_icon="🍵",
    layout="wide",
    initial_sidebar_state="expanded"
)


# =============== KẾT NỐI POSTGRES ===============
_ENGINE: Optional[Engine] = None

def _normalize_pg_url(url: str) -> str:
    """Chuẩn hoá URL Postgres để chạy ổn trên Streamlit Cloud/Supabase."""
    url = url.strip()
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg2://", 1)
    if url.startswith("postgresql://") and "+psycopg2" not in url:
        url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
    # dùng SSL bắt buộc nếu chưa có
    if "sslmode=" not in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}sslmode=require"
    return url

def get_conn() -> Connection:
    """Trả về một connection đang mở (tự tái sử dụng engine)."""
    global _ENGINE
    # Ưu tiên st.secrets, sau đó tới biến môi trường
    pg_url = ""
    try:
        pg_url = st.secrets["DATABASE_URL"]
    except Exception:
        pg_url = os.getenv("DATABASE_URL", "")

    if not pg_url:
        st.error("❌ Thiếu DATABASE_URL (trong Secrets hoặc biến môi trường).")
        st.stop()

    if _ENGINE is None:
        _ENGINE = create_engine(_normalize_pg_url(pg_url), pool_pre_ping=True, future=True)

    try:
        return _ENGINE.connect()
    except Exception as e:
        st.error(f"❌ Không kết nối được Postgres: {e}")
        st.stop()


# =============== TIỆN ÍCH SQL ===============
def run_sql(conn: Connection, sql: str, params: Any = None):
    """Chạy lệnh SQL (hỗ trợ ?, :name). Tự commit, bỏ qua nếu không cần."""
    if isinstance(params, (list, tuple)):
        # chuyển ? -> :p1, :p2...
        idx = 1
        def repl(_):
            nonlocal idx
            s = f":p{idx}"; idx += 1
            return s
        sql = re.sub(r"\?", repl, sql)
        params = {f"p{i+1}": v for i, v in enumerate(params)}

    res = conn.execute(text(sql), params or {})
    try:
        conn.commit()
    except Exception:
        pass
    return res

def fetch_df(conn: Connection, sql: str, params: Any = None) -> pd.DataFrame:
    """Đọc nhanh vào DataFrame (hỗ trợ ?, :name)."""
    if isinstance(params, (list, tuple)):
        idx = 1
        def repl(_):
            nonlocal idx
            s = f":p{idx}"; idx += 1
            return s
        sql = re.sub(r"\?", repl, sql)
        params = {f"p{i+1}": v for i, v in enumerate(params)}
    return pd.read_sql_query(text(sql), conn, params=params or {})


# =============== AUTH & NHẬT KÝ ===============
def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def write_audit(conn: Connection, action: str, detail: str = ""):
    try:
        who = st.session_state.get("user", {}).get("email", "anonymous")
        run_sql(conn,
            "INSERT INTO syslog(ts,actor,action,detail) VALUES (NOW(),:a,:b,:c)",
            {"a": who, "b": action, "c": detail[:1000]}
        )
    except Exception:
        pass

def has_perm(user: Dict[str, Any], perm: str) -> bool:
    if not user: return False
    if user.get("role") == "SuperAdmin": return True
    perms = (user.get("perms") or "").split(",")
    return perm in perms

def login_form(conn: Connection):
    st.markdown("### 🔐 Đăng nhập hệ thống")
    email = st.text_input("Email", key="lg_email")
    pw    = st.text_input("Mật khẩu", type="password", key="lg_pw")
    if st.button("Đăng nhập", type="primary", use_container_width=True):
        df = fetch_df(conn,
            "SELECT email,display,password,role,store_code,perms FROM users WHERE email=:e",
            {"e": email.strip()}
        )
        if df.empty:
            st.error("Sai tài khoản hoặc mật khẩu."); return
        row = df.iloc[0]
        if row["password"] != sha256(pw):
            st.error("Sai tài khoản hoặc mật khẩu."); return
        user = {
            "email":   row["email"],
            "display": row["display"] or row["email"],
            "role":    row["role"] or "User",
            "perms":   row["perms"] or "",
            "store":   row["store_code"] or ""
        }
        st.session_state["user"]  = user
        if user["store"]:
            st.session_state["store"] = user["store"]
        write_audit(conn, "LOGIN", user["email"])
        st.rerun()

def require_login(conn: Connection) -> Dict[str, Any]:
    if "user" not in st.session_state:
        login_form(conn)
        st.stop()
    return st.session_state["user"]

def do_logout(conn: Connection):
    email = st.session_state.get("user", {}).get("email", "")
    write_audit(conn, "LOGOUT", email)
    st.session_state.clear()
    st.rerun()


# =============== HEADER + CHỌN CỬA HÀNG ===============
def header_top(conn: Connection, user: Dict[str, Any]):
    """Header phải gọi đầu trang. Có popover đổi mật khẩu & nút đăng xuất."""
    c1, c2 = st.columns([0.75, 0.25])
    with c1:
        st.markdown("## 🍵 Quản Trị Trà Trái Cây")
        st.caption("Kết nối: Postgres (Supabase)")
    with c2:
        with st.popover(f"👤 {user.get('display','(user)')}"):
            st.caption(user.get("email", ""))
            st.markdown("---")
            st.markdown("**Đổi mật khẩu**")
            with st.form("fm_pw", clear_on_submit=True):
                old = st.text_input("Mật khẩu cũ", type="password")
                new1 = st.text_input("Mật khẩu mới", type="password")
                new2 = st.text_input("Xác nhận", type="password")
                ok = st.form_submit_button("Cập nhật")
            if ok:
                df = fetch_df(conn, "SELECT password FROM users WHERE email=:e", {"e": user["email"]})
                if df.empty or df.iloc[0]["password"] != sha256(old):
                    st.error("Mật khẩu cũ không đúng.")
                elif not new1 or new1 != new2:
                    st.error("Xác nhận chưa khớp.")
                else:
                    run_sql(conn, "UPDATE users SET password=:p WHERE email=:e",
                            {"p": sha256(new1), "e": user["email"]})
                    write_audit(conn, "CHANGE_PW", user["email"])
                    st.success("Đã đổi mật khẩu. Vui lòng đăng nhập lại.")
                    do_logout(conn)
            st.markdown("---")
            if st.button("Đăng xuất", use_container_width=True):
                do_logout(conn)


def store_selector(conn: Connection, user: Dict[str, Any]) -> str:
    """
    Selectbox 'Cửa hàng' ở sidebar.
    - SuperAdmin: thấy tất cả cửa hàng + lựa chọn 'Tất cả'.
    - Admin/User: mặc định theo store_code của user; nếu không có -> phải chọn.
    Trả về mã cửa hàng hiện hành (chuỗi rỗng nếu 'Tất cả').
    """
    st.sidebar.markdown("### 🏬 Cửa hàng")

    df = fetch_df(conn, "SELECT code,name FROM stores ORDER BY name")
    opts = df.assign(lbl=lambda x: x["code"] + " — " + x["name"])["lbl"].tolist()

    allow_all = (user.get("role") == "SuperAdmin")
    labels = (["(Tất cả)"] if allow_all else []) + opts

    # giá trị mặc định
    default_store = st.session_state.get("store", user.get("store", ""))
    if default_store:
        try:
            default_idx = labels.index(next(l for l in labels if l.startswith(default_store)))
        except StopIteration:
            default_idx = 0
    else:
        default_idx = 0

    pick = st.sidebar.selectbox(
        "Đang thao tác tại",
        labels,
        index=min(default_idx, len(labels)-1) if labels else 0,
        key="__pick_store__"
    )

    if pick == "(Tất cả)":
        st.session_state["store"] = ""   # rỗng = tất cả
    else:
        st.session_state["store"] = pick.split(" — ", 1)[0]

    return st.session_state["store"]


# =============== TIỆN ÍCH KHÁC (dùng chung) ===============
def money(v: float) -> str:
    try:
        return f"{float(v):,.0f}".replace(",", ".")
    except Exception:
        return "0"

def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
