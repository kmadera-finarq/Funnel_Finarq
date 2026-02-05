import time
import streamlit as st
import pandas as pd
from supabase import create_client, Client
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from postgrest.exceptions import APIError
import plotly.graph_objects as go
import plotly.express as px  # opcional, pero útil para templates


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
st.set_page_config(page_title=st.secrets.get("APP_NAME", "Funnel de ventas"), layout="wide")

# Validación mínima de secrets
required = ["SUPABASE_URL", "SUPABASE_ANON_KEY"]
missing = [k for k in required if not st.secrets.get(k)]
if missing:
    st.error(f"Faltan secretos en .streamlit/secrets.toml: {', '.join(missing)}")
    st.stop()

# -----------------------------------------------------------------------------
# Supabase client (singleton cacheado)
# -----------------------------------------------------------------------------
@st.cache_resource
def get_supabase() -> Client:
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_ANON_KEY"])

supabase: Client = get_supabase()

@st.cache_resource
def get_supabase_admin() -> Client:
    return create_client(
        st.secrets["SUPABASE_URL"],
        st.secrets["SUPABASE_SERVICE_ROLE_KEY"]
    )

supabase_admin: Client = get_supabase_admin()


# -----------------------------------------------------------------------------
# Auth state + cache buster + umbrales
# -----------------------------------------------------------------------------
if "session" not in st.session_state:
    st.session_state.session = None
if "user" not in st.session_state:
    st.session_state.user = None
# cache busters
if "capturas_cache_buster" not in st.session_state:
    st.session_state.capturas_cache_buster = 0
if "obs_cache_buster" not in st.session_state:
    st.session_state.obs_cache_buster = 0

# Umbrales semáforo (Clientes/Total)
if "sem_red_max" not in st.session_state:
    st.session_state.sem_red_max = 0.10  # 10%
if "sem_yellow_max" not in st.session_state:
    st.session_state.sem_yellow_max = 0.25  # 25%

JWT_SKEW_SECONDS = 60
NUKE_PASSWORD = st.secrets.get("NUKE_PASSWORD", "")

# -------- helpers de sesión/jwt --------
def _get_expires_at(sess) -> int | None:
    if not sess:
        return None
    exp = getattr(sess, "expires_at", None)
    if exp is not None:
        try:
            return int(exp)
        except Exception:
            pass
    if hasattr(sess, "__dict__") and "expires_at" in sess.__dict__:
        try:
            return int(sess.__dict__["expires_at"])
        except Exception:
            pass
    return None

def _refresh_session(force: bool = False) -> bool:
    try:
        sess = st.session_state.get("session")
        if not sess:
            return False
        try:
            data = supabase.auth.refresh_session()
        except TypeError:
            data = supabase.auth.refresh_session({"refresh_token": getattr(sess, "refresh_token", None)})
        if data and getattr(data, "session", None):
            st.session_state.session = data.session
            st.session_state.user = data.user or st.session_state.get("user")
            return True
        if getattr(data, "access_token", None):
            st.session_state.session = data
            return True
    except Exception:
        return False
    return False

def _ensure_valid_session() -> bool:
    sess = st.session_state.get("session")
    if not sess:
        return False
    exp = _get_expires_at(sess)
    now = int(time.time())
    if exp is None:
        try:
            current = supabase.auth.get_session()
            if current and getattr(current, "session", None):
                st.session_state.session = current.session
                st.session_state.user = current.user or st.session_state.get("user")
                exp = _get_expires_at(st.session_state.session)
        except Exception:
            pass
    if exp is None or exp <= now + JWT_SKEW_SECONDS:
        ok = _refresh_session(force=True)
        return ok
    return True

def _attach_postgrest_token_if_any():
    if not _ensure_valid_session():
        return
    sess = st.session_state.get("session")
    if sess and getattr(sess, "access_token", None):
        supabase.postgrest.auth(sess.access_token)

def _retry_on_jwt_expired(func, *args, **kwargs):
    try:
        _attach_postgrest_token_if_any()
        return func(*args, **kwargs)
    except APIError as e:
        msg = str(e)
        if any(x in msg for x in ("JWT expired", "PGRST303", "Invalid JWT")):
            if _refresh_session(force=True):
                _attach_postgrest_token_if_any()
                return func(*args, **kwargs)
        raise

def _format_api_error(e: APIError) -> str:
    try:
        payload = getattr(e, "args", [{}])[0]
        if isinstance(payload, dict):
            parts = []
            for k in ("code", "message", "hint", "details"):
                v = payload.get(k)
                if v:
                    parts.append(f"{k}: {v}")
            return " | ".join(parts) if parts else str(e)
    except Exception:
        pass
    return str(e)

def login(email: str, password: str):
    try:
        res = supabase.auth.sign_in_with_password({"email": email, "password": password})
        st.session_state.session = res.session
        st.session_state.user = res.user
        _attach_postgrest_token_if_any()
        st.success("Sesión iniciada")
        st.rerun()
    except Exception as e:
        st.error(f"Error de inicio de sesión: {e}")

def logout():
    try:
        supabase.auth.sign_out()
    except Exception:
        pass
    st.session_state.session = None
    st.session_state.user = None
    st.rerun()

# En cada rerun, asegurar token adjunto
_attach_postgrest_token_if_any()


# -----------------------------------------------------------------------------
# Login UI
# -----------------------------------------------------------------------------
if st.session_state.user is None:
    col1, col2 = st.columns([4, 1])
    with col1:
        st.title("Funnel de Ventas")
    with col2:
        st.image("assets/LOGO_FINARQ.png", width=300)

    with st.form("login"):
        email = st.text_input("Correo", placeholder="tucorreo@empresa.com")
        pwd = st.text_input("Contraseña", type="password")
        ok = st.form_submit_button("Entrar", width="stretch")

    if ok:
        login(email, pwd)
    st.stop()


user = st.session_state.user
st.sidebar.write(f"Usuario: **{user.email}**")
if st.sidebar.button("Cerrar sesión", width="stretch"):
    logout()

ALIAS = user.email.split("@")[0]

# -----------------------------------------------------------------------------
# Utils / Data access
# -----------------------------------------------------------------------------
def is_admin(uid: str) -> bool:
    _attach_postgrest_token_if_any()
    def _call():
        return supabase.table("admins").select("user_id").eq("user_id", uid).execute()
    try:
        res = _retry_on_jwt_expired(_call)
        return bool(res.data)
    except Exception:
        return False

def _query_capturas(
    *,
    uid: str,
    is_admin_flag: bool,
    scope: str,
    date_from: date | None = None,
    date_to_exclusive: date | None = None,
    tipo: str | None = None,
    asesor: str | None = None,
    estatus: str | None = None,
    limit: int = 5000,
):
    _attach_postgrest_token_if_any()
    def _call():
        q = supabase.table("capturas").select("*")
        if scope == "mine" and not is_admin_flag:
            q = q.eq("user_id", uid)
        if date_from is not None:
            q = q.gte("fecha", date_from.isoformat())
        if date_to_exclusive is not None:
            q = q.lt("fecha", date_to_exclusive.isoformat())
        if tipo:
            q = q.eq("tipo", tipo)
        if asesor:
            q = q.eq("asesor", asesor)
        if estatus:
            q = q.eq("estatus", estatus)
        q = q.order("fecha", desc=True).order("ts", desc=True).limit(limit)
        return q.execute()
    res = _retry_on_jwt_expired(_call)

    df = pd.DataFrame(res.data or [])
    if not df.empty:
        required_cols = [
            "id","fecha","referenciador","cliente","producto","tipo",
            "estatus","asesor","ts","user_id",
            "monto_estimado","monto_real",
            "nota","prob_cierre"
            ]

        for c in required_cols:
            if c not in df.columns:
                df[c] = pd.NA
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce").dt.date
        for c in ("referenciador","cliente","producto","tipo","estatus","asesor"):
            if df[c].dtype != object:
                df[c] = df[c].astype("string")
        # numéricos seguros
        for numc in ("monto_estimado","monto_real","prob_cierre"):
            if numc not in df.columns:
                df[numc] = pd.NA
            df[numc] = pd.to_numeric(df[numc], errors="coerce")
    else:
        df = pd.DataFrame(columns=[
            "id","fecha","referenciador","cliente","producto","tipo",
            "estatus","asesor","ts","user_id","monto_estimado","monto_real","nota", "prob_cierre"
        ])
    return df

@st.cache_data(ttl=20)
def load_capturas_filtered(
    cache_buster: int,
    *,
    uid: str,
    is_admin_flag: bool,
    scope: str,
    date_from: date | None = None,
    date_to_exclusive: date | None = None,
    tipo: str | None = None,
    asesor: str | None = None,
    estatus: str | None = None,
    limit: int = 5000,
):
    return _query_capturas(
        uid=uid,
        is_admin_flag=is_admin_flag,
        scope=scope,
        date_from=date_from,
        date_to_exclusive=date_to_exclusive,
        tipo=tipo,
        asesor=asesor,
        estatus=estatus,
        limit=limit,
    )

# --------- Observaciones: DAO helpers ---------
def _query_observaciones_for_user(pending_only: bool = True):
    _attach_postgrest_token_if_any()
    def _call():
        q = supabase.table("observaciones").select("*").eq("asesor_user_id", user.id)
        if pending_only:
            q = q.eq("done", False)
        q = q.order("created_at", desc=True)
        return q.execute()
    res = _retry_on_jwt_expired(_call)
    return pd.DataFrame(res.data or [])

def _query_observaciones_admin(date_from=None, date_to_exclusive=None, asesor_user_id=None):
    _attach_postgrest_token_if_any()
    def _call():
        q = supabase.table("observaciones").select("*")
        if date_from is not None:
            q = q.gte("created_at", f"{date_from.isoformat()} 00:00:00")
        if date_to_exclusive is not None:
            q = q.lt("created_at", f"{date_to_exclusive.isoformat()} 00:00:00")
        if asesor_user_id:
            q = q.eq("asesor_user_id", asesor_user_id)
        q = q.order("created_at", desc=True)
        return q.execute()
    res = _retry_on_jwt_expired(_call)
    return pd.DataFrame(res.data or [])

def _get_asesores_map(limit: int = 10000):
    """
    Devuelve dict {alias_asesor -> user_id} usando capturas recientes (ts desc).
    Garantiza alias únicos tomando el user_id más reciente visto.
    """
    _attach_postgrest_token_if_any()
    def _call():
        return supabase.table("capturas") \
            .select("asesor,user_id,ts") \
            .order("ts", desc=True) \
            .limit(limit) \
            .execute()
    res = _retry_on_jwt_expired(_call)
    ases_map = {}
    rows = res.data or []
    for r in rows:
        alias = r.get("asesor")
        uid = r.get("user_id")
        if alias and uid and alias not in ases_map:
            ases_map[alias] = uid
    return ases_map

def _get_metas_mes(periodo: date):
    _attach_postgrest_token_if_any()
    def _call():
        return supabase.table("metas_asesor").select("*").eq("periodo", periodo.isoformat()).execute()
    res = _retry_on_jwt_expired(_call)
    return pd.DataFrame(res.data or [])

def _get_meta_asesor_sum(uid: str, date_from: date | None, date_to_exclusive: date | None) -> float:
    _attach_postgrest_token_if_any()
    def _call():
        q = supabase.table("metas_asesor").select("meta_mxn,periodo").eq("asesor_user_id", uid)
        if date_from is not None:
            q = q.gte("periodo", date_from.isoformat())
        if date_to_exclusive is not None:
            q = q.lt("periodo", date_to_exclusive.isoformat())
        return q.execute()
    res = _retry_on_jwt_expired(_call)
    df = pd.DataFrame(res.data or [])
    if df.empty:
        return 0.0
    return float(pd.to_numeric(df["meta_mxn"], errors="coerce").fillna(0).sum())


# Orden lógico de estatus
# Opciones globales de estatus (UNIFICADAS)
ESTATUS_OPTIONS = [
    "Acercamiento",
    "Propuesta",
    "Documentación",
    "Cliente",
    "Cancelado"
]

# Orden lógico del funnel
# Cancelado = 0 para que no cuente como avance
ESTATUS_ORDER = {
    "Acercamiento": 1,
    "Propuesta": 2,
    "Documentación": 3,
    "Cliente": 4,
    "Cancelado": 0
}

def _quarter_bounds(d: date):
    q = (d.month - 1) // 3  # 0..3
    start_month = q * 3 + 1
    start = date(d.year, start_month, 1)
    end = start + relativedelta(months=3)
    return start, end



# ---------------------- MÉTRICA Y SEMÁFOROS ----------------------
def get_thresholds():
    red = st.session_state.get("sem_red_max", 0.10)
    yellow = st.session_state.get("sem_yellow_max", 0.25)
    red = max(0.0, min(red, 0.9))
    yellow = max(red, min(yellow, 0.95))
    return red, yellow

def conversion_closed_over_total(total_reg: int, clientes: int):
    if total_reg <= 0:
        return 0.0, "—"
    red_max, yellow_max = get_thresholds()
    frac = clientes / total_reg
    pct = frac * 100.0
    if frac <= red_max:
        light = "🔴"
    elif frac <= yellow_max:
        light = "🟡"
    else:
        light = "🟢"
    return pct, light

# ---- Vista pública para tablas simples ----
DISPLAY_COLS = [
    "asesor","cliente","producto","tipo","estatus","fecha","referenciador","prob_cierre",
    "monto_estimado","monto_real","nota"
]


def df_public_view(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    cols = [c for c in DISPLAY_COLS if c in df.columns]
    return df[cols].sort_values(["fecha", "cliente"], ascending=[False, True])

ESTATUS_COLORS = {
    "Cliente":        "#636EFA",  # azul Plotly
    "Documentación":  "#EF553B",  # rojo Plotly
    "Acercamiento":   "#00CC96",  # verde Plotly
    "Propuesta":      "#AB63FA",  # morado Plotly
    "Cancelado":      "#FFA15A",  # naranja Plotly
}


#colores en las filas según estatus
def style_rows_by_estatus(df: pd.DataFrame):
    if df is None or df.empty or "estatus" not in df.columns:
        return df



    def _row_style(row):
        est = row.get("estatus")
        bg = ESTATUS_COLORS.get(est)

        if bg:
            # mismo color que la gráfica + transparencia + texto oscuro
            return [f"background-color: {bg}20; color: #111111;"] * len(row)

        return [""] * len(row)

    return df.style.apply(_row_style, axis=1)


    return df.style.apply(_row_style, axis=1)

# -----------------------------------------------------------------------------
# UI (Header con logo)
# -----------------------------------------------------------------------------
col1, col2 = st.columns([4, 1])
with col1:
    st.title("Funnel de Ventas")
with col2:
    st.image("assets/LOGO_FINARQ.png", width=300)

# Decidir pestañas según rol
ADMIN_FLAG_GLOBAL = is_admin(st.session_state.user.id)

tabs = st.tabs(["🧑‍💼 Mi tablero", "📊 Visor", "⚙️ Config"])
TAB_INDIV, TAB_CONG, TAB_CFG = tabs


# -------------------- Mi tablero (solo asesores / no admin) -------------------
with TAB_INDIV:
        st.subheader("Captura de registro")

        # ✅ Lista de productos (catalogada, con respaldo si está vacío)
        @st.cache_data(ttl=60)
        def load_productos():
            try:
                _attach_postgrest_token_if_any()
                res = supabase.table("productos_config").select("producto,activo").eq("activo", True).order("producto").execute()
                prods = [r["producto"] for r in (res.data or []) if r.get("producto")]
                if not prods:
                    prods = ["Divisas","Inversiones","Factoraje","Arrendamiento","TPV","Crédito TPV","Créditos"]
                return prods
            except Exception:
                return ["Divisas","Inversiones","Factoraje","Arrendamiento","TPV","Crédito TPV","Créditos"]

        productos = load_productos()

        REFERENCIADORES = [
            "Andrea", "Amanda", "Ángel", "Angie", "Ariadna", "BNI", "César", "Cornelio", "Eduardo", "Fátima",
            "Gilberto", "Integra", "Jorge", "Karen", "Lupita", "Mafer", "Marco",
            "Paco", "Pepe", "Ricardo", "Vania", "Ximena",
        ]

        with st.form("form_lead_simple", clear_on_submit=True):
            fecha = st.date_input("Fecha *", value=date.today(), key="fecha_form")
            cliente = st.text_input("Cliente *").strip()
            referenciador = st.selectbox(
                "Referenciador *",
                REFERENCIADORES,
                index=REFERENCIADORES.index("Jorge") if "Jorge" in REFERENCIADORES else 0,
                key="referenciador_form"
            )
            producto = st.selectbox("Producto *", productos)
            tipo = st.selectbox("Tipo de cliente *", ["Nuevo","BAU"])
            estatus = st.selectbox("Estatus *", ESTATUS_OPTIONS)



            # ---- NUEVO: monto estimado
            monto_estimado = st.number_input(
                "Ingreso estimado (MXN) *",
                min_value=0.0, step=100.0, format="%.2f", key="monto_estimado_form"
            )

            nota = st.text_area("Notas / comentarios (opcional)", placeholder="Ej. Cliente pidió llamada el viernes...")

            prob_cierre = st.number_input(
                "Probabilidad de cierre (%)",
                min_value=0.0, max_value=100.0, step=1.0, value=50.0,
                key="prob_cierre_form"
            )

            ok = st.form_submit_button("Guardar", type="primary", width="stretch")

        if ok:
            if (not cliente or not producto or not tipo or not estatus
                or fecha is None or not referenciador or monto_estimado is None):
                st.warning("Completa los campos obligatorios *.")
            else:
                payload = {
                    "asesor": ALIAS,
                    "fecha": fecha.isoformat(),
                    "cliente": cliente,
                    "referenciador": referenciador,
                    "producto": producto,
                    "tipo": tipo,
                    "estatus": estatus,
                    "monto_estimado": float(monto_estimado),  
                    "nota": (nota.strip() or None),
                    "prob_cierre": float(prob_cierre),
                }
                # (Opcional) si quieres obligar 'monto_real' al crear en 'Cliente', añade inputs y validación aquí.
                try:
                    def _call():
                        return supabase.table("capturas").insert(payload).execute()
                    _retry_on_jwt_expired(_call)
                    st.success("¡Registro guardado!")
                    st.session_state.capturas_cache_buster += 1
                except APIError as e:
                    st.error(f"No se pudo guardar el registro: {_format_api_error(e)}")
                except Exception as e:
                    st.error(f"No se pudo guardar el registro: {e}")

        # 🔔 Observaciones del admin (notificaciones)
        st.markdown("### 🔔 Observaciones del administrador")
        df_obs = _query_observaciones_for_user(pending_only=True)
        if df_obs.empty:
            st.success("No tienes observaciones pendientes. ✅")
        else:
            with st.form("obs_form"):
                checks = {}
                for _, row in df_obs.iterrows():
                    obs_id = row["id"]
                    cliente_txt = row.get("cliente") or "—"
                    msg = row.get("mensaje") or ""
                    created_at = row.get("created_at")
                    created_str = ""
                    if created_at:
                        try:
                            created_str = str(pd.to_datetime(created_at).strftime("%Y-%m-%d %H:%M"))
                        except Exception:
                            created_str = str(created_at)
                    label = f"**{cliente_txt}** — {msg}  \n_(creada: {created_str})_"
                    checks[obs_id] = st.checkbox(label, key=f"obs_{obs_id}", value=False)
                submit_done = st.form_submit_button("Marcar seleccionadas como realizadas ✅", width="stretch")

            if submit_done:
                try:
                    total = 0
                    for obs_id, checked in checks.items():
                        if checked:
                            def _upd():
                                return supabase.table("observaciones").update({
                                    "done": True,
                                    "done_at": datetime.utcnow().isoformat() + "Z",
                                    "done_by_user_id": user.id
                                }).eq("id", obs_id).execute()
                            _retry_on_jwt_expired(_upd)
                            total += 1
                    if total > 0:
                        st.success(f"Se marcaron {total} observación(es) como realizadas.")
                        st.session_state.obs_cache_buster += 1
                        st.rerun()
                    else:
                        st.info("No seleccionaste ninguna observación.")
                except APIError as e:
                    st.error(f"No se pudieron actualizar observaciones: {_format_api_error(e)}")
                except Exception as e:
                    st.error(f"No se pudieron actualizar observaciones: {e}")

        st.markdown("---")
        st.markdown("### Mis registros")
        ADMIN_FLAG = False

        colf1, colf2 = st.columns([1,1])
        with colf1:
            mes_inicio = st.date_input("Mes a analizar", value=date.today().replace(day=1), key="mes_indiv").replace(day=1)
        with colf2:
            tipo_sel = st.radio("Tipo de cliente", ["Todos","Nuevo","BAU"], horizontal=True)
       
        periodo_sel = st.radio(
            "Periodo",
            ["Mes", "Trimestre", "Todo"],
            horizontal=True,
            key="periodo_asesor"
)

       # -------- Filtros del asesor --------
        tipo_param = None if tipo_sel == "Todos" else tipo_sel

        estatus_sel = st.selectbox(
        "Estatus",
        ["Todos"] + ESTATUS_OPTIONS,
        key="estatus_filtro_asesor"
        )

        estatus_param = None if estatus_sel == "Todos" else estatus_sel

        # ======== HISTORIAL (ASESOR) — con opción para ver TODO el histórico =========

        st.markdown("#### Historial")



        if periodo_sel == "Todo":
            date_from = None
            date_to_exclusive = None
        elif periodo_sel == "Trimestre":
            date_from, date_to_exclusive = _quarter_bounds(mes_inicio)
        else:  # Mes
            date_from = mes_inicio
            date_to_exclusive = mes_inicio + relativedelta(months=1)

        df_f = load_capturas_filtered(
            st.session_state.capturas_cache_buster,
            uid=st.session_state.user.id,
            is_admin_flag=False,
            scope="mine",
            date_from=date_from,
            date_to_exclusive=date_to_exclusive,
            tipo=tipo_param,
            estatus=estatus_param   
        )


        st.dataframe(style_rows_by_estatus(df_public_view(df_f)), use_container_width=True)


        

        # Métricas (Clientes/Total)
        if df_f.empty:
            total_reg = acerc = propuestas = docs = clientes = cancelados = 0
        else:
            total_reg  = len(df_f)
            acerc      = int((df_f["estatus"] == "Acercamiento").sum())
            propuestas = int((df_f["estatus"] == "Propuesta").sum())
            docs       = int((df_f["estatus"] == "Documentación").sum())
            clientes   = int((df_f["estatus"] == "Cliente").sum())
            cancelados = int((df_f["estatus"] == "Cancelado").sum())


        c0, c1, c2, c3, c4, c5 = st.columns(6)
        c0.metric("Total registrados", f"{total_reg}")
        c1.metric("Acercamientos", f"{acerc}")
        c2.metric("Propuestas", f"{propuestas}")
        c3.metric("Documentación", f"{docs}")
        c4.metric("Clientes", f"{clientes}")
        c5.metric("Cancelados", f"{cancelados}")

        # ===================== KPI: Ingreso total esperado (prob > 51%) =====================
        st.markdown("### Ingreso total esperado")

        umbral = 51.0

        df_tmp = df_f.copy()
        df_tmp["prob_cierre"] = pd.to_numeric(df_tmp["prob_cierre"], errors="coerce")
        df_tmp["monto_estimado"] = pd.to_numeric(df_tmp["monto_estimado"], errors="coerce")

        ingreso_esperado_total = (
            df_tmp.loc[df_tmp["prob_cierre"] > umbral, "monto_estimado"]
            .fillna(0)
            .sum()
            if not df_tmp.empty
            else 0.0
        )

        st.metric(
            f"Ingreso total esperado (prob > {int(umbral)}%)",
            f"${ingreso_esperado_total:,.2f}"
        )

        meta_total = _get_meta_asesor_sum(st.session_state.user.id, date_from, date_to_exclusive)


        st.metric("Meta del periodo (MXN)", f"${meta_total:,.2f}")
        

        st.metric("Brecha (Meta - Esperado)", f"${(meta_total - ingreso_esperado_total):,.2f}")

                # ===================== Gráfica de pastel: estatus =====================
        st.markdown("#### Distribución de estatus")

        if df_f.empty:
            st.info("Sin datos para graficar.")
        else:
            # Por si acaso ESTATUS_OPTIONS no existe (fallback)
            try:
                _opts = ESTATUS_OPTIONS
            except NameError:
                _opts = ["Acercamiento","Propuesta","Documentación","Cliente","Cancelado"]

            vc = df_f["estatus"].fillna("—").value_counts()

            labels = [s for s in _opts if s in vc.index]
            values = [int(vc.get(s, 0)) for s in labels]

            fig_pie = go.Figure(data=[
                go.Pie(
                    labels=labels,
                    values=values,
                    hole=0.35,
                    textinfo="label+percent",
                    hovertemplate="<b>%{label}</b><br>Registros: %{value}<br>%{percent}<extra></extra>",
                )
            ])
            fig_pie.update_layout(
                template="plotly_white",
                height=380,
                margin=dict(l=10, r=10, t=40, b=10),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
            )

            st.plotly_chart(fig_pie, use_container_width=True)


        # ===== NUEVO: métricas de montos por asesor =====
        sum_est = float(df_f["monto_estimado"].fillna(0).sum()) if not df_f.empty else 0.0
        sum_real = float(df_f.loc[df_f["estatus"]=="Cliente","monto_real"].fillna(0).sum()) if not df_f.empty else 0.0

        c5, c6 = st.columns(2)
        c5.metric("Suma ingresos estimados (MXN)", f"{sum_est:,.2f}")
        c6.metric("Suma ingresos reales (MXN)", f"{sum_real:,.2f}")

        # ===== NUEVO: gráfica Estimado vs Real (mes seleccionado) =====
        # ===== Gráfica de líneas (Plotly): Estimado vs Real por día (con opción acumulado) =====
        st.markdown("#### Estimado vs Real")

        if df_f.empty:
            st.info("Sin datos para graficar en el periodo seleccionado.")
        else:
            dfg = df_f.copy()
            dfg["fecha"] = pd.to_datetime(dfg["fecha"], errors="coerce")

            # Agregación diaria
            daily = dfg.groupby("fecha", as_index=True).agg(
                estimado=("monto_estimado", "sum"),
                # Real: tu regla original: contar real solo para filas con estatus Cliente
                real=("monto_real", lambda s: dfg.loc[s.index].assign(
                    _ok=(dfg.loc[s.index, "estatus"] == "Cliente")
                ).pipe(lambda t: t.loc[t["_ok"], "monto_real"].fillna(0).sum()))
            )

            # El lambda anterior deja un número total por día; aseguremos tipo numérico
            daily["estimado"] = pd.to_numeric(daily["estimado"], errors="coerce").fillna(0.0)
            daily["real"]     = pd.to_numeric(daily["real"], errors="coerce").fillna(0.0)

            
            # Índice completo de fechas según el periodo seleccionado
            if date_from is not None and date_to_exclusive is not None:
                full_idx = pd.date_range(
                    start=date_from,
                    end=date_to_exclusive - timedelta(days=1),
                    freq="D"
                )
            else:
                # Acumulado: usar rango real de datos
                full_idx = pd.date_range(
                    start=dfg["fecha"].min(),
                    end=dfg["fecha"].max(),
                    freq="D"
                )


            daily = daily.reindex(full_idx, fill_value=0.0)
            

            # Toggle acumulado
            acumular = st.checkbox("Mostrar acumulado", value=True, help="Activa para ver líneas acumuladas del mes.")
            plot_df = daily.copy()
            if acumular:
                plot_df["estimado"] = plot_df["estimado"].cumsum()
                plot_df["real"] = plot_df["real"].cumsum()

            # ---------- Plotly: líneas bonitas
            fig = go.Figure()

            fig.add_trace(go.Bar(
                x=plot_df.index, y=plot_df["estimado"],
                name="Estimado",
                hovertemplate="<b>%{x|%d-%b}</b><br>Estimado: $%{y:,.2f}<extra></extra>"
            ))

            fig.add_trace(go.Bar(
                x=plot_df.index, y=plot_df["real"],
                name="Real",
                hovertemplate="<b>%{x|%d-%b}</b><br>Real: $%{y:,.2f}<extra></extra>"
            ))

            # Estética general
            fig.update_layout(
                template="plotly_white",
                title="Ingresos estimados vs reales",
                xaxis_title="Fecha",
                yaxis_title="MXN",
                barmode="group",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
                hovermode="x unified",
                margin=dict(l=10, r=10, t=60, b=10),
                height=420,
            )

            # Eje X con rango del mes y slider/zoom cómodo
            # Eje X con rango dinámico (mes o histórico)
            # Rango del eje X según el periodo seleccionado
            if date_from is not None and date_to_exclusive is not None:
                x_start = date_from
                x_end = date_to_exclusive - timedelta(days=1)
            else:
                x_start = dfg["fecha"].min().date() if not dfg.empty else mes_inicio
                x_end = dfg["fecha"].max().date() if not dfg.empty else mes_inicio

            fig.update_xaxes(
                range=[full_idx.min(), full_idx.max()],
                tickformat="%d-%b",
                rangeslider=dict(visible=True)
            )



            
            # Eje Y con formato y pequeña separación superior
            fig.update_yaxes(tickprefix="$", separatethousands=True)

            st.plotly_chart(fig, width="stretch")



#######
        st.markdown("#### Clientes con solo acercamiento")
        if df_f.empty:
            st.write("—")
        else:
            tmp = df_f.copy()
            tmp["estatus_rank"] = tmp["estatus"].map(ESTATUS_ORDER).fillna(0)
            max_status = tmp.groupby("cliente", as_index=False)["estatus_rank"].max()
            solo_acerc = max_status[max_status["estatus_rank"] == ESTATUS_ORDER["Acercamiento"]]["cliente"].tolist()
            st.write(", ".join(sorted(set(solo_acerc))) if solo_acerc else "—")

        # ========= Edición de estatus por los asesores (con monto_real requerido si Cliente) =========
        st.markdown("#### Editar estatus de mis registros")
        if df_f.empty:
            st.write("—")
        else:
            cols_edit = ["id","cliente","producto","tipo","estatus","fecha","referenciador","monto_estimado","monto_real","nota","prob_cierre"]
            for c in cols_edit:
                if c not in df_f.columns:
                    df_f[c] = pd.NA
            df_edit_src = df_f[cols_edit].copy()

            # Usar ID como índice (oculto)
            df_edit_src["id_str"] = df_edit_src["id"].astype(str)

            df_edit_src["Eliminar"] = False

            df_view = df_edit_src.set_index("id_str")[[
                "cliente","producto","tipo","estatus","fecha","referenciador",
                "monto_estimado","monto_real","nota", "prob_cierre", "Eliminar"
            ]]

            edited = st.data_editor(
                df_view,
                key="editor_mis_registros",
                width="stretch",
                column_config={
                    "estatus": st.column_config.SelectboxColumn(
                        "Estatus",
                        options=ESTATUS_OPTIONS,
                        required=True,
                    ),
                    "cliente": st.column_config.TextColumn("Cliente", disabled=True),
                    "producto": st.column_config.TextColumn("Producto", disabled=True),
                    "tipo": st.column_config.TextColumn("Tipo", disabled=True),
                    "fecha": st.column_config.DateColumn("Fecha", disabled=True),
                    "referenciador": st.column_config.TextColumn("Referenciador", disabled=True),
                    "monto_estimado": st.column_config.NumberColumn("Estimado (MXN)", disabled=True, format="%.2f"),
                    "monto_real": st.column_config.NumberColumn("Real (MXN)", step=100.0, format="%.2f"),
                    "nota": st.column_config.TextColumn("Notas", help="Notas internas del asesor", width="large"),
                    "prob_cierre": st.column_config.NumberColumn("Prob. cierre (%)", min_value=0.0, max_value=100.0, step=1.0, format="%.0f"),
                    "Eliminar": st.column_config.CheckboxColumn("Eliminar"),

                },
                disabled=["cliente","producto","tipo","fecha","referenciador"], 
                hide_index=True,
            )

            if st.button("Guardar cambios de estatus", type="primary", width="stretch"):
                try:
                    # Mapas originales
                    src_status = {str(r["id"]): r["estatus"] for _, r in df_edit_src.iterrows()}
                    src_real   = {str(r["id"]): r.get("monto_real") for _, r in df_edit_src.iterrows()}
                    src_nota = {str(r["id"]): r.get("nota") for _, r in df_edit_src.iterrows()}
                    src_prob = {str(r["id"]): r.get("prob_cierre") for _, r in df_edit_src.iterrows()}



                    def _num_norm(x):
                        try:
                            return None if x is None or pd.isna(x) else float(x)
                        except Exception:
                            return None

                    def _txt_norm(x):
                        if x is None or pd.isna(x):
                            return None
                        s = str(x).strip()
                        return s if s else None

                    changes = []      # [(id, dict_update)]
                    to_delete = []
                    invalid_rows = [] # [(id, reason)]


                    for rid_str, row in edited.iterrows():
                        new_status = row["estatus"]
                        new_real_val = row.get("monto_real")
                        old_status = src_status.get(str(rid_str))
                        old_real   = src_real.get(str(rid_str))
                        old_nota = src_nota.get(str(rid_str))
                        new_nota = row.get("nota")
                        old_prob = src_prob.get(str(rid_str))
                        new_prob = row.get("prob_cierre")

                        if bool(row.get("Eliminar", False)) is True:
                            to_delete.append(str(rid_str))
                            continue


                        if _num_norm(new_prob) != _num_norm(old_prob):
                            p = _num_norm(new_prob)
                            if p is not None:
                                p = max(0.0, min(100.0, p))
                            upd["prob_cierre"] = p
                            changed = True

                        if to_delete:
                            for rid in to_delete:
                                def _del():
                                    # Seguridad: solo borrar registros del usuario actual
                                    return supabase.table("capturas").delete().eq("id", rid).eq("user_id", user.id).execute()
                                _retry_on_jwt_expired(_del)


                    

                        # Detectar cambios
                        upd = {}
                        changed = False
                        if old_status != new_status:
                            upd["estatus"] = new_status
                            changed = True
                        if _num_norm(new_real_val) != _num_norm(old_real):
                            upd["monto_real"] = _num_norm(new_real_val)
                            changed = True
                        if _txt_norm(new_nota) != _txt_norm(old_nota):
                            upd["nota"] = _txt_norm(new_nota)
                            changed = True

                        if changed:
                            changes.append((rid_str, upd))

                    if invalid_rows:
                        st.error("No se guardaron cambios. Revisa:")
                        for rid, reason in invalid_rows:
                            st.write(f"- ID {rid}: {reason}")
                    elif not changes:
                        st.info("No hay cambios por guardar.")
                    else:
                        for rid_str, upd in changes:
                            def _call_upd():
                                return supabase.table("capturas").update(upd).eq("id", rid_str).execute()
                            _retry_on_jwt_expired(_call_upd)
                        st.success(f"Actualizados {len(changes)} registro(s).")
                        st.session_state.capturas_cache_buster += 1
                        st.rerun()
                except APIError as e:
                    st.error(f"No se pudieron guardar los cambios: {_format_api_error(e)}")
                except Exception as e:
                    st.error(f"No se pudieron guardar los cambios: {e}")

# -------------------- Conglomerado (admins) --------------------
with TAB_CONG:
    ADMIN_FLAG = ADMIN_FLAG_GLOBAL
    if not ADMIN_FLAG:
        st.info("Solo administradores pueden ver el visor.")
    else:
        

        st.markdown("### 🎯 Asignar meta mensual")

        ases_map = _get_asesores_map()
        asesores_select = ["(Yo)"] + sorted(list(ases_map.keys()))

        c1, c2, c3 = st.columns([1,1,1])
        with c1:
            asesor_meta = st.selectbox("Asesor", asesores_select, key="meta_asesor")
            if asesor_meta == "(Yo)":
                meta_user_id = user.id
                meta_alias = ALIAS
            else:
                meta_user_id = ases_map[asesor_meta]
                meta_alias = asesor_meta
        with c2:
            mes_meta = st.date_input("Mes (meta)", value=date.today().replace(day=1), key="meta_mes").replace(day=1)
        with c3:
            meta_mxn = st.number_input("Meta (MXN)", min_value=0.0, step=1000.0, format="%.2f", key="meta_val")

        if st.button("Guardar meta", type="primary", key="btn_guardar_meta"):
            try:
                payload = {
                    "asesor_user_id": meta_user_id,
                    "asesor_alias": meta_alias,
                    "periodo": mes_meta.isoformat(),
                    "meta_mxn": float(meta_mxn)
                }
                def _upsert():
                    return supabase.table("metas_asesor").upsert(payload, on_conflict="asesor_user_id,periodo").execute()
                _retry_on_jwt_expired(_upsert)
                st.success("Meta guardada ✅")
            except APIError as e:
                st.error(f"No se pudo guardar: {_format_api_error(e)}")
            except Exception as e:
                st.error(f"No se pudo guardar: {e}")

        
        st.subheader("Resumen por asesor")
        
        periodo_admin = st.radio(
            "Periodo (admin)",
            ["Mes", "Trimestre", "Acumulado"],
            horizontal=True,
            key="periodo_admin"
        )

        col1, col2 = st.columns([1,1])
        with col1:
            mes_cong = st.date_input("Mes a analizar", value=date.today().replace(day=1),
                                     key="mes_analizar_cong").replace(day=1)
        with col2:
            tipo_cong = st.radio("Tipo de cliente", ["Todos","Nuevo","BAU"], horizontal=True, key="tipo_cong")

        mes_cong_fin = mes_cong + relativedelta(months=1)
        tipo_cong_param = None if tipo_cong == "Todos" else tipo_cong

        if periodo_admin == "Acumulado":
            date_from = None
            date_to_exclusive = None
        elif periodo_admin == "Trimestre":
            date_from, date_to_exclusive = _quarter_bounds(mes_cong)
        else:  # Mes
            date_from = mes_cong
            date_to_exclusive = mes_cong + relativedelta(months=1)

        df_month = load_capturas_filtered(
            st.session_state.capturas_cache_buster,
            uid=st.session_state.user.id,
            is_admin_flag=ADMIN_FLAG,
            scope="all",
            date_from=date_from,
            date_to_exclusive=date_to_exclusive,
            tipo=tipo_cong_param
        )



        # ---- Resumen por asesor
        st.markdown("### Resumen por asesor")
        if df_month.empty:
            st.write("Sin datos para el filtro.")
        else:
            df_month = df_month.copy()
            if "asesor" not in df_month.columns:
                df_month["asesor"] = pd.NA


            resumen_rows = []

            df_metas = _get_metas_mes(mes_cong)
            meta_map = {}
            if not df_metas.empty:
                for _, r in df_metas.iterrows():
                    meta_map[str(r.get("asesor_alias"))] = float(r.get("meta_mxn") or 0.0)

            for ases, chunk in df_month.groupby("asesor"):
                total_reg = len(chunk)
                ac = int((chunk["estatus"] == "Acercamiento").sum())
                p  = int((chunk["estatus"] == "Propuesta").sum())
                d  = int((chunk["estatus"] == "Documentación").sum())
                c  = int((chunk["estatus"] == "Cliente").sum())
                conv_pct, light = conversion_closed_over_total(total_reg, c)
                avg_prob = float(pd.to_numeric(chunk["prob_cierre"], errors="coerce").dropna().mean()) if "prob_cierre" in chunk.columns else 0.0


                sum_est = float(chunk["monto_estimado"].fillna(0).sum())
                sum_real = float(chunk.loc[chunk["estatus"]=="Cliente","monto_real"].fillna(0).sum())

                umbral = 51.0
                tmp = chunk.copy()
                tmp["prob_cierre"] = pd.to_numeric(tmp["prob_cierre"], errors="coerce")
                tmp["monto_estimado"] = pd.to_numeric(tmp["monto_estimado"], errors="coerce")

                esperado = float(tmp.loc[tmp["prob_cierre"] > umbral, "monto_estimado"].fillna(0).sum())
                meta = float(meta_map.get(str(ases), 0.0))
                brecha = meta - esperado

                resumen_rows.append({
                    "asesor": (ases if ases is not pd.NA and ases is not None else "—"),
                    "Total": total_reg,
                    "Acercamientos": ac,
                    "Propuestas": p,
                    "Documentación": d,
                    "Clientes": c,
                    "Estimado (MXN)": round(sum_est, 2),
                    "Real (MXN)": round(sum_real, 2),
                    "Tasa de conversión (Clientes/Total) %": round(conv_pct, 2),
                    "Semáforo": light,
                    "Meta (MXN)": round(meta, 2),
                    "Esperado >51% (MXN)": round(esperado, 2),
                    "Brecha (MXN)": round(brecha, 2),
                    "Prob. cierre promedio (%)": round(avg_prob, 1),


                })
            df_resumen = pd.DataFrame(resumen_rows).sort_values("asesor")
            st.dataframe(df_resumen, width="stretch")

            st.markdown("### 📌 Vista por asesor (estatus + ingreso esperado)")

            if df_month.empty:
                st.info("Sin datos para el periodo seleccionado.")
            else:
                umbral = 51.0

                df_month = df_month.copy()
                df_month["asesor"] = df_month["asesor"].astype("string")
                df_month["asesor"] = df_month["asesor"].fillna("—").replace("", "—")


                for ases, chunk in df_month.groupby("asesor"):
                    ases_name = "—" if (ases is None or pd.isna(ases) or str(ases).strip() == "") else str(ases)


                    # Ingreso esperado: suma monto_estimado donde prob_cierre > 51
                    tmp = chunk.copy()
                    tmp["prob_cierre"] = pd.to_numeric(tmp["prob_cierre"], errors="coerce")
                    tmp["monto_estimado"] = pd.to_numeric(tmp["monto_estimado"], errors="coerce")
                    esperado = float(tmp.loc[tmp["prob_cierre"] > umbral, "monto_estimado"].fillna(0).sum())

                    # Pie de estatus
                    vc = tmp["estatus"].fillna("—").value_counts()
                    labels = vc.index.tolist()
                    values = vc.values.tolist()

                    fig_pie = go.Figure(data=[go.Pie(labels=labels, values=values, hole=0.35)])
                    fig_pie.update_layout(template="plotly_white", height=320, margin=dict(l=10,r=10,t=30,b=10))

                    c1, c2 = st.columns([1, 2])
                    with c1:
                        st.subheader(f"Asesor: {ases_name}")
                        st.metric(f"Ingreso esperado (prob > {int(umbral)}%)", f"${esperado:,.2f}")
                    with c2:
                        st.plotly_chart(fig_pie, use_container_width=True)

                    st.divider()


        red_max, yellow_max = get_thresholds()
        st.caption(f"Semáforo: 🔴 ≤ {int(red_max*100)}%  |  🟡 ≤ {int(yellow_max*100)}%  |  🟢 > {int(yellow_max*100)}%")

        # ---- Registros por asesor (con 'Todos')
        st.markdown("### Registros por asesor")
        if not df_month.empty and "asesor" in df_month.columns:
            asesores_base = sorted(df_month["asesor"].dropna().unique().tolist())
        else:
            asesores_base = []
        asesores_lista = ["Todos"] + asesores_base
        tipos_lista = ["Todos","Nuevo","BAU"]

        colf1, colf2 = st.columns([1,1])
        with colf1:
            ases_sel = st.selectbox("Asesor", asesores_lista, key="asesor_cong")
        with colf2:
            tipo_sel = st.selectbox("Tipo de cliente", tipos_lista, key="tipo_cong_det")

        asesor_param = None if ases_sel == "Todos" else ases_sel
        tipo_param_det = None if tipo_sel == "Todos" else tipo_sel

        # ===== Registros por asesor (con opción TODO el histórico) =====
        # Fechas según periodo_admin (Mes / Trimestre / Acumulado)
        if periodo_admin == "Acumulado":
            det_from = None
            det_to_exclusive = None
        elif periodo_admin == "Trimestre":
            det_from, det_to_exclusive = _quarter_bounds(mes_cong)
        else:  # Mes
            det_from = mes_cong
            det_to_exclusive = mes_cong + relativedelta(months=1)

        df_det = load_capturas_filtered(
            st.session_state.capturas_cache_buster,
            uid=st.session_state.user.id,
            is_admin_flag=ADMIN_FLAG,
            scope="all",
            date_from=det_from,
            date_to_exclusive=det_to_exclusive,
            asesor=asesor_param,
            tipo=tipo_param_det
        )


        st.dataframe(
            style_rows_by_estatus(df_public_view(df_det)),
            use_container_width=True
        )



        # ===================== 📝 Crear observación por ASESOR =====================
        st.markdown("### 📝 Crear observación para un asesor")
        ases_map = _get_asesores_map()  # {alias -> user_id} desde capturas recientes
        asesores_select = sorted(list(ases_map.keys()))
        if not asesores_select:
            st.info("No hay asesores detectados en capturas para crear observaciones.")
        else:
            col_a1, col_a2 = st.columns([1,1])
            with col_a1:
                asesor_elegido = st.selectbox("Selecciona asesor", asesores_select, key="obs_asesor_admin")
            with col_a2:
                cliente_rel = st.text_input("Relacionado con cliente (opcional)", placeholder="Ej. Alitas 23")

            obs_msg = st.text_area("Observación", placeholder="Ej. Llamar al cliente para confirmar documentación...")
            btn_obs = st.button("Agregar observación", type="primary")

            if btn_obs:
                if not obs_msg.strip():
                    st.warning("Escribe una observación.")
                else:
                    try:
                        payload = {
                            "captura_id_text": "general",         # no ligada a un registro concreto
                            "asesor_user_id": ases_map[asesor_elegido],
                            "asesor_alias": asesor_elegido,
                            "cliente": (cliente_rel.strip() or None),
                            "mensaje": obs_msg.strip(),
                            "created_by_user_id": user.id,
                        }
                        def _ins():
                            return supabase.table("observaciones").insert(payload).execute()
                        _retry_on_jwt_expired(_ins)
                        st.success("Observación creada y notificada al asesor. 🔔")
                        st.session_state.obs_cache_buster += 1
                    except APIError as e:
                        st.error(f"No se pudo crear la observación: {_format_api_error(e)}")
                    except Exception as e:
                        st.error(f"No se pudo crear la observación: {e}")

        # ===================== 📋 Observaciones (panel del administrador) =====================
        st.markdown("---")
        st.markdown("### 📋 Observaciones (panel del administrador)")

        default_from = (date.today() - timedelta(days=120))
        use_date_filter = st.checkbox("Filtrar por fechas", value=False)
        if use_date_filter:
            col_o1, col_o2 = st.columns([1,1])
            with col_o1:
                obs_from = st.date_input("Desde", value=default_from, key="obs_from")
            with col_o2:
                obs_to = st.date_input("Hasta (exclusivo)", value=date.today() + timedelta(days=1), key="obs_to")
        else:
            obs_from = default_from
            obs_to = None  # sin to_exclusive

        # Filtro por asesor (map con user_id)
        ases_map_all = _get_asesores_map()
        asesores_admin = ["Todos"] + sorted(list(ases_map_all.keys()))
        ases_fil = st.selectbox("Asesor", asesores_admin, key="obs_asesor_filtro")
        ases_user_filter = None if ases_fil == "Todos" else ases_map_all.get(ases_fil)

        df_obs_admin = _query_observaciones_admin(obs_from, obs_to, asesor_user_id=ases_user_filter)

        if df_obs_admin.empty:
            st.write("Sin observaciones para el criterio seleccionado.")
        else:
            # Vista limpia: ocultamos ID internos
            df_obs_admin_ed = df_obs_admin.copy()

            # Agregamos columna de eliminar (solo UI)
            df_obs_admin_ed["Eliminar"] = False

            df_obs_admin_ed = df_obs_admin_ed[[
                "id", "created_at","asesor_alias","cliente","mensaje","done","Eliminar"
            ]].sort_values("created_at", ascending=False)

            st.caption("Marca/Desmarca la columna **Hecha** y guarda los cambios.")
            edited_obs = st.data_editor(
                df_obs_admin_ed.rename(columns={
                    "created_at": "Creada",
                    "asesor_alias": "Asesor",
                    "cliente": "Cliente",
                    "mensaje": "Observación",
                    "done": "Hecha",
                }),
                key="editor_obs_admin",
                use_container_width=True,
                hide_index=True,
                column_config={
                    "id": st.column_config.TextColumn("id", disabled=True),
                    "Creada": st.column_config.DatetimeColumn("Creada", disabled=True),
                    "Asesor": st.column_config.TextColumn("Asesor", disabled=True),
                    "Cliente": st.column_config.TextColumn("Cliente", disabled=True),
                    "Observación": st.column_config.TextColumn("Observación", disabled=True),
                    "Hecha": st.column_config.CheckboxColumn("Hecha"),
                    "Eliminar": st.column_config.CheckboxColumn("Eliminar"),
                }
            )


            # Para detectar cambios, reconstruimos el id usando merge con df original por columnas visibles
            if st.button("Guardar cambios de observaciones", type="primary"):
                try:
                    # edited_obs ya trae id, Hecha, Eliminar
                    df_e = edited_obs.copy()
                    # Normaliza nombres (porque renombraste columnas)
                    # Si id no se renombró, queda como "id"
                    # Hecha queda como "Hecha", Eliminar como "Eliminar"

                    # 1) Borrados
                    to_delete = df_e.loc[df_e["Eliminar"] == True, "id"].astype(str).tolist()
                    if to_delete:
                        for oid in to_delete:
                            def _del():
                                return supabase.table("observaciones").delete().eq("id", oid).execute()
                            _retry_on_jwt_expired(_del)

                    # 2) Cambios de Hecha
                    # Cargamos base original para comparar
                    base = df_obs_admin[["id","done"]].copy()
                    base["id"] = base["id"].astype(str)

                    merged = base.merge(df_e[["id","Hecha"]], on="id", how="left")
                    updates = []
                    for _, r in merged.iterrows():
                        old = bool(r["done"])
                        new = bool(r["Hecha"])
                        if new != old and str(r["id"]) not in set(to_delete):
                            updates.append((str(r["id"]), new))

                    for oid, new_done in updates:
                        if new_done:
                            payload = {"done": True, "done_at": datetime.utcnow().isoformat() + "Z", "done_by_user_id": user.id}
                        else:
                            payload = {"done": False, "done_at": None, "done_by_user_id": None}
                        def _upd():
                            return supabase.table("observaciones").update(payload).eq("id", oid).execute()
                        _retry_on_jwt_expired(_upd)

                    st.success(f"Listo ✅ Eliminadas: {len(to_delete)} | Actualizadas: {len(updates)}")
                    st.session_state.obs_cache_buster += 1
                    st.rerun()

                except APIError as e:
                    st.error(f"No se pudieron actualizar observaciones: {_format_api_error(e)}")
                except Exception as e:
                    st.error(f"No se pudieron actualizar observaciones: {e}")


        # ---- Borrado masivo (solo admins) hola hoa hola
 
# -------------------- Config (admins) --------------------
with TAB_CFG:
    ADMIN_FLAG = ADMIN_FLAG_GLOBAL
    if not ADMIN_FLAG:
        st.info("No eres admin.")
    else:
        st.subheader("Parámetros de conversión")
        st.caption("Ajusta los umbrales de semáforo para la tasa Clientes/Total. Se guardan en esta sesión.")

        cur_red, cur_yellow = get_thresholds()
        red_pct = st.slider("Límite ROJO (≤)", min_value=0, max_value=50, value=int(cur_red*100), step=1, help="Porcentaje hasta el cual se muestra 🔴")
        yellow_pct = st.slider("Límite AMARILLO (≤)", min_value=red_pct, max_value=80, value=int(cur_yellow*100), step=1, help="Porcentaje hasta el cual se muestra 🟡 (por encima es 🟢)")

        if st.button("Guardar umbrales", type="primary", width="content"):
            st.session_state.sem_red_max = red_pct / 100.0
            st.session_state.sem_yellow_max = yellow_pct / 100.0
            st.success(f"Umbrales actualizados: 🔴 ≤ {red_pct}% | 🟡 ≤ {yellow_pct}% | 🟢 > {yellow_pct}%")
            st.rerun()

        st.divider()
        st.subheader("🔐 Resetear contraseña de usuario (Admin)")

        # Obtener usuarios (correo + id)
        try:
            users = supabase_admin.auth.admin.list_users()
        except Exception as e:
            st.error(f"No se pudieron cargar los usuarios: {e}")
            users = []

        if users:
            user_map = {u.email: u.id for u in users if u.email}
            selected_email = st.selectbox("Selecciona usuario", sorted(user_map.keys()))
            new_pwd = st.text_input("Nueva contraseña", type="password")
            new_pwd2 = st.text_input("Confirmar nueva contraseña", type="password")

            if st.button("Cambiar contraseña", type="primary"):
                if not new_pwd or len(new_pwd) < 8:
                    st.warning("La contraseña debe tener al menos 8 caracteres.")
                elif new_pwd != new_pwd2:
                    st.warning("Las contraseñas no coinciden.")
                else:
                    try:
                        supabase_admin.auth.admin.update_user_by_id(
                            user_map[selected_email],
                            {"password": new_pwd}
                        )
                        st.success(f"Contraseña actualizada para {selected_email}")
                    except Exception as e:
                        st.error(f"No se pudo cambiar la contraseña: {e}")
        else:
            st.info("No hay usuarios disponibles.")
    
            
