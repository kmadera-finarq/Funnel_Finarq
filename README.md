# 🚀 Funnel de Ventas – FINARQ

Aplicación interna de **FINARQ** para gestionar el funnel de ventas de asesores, controlando ingresos estimados vs reales, métricas de conversión y panel administrativo con observaciones.

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://tu-app.streamlit.app)

---

## 📌 Funcionalidades principales
- **Login seguro** con Supabase (roles: Asesor y Admin).
- **Registro de clientes** con:
  - Producto
  - Tipo de cliente (Nuevo / BAU)
  - Estatus del funnel (Acercamiento / Propuesta / Documentación / Cliente)
  - Ingreso **estimado** y, al cerrar cliente, ingreso **real** obligatorio.
- **Gráfica interactiva (Plotly)**: ingresos estimados vs reales por día, con opción de acumulado.
- **Métricas**:
  - Conversión (clientes / total registrados).
  - Semáforo configurable (umbral rojo/amarillo/verde).
  - Totales de ingresos esperados y reales.
- **Panel administrativo**:
  - Resumen por asesor.
  - Creación y seguimiento de observaciones.
  - Configuración de parámetros de conversión.
- **RLS en Supabase**: cada asesor solo ve y edita sus registros; admin ve todo.

---

## 🛠️ Requisitos
- Python 3.10 o superior
- Dependencias listadas en `requirements.txt`

Instalación rápida:
```bash
pip install -r requirements.txt
