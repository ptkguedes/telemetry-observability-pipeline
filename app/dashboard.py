"""
Painel de observabilidade em tempo real (Streamlit).

Consome as metricas de duas formas selecionaveis na barra lateral:
    * "API (FastAPI)" -> chamadas HTTP aos endpoints /metrics/*  (padrao)
    * "Banco (SQLite)" -> leitura direta do arquivo telemetry.db (fallback)

Conteudo do painel:
    1. KPIs: total de requisicoes, latencia media (ms) e taxa de erro (%).
    2. Grafico de linha da latencia ao longo do tempo (com media movel).
    3. Tabela de alertas destacando em vermelho os eventos com error_flag = 1.

Execucao:
    streamlit run app/dashboard.py
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

# Permite a execucao via `streamlit run app/dashboard.py` (sem pacote pai).
if __package__ in (None, ""):  # pragma: no cover - conveniencia de execucao
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app import database  # noqa: E402  (import apos ajuste de sys.path)

# ---------------------------------------------------------------------------
# Configuracao geral
# ---------------------------------------------------------------------------
API_BASE_URL: str = os.getenv("TELEMETRY_API_URL", "http://127.0.0.1:8000").rstrip("/")
REQUEST_TIMEOUT: float = 5.0

# Limiares usados para colorir os KPIs (SLO simplificado do ambiente).
LATENCY_WARNING_MS: float = 300.0
ERROR_RATE_WARNING_PERCENT: float = 5.0

st.set_page_config(
    page_title="Telemetry Observability",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ---------------------------------------------------------------------------
# Acesso aos dados - API
# ---------------------------------------------------------------------------
def _api_get(path: str, params: dict[str, Any] | None = None) -> Any:
    """Executa um GET na API de observabilidade.

    Args:
        path: caminho do endpoint (ex.: '/metrics/summary').
        params: query string opcional.

    Returns:
        Corpo da resposta desserializado.

    Raises:
        requests.RequestException: em falha de rede, timeout ou status != 2xx.
    """
    response = requests.get(f"{API_BASE_URL}{path}", params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def load_summary(minutes: int, source: str) -> tuple[dict[str, Any], str | None]:
    """Carrega as metricas agregadas da fonte escolhida.

    Args:
        minutes: janela temporal em minutos.
        source: 'api' ou 'db'.

    Returns:
        Tupla (metricas, aviso). `aviso` e preenchido quando houve fallback
        automatico da API para o banco.
    """
    if source == "api":
        try:
            return _api_get("/metrics/summary", {"minutes": minutes}), None
        except requests.RequestException as exc:
            aviso = f"API indisponivel ({exc.__class__.__name__}). Lendo direto do SQLite."
            try:
                return database.fetch_summary(minutes=minutes), aviso
            except database.DatabaseError as db_exc:
                return _empty_summary(minutes), f"{aviso} Falha tambem no banco: {db_exc}"

    try:
        return database.fetch_summary(minutes=minutes), None
    except database.DatabaseError as exc:
        return _empty_summary(minutes), f"Falha ao ler o banco: {exc}"


def load_logs(limit: int, source: str) -> tuple[pd.DataFrame, str | None]:
    """Carrega os eventos recentes e devolve um DataFrame normalizado.

    Args:
        limit: quantidade de registros.
        source: 'api' ou 'db'.

    Returns:
        Tupla (dataframe, aviso).
    """
    aviso: str | None = None
    registros: list[dict[str, Any]] = []

    if source == "api":
        try:
            registros = _api_get("/metrics/recent", {"limit": limit})
        except requests.RequestException as exc:
            aviso = f"API indisponivel ({exc.__class__.__name__}). Lendo direto do SQLite."
            try:
                registros = database.fetch_recent_logs(limit=limit)
            except database.DatabaseError as db_exc:
                aviso = f"{aviso} Falha tambem no banco: {db_exc}"
    else:
        try:
            registros = database.fetch_recent_logs(limit=limit)
        except database.DatabaseError as exc:
            aviso = f"Falha ao ler o banco: {exc}"

    return _to_dataframe(registros), aviso


def _empty_summary(minutes: int) -> dict[str, Any]:
    """Retorna um sumario zerado, usado quando nenhuma fonte responde."""
    return {
        "window_minutes": minutes,
        "total_requests": 0,
        "avg_latency_ms": 0.0,
        "max_latency_ms": 0.0,
        "error_count": 0,
        "error_rate_percent": 0.0,
        "generated_at": database.utc_timestamp(),
    }


def _to_dataframe(registros: list[dict[str, Any]]) -> pd.DataFrame:
    """Converte a lista de eventos em DataFrame ordenado cronologicamente."""
    colunas = ["id", "timestamp", "service_name", "latency_ms", "status_code", "error_flag"]
    if not registros:
        return pd.DataFrame(columns=colunas)

    frame = pd.DataFrame(registros)
    # Timestamps sao gravados em UTC pela camada de persistencia.
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    frame["latency_ms"] = pd.to_numeric(frame["latency_ms"], errors="coerce")
    frame["error_flag"] = pd.to_numeric(frame["error_flag"], errors="coerce").fillna(0).astype(int)
    return frame.sort_values("timestamp").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Componentes visuais
# ---------------------------------------------------------------------------
def formatar_numero(valor: float, decimais: int = 2) -> str:
    """Formata numeros no padrao pt-BR (ponto de milhar, virgula decimal).

    Args:
        valor: numero a formatar.
        decimais: casas decimais exibidas (0 para inteiros).

    Returns:
        Texto formatado, ex.: 2659.63 -> '2.659,63'.
    """
    texto = f"{valor:,.{decimais}f}"
    # Troca os separadores en-US por pt-BR usando um marcador temporario.
    return texto.replace(",", "\x00").replace(".", ",").replace("\x00", ".")


def render_kpis(resumo: dict[str, Any]) -> None:
    """Renderiza os indicadores principais no topo da pagina."""
    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        label="Total de Requisicoes",
        value=formatar_numero(int(resumo["total_requests"]), decimais=0),
        help=f"Eventos coletados nos ultimos {resumo['window_minutes']} minutos.",
    )

    latencia = float(resumo["avg_latency_ms"])
    col2.metric(
        label="Latencia Media (ms)",
        value=formatar_numero(latencia),
        delta="dentro do SLO" if latencia <= LATENCY_WARNING_MS else "acima do SLO",
        delta_color="normal" if latencia <= LATENCY_WARNING_MS else "inverse",
    )

    taxa_erro = float(resumo["error_rate_percent"])
    col3.metric(
        label="Taxa de Erro (%)",
        value=formatar_numero(taxa_erro),
        delta=f"{int(resumo['error_count'])} falha(s)",
        delta_color="normal" if taxa_erro <= ERROR_RATE_WARNING_PERCENT else "inverse",
    )

    col4.metric(
        label="Latencia Maxima (ms)",
        value=formatar_numero(float(resumo["max_latency_ms"])),
        help="Pior latencia observada na janela analisada.",
    )


def render_latency_chart(frame: pd.DataFrame) -> None:
    """Desenha o grafico de linha da latencia ao longo do tempo."""
    st.subheader("Latencia ao longo do tempo")

    if frame.empty:
        st.info("Sem dados de telemetria. Inicie o producer: `python -m app.producer`.")
        return

    figura = go.Figure()

    # Serie principal: latencia observada evento a evento.
    figura.add_trace(
        go.Scatter(
            x=frame["timestamp"],
            y=frame["latency_ms"],
            mode="lines+markers",
            name="Latencia (ms)",
            line={"color": "#2E86DE", "width": 2},
            marker={"size": 5},
            hovertemplate="%{x|%H:%M:%S}<br>%{y:.2f} ms<extra></extra>",
        )
    )

    # Media movel para suavizar o ruido e evidenciar tendencia.
    janela = max(3, min(15, len(frame) // 4))
    if len(frame) >= janela:
        figura.add_trace(
            go.Scatter(
                x=frame["timestamp"],
                y=frame["latency_ms"].rolling(window=janela, min_periods=1).mean(),
                mode="lines",
                name=f"Media movel ({janela})",
                line={"color": "#F39C12", "width": 2, "dash": "dash"},
            )
        )

    # Destaque dos eventos com falha sobre a linha de latencia.
    falhas = frame[frame["error_flag"] == 1]
    if not falhas.empty:
        figura.add_trace(
            go.Scatter(
                x=falhas["timestamp"],
                y=falhas["latency_ms"],
                mode="markers",
                name="Falhas",
                marker={"size": 11, "color": "#E74C3C", "symbol": "x"},
                hovertemplate=(
                    "%{x|%H:%M:%S}<br>%{y:.2f} ms<br>status %{customdata}<extra></extra>"
                ),
                customdata=falhas["status_code"],
            )
        )

    figura.update_layout(
        height=420,
        margin={"l": 10, "r": 10, "t": 30, "b": 10},
        xaxis_title="Horario (UTC)",
        yaxis_title="Latencia (ms)",
        hovermode="x unified",
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0},
    )

    # `width="stretch"` substitui o antigo `use_container_width=True`.
    st.plotly_chart(figura, width="stretch")


def _destacar_falhas(linha: pd.Series) -> list[str]:
    """Aplica fundo vermelho as linhas cujo `error_flag` e 1."""
    if int(linha.get("error_flag", 0)) == 1:
        return ["background-color: #7B1E1E; color: #FFFFFF; font-weight: 600"] * len(linha)
    return [""] * len(linha)


def render_alerts_table(frame: pd.DataFrame, apenas_falhas: bool) -> None:
    """Renderiza a tabela de eventos com destaque para as falhas."""
    st.subheader("Alertas e eventos recentes")

    if frame.empty:
        st.info("Nenhum evento para exibir.")
        return

    tabela = frame.sort_values("timestamp", ascending=False).copy()
    if apenas_falhas:
        tabela = tabela[tabela["error_flag"] == 1]

    if tabela.empty:
        st.success("Nenhuma falha registrada na janela analisada.")
        return

    # Formata o timestamp para leitura humana mantendo a referencia UTC.
    tabela["timestamp"] = tabela["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    tabela = tabela.rename(
        columns={
            "id": "ID",
            "timestamp": "Timestamp (UTC)",
            "service_name": "Servico",
            "latency_ms": "Latencia (ms)",
            "status_code": "Status",
            "error_flag": "error_flag",
        }
    )

    estilo = (
        tabela.style.apply(_destacar_falhas, axis=1)
        .format({"Latencia (ms)": "{:.2f}"})
        .hide(axis="index")
    )

    st.dataframe(estilo, width="stretch", height=380)

    total_falhas = int(frame["error_flag"].sum())
    if total_falhas:
        st.error(f"{total_falhas} evento(s) com falha destacado(s) em vermelho.")


# ---------------------------------------------------------------------------
# Aplicacao
# ---------------------------------------------------------------------------
def main() -> None:
    """Monta o painel completo e trata o auto-refresh."""
    st.title("📡 Pipeline de Observabilidade e Telemetria")
    st.caption(
        "Monitoramento em tempo real de servicos de missao critica "
        f"| fonte de dados: `{API_BASE_URL}` ou `{database.get_database_path().name}`"
    )

    # ----------------------------- Sidebar ---------------------------------
    with st.sidebar:
        st.header("Configuracoes")

        # Todos os widgets usam `key` explicito: isso mantem o estado estavel
        # entre reruns e permite controlar o painel em testes automatizados.
        fonte_label = st.radio(
            "Fonte de dados",
            options=["API (FastAPI)", "Banco (SQLite)"],
            index=0,
            key="data_source",
            help="A API e a fonte preferencial; o banco serve como fallback.",
        )
        fonte = "api" if fonte_label.startswith("API") else "db"

        janela = st.slider(
            "Janela de analise (minutos)",
            min_value=1,
            max_value=120,
            value=15,
            step=1,
            key="window_minutes",
        )
        limite = st.slider(
            "Registros no grafico",
            min_value=50,
            max_value=500,
            value=200,
            step=50,
            key="record_limit",
        )
        apenas_falhas = st.checkbox(
            "Mostrar apenas falhas na tabela", value=False, key="only_failures"
        )

        st.divider()
        auto_refresh = st.toggle("Atualizacao automatica", value=True, key="auto_refresh")
        intervalo = st.slider(
            "Intervalo de atualizacao (s)",
            min_value=2,
            max_value=30,
            value=5,
            step=1,
            disabled=not auto_refresh,
            key="refresh_interval",
        )
        if st.button("Atualizar agora", key="manual_refresh"):
            st.rerun()

        st.divider()
        saude = database.check_health()
        if saude["database_connected"] and saude["table_ready"]:
            st.success(f"Banco OK · {saude['total_records']} eventos")
        else:
            st.error("Banco indisponivel ou sem schema")

    # ------------------------------ Dados ----------------------------------
    resumo, aviso_resumo = load_summary(minutes=janela, source=fonte)
    eventos, aviso_logs = load_logs(limit=limite, source=fonte)

    for aviso in {aviso_resumo, aviso_logs} - {None}:
        st.warning(aviso)

    # ------------------------------ Layout ---------------------------------
    render_kpis(resumo)
    st.divider()
    render_latency_chart(eventos)
    st.divider()
    render_alerts_table(eventos, apenas_falhas=apenas_falhas)

    st.caption(
        f"Ultima atualizacao (UTC): {resumo['generated_at']} "
        f"· janela de {resumo['window_minutes']} min "
        f"· {len(eventos)} evento(s) carregado(s)"
    )

    # --------------------------- Auto-refresh ------------------------------
    # Estrategia simples e sem dependencias extras: aguarda o intervalo e
    # re-executa o script, que e o modelo de renderizacao do Streamlit.
    if auto_refresh:
        time.sleep(intervalo)
        st.rerun()


# O Streamlit executa este arquivo como script, portanto `__name__` e
# "__main__" e o painel e montado na sequencia.
if __name__ == "__main__":  # pragma: no cover
    main()
