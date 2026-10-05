"""
API REST de observabilidade (FastAPI).

Expoe as metricas do pipeline de telemetria com documentacao Swagger/OpenAPI
gerada automaticamente:

    GET /health           -> status da API e do banco de dados
    GET /metrics/summary  -> agregados (latencia media, total, taxa de erro)
    GET /metrics/recent   -> ultimos N registros (padrao 50)
    GET /metrics/alerts   -> ultimos eventos com error_flag = 1

Execucao:
    uvicorn app.main:app --reload
    # Swagger UI: http://127.0.0.1:8000/docs
"""

from __future__ import annotations

import logging
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, AsyncIterator, Literal

from fastapi import FastAPI, HTTPException, Query, status
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel, Field

# Permite `python app/main.py` alem de `uvicorn app.main:app`.
if __package__ in (None, ""):  # pragma: no cover - conveniencia de execucao
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app import database  # noqa: E402  (import apos ajuste de sys.path)

logger = logging.getLogger("api")

API_VERSION = "1.0.0"


# ---------------------------------------------------------------------------
# Schemas (Pydantic v2) - contratos de entrada/saida documentados no Swagger
# ---------------------------------------------------------------------------
class TelemetryLog(BaseModel):
    """Representa um evento de telemetria persistido."""

    id: int = Field(..., description="Identificador sequencial do evento.", examples=[1042])
    timestamp: str = Field(
        ...,
        description="Data/hora do evento em UTC.",
        examples=["2026-09-28 12:34:56.789"],
    )
    service_name: str = Field(
        ..., description="Servico observado.", examples=["payment-gateway"]
    )
    latency_ms: float = Field(..., description="Latencia em milissegundos.", examples=[87.42])
    status_code: int = Field(..., description="Status HTTP retornado.", examples=[200])
    error_flag: int = Field(
        ..., description="0 = sucesso, 1 = falha.", ge=0, le=1, examples=[0]
    )


class MetricsSummary(BaseModel):
    """Metricas agregadas de uma janela temporal."""

    window_minutes: int = Field(..., description="Janela analisada, em minutos.")
    total_requests: int = Field(..., description="Total de requisicoes na janela.")
    avg_latency_ms: float = Field(..., description="Latencia media em milissegundos.")
    max_latency_ms: float = Field(..., description="Maior latencia observada (ms).")
    error_count: int = Field(..., description="Quantidade de eventos com falha.")
    error_rate_percent: float = Field(..., description="Taxa de erro em porcentagem.")
    generated_at: str = Field(..., description="Momento do calculo (UTC).")


class HealthStatus(BaseModel):
    """Resultado do health check da API e da camada de dados."""

    status: Literal["ok", "degraded"] = Field(..., description="Situacao geral do servico.")
    api_version: str = Field(..., description="Versao da API.")
    database_connected: bool = Field(..., description="Conexao com o SQLite disponivel.")
    table_ready: bool = Field(..., description="Tabela telemetry_logs existe.")
    database_path: str = Field(..., description="Arquivo SQLite em uso.")
    total_records: int = Field(..., description="Total de eventos armazenados.")
    detail: str | None = Field(default=None, description="Causa da degradacao, se houver.")


# ---------------------------------------------------------------------------
# Ciclo de vida da aplicacao
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    """Garante o schema no startup e registra o shutdown."""
    try:
        path = database.init_db()
        logger.info("API conectada ao banco de telemetria: %s", path)
    except database.DatabaseError as exc:
        # Nao impede o boot: /health passa a reportar estado degradado.
        logger.error("Nao foi possivel inicializar o banco no startup: %s", exc)
    yield
    logger.info("Encerrando a API de observabilidade.")


app = FastAPI(
    title="Telemetry Observability API",
    description=(
        "API de observabilidade em tempo real para ambientes de missao critica. "
        "Fornece metricas agregadas, historico recente e alertas de falha "
        "coletados pelo pipeline de telemetria."
    ),
    version=API_VERSION,
    lifespan=lifespan,
    contact={"name": "Plataforma de Observabilidade"},
    license_info={"name": "MIT"},
)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/", include_in_schema=False)
async def root() -> RedirectResponse:
    """Redireciona a raiz para a documentacao interativa."""
    return RedirectResponse(url="/docs")


@app.get(
    "/health",
    response_model=HealthStatus,
    tags=["Infraestrutura"],
    summary="Checagem de status da API e do banco",
    responses={503: {"model": HealthStatus, "description": "Servico degradado."}},
)
async def health() -> JSONResponse:
    """Verifica a disponibilidade da API e da camada de persistencia.

    Retorna HTTP 200 quando tudo esta operacional e HTTP 503 quando o banco
    esta inacessivel ou o schema ainda nao foi criado.
    """
    report = database.check_health()
    healthy = bool(report["database_connected"] and report["table_ready"])

    payload = HealthStatus(
        status="ok" if healthy else "degraded",
        api_version=API_VERSION,
        database_connected=bool(report["database_connected"]),
        table_ready=bool(report["table_ready"]),
        database_path=str(report["database_path"]),
        total_records=int(report["total_records"]),
        detail=report.get("detail"),
    )

    return JSONResponse(
        content=payload.model_dump(),
        status_code=status.HTTP_200_OK if healthy else status.HTTP_503_SERVICE_UNAVAILABLE,
    )


@app.get(
    "/metrics/summary",
    response_model=MetricsSummary,
    tags=["Metricas"],
    summary="Metricas agregadas dos ultimos N minutos",
)
async def metrics_summary(
    minutes: Annotated[
        int,
        Query(
            ge=1,
            le=1440,
            description="Tamanho da janela temporal em minutos (1 a 1440).",
        ),
    ] = 5,
) -> MetricsSummary:
    """Calcula latencia media, total de requisicoes e taxa de erro da janela.

    Quando a janela nao possui eventos, todos os indicadores retornam zero.
    """
    try:
        return MetricsSummary(**database.fetch_summary(minutes=minutes))
    except ValueError as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
    except database.DatabaseError as exc:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Banco de telemetria indisponivel: {exc}",
        ) from exc


@app.get(
    "/metrics/recent",
    response_model=list[TelemetryLog],
    tags=["Metricas"],
    summary="Ultimos registros de telemetria",
)
async def metrics_recent(
    limit: Annotated[
        int,
        Query(ge=1, le=1000, description="Quantidade de registros a retornar."),
    ] = 50,
) -> list[TelemetryLog]:
    """Retorna os eventos mais recentes, do mais novo para o mais antigo.

    O padrao de 50 registros atende ao consumo direto pelo dashboard.
    """
    try:
        rows = database.fetch_recent_logs(limit=limit)
    except ValueError as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
    except database.DatabaseError as exc:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Banco de telemetria indisponivel: {exc}",
        ) from exc
    return [TelemetryLog(**row) for row in rows]


@app.get(
    "/metrics/alerts",
    response_model=list[TelemetryLog],
    tags=["Metricas"],
    summary="Alertas recentes (apenas falhas)",
)
async def metrics_alerts(
    limit: Annotated[
        int,
        Query(ge=1, le=500, description="Quantidade de alertas a retornar."),
    ] = 20,
) -> list[TelemetryLog]:
    """Retorna apenas os eventos com `error_flag = 1` (status 4xx/5xx)."""
    try:
        rows = database.fetch_alerts(limit=limit)
    except ValueError as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
    except database.DatabaseError as exc:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Banco de telemetria indisponivel: {exc}",
        ) from exc
    return [TelemetryLog(**row) for row in rows]


# ---------------------------------------------------------------------------
# Execucao direta: `python app/main.py`
# ---------------------------------------------------------------------------
if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    uvicorn.run("app.main:app", host="127.0.0.1", port=8000, reload=True)
