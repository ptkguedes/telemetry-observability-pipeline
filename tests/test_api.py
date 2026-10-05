"""
Testes de integracao dos endpoints da API FastAPI (`app/main.py`).

Utiliza o `TestClient` do FastAPI (baseado em httpx) apontando para um banco
SQLite temporario. O `TestClient` e usado como context manager para que o
`lifespan` da aplicacao execute o bootstrap do schema.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

import pytest
from fastapi.testclient import TestClient

from app import database
from app.main import API_VERSION, app


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def client(tmp_path: Path) -> Iterator[TestClient]:
    """Cliente HTTP de teste ligado a um banco temporario e limpo."""
    caminho_original = database.get_database_path()
    database.configure_database(tmp_path / "telemetry_api_test.db")

    # O context manager dispara o lifespan (init_db) da aplicacao.
    with TestClient(app) as test_client:
        try:
            yield test_client
        finally:
            database.configure_database(caminho_original)


def _semear_eventos() -> None:
    """Insere um conjunto conhecido de eventos: 3 sucessos e 1 falha."""
    database.insert_log("auth-service", 100.0, 200, 0)
    database.insert_log("auth-service", 200.0, 200, 0)
    database.insert_log("payment-gateway", 300.0, 204, 0)
    database.insert_log("flight-control", 800.0, 503, 1)


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------
def test_health_retorna_ok(client: TestClient) -> None:
    """Com banco acessivel e schema criado, /health responde 200/ok."""
    resposta = client.get("/health")
    corpo = resposta.json()

    assert resposta.status_code == 200
    assert corpo["status"] == "ok"
    assert corpo["api_version"] == API_VERSION
    assert corpo["database_connected"] is True
    assert corpo["table_ready"] is True
    assert corpo["total_records"] == 0


def test_health_contabiliza_registros(client: TestClient) -> None:
    """O health check deve refletir o total de eventos gravados."""
    _semear_eventos()
    corpo = client.get("/health").json()

    assert corpo["total_records"] == 4


def test_health_reporta_degradado_sem_schema(client: TestClient, tmp_path: Path) -> None:
    """Sem a tabela de telemetria, /health deve responder 503/degraded."""
    caminho_atual = database.get_database_path()
    database.configure_database(tmp_path / "api_sem_schema.db")
    try:
        resposta = client.get("/health")
        assert resposta.status_code == 503
        assert resposta.json()["status"] == "degraded"
        assert resposta.json()["table_ready"] is False
    finally:
        database.configure_database(caminho_atual)


# ---------------------------------------------------------------------------
# /metrics/summary
# ---------------------------------------------------------------------------
def test_summary_com_banco_vazio(client: TestClient) -> None:
    """Sem eventos, todos os indicadores devem ser zero."""
    corpo = client.get("/metrics/summary").json()

    assert corpo["total_requests"] == 0
    assert corpo["avg_latency_ms"] == 0.0
    assert corpo["error_rate_percent"] == 0.0
    assert corpo["window_minutes"] == 5  # valor padrao do endpoint


def test_summary_calcula_metricas(client: TestClient) -> None:
    """Latencia media, total e taxa de erro devem ser consistentes."""
    _semear_eventos()
    corpo = client.get("/metrics/summary", params={"minutes": 10}).json()

    assert corpo["window_minutes"] == 10
    assert corpo["total_requests"] == 4
    assert corpo["avg_latency_ms"] == pytest.approx(350.0)
    assert corpo["max_latency_ms"] == pytest.approx(800.0)
    assert corpo["error_count"] == 1
    assert corpo["error_rate_percent"] == pytest.approx(25.0)


def test_summary_ignora_eventos_antigos(client: TestClient) -> None:
    """Eventos fora da janela solicitada nao devem ser considerados."""
    antigo = (datetime.now(timezone.utc) - timedelta(minutes=90)).strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )[:-3]
    database.insert_log("auth-service", 50.0, 500, 1, timestamp=antigo)
    database.insert_log("auth-service", 80.0, 200, 0)

    corpo = client.get("/metrics/summary", params={"minutes": 5}).json()

    assert corpo["total_requests"] == 1
    assert corpo["error_count"] == 0


@pytest.mark.parametrize("minutos_invalidos", [0, -1, 1441, "abc"])
def test_summary_valida_parametro_minutes(client: TestClient, minutos_invalidos: object) -> None:
    """Valores fora do contrato devem retornar 422 (validacao do Pydantic)."""
    resposta = client.get("/metrics/summary", params={"minutes": minutos_invalidos})
    assert resposta.status_code == 422


# ---------------------------------------------------------------------------
# /metrics/recent
# ---------------------------------------------------------------------------
def test_recent_com_banco_vazio(client: TestClient) -> None:
    """Sem eventos, a resposta deve ser uma lista vazia."""
    resposta = client.get("/metrics/recent")

    assert resposta.status_code == 200
    assert resposta.json() == []


def test_recent_retorna_contrato_completo(client: TestClient) -> None:
    """Cada item deve conter todos os campos do modelo TelemetryLog."""
    _semear_eventos()
    corpo = client.get("/metrics/recent").json()

    assert len(corpo) == 4
    assert set(corpo[0]) == {
        "id",
        "timestamp",
        "service_name",
        "latency_ms",
        "status_code",
        "error_flag",
    }
    # Ordenacao: do mais recente para o mais antigo.
    assert [item["id"] for item in corpo] == [4, 3, 2, 1]


def test_recent_respeita_limite(client: TestClient) -> None:
    """O parametro `limit` deve truncar a quantidade de registros."""
    for indice in range(12):
        database.insert_log("telemetry-ingest", 20.0 + indice, 200, 0)

    corpo = client.get("/metrics/recent", params={"limit": 5}).json()
    assert len(corpo) == 5


def test_recent_limite_padrao_e_50(client: TestClient) -> None:
    """Sem `limit`, o endpoint deve devolver no maximo 50 registros."""
    for _ in range(60):
        database.insert_log("auth-service", 30.0, 200, 0)

    corpo = client.get("/metrics/recent").json()
    assert len(corpo) == 50


@pytest.mark.parametrize("limite_invalido", [0, -3, 1001])
def test_recent_valida_limite(client: TestClient, limite_invalido: int) -> None:
    """Limites fora da faixa 1..1000 devem retornar 422."""
    resposta = client.get("/metrics/recent", params={"limit": limite_invalido})
    assert resposta.status_code == 422


# ---------------------------------------------------------------------------
# /metrics/alerts
# ---------------------------------------------------------------------------
def test_alerts_retorna_somente_falhas(client: TestClient) -> None:
    """O endpoint de alertas deve filtrar apenas `error_flag = 1`."""
    _semear_eventos()
    database.insert_log("payment-gateway", 1200.0, 500, 1)

    corpo = client.get("/metrics/alerts").json()

    assert len(corpo) == 2
    assert all(item["error_flag"] == 1 for item in corpo)


# ---------------------------------------------------------------------------
# Documentacao automatica
# ---------------------------------------------------------------------------
def test_documentacao_swagger_disponivel(client: TestClient) -> None:
    """Swagger UI e OpenAPI devem ser servidos automaticamente."""
    assert client.get("/docs").status_code == 200

    esquema = client.get("/openapi.json")
    assert esquema.status_code == 200
    caminhos = esquema.json()["paths"]
    assert {"/health", "/metrics/summary", "/metrics/recent"} <= set(caminhos)


def test_raiz_redireciona_para_docs(client: TestClient) -> None:
    """A rota `/` deve redirecionar (307) para a documentacao."""
    resposta = client.get("/", follow_redirects=False)

    assert resposta.status_code == 307
    assert resposta.headers["location"] == "/docs"
