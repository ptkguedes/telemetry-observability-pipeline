"""
Testes unitarios da camada de persistencia (`app/database.py`).

Cada teste roda contra um arquivo SQLite temporario e isolado, criado pela
fixture `banco_temporario`, de modo que o `telemetry.db` de desenvolvimento
nunca e tocado.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

import pytest

from app import database


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def banco_temporario(tmp_path: Path) -> Iterator[Path]:
    """Aponta o modulo para um banco temporario e restaura o original no fim."""
    caminho_original = database.get_database_path()
    caminho_teste = tmp_path / "telemetry_test.db"

    database.configure_database(caminho_teste)
    database.init_db()
    try:
        yield caminho_teste
    finally:
        database.configure_database(caminho_original)


def _timestamp_com_offset(minutos: int) -> str:
    """Gera um timestamp UTC deslocado em `minutos` (negativo = passado)."""
    momento = datetime.now(timezone.utc) + timedelta(minutes=minutos)
    return momento.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
def test_init_db_cria_arquivo_e_tabela(banco_temporario: Path) -> None:
    """O bootstrap deve criar o arquivo e a tabela `telemetry_logs`."""
    assert banco_temporario.exists()

    with database.get_connection() as conexao:
        tabela = conexao.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='telemetry_logs';"
        ).fetchone()
    assert tabela is not None


def test_init_db_e_idempotente(banco_temporario: Path) -> None:
    """Chamar `init_db` novamente nao deve falhar nem apagar dados."""
    database.insert_log("auth-service", 50.0, 200, 0)
    database.init_db()
    assert database.count_logs() == 1


def test_schema_possui_todas_as_colunas_exigidas(banco_temporario: Path) -> None:
    """Valida o contrato de colunas da tabela de telemetria."""
    with database.get_connection() as conexao:
        colunas = {
            linha["name"]: linha["type"]
            for linha in conexao.execute("PRAGMA table_info(telemetry_logs);").fetchall()
        }

    assert colunas == {
        "id": "INTEGER",
        "timestamp": "DATETIME",
        "service_name": "TEXT",
        "latency_ms": "FLOAT",
        "status_code": "INTEGER",
        "error_flag": "INTEGER",
    }


# ---------------------------------------------------------------------------
# Insercao
# ---------------------------------------------------------------------------
def test_insert_log_retorna_id_incremental(banco_temporario: Path) -> None:
    """Cada insercao deve devolver a chave primaria gerada."""
    primeiro = database.insert_log("auth-service", 42.5, 200, 0)
    segundo = database.insert_log("payment-gateway", 512.0, 500, 1)

    assert primeiro == 1
    assert segundo == 2
    assert database.count_logs() == 2


def test_insert_log_persiste_valores_corretos(banco_temporario: Path) -> None:
    """Os valores gravados devem corresponder exatamente aos enviados."""
    database.insert_log("flight-control", 123.456, 503, 1)
    registro = database.fetch_recent_logs(limit=1)[0]

    assert registro["service_name"] == "flight-control"
    assert registro["latency_ms"] == pytest.approx(123.456)
    assert registro["status_code"] == 503
    assert registro["error_flag"] == 1
    assert registro["timestamp"]  # preenchido automaticamente em UTC


def test_insert_log_deriva_error_flag_do_status(banco_temporario: Path) -> None:
    """Sem `error_flag` explicito, status >= 400 deve marcar falha."""
    database.insert_log("auth-service", 30.0, 200)
    database.insert_log("auth-service", 900.0, 500)

    registros = database.fetch_recent_logs(limit=2)
    flags = {registro["status_code"]: registro["error_flag"] for registro in registros}

    assert flags[200] == 0
    assert flags[500] == 1


@pytest.mark.parametrize(
    ("service_name", "latency_ms", "status_code", "error_flag"),
    [
        ("", 10.0, 200, 0),  # servico vazio
        ("   ", 10.0, 200, 0),  # servico apenas com espacos
        ("auth-service", -1.0, 200, 0),  # latencia negativa
        ("auth-service", 10.0, 99, 0),  # status fora da faixa HTTP
        ("auth-service", 10.0, 600, 0),  # status fora da faixa HTTP
        ("auth-service", 10.0, 200, 2),  # error_flag invalido
        ("auth-service", "rapido", 200, 0),  # latencia nao numerica
    ],
)
def test_insert_log_rejeita_dados_invalidos(
    banco_temporario: Path,
    service_name: str,
    latency_ms: float,
    status_code: int,
    error_flag: int,
) -> None:
    """Entradas invalidas devem levantar `ValueError` antes de tocar o banco."""
    with pytest.raises(ValueError):
        database.insert_log(service_name, latency_ms, status_code, error_flag)  # type: ignore[arg-type]

    assert database.count_logs() == 0


# ---------------------------------------------------------------------------
# Consultas
# ---------------------------------------------------------------------------
def test_fetch_recent_logs_ordena_do_mais_novo_para_o_mais_antigo(
    banco_temporario: Path,
) -> None:
    """A consulta deve devolver os registros em ordem decrescente de id."""
    for indice in range(5):
        database.insert_log("auth-service", 10.0 + indice, 200, 0)

    registros = database.fetch_recent_logs(limit=10)
    ids = [registro["id"] for registro in registros]

    assert ids == sorted(ids, reverse=True)
    assert ids[0] == 5


def test_fetch_recent_logs_respeita_o_limite(banco_temporario: Path) -> None:
    """O parametro `limit` deve truncar o resultado."""
    for _ in range(10):
        database.insert_log("telemetry-ingest", 25.0, 200, 0)

    assert len(database.fetch_recent_logs(limit=3)) == 3


def test_fetch_recent_logs_em_banco_vazio(banco_temporario: Path) -> None:
    """Banco sem eventos deve retornar lista vazia, nao erro."""
    assert database.fetch_recent_logs() == []


@pytest.mark.parametrize("limite_invalido", [0, -5, 1001])
def test_fetch_recent_logs_valida_limite(
    banco_temporario: Path, limite_invalido: int
) -> None:
    """Limites fora da faixa 1..1000 devem ser rejeitados."""
    with pytest.raises(ValueError):
        database.fetch_recent_logs(limit=limite_invalido)


def test_fetch_summary_calcula_metricas_agregadas(banco_temporario: Path) -> None:
    """Media de latencia, total e taxa de erro devem refletir os dados."""
    database.insert_log("auth-service", 100.0, 200, 0)
    database.insert_log("auth-service", 200.0, 200, 0)
    database.insert_log("payment-gateway", 300.0, 500, 1)
    database.insert_log("payment-gateway", 400.0, 503, 1)

    resumo = database.fetch_summary(minutes=5)

    assert resumo["total_requests"] == 4
    assert resumo["avg_latency_ms"] == pytest.approx(250.0)
    assert resumo["max_latency_ms"] == pytest.approx(400.0)
    assert resumo["error_count"] == 2
    assert resumo["error_rate_percent"] == pytest.approx(50.0)
    assert resumo["window_minutes"] == 5


def test_fetch_summary_em_banco_vazio_retorna_zeros(banco_temporario: Path) -> None:
    """Sem dados na janela, nenhuma divisao por zero deve ocorrer."""
    resumo = database.fetch_summary(minutes=10)

    assert resumo["total_requests"] == 0
    assert resumo["avg_latency_ms"] == 0.0
    assert resumo["error_rate_percent"] == 0.0


def test_fetch_summary_ignora_eventos_fora_da_janela(banco_temporario: Path) -> None:
    """Registros antigos nao devem entrar no calculo da janela temporal."""
    database.insert_log(
        "auth-service", 999.0, 500, 1, timestamp=_timestamp_com_offset(-120)
    )
    database.insert_log("auth-service", 50.0, 200, 0)

    resumo = database.fetch_summary(minutes=5)

    assert resumo["total_requests"] == 1
    assert resumo["avg_latency_ms"] == pytest.approx(50.0)
    assert resumo["error_count"] == 0


@pytest.mark.parametrize("janela_invalida", [0, -1, 1441])
def test_fetch_summary_valida_janela(banco_temporario: Path, janela_invalida: int) -> None:
    """Janelas fora da faixa 1..1440 minutos devem ser rejeitadas."""
    with pytest.raises(ValueError):
        database.fetch_summary(minutes=janela_invalida)


def test_fetch_alerts_retorna_somente_falhas(banco_temporario: Path) -> None:
    """A consulta de alertas deve filtrar `error_flag = 1`."""
    database.insert_log("auth-service", 40.0, 200, 0)
    database.insert_log("payment-gateway", 1500.0, 500, 1)
    database.insert_log("flight-control", 2200.0, 503, 1)

    alertas = database.fetch_alerts(limit=10)

    assert len(alertas) == 2
    assert all(alerta["error_flag"] == 1 for alerta in alertas)
    assert {alerta["status_code"] for alerta in alertas} == {500, 503}


# ---------------------------------------------------------------------------
# Health check e tratamento de erro
# ---------------------------------------------------------------------------
def test_check_health_reporta_banco_saudavel(banco_temporario: Path) -> None:
    """Com o schema criado, o diagnostico deve ser positivo."""
    database.insert_log("auth-service", 15.0, 200, 0)
    saude = database.check_health()

    assert saude["database_connected"] is True
    assert saude["table_ready"] is True
    assert saude["total_records"] == 1
    assert saude["database_path"] == str(banco_temporario)


def test_check_health_reporta_tabela_ausente(tmp_path: Path) -> None:
    """Banco sem schema deve ser reportado como degradado, sem excecao."""
    caminho_original = database.get_database_path()
    database.configure_database(tmp_path / "sem_schema.db")
    try:
        saude = database.check_health()
        assert saude["database_connected"] is True
        assert saude["table_ready"] is False
    finally:
        database.configure_database(caminho_original)


def test_consulta_sem_schema_levanta_database_error(tmp_path: Path) -> None:
    """Erros do driver devem ser encapsulados em `DatabaseError`."""
    caminho_original = database.get_database_path()
    database.configure_database(tmp_path / "vazio.db")
    try:
        with pytest.raises(database.DatabaseError):
            database.fetch_recent_logs(limit=5)
    finally:
        database.configure_database(caminho_original)


def test_configure_database_altera_o_destino(tmp_path: Path) -> None:
    """`configure_database` deve trocar o arquivo alvo em tempo de execucao."""
    caminho_original = database.get_database_path()
    novo_caminho = tmp_path / "subpasta" / "outro.db"
    try:
        database.configure_database(novo_caminho)
        database.init_db()
        assert database.get_database_path() == novo_caminho
        assert novo_caminho.exists()
    finally:
        database.configure_database(caminho_original)
