"""
Camada de persistencia do Pipeline de Observabilidade e Telemetria.

Responsabilidades deste modulo:
    * Abrir/fechar conexoes com o SQLite de forma segura (context manager).
    * Criar o schema da tabela `telemetry_logs` e seus indices.
    * Inserir eventos de telemetria validados.
    * Executar consultas agregadas (SQL limpo) usadas pela API e pelo dashboard.

Decisoes de arquitetura:
    * Uma conexao por operacao: o SQLite nao compartilha conexoes entre threads
      com seguranca, e neste pipeline tres processos distintos (producer, API e
      dashboard) acessam o mesmo arquivo simultaneamente.
    * WAL (Write-Ahead Logging): permite leituras concorrentes enquanto o
      producer escreve, evitando erros de "database is locked".
    * Timestamps sempre em UTC, no formato 'YYYY-MM-DD HH:MM:SS.mmm', o que
      mantem a ordenacao lexicografica identica a ordenacao cronologica e
      permite comparar com as funcoes nativas `datetime('now', '-N minutes')`.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

# ---------------------------------------------------------------------------
# Configuracao de log estruturado do modulo
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Localizacao do banco de dados
# ---------------------------------------------------------------------------
# Raiz do projeto (.../telemetry-observability-pipeline)
BASE_DIR: Path = Path(__file__).resolve().parent.parent

# Caminho padrao do arquivo SQLite. Pode ser sobrescrito pela variavel de
# ambiente TELEMETRY_DB_PATH (util em CI, containers e testes).
DEFAULT_DB_PATH: Path = BASE_DIR / "telemetry.db"
_DB_PATH: Path = Path(os.getenv("TELEMETRY_DB_PATH", str(DEFAULT_DB_PATH)))

# Nome da tabela principal, centralizado para evitar strings duplicadas.
TABLE_NAME: str = "telemetry_logs"

# Servicos monitorados pelo pipeline (usados pelo producer e pela documentacao).
KNOWN_SERVICES: tuple[str, ...] = (
    "auth-service",
    "payment-gateway",
    "flight-control",
    "telemetry-ingest",
    "notification-hub",
)

# ---------------------------------------------------------------------------
# DDL - schema da tabela e indices de apoio as consultas agregadas
# ---------------------------------------------------------------------------
SCHEMA_SQL: str = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id            INTEGER  PRIMARY KEY AUTOINCREMENT,
    timestamp     DATETIME DEFAULT CURRENT_TIMESTAMP,
    service_name  TEXT     NOT NULL,
    latency_ms    FLOAT    NOT NULL,
    status_code   INTEGER  NOT NULL,
    error_flag    INTEGER  NOT NULL DEFAULT 0 CHECK (error_flag IN (0, 1))
);

-- Indice para as janelas temporais de /metrics/summary e do grafico de latencia.
CREATE INDEX IF NOT EXISTS idx_telemetry_timestamp
    ON {TABLE_NAME} (timestamp DESC);

-- Indice para filtrar/agrupar por servico.
CREATE INDEX IF NOT EXISTS idx_telemetry_service
    ON {TABLE_NAME} (service_name);

-- Indice parcial que acelera a tabela de alertas (apenas falhas).
CREATE INDEX IF NOT EXISTS idx_telemetry_errors
    ON {TABLE_NAME} (timestamp DESC) WHERE error_flag = 1;
"""


# ---------------------------------------------------------------------------
# Excecao de dominio
# ---------------------------------------------------------------------------
class DatabaseError(RuntimeError):
    """Erro de persistencia tratado, exposto para as camadas superiores.

    Encapsula qualquer `sqlite3.Error` para que a API e o dashboard nao
    precisem conhecer detalhes do driver de banco de dados.
    """


# ---------------------------------------------------------------------------
# Utilitarios de configuracao
# ---------------------------------------------------------------------------
def configure_database(db_path: str | Path) -> Path:
    """Define, em tempo de execucao, qual arquivo SQLite sera utilizado.

    Usado principalmente pelos testes (banco temporario) e por deploys que
    apontam para um volume especifico.

    Args:
        db_path: caminho do arquivo `.db`.

    Returns:
        O caminho efetivamente configurado.
    """
    global _DB_PATH
    _DB_PATH = Path(db_path)
    logger.debug("Banco de dados configurado para: %s", _DB_PATH)
    return _DB_PATH


def get_database_path() -> Path:
    """Retorna o caminho do arquivo SQLite em uso."""
    return _DB_PATH


def utc_timestamp() -> str:
    """Gera o timestamp atual em UTC no formato aceito pelo SQLite.

    Returns:
        String no padrao 'YYYY-MM-DD HH:MM:SS.mmm' (milissegundos truncados).
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


# ---------------------------------------------------------------------------
# Conexao
# ---------------------------------------------------------------------------
@contextmanager
def get_connection() -> Iterator[sqlite3.Connection]:
    """Context manager que entrega uma conexao SQLite pronta para uso.

    Garante commit em caso de sucesso, rollback em caso de falha e fechamento
    da conexao em qualquer cenario.

    Yields:
        sqlite3.Connection com `row_factory` configurado para `sqlite3.Row`.

    Raises:
        DatabaseError: se a conexao nao puder ser aberta ou a transacao falhar.
    """
    connection: sqlite3.Connection | None = None
    try:
        # Garante que o diretorio de destino exista (ex.: volume recem-montado).
        _DB_PATH.parent.mkdir(parents=True, exist_ok=True)

        connection = sqlite3.connect(
            _DB_PATH,
            timeout=10.0,  # aguarda ate 10s por locks do producer
            isolation_level="DEFERRED",
        )
        connection.row_factory = sqlite3.Row
        # WAL + synchronous NORMAL: melhor throughput de escrita continua e
        # leituras concorrentes sem bloqueio.
        connection.execute("PRAGMA journal_mode = WAL;")
        connection.execute("PRAGMA synchronous = NORMAL;")
        connection.execute("PRAGMA foreign_keys = ON;")

        yield connection
        connection.commit()
    except sqlite3.Error as exc:
        if connection is not None:
            connection.rollback()
        logger.error("Falha na operacao com o SQLite (%s): %s", _DB_PATH, exc)
        raise DatabaseError(f"Erro de banco de dados: {exc}") from exc
    finally:
        if connection is not None:
            connection.close()


# ---------------------------------------------------------------------------
# DDL / bootstrap
# ---------------------------------------------------------------------------
def init_db() -> Path:
    """Cria a tabela `telemetry_logs` e os indices, se ainda nao existirem.

    Operacao idempotente: pode ser chamada no startup da API, do producer e do
    dashboard sem efeitos colaterais.

    Returns:
        Caminho do arquivo de banco inicializado.

    Raises:
        DatabaseError: se o schema nao puder ser criado.
    """
    try:
        with get_connection() as connection:
            connection.executescript(SCHEMA_SQL)
    except DatabaseError:
        # Log ja emitido em get_connection; apenas propaga para o chamador.
        raise
    logger.info("Schema de telemetria pronto em: %s", _DB_PATH)
    return _DB_PATH


# ---------------------------------------------------------------------------
# Escrita
# ---------------------------------------------------------------------------
def insert_log(
    service_name: str,
    latency_ms: float,
    status_code: int,
    error_flag: int | None = None,
    timestamp: str | None = None,
) -> int:
    """Persiste um evento de telemetria.

    Args:
        service_name: nome do servico observado (ex.: 'payment-gateway').
        latency_ms: latencia da requisicao em milissegundos (>= 0).
        status_code: status HTTP retornado (100..599).
        error_flag: 0 para sucesso, 1 para falha. Quando `None`, e derivado
            automaticamente de `status_code >= 400`.
        timestamp: timestamp UTC ('YYYY-MM-DD HH:MM:SS[.mmm]'). Quando `None`,
            usa o horario atual em UTC.

    Returns:
        O `id` (chave primaria) do registro inserido.

    Raises:
        ValueError: se algum campo violar as regras de negocio.
        DatabaseError: se a escrita no SQLite falhar.
    """
    # --- Validacao defensiva antes de tocar o banco -------------------------
    if not isinstance(service_name, str) or not service_name.strip():
        raise ValueError("service_name e obrigatorio e deve ser texto nao vazio.")

    try:
        latency_value = float(latency_ms)
    except (TypeError, ValueError) as exc:
        raise ValueError("latency_ms deve ser numerico.") from exc
    if latency_value < 0:
        raise ValueError("latency_ms nao pode ser negativo.")

    try:
        status_value = int(status_code)
    except (TypeError, ValueError) as exc:
        raise ValueError("status_code deve ser um inteiro.") from exc
    if not 100 <= status_value <= 599:
        raise ValueError("status_code deve estar entre 100 e 599.")

    # Deriva a flag de erro quando nao informada explicitamente.
    flag_value = int(status_value >= 400) if error_flag is None else int(error_flag)
    if flag_value not in (0, 1):
        raise ValueError("error_flag deve ser 0 (ok) ou 1 (falha).")

    event_timestamp = timestamp or utc_timestamp()

    sql = f"""
        INSERT INTO {TABLE_NAME}
            (timestamp, service_name, latency_ms, status_code, error_flag)
        VALUES
            (?, ?, ?, ?, ?)
    """

    try:
        with get_connection() as connection:
            cursor = connection.execute(
                sql,
                (
                    event_timestamp,
                    service_name.strip(),
                    round(latency_value, 3),
                    status_value,
                    flag_value,
                ),
            )
            return int(cursor.lastrowid or 0)
    except DatabaseError:
        logger.error("Nao foi possivel inserir o evento de '%s'.", service_name)
        raise


# ---------------------------------------------------------------------------
# Leitura / agregacoes
# ---------------------------------------------------------------------------
def fetch_recent_logs(limit: int = 50) -> list[dict[str, Any]]:
    """Retorna os registros mais recentes de telemetria.

    Args:
        limit: quantidade maxima de registros (1..1000). Padrao 50.

    Returns:
        Lista de dicionarios ordenada do mais recente para o mais antigo.

    Raises:
        ValueError: se `limit` estiver fora da faixa permitida.
        DatabaseError: se a consulta falhar.
    """
    limit_value = int(limit)
    if not 1 <= limit_value <= 1000:
        raise ValueError("limit deve estar entre 1 e 1000.")

    sql = f"""
        SELECT id, timestamp, service_name, latency_ms, status_code, error_flag
        FROM {TABLE_NAME}
        ORDER BY id DESC
        LIMIT ?
    """

    try:
        with get_connection() as connection:
            rows = connection.execute(sql, (limit_value,)).fetchall()
            return [dict(row) for row in rows]
    except DatabaseError:
        logger.error("Falha ao consultar os ultimos %s registros.", limit_value)
        raise


def fetch_summary(minutes: int = 5) -> dict[str, Any]:
    """Calcula as metricas agregadas de uma janela temporal.

    Metricas retornadas: total de requisicoes, latencia media, latencia maxima,
    contagem de erros e taxa de erro em porcentagem.

    Args:
        minutes: tamanho da janela em minutos (1..1440). Padrao 5.

    Returns:
        Dicionario com as chaves: `window_minutes`, `total_requests`,
        `avg_latency_ms`, `max_latency_ms`, `error_count`, `error_rate_percent`
        e `generated_at`.

    Raises:
        ValueError: se `minutes` estiver fora da faixa permitida.
        DatabaseError: se a consulta falhar.
    """
    window = int(minutes)
    if not 1 <= window <= 1440:
        raise ValueError("minutes deve estar entre 1 e 1440.")

    sql = f"""
        SELECT
            COUNT(*)                     AS total_requests,
            AVG(latency_ms)              AS avg_latency_ms,
            MAX(latency_ms)              AS max_latency_ms,
            COALESCE(SUM(error_flag), 0) AS error_count
        FROM {TABLE_NAME}
        WHERE timestamp >= datetime('now', ?)
    """

    try:
        with get_connection() as connection:
            row = connection.execute(sql, (f"-{window} minutes",)).fetchone()
    except DatabaseError:
        logger.error("Falha ao agregar metricas dos ultimos %s minutos.", window)
        raise

    total = int(row["total_requests"] or 0)
    errors = int(row["error_count"] or 0)
    # Evita divisao por zero quando a janela nao possui registros.
    error_rate = round((errors / total) * 100, 2) if total else 0.0

    return {
        "window_minutes": window,
        "total_requests": total,
        "avg_latency_ms": round(float(row["avg_latency_ms"] or 0.0), 2),
        "max_latency_ms": round(float(row["max_latency_ms"] or 0.0), 2),
        "error_count": errors,
        "error_rate_percent": error_rate,
        "generated_at": utc_timestamp(),
    }


def fetch_alerts(limit: int = 20) -> list[dict[str, Any]]:
    """Retorna os eventos de falha mais recentes (`error_flag = 1`).

    Args:
        limit: quantidade maxima de alertas (1..500). Padrao 20.

    Returns:
        Lista de dicionarios com as falhas, da mais recente para a mais antiga.

    Raises:
        ValueError: se `limit` estiver fora da faixa permitida.
        DatabaseError: se a consulta falhar.
    """
    limit_value = int(limit)
    if not 1 <= limit_value <= 500:
        raise ValueError("limit deve estar entre 1 e 500.")

    sql = f"""
        SELECT id, timestamp, service_name, latency_ms, status_code, error_flag
        FROM {TABLE_NAME}
        WHERE error_flag = 1
        ORDER BY id DESC
        LIMIT ?
    """

    try:
        with get_connection() as connection:
            rows = connection.execute(sql, (limit_value,)).fetchall()
            return [dict(row) for row in rows]
    except DatabaseError:
        logger.error("Falha ao consultar alertas de erro.")
        raise


def count_logs() -> int:
    """Conta o total de eventos armazenados na tabela.

    Returns:
        Numero total de registros.

    Raises:
        DatabaseError: se a consulta falhar.
    """
    try:
        with get_connection() as connection:
            row = connection.execute(f"SELECT COUNT(*) AS total FROM {TABLE_NAME}").fetchone()
            return int(row["total"] or 0)
    except DatabaseError:
        logger.error("Falha ao contar registros de telemetria.")
        raise


def check_health() -> dict[str, Any]:
    """Executa um diagnostico leve da camada de dados.

    Verifica se o arquivo responde a um `SELECT 1` e se a tabela principal
    existe, sem lancar excecao: o resultado e sempre um relatorio.

    Returns:
        Dicionario com `database_connected`, `table_ready`, `database_path`,
        `total_records` e, opcionalmente, `detail` com a causa da falha.
    """
    report: dict[str, Any] = {
        "database_connected": False,
        "table_ready": False,
        "database_path": str(_DB_PATH),
        "total_records": 0,
    }

    try:
        with get_connection() as connection:
            connection.execute("SELECT 1;").fetchone()
            report["database_connected"] = True

            table = connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?;",
                (TABLE_NAME,),
            ).fetchone()
            report["table_ready"] = table is not None

            if report["table_ready"]:
                row = connection.execute(
                    f"SELECT COUNT(*) AS total FROM {TABLE_NAME}"
                ).fetchone()
                report["total_records"] = int(row["total"] or 0)
    except DatabaseError as exc:
        # Health check nunca propaga erro: apenas reporta o estado degradado.
        report["detail"] = str(exc)
        logger.warning("Health check do banco falhou: %s", exc)

    return report


# ---------------------------------------------------------------------------
# Execucao direta: cria o banco/schema via `python -m app.database`
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    path = init_db()
    print(f"[OK] Banco de telemetria inicializado em: {path}")
    print(f"[OK] Registros existentes: {count_logs()}")
