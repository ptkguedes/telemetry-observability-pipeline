"""
Producer de telemetria sintetica (simulador de ambiente de missao critica).

Gera eventos realistas e os grava continuamente no SQLite:
    * ~80% de trafego saudavel  -> 2xx com latencia baixa (perfil log-normal).
    * ~20% de trafego degradado -> 4xx/5xx com latencia alta (cauda longa).

Uso:
    python -m app.producer                      # intervalo padrao de 2 segundos
    python -m app.producer --interval 0.5       # alta frequencia
    python -m app.producer --batch 3            # 3 eventos por ciclo
    python -m app.producer --max-events 100     # para automaticamente apos 100
    python -m app.producer --db ./outro.db      # banco alternativo

Encerramento: Ctrl+C (KeyboardInterrupt) finaliza de forma graciosa e imprime
um resumo da sessao.
"""

from __future__ import annotations

import argparse
import logging
import random
import signal
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from types import FrameType

from opentelemetry.trace import Status, StatusCode

# Permite executar tanto como modulo (`python -m app.producer`) quanto como
# script direto (`python app/producer.py`).
if __package__ in (None, ""):  # pragma: no cover - conveniencia de execucao
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app import database, tracing  # noqa: E402  (import apos ajuste de sys.path)

logger = logging.getLogger("producer")

# ---------------------------------------------------------------------------
# Parametros da simulacao
# ---------------------------------------------------------------------------
DEFAULT_INTERVAL_SECONDS: float = 2.0
ERROR_PROBABILITY: float = 0.20  # 20% dos eventos representam falhas

# Pesos de trafego por servico: gateways recebem mais chamadas que o resto.
SERVICE_WEIGHTS: dict[str, float] = {
    "auth-service": 0.25,
    "payment-gateway": 0.25,
    "flight-control": 0.20,
    "telemetry-ingest": 0.20,
    "notification-hub": 0.10,
}

# Distribuicao dos status de sucesso e de falha.
SUCCESS_STATUS_WEIGHTS: dict[int, float] = {200: 0.90, 201: 0.07, 204: 0.03}
ERROR_STATUS_WEIGHTS: dict[int, float] = {500: 0.45, 503: 0.35, 404: 0.20}

# Faixas de latencia (ms) por cenario.
HEALTHY_LATENCY_RANGE: tuple[float, float] = (8.0, 250.0)
DEGRADED_LATENCY_RANGE: tuple[float, float] = (400.0, 3000.0)


@dataclass
class ProducerStats:
    """Acumula as estatisticas da sessao para o resumo final."""

    total: int = 0
    errors: int = 0
    failed_writes: int = 0
    latencies: list[float] = field(default_factory=list)
    started_at: float = field(default_factory=time.monotonic)

    @property
    def success(self) -> int:
        """Quantidade de eventos saudaveis gravados."""
        return self.total - self.errors

    @property
    def error_rate(self) -> float:
        """Taxa de erro da sessao em porcentagem."""
        return round((self.errors / self.total) * 100, 2) if self.total else 0.0

    @property
    def avg_latency(self) -> float:
        """Latencia media da sessao em milissegundos."""
        return round(sum(self.latencies) / len(self.latencies), 2) if self.latencies else 0.0

    @property
    def elapsed(self) -> float:
        """Tempo total de execucao em segundos."""
        return round(time.monotonic() - self.started_at, 1)


# ---------------------------------------------------------------------------
# Geracao de eventos
# ---------------------------------------------------------------------------
def _weighted_choice(weights: dict) -> object:
    """Sorteia uma chave de um dicionario {valor: peso}."""
    keys = list(weights.keys())
    return random.choices(keys, weights=[weights[k] for k in keys], k=1)[0]


def _healthy_latency() -> float:
    """Latencia de requisicao saudavel.

    Usa distribuicao log-normal para reproduzir o comportamento tipico de APIs:
    maioria dos valores concentrada em torno de ~60ms com uma cauda curta.
    """
    value = random.lognormvariate(mu=4.1, sigma=0.55)
    low, high = HEALTHY_LATENCY_RANGE
    return round(min(max(value, low), high), 2)


def _degraded_latency() -> float:
    """Latencia de requisicao degradada (timeout/retry/saturacao)."""
    low, high = DEGRADED_LATENCY_RANGE
    # Triangular enviesada para o piso da faixa: picos extremos sao raros.
    return round(random.triangular(low, high, low + (high - low) * 0.25), 2)


def generate_event() -> dict[str, object]:
    """Produz um unico evento de telemetria sintetico.

    Returns:
        Dicionario com `service_name`, `latency_ms`, `status_code` e `error_flag`.
    """
    service_name = str(_weighted_choice(SERVICE_WEIGHTS))
    is_error = random.random() < ERROR_PROBABILITY

    if is_error:
        status_code = int(_weighted_choice(ERROR_STATUS_WEIGHTS))
        latency_ms = _degraded_latency()
    else:
        status_code = int(_weighted_choice(SUCCESS_STATUS_WEIGHTS))
        latency_ms = _healthy_latency()

    return {
        "service_name": service_name,
        "latency_ms": latency_ms,
        "status_code": status_code,
        "error_flag": int(is_error),
    }


# ---------------------------------------------------------------------------
# Loop principal
# ---------------------------------------------------------------------------
def run(
    interval: float = DEFAULT_INTERVAL_SECONDS,
    batch_size: int = 1,
    max_events: int = 0,
) -> ProducerStats:
    """Executa o loop de producao de telemetria.

    Args:
        interval: pausa em segundos entre os ciclos de geracao.
        batch_size: quantidade de eventos gerados por ciclo.
        max_events: limite total de eventos (0 = executar indefinidamente).

    Returns:
        `ProducerStats` com o resumo da sessao (mesmo apos Ctrl+C).
    """
    database.init_db()
    stats = ProducerStats()
    # Sem provider configurado este tracer e no-op: o loop nao muda de forma.
    tracer = tracing.get_tracer("app.producer")

    limite = "infinito" if max_events <= 0 else str(max_events)
    logger.info(
        "Producer iniciado | banco=%s | intervalo=%.2fs | lote=%d | limite=%s",
        database.get_database_path(),
        interval,
        batch_size,
        limite,
    )
    logger.info("Pressione Ctrl+C para encerrar de forma graciosa.")

    try:
        while max_events <= 0 or stats.total < max_events:
            for _ in range(batch_size):
                if 0 < max_events <= stats.total:
                    break

                # Um span por evento (nao por ciclo): os atributos pedidos sao
                # do evento sintetico e, com `--batch N`, ficam N spans irmaos,
                # cada um com o INSERT do SQLite como filho.
                with tracer.start_as_current_span("producer.gerar_evento") as span:
                    event = generate_event()
                    span.set_attribute(
                        tracing.ATTR_SERVICE_NAME, str(event["service_name"])
                    )
                    span.set_attribute(
                        tracing.ATTR_LATENCY_MS,
                        float(event["latency_ms"]),  # type: ignore[arg-type]
                    )
                    span.set_attribute(
                        tracing.ATTR_STATUS_CODE,
                        int(event["status_code"]),  # type: ignore[arg-type]
                    )
                    span.set_attribute(
                        tracing.ATTR_ERROR_FLAG,
                        int(event["error_flag"]),  # type: ignore[arg-type]
                    )

                    try:
                        event_id = database.insert_log(
                            service_name=str(event["service_name"]),
                            latency_ms=float(event["latency_ms"]),  # type: ignore[arg-type]
                            status_code=int(event["status_code"]),  # type: ignore[arg-type]
                            error_flag=int(event["error_flag"]),  # type: ignore[arg-type]
                        )
                    except (database.DatabaseError, ValueError) as exc:
                        # Falha de escrita nao derruba o producer: registra e segue.
                        stats.failed_writes += 1
                        span.record_exception(exc)
                        span.set_status(
                            Status(StatusCode.ERROR, f"Evento descartado: {exc}")
                        )
                        logger.warning("Evento descartado (%s): %s", type(exc).__name__, exc)
                        continue

                    span.set_attribute(tracing.ATTR_EVENT_ID, event_id)

                stats.total += 1
                stats.errors += int(event["error_flag"])  # type: ignore[arg-type]
                stats.latencies.append(float(event["latency_ms"]))  # type: ignore[arg-type]

                nivel = logging.WARNING if event["error_flag"] else logging.INFO
                logger.log(
                    nivel,
                    "#%-6s %-18s status=%s latencia=%8.2fms %s",
                    event_id,
                    event["service_name"],
                    event["status_code"],
                    event["latency_ms"],
                    "[FALHA]" if event["error_flag"] else "[OK]",
                )

            if max_events <= 0 or stats.total < max_events:
                time.sleep(interval)
    except KeyboardInterrupt:
        # Encerramento gracioso solicitado pelo operador (Ctrl+C).
        print()  # quebra a linha do "^C" no terminal
        logger.info("Interrupcao recebida. Encerrando o producer...")
    except database.DatabaseError as exc:
        logger.error("Erro fatal de banco de dados, abortando: %s", exc)
    finally:
        # Flush antes do resumo: garante que os spans do ciclo cheguem ao
        # Jaeger e mantem o resumo como ultima coisa impressa no terminal.
        tracing.shutdown_tracing()
        _print_summary(stats)

    return stats


def _print_summary(stats: ProducerStats) -> None:
    """Imprime o resumo consolidado da sessao de producao."""
    logger.info("-" * 62)
    logger.info("Resumo da sessao do producer")
    logger.info("  Eventos gravados .....: %d", stats.total)
    logger.info("  Sucessos .............: %d", stats.success)
    logger.info("  Falhas (error_flag=1) : %d (%.2f%%)", stats.errors, stats.error_rate)
    logger.info("  Escritas descartadas .: %d", stats.failed_writes)
    logger.info("  Latencia media .......: %.2f ms", stats.avg_latency)
    logger.info("  Duracao ..............: %.1f s", stats.elapsed)
    logger.info("-" * 62)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Interpreta os argumentos de linha de comando."""
    parser = argparse.ArgumentParser(
        description="Gerador continuo de eventos sinteticos de telemetria.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_INTERVAL_SECONDS,
        help="Intervalo em segundos entre os ciclos (padrao: 2.0).",
    )
    parser.add_argument(
        "--batch",
        type=int,
        default=1,
        help="Eventos gerados por ciclo (padrao: 1).",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Limite de eventos; 0 executa indefinidamente (padrao: 0).",
    )
    parser.add_argument(
        "--db",
        type=str,
        default=None,
        help="Caminho alternativo para o arquivo SQLite.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Semente aleatoria para tornar a simulacao reproduzivel.",
    )
    args = parser.parse_args(argv)

    if args.interval < 0:
        parser.error("--interval nao pode ser negativo.")
    if args.batch < 1:
        parser.error("--batch deve ser >= 1.")
    return args


def _handle_sigterm(signum: int, frame: FrameType | None) -> None:  # pragma: no cover
    """Converte SIGTERM em KeyboardInterrupt para reaproveitar o shutdown."""
    raise KeyboardInterrupt


def main(argv: list[str] | None = None) -> int:
    """Ponto de entrada do producer.

    Returns:
        Codigo de saida do processo (0 = sucesso).
    """
    args = _parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.seed is not None:
        random.seed(args.seed)
    if args.db:
        database.configure_database(args.db)

    # Tracing antes do primeiro acesso ao banco: a instrumentacao do sqlite3 so
    # alcanca as conexoes abertas depois dela.
    tracing.configure_tracing("telemetry-producer")
    tracing.instrument_sqlite3()

    # Em ambientes containerizados o orquestrador envia SIGTERM ao parar.
    try:
        signal.signal(signal.SIGTERM, _handle_sigterm)
    except (ValueError, AttributeError, OSError):  # pragma: no cover
        pass  # sinal indisponivel (ex.: thread secundaria no Windows)

    run(interval=args.interval, batch_size=args.batch, max_events=args.max_events)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
