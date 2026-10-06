"""
Testes do kill switch do tracing (`app/tracing.py`).

Nao exercitam o caminho com Jaeger ligado de proposito: a suite roda com
`TELEMETRY_TRACING_ENABLED=false` (ver `tests/conftest.py`) e o que precisa
ser garantido aqui e que, nesse estado, o setup e inteiramente no-op - nenhum
provider construido, nenhum span gravado, nenhuma conexao de rede aberta.
"""

from __future__ import annotations

from app import tracing


def test_kill_switch_desliga_o_tracing() -> None:
    """Com as variaveis do conftest, `tracing_enabled()` e False."""
    assert tracing.tracing_enabled() is False


def test_configure_tracing_e_no_op_quando_desabilitado() -> None:
    """O setup nao cria provider nem exporter; chamar duas vezes nao muda nada."""
    assert tracing.configure_tracing("telemetry-test") is None
    assert tracing.configure_tracing("telemetry-test") is None
    assert tracing.is_configured() is False

    # As instrumentacoes automaticas tambem viram no-op.
    tracing.instrument_sqlite3()
    tracing.instrument_requests()
    assert tracing.is_configured() is False


def test_spans_nao_gravam_e_shutdown_nao_levanta() -> None:
    """Sem provider, os spans sao no-op e flush/shutdown sao silenciosos."""
    span = tracing.get_tracer("tests.tracing").start_span("span-de-teste")
    try:
        assert span.is_recording() is False
    finally:
        span.end()

    assert tracing.force_flush() is False
    tracing.shutdown_tracing()  # nao deve levantar excecao
