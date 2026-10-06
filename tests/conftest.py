"""
Configuracao global da suite de testes.

Este arquivo existe por um motivo unico: **a suite nao emite spans pela rede**.

O pytest importa `conftest.py` antes de qualquer modulo de teste, portanto as
variaveis abaixo sao definidas antes de `app.main` / `app.producer` serem
importados e chamarem `tracing.configure_tracing(...)`. Com o kill switch
ligado nenhum `TracerProvider`, `BatchSpanProcessor` ou exporter OTLP e
construido - logo nenhuma conexao gRPC com o Jaeger e aberta, nenhum teste
depende de um container no ar e a suite nao fica pendurada esperando rede.
"""

from __future__ import annotations

import os

# Kill switch proprio do pipeline (ver `app/tracing.py`).
os.environ["TELEMETRY_TRACING_ENABLED"] = "false"
# Cinto e suspensorio: desliga tambem o SDK do OpenTelemetry na raiz, cobrindo
# qualquer provider que venha a ser criado fora de `app/tracing.py`.
os.environ["OTEL_SDK_DISABLED"] = "true"

import pytest  # noqa: E402  (import apos a configuracao de ambiente)

from app import tracing  # noqa: E402  (import apos a configuracao de ambiente)


@pytest.fixture(scope="session", autouse=True)
def tracing_desligado() -> None:
    """Reafirma, na sessao inteira, que o tracing esta desligado.

    Falha cedo e com mensagem clara caso alguem remova as variaveis acima ou
    altere o contrato do kill switch.
    """
    assert not tracing.tracing_enabled(), (
        "A suite precisa rodar com o tracing desligado: "
        "verifique TELEMETRY_TRACING_ENABLED / OTEL_SDK_DISABLED em tests/conftest.py."
    )
