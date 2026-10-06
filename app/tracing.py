"""
Tracing distribuido do Pipeline de Observabilidade e Telemetria.

Modulo unico de setup do OpenTelemetry. Os tres processos do pipeline
(`app/main.py`, `app/producer.py` e `app/dashboard.py`) chamam as funcoes
daqui para exportar spans via OTLP/gRPC para o Jaeger.

Responsabilidades deste modulo:
    * Montar um `TracerProvider` com `Resource` (service.name + service.version).
    * Exportar spans em lote (`BatchSpanProcessor` + `OTLPSpanExporter` gRPC).
    * Ligar/desligar as instrumentacoes automaticas (sqlite3, requests, FastAPI).
    * Entregar `force_flush`/`shutdown` explicitos para quem encerra graciosamente.

Decisoes de arquitetura:
    * Idempotencia obrigatoria. `trace.set_tracer_provider()` e one-shot: a
      segunda chamada loga "Overriding of current TracerProvider is not allowed"
      e mantem o provider antigo - mas o provider novo ja teria sido construido,
      vazando uma thread de export e uma conexao gRPC. Por isso o estado fica em
      variaveis de modulo protegidas por `threading.Lock`, impedindo a
      *construcao* e nao apenas o `set`. Isso importa porque o Streamlit
      re-executa o script a cada rerun e o `uvicorn --reload` reimporta o app.
    * Kill switch. `TELEMETRY_TRACING_ENABLED=false` (ou `OTEL_SDK_DISABLED=true`)
      torna todo o setup um no-op: nenhum exporter e criado e nada vai pela rede.
      E o que a suite de testes usa (ver `tests/conftest.py`).
    * Tracing nunca derruba a aplicacao. Todo o setup e as instrumentacoes
      ficam dentro de `try/except Exception`: uma falha vira log e o pipeline
      segue funcionando sem traces.
    * `get_tracer()` sempre devolve um tracer. Sem provider global o SDK entrega
      um no-op, entao os chamadores nunca precisam de `if tracing_enabled()`.
    * Shutdown e definitivo no processo. Pela mesma limitacao one-shot do
      `trace.set_tracer_provider()`, depois de `shutdown_tracing()` nao existe
      como voltar a exportar: o getter global continuaria devolvendo o provider
      encerrado, entao um provider novo nasceria vivo e ignorado - thread de
      export e canal gRPC vazados, com `is_configured()`/`force_flush()`
      respondendo sobre um provider que descarta tudo. Por isso o shutdown liga
      `_shutdown_done` e `configure_tracing()` passa a *recusar* a
      reconfiguracao com log de erro, em vez de fingir que reconfigurou.
      `_provider` vai para `None` no shutdown justamente para que
      `is_configured()` e `force_flush()` respondam `False` - o estado real.
    * O set `_instrumented` **nao** e limpo no shutdown. Encerrar o provider nao
      desinstrumenta `sqlite3`/`requests`/FastAPI: os wrappers seguem globais e
      ativos no processo. Esquecer isso faria uma chamada posterior a
      `instrument_*()` passar pela guarda do modulo e bater na do proprio
      instrumentor ("Attempting to instrument while already instrumented"),
      exatamente o que estas guardas existem para evitar.
"""

from __future__ import annotations

import logging
import os
import threading

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from app import __version__ as APP_VERSION

# ---------------------------------------------------------------------------
# Configuracao de log estruturado do modulo
# ---------------------------------------------------------------------------
# Segue o padrao dos demais processos ("api", "producer").
logger = logging.getLogger("tracing")

# ---------------------------------------------------------------------------
# Endpoint e timeout do exporter OTLP
# ---------------------------------------------------------------------------
# Collector OTLP/gRPC do Jaeger all-in-one (a UI fica na 16686).
DEFAULT_OTLP_ENDPOINT: str = "http://127.0.0.1:4317"

# Timeout curto de proposito: com o Jaeger fora do ar, o `force_flush` do
# shutdown nao pode prender o encerramento do producer.
DEFAULT_EXPORT_TIMEOUT_SECONDS: float = 5.0

ENV_ENDPOINT: str = "OTEL_EXPORTER_OTLP_ENDPOINT"
ENV_TIMEOUT: str = "OTEL_EXPORTER_OTLP_TIMEOUT"
ENV_TRACING_ENABLED: str = "TELEMETRY_TRACING_ENABLED"
ENV_SDK_DISABLED: str = "OTEL_SDK_DISABLED"

# Valores aceitos como "desligado" no kill switch.
_DISABLED_VALUES: frozenset[str] = frozenset({"0", "false", "no", "off"})

# ---------------------------------------------------------------------------
# Atributos de span padronizados
# ---------------------------------------------------------------------------
# Chaves estaveis em ingles, no estilo das semantic conventions, com prefixo
# proprio para nao colidir com atributos reservados do OpenTelemetry.
ATTR_SERVICE_NAME: str = "telemetry.service_name"
ATTR_LATENCY_MS: str = "telemetry.latency_ms"
ATTR_STATUS_CODE: str = "telemetry.status_code"
ATTR_ERROR_FLAG: str = "telemetry.error_flag"
ATTR_EVENT_ID: str = "telemetry.event_id"
ATTR_DASHBOARD_SOURCE: str = "dashboard.source"
ATTR_DASHBOARD_WINDOW: str = "dashboard.window_minutes"
ATTR_DASHBOARD_LIMIT: str = "dashboard.record_limit"
ATTR_DASHBOARD_FALLBACK: str = "dashboard.fallback"
ATTR_DASHBOARD_OPERATION: str = "dashboard.operation"
ATTR_DASHBOARD_FALLBACK_REASON: str = "dashboard.fallback_reason"
# Nome de evento (span event), nao de atributo.
EVENT_DASHBOARD_FALLBACK: str = "dashboard.fallback_sqlite"

# ---------------------------------------------------------------------------
# Estado de modulo (idempotencia)
# ---------------------------------------------------------------------------
# O modulo vive em `sys.modules`, portanto este estado sobrevive ao rerun do
# script pelo Streamlit. O lock cobre importes concorrentes (workers/threads).
_lock = threading.Lock()
_provider: TracerProvider | None = None
_service_name: str | None = None
_instrumented: set[str] = set()
_setup_failed: bool = False
# Ligado por `shutdown_tracing()`: marca que este processo ja encerrou o
# tracing e nao pode mais reconfigurar (ver "Decisoes de arquitetura").
_shutdown_done: bool = False
# Garante que o aviso de "spans serao descartados" saia uma unica vez.
_shutdown_warned: bool = False


# ---------------------------------------------------------------------------
# Kill switch
# ---------------------------------------------------------------------------
def tracing_enabled() -> bool:
    """Informa se o tracing deve ser configurado neste processo.

    A variavel e lida a cada chamada (sem cache) para que a suite de testes
    possa desligar o tracing via ambiente antes de importar a aplicacao.

    Returns:
        `False` se `TELEMETRY_TRACING_ENABLED` estiver em `_DISABLED_VALUES` ou
        se `OTEL_SDK_DISABLED` for `true`. Habilitado por padrao.
    """
    if os.getenv(ENV_TRACING_ENABLED, "").strip().lower() in _DISABLED_VALUES:
        return False
    if os.getenv(ENV_SDK_DISABLED, "").strip().lower() == "true":
        return False
    return True


def otlp_endpoint() -> str:
    """Retorna o endpoint OTLP configurado (ou o padrao local do Jaeger).

    Publico porque os processos registram em log para onde estao exportando.
    """
    return os.getenv(ENV_ENDPOINT, "").strip() or DEFAULT_OTLP_ENDPOINT


def _export_timeout() -> float:
    """Retorna o timeout do exporter em segundos, tolerando valor invalido."""
    bruto = os.getenv(ENV_TIMEOUT, "").strip()
    if not bruto:
        return DEFAULT_EXPORT_TIMEOUT_SECONDS
    try:
        valor = float(bruto)
    except ValueError:
        logger.warning(
            "%s='%s' nao e numerico; usando %.1fs.",
            ENV_TIMEOUT,
            bruto,
            DEFAULT_EXPORT_TIMEOUT_SECONDS,
        )
        return DEFAULT_EXPORT_TIMEOUT_SECONDS
    return valor if valor > 0 else DEFAULT_EXPORT_TIMEOUT_SECONDS


# ---------------------------------------------------------------------------
# Setup do provider
# ---------------------------------------------------------------------------
def configure_tracing(service_name: str) -> TracerProvider | None:
    """Configura o `TracerProvider` global do processo.

    Idempotente: a segunda chamada devolve o mesmo provider sem reconstruir
    exporter nem processador de spans.

    Depois de `shutdown_tracing()` a reconfiguracao e **recusada**: o provider
    global do processo nao pode ser substituido, logo um provider novo so
    vazaria recursos sem exportar nada (ver "Decisoes de arquitetura").

    Args:
        service_name: valor de `service.name` no Jaeger (ex.: 'telemetry-api').

    Returns:
        O `TracerProvider` configurado, ou `None` se o tracing estiver
        desligado pelo kill switch, se o setup tiver falhado ou se o tracing
        ja tiver sido encerrado neste processo.
    """
    global _provider, _service_name, _setup_failed

    if not tracing_enabled():
        logger.debug("Tracing desabilitado por ambiente; setup ignorado.")
        return None

    with _lock:
        if _shutdown_done:
            # Recusa antes de construir qualquer coisa: nenhum Resource,
            # nenhum TracerProvider, nenhuma thread de export, nenhum canal
            # gRPC. `logger.error` porque isso indica um bug de ciclo de vida
            # no processo chamador, nao uma condicao esperada.
            logger.error(
                "Tracing ja foi encerrado neste processo; reconfiguracao recusada "
                "(trace.set_tracer_provider e one-shot: os spans iriam para o provider "
                "encerrado). Reinicie o processo para voltar a exportar. (service=%s)",
                service_name,
            )
            return None

        if _provider is not None:
            # Ja configurado neste processo (rerun do Streamlit, reimport do
            # uvicorn --reload): devolve o mesmo provider, sem efeito colateral.
            if _service_name != service_name:
                logger.debug(
                    "Tracing ja configurado como '%s'; mantendo (pedido: '%s').",
                    _service_name,
                    service_name,
                )
            return _provider

        if _setup_failed:
            # Nao insiste a cada rerun quando o setup ja falhou uma vez.
            return None

        endpoint = otlp_endpoint()
        try:
            resource = Resource.create(
                {"service.name": service_name, "service.version": APP_VERSION}
            )
            provider = TracerProvider(resource=resource)
            exporter = OTLPSpanExporter(
                endpoint=endpoint,
                insecure=True,
                timeout=_export_timeout(),
            )
            provider.add_span_processor(BatchSpanProcessor(exporter))
            trace.set_tracer_provider(provider)
        except Exception as exc:  # noqa: BLE001 - tracing nunca derruba a app
            _setup_failed = True
            logger.error(
                "Falha ao configurar o tracing (service=%s, endpoint=%s): %s",
                service_name,
                endpoint,
                exc,
            )
            return None

        _provider = provider
        _service_name = service_name
        logger.info(
            "Tracing ativo | service.name=%s | versao=%s | OTLP=%s",
            service_name,
            APP_VERSION,
            endpoint,
        )
        return _provider


def is_configured() -> bool:
    """Informa se este processo possui um `TracerProvider` proprio e ativo.

    Responde `False` depois de `shutdown_tracing()`, porque nesse ponto o
    provider do processo esta encerrado e nao exporta mais nada.
    """
    return _provider is not None


def get_tracer(name: str) -> trace.Tracer:
    """Devolve um tracer para o modulo informado.

    Sempre retorna um objeto utilizavel: sem provider global o SDK entrega um
    tracer no-op, cujos spans nao gravam e nao vao pela rede. Depois de
    `shutdown_tracing()` o getter global ainda devolve o provider encerrado,
    que *descarta* os spans - por isso o aviso unico abaixo, para que esse
    descarte nunca seja silencioso.

    Args:
        name: nome do instrumentador (ex.: 'app.producer').

    Returns:
        Tracer do OpenTelemetry.
    """
    global _shutdown_warned

    # Sem lock de proposito: este e o caminho quente e o pior caso de uma
    # corrida e a mesma mensagem sair duas vezes.
    if _shutdown_done and not _shutdown_warned:
        _shutdown_warned = True
        logger.warning(
            "Tracer '%s' solicitado apos o encerramento do tracing neste processo: "
            "os spans criados daqui em diante serao descartados pelo provider encerrado.",
            name,
        )
    return trace.get_tracer(name, APP_VERSION)


# ---------------------------------------------------------------------------
# Instrumentacoes automaticas
# ---------------------------------------------------------------------------
def _recusado_por_shutdown(acao: str) -> bool:
    """Informa se uma instrumentacao deve ser recusada por shutdown previo.

    Chamado com `_lock` em maos. Instrumentar depois do shutdown nao tem
    efeito util: os wrappers passariam a alimentar o provider encerrado.

    Args:
        acao: alvo da instrumentacao, usado no log (ex.: 'sqlite3').

    Returns:
        `True` quando o tracing deste processo ja foi encerrado.
    """
    if not _shutdown_done:
        return False
    logger.warning(
        "Instrumentacao de %s ignorada: o tracing ja foi encerrado neste processo.",
        acao,
    )
    return True



def instrument_sqlite3() -> None:
    """Instrumenta o driver `sqlite3` (spans de INSERT/SELECT).

    No-op quando o tracing esta desligado ou quando ja foi instrumentado neste
    processo. A instrumentacao e global ao modulo `sqlite3`, por isso a guarda.
    """
    if not tracing_enabled():
        return

    with _lock:
        if "sqlite3" in _instrumented or _recusado_por_shutdown("sqlite3"):
            return
        try:
            from opentelemetry.instrumentation.sqlite3 import SQLite3Instrumentor

            SQLite3Instrumentor().instrument(tracer_provider=_provider)
        except Exception as exc:  # noqa: BLE001 - tracing nunca derruba a app
            logger.warning("Nao foi possivel instrumentar o sqlite3: %s", exc)
            return
        _instrumented.add("sqlite3")
        logger.debug("Instrumentacao do sqlite3 habilitada.")


def instrument_requests() -> None:
    """Instrumenta a biblioteca `requests`.

    Alem de criar o span do cliente HTTP, injeta o cabecalho `traceparent`
    (W3C Trace Context) na requisicao: e isso que costura dashboard -> API -> SQLite
    em um unico trace.
    """
    if not tracing_enabled():
        return

    with _lock:
        if "requests" in _instrumented or _recusado_por_shutdown("requests"):
            return
        try:
            from opentelemetry.instrumentation.requests import RequestsInstrumentor

            RequestsInstrumentor().instrument(tracer_provider=_provider)
        except Exception as exc:  # noqa: BLE001 - tracing nunca derruba a app
            logger.warning("Nao foi possivel instrumentar o requests: %s", exc)
            return
        _instrumented.add("requests")
        logger.debug("Instrumentacao do requests habilitada.")


def instrument_fastapi_app(app: object) -> None:
    """Instrumenta uma aplicacao FastAPI (span por request + contexto recebido).

    Args:
        app: instancia de `fastapi.FastAPI`.

    Os spans internos de ASGI (`http send` / `http receive`) sao excluidos:
    eles apenas duplicam cada request na UI do Jaeger sem agregar informacao.
    """
    if not tracing_enabled():
        return

    with _lock:
        # `_is_instrumented_by_opentelemetry` e a flag que o proprio
        # FastAPIInstrumentor usa; checar antes evita o warning de dupla
        # instrumentacao quando o uvicorn --reload reimporta o modulo.
        if (
            "fastapi" in _instrumented
            or getattr(app, "_is_instrumented_by_opentelemetry", False)
            or _recusado_por_shutdown("FastAPI")
        ):
            return
        try:
            from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

            FastAPIInstrumentor.instrument_app(
                app,
                tracer_provider=_provider,
                exclude_spans=["send", "receive"],
            )
        except Exception as exc:  # noqa: BLE001 - tracing nunca derruba a app
            logger.warning("Nao foi possivel instrumentar a aplicacao FastAPI: %s", exc)
            return
        _instrumented.add("fastapi")
        logger.debug("Instrumentacao do FastAPI habilitada.")


# ---------------------------------------------------------------------------
# Flush / shutdown explicitos
# ---------------------------------------------------------------------------
def force_flush(timeout_millis: int = 3000) -> bool:
    """Forca o envio dos spans ainda em buffer.

    Args:
        timeout_millis: tempo maximo de espera, em milissegundos.

    Returns:
        `True` se o buffer foi esvaziado no prazo; `False` se nao ha provider
        configurado (inclusive depois de `shutdown_tracing()`) ou se o flush
        falhou/expirou.
    """
    if _provider is None:
        return False
    try:
        return bool(_provider.force_flush(timeout_millis))
    except Exception as exc:  # noqa: BLE001 - tracing nunca derruba a app
        logger.warning("Falha no flush de spans: %s", exc)
        return False


def shutdown_tracing(timeout_millis: int = 3000) -> None:
    """Esvazia o buffer e encerra o provider de tracing.

    Pensado para o shutdown gracioso do producer. Nunca levanta excecao.

    **Definitivo no processo**: depois desta chamada `is_configured()` e
    `force_flush()` respondem `False` e `configure_tracing()` recusa reconfigurar
    (ver "Decisoes de arquitetura"). Para voltar a exportar, reinicie o processo.

    Args:
        timeout_millis: tempo maximo do flush final, em milissegundos.
    """
    global _provider, _service_name, _shutdown_done

    with _lock:
        provider = _provider
        if provider is None:
            return
        try:
            provider.force_flush(timeout_millis)
            provider.shutdown()
        except Exception as exc:  # noqa: BLE001 - tracing nunca derruba a app
            logger.warning("Falha ao encerrar o tracing: %s", exc)
        finally:
            # `_provider = None` e o que faz `is_configured()`/`force_flush()`
            # dizerem a verdade: nao ha mais provider ativo aqui.
            _provider = None
            _service_name = None
            # Marca o processo como encerrado para o tracing: `configure_tracing()`
            # passa a recusar, em vez de criar um provider vivo e ignorado.
            _shutdown_done = True
            # `_instrumented` nao e limpo: as instrumentacoes automaticas seguem
            # globalmente ativas apos o shutdown, e limpar o set so faria uma
            # chamada posterior cair na guarda do instrumentor. `_setup_failed`
            # tambem fica como esta: aqui ele e sempre False (o retorno
            # antecipado acima cobre o caso sem provider), entao "zerar" nao
            # descreveria nada real.
            logger.debug("Tracing encerrado (definitivo neste processo).")
