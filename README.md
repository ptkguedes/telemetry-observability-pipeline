# 📡 Pipeline de Observabilidade e Telemetria em Tempo Real

Pipeline completo de **coleta, persistência, processamento e visualização** de logs de telemetria operacional de um ambiente de missão crítica (autenticação, pagamentos, controle de voo, ingestão de telemetria e notificações).

O projeto simula o ciclo de vida real de um dado de observabilidade: um **producer** gera eventos, o **SQLite** persiste, a **API FastAPI** processa e agrega, e o **dashboard Streamlit** apresenta KPIs, tendência de latência e alertas.

---

## 1. Arquitetura

```
┌──────────────────┐        insert        ┌────────────────────┐
│  app/producer.py │ ───────────────────▶ │   telemetry.db     │
│  (simulador)     │   ~1 evento / 2s     │   SQLite + WAL     │
│  80% OK / 20% KO │                      │  telemetry_logs    │
└────────┬─────────┘                      └─────────┬──────────┘
         ╎                                          │ SQL agregado
         ╎                                          ▼
         ╎                                ┌────────────────────┐
         ╎                                │    app/main.py     │
         ╎                                │  FastAPI + Swagger │
         ╎                                │  /metrics/*        │
         ╎                                │  /health           │
         ╎                                └────┬───────────┬───┘
         ╎                                     │ HTTP JSON ╎
         ╎                                     │ traceparent
         ╎                                     ▼           ╎
         ╎                           ┌────────────────────┐ ╎
         ╎                           │  app/dashboard.py  │ ╎
         ╎                           │     Streamlit      │ ╎
         ╎                           │ KPIs · gráfico ·   │ ╎
         ╎                           │ tabela de alertas  │ ╎
         ╎                           └─────────┬──────────┘ ╎
         ╎  spans (OTLP/gRPC :4317)            ╎            ╎
         └──────────────────┐   ┌──────────────┘   ┌─────────┘
                            ▼   ▼                  ▼
                  ┌────────────────────────────────────────┐
                  │          Jaeger all-in-one             │
                  │   OTLP :4317 (gRPC) · :4318 (HTTP)     │
                  │   UI de traces em :16686               │
                  │   telemetry-producer · telemetry-api   │
                  │   telemetry-dashboard                  │
                  └────────────────────────────────────────┘
```

As linhas cheias (`───▶`) são o **fluxo de dados**; as pontilhadas (`╎`) são a **exportação de spans**. Cada um dos três processos exporta seus próprios spans para o Jaeger via OTLP, e o cabeçalho `traceparent` propagado pelo `requests` costura dashboard → API → SQLite em um **único trace**.

### Camadas e responsabilidades

| Camada | Arquivo | Responsabilidade |
|---|---|---|
| Ingestão | `app/producer.py` | Gera eventos sintéticos realistas (perfil log-normal para tráfego saudável, cauda longa para degradado) e grava continuamente. |
| Persistência | `app/database.py` | Única porta de acesso ao SQLite: schema, índices, validação de entrada, consultas agregadas e health check. |
| Processamento / API | `app/main.py` | Expõe as agregações via REST com contratos Pydantic e documentação OpenAPI automática. |
| Visualização | `app/dashboard.py` | Painel em tempo real com auto-refresh, consumindo a API (com fallback para leitura direta do banco). |
| Tracing distribuído | `app/tracing.py` | Setup único do OpenTelemetry: `TracerProvider`, exportação OTLP para o Jaeger, instrumentações automáticas (FastAPI, `sqlite3`, `requests`) e `flush`/`shutdown` explícitos. |
| Qualidade | `tests/` | Testes unitários da persistência e testes de integração dos endpoints. |

### Decisões técnicas relevantes

- **Uma conexão por operação.** O SQLite não compartilha conexões entre threads com segurança e três processos distintos (producer, API, dashboard) acessam o mesmo arquivo. O context manager `get_connection()` garante `commit`/`rollback`/`close`.
- **WAL (Write-Ahead Logging).** Permite leituras concorrentes enquanto o producer escreve, evitando `database is locked`.
- **Timestamps em UTC** no formato `YYYY-MM-DD HH:MM:SS.mmm`. A ordenação lexicográfica coincide com a cronológica e as comparações usam funções nativas (`datetime('now', '-N minutes')`).
- **Erros encapsulados.** Qualquer `sqlite3.Error` é convertido em `DatabaseError`; as camadas superiores nunca lidam com o driver. Validações de negócio levantam `ValueError`, traduzido para HTTP 422 pela API.
- **Banco configurável** por `TELEMETRY_DB_PATH` ou por `database.configure_database()`, o que permite testes isolados em arquivos temporários.
- **Tracing opcional, idempotente e não intrusivo.** O setup fica todo em `app/tracing.py` e obedece a três regras: (1) um **kill switch** (`TELEMETRY_TRACING_ENABLED=false`) transforma a configuração em no-op — é assim que a suíte de testes roda sem abrir conexão de rede; (2) **idempotência** por estado de módulo protegido com `threading.Lock`, porque `trace.set_tracer_provider()` é one-shot e o Streamlit re-executa o script a cada rerun enquanto o `uvicorn --reload` reimporta a aplicação; (3) **falha de exportação nunca derruba a aplicação** — com o Jaeger fora do ar, o único efeito visível é uma linha de log `Failed to export traces`, e API, producer e painel continuam respondendo normalmente.
- **Cursores explícitos no SQLite.** A instrumentação DBAPI do OpenTelemetry envolve `connection.cursor()`, não o atalho `connection.execute()`. As consultas de dados usam cursor explícito justamente para que o SQL apareça como span; o `check_health()` mantém o atalho de propósito, para não poluir o trace com o `/health` chamado a cada rerun do painel.

### Modelo de dados — tabela `telemetry_logs`

| Coluna | Tipo | Descrição |
|---|---|---|
| `id` | `INTEGER PRIMARY KEY AUTOINCREMENT` | Identificador sequencial do evento. |
| `timestamp` | `DATETIME DEFAULT CURRENT_TIMESTAMP` | Data/hora do evento em UTC. |
| `service_name` | `TEXT` | Serviço observado (`auth-service`, `payment-gateway`, `flight-control`, ...). |
| `latency_ms` | `FLOAT` | Latência da requisição em milissegundos. |
| `status_code` | `INTEGER` | Status HTTP (`200`, `201`, `204`, `404`, `500`, `503`). |
| `error_flag` | `INTEGER` | `0` = sucesso, `1` = falha (`CHECK` no schema). |

Índices: `timestamp DESC` (janelas temporais), `service_name` (agrupamento) e um índice parcial sobre falhas (`error_flag = 1`) para a tabela de alertas.

---

## 2. Requisitos

- Python **3.11+**
- O banco é o arquivo local `telemetry.db`, criado automaticamente — nenhum serviço externo é necessário para o pipeline de dados.
- **Docker opcional**, usado apenas para subir o Jaeger e visualizar os traces (seção 5). Sem Docker o pipeline roda igual: basta desligar o tracing com `TELEMETRY_TRACING_ENABLED=false` (ou simplesmente conviver com a linha de log de falha de exportação).

---

## 3. Instalação

```powershell
# 1. Entre no diretório do projeto
cd telemetry-observability-pipeline

# 2. Crie e ative um ambiente virtual
python -m venv .venv
.\.venv\Scripts\Activate.ps1        # Windows PowerShell
# source .venv/bin/activate         # Linux / macOS

# 3. Instale as dependências
pip install -r requirements.txt
```

> **Sobre as versões:** `requirements.txt` fixa (pin) todas as dependências para builds reprodutíveis; o conjunto foi validado em Python 3.14 e é compatível com 3.11+. A biblioteca de gráficos escolhida é o **Plotly** (interativo, integração nativa com o Streamlit); `matplotlib` não é necessário. O painel usa `width="stretch"`, portanto exige **Streamlit >= 1.48**.

---

## 4. Como rodar

Os três processos são independentes. Abra **um terminal para cada um**, com o ambiente virtual ativado, a partir da raiz do projeto.

### 4.1. Inicializar o banco (opcional)

O schema é criado automaticamente pelo producer e pela API. Para criá-lo manualmente:

```powershell
python -m app.database
```

### 4.2. Terminal 1 — Producer (coleta)

Gera um evento a cada 2 segundos e grava no SQLite.

```powershell
python -m app.producer
```

Opções disponíveis:

| Flag | Padrão | Descrição |
|---|---|---|
| `--interval` | `2.0` | Segundos entre os ciclos de geração. |
| `--batch` | `1` | Eventos gerados por ciclo. |
| `--max-events` | `0` | Limite total de eventos (`0` = indefinido). |
| `--db` | `./telemetry.db` | Caminho alternativo do arquivo SQLite. |
| `--seed` | — | Semente aleatória para simulação reproduzível. |

```powershell
# Exemplo: popular rapidamente 300 eventos para ver o dashboard com dados
python -m app.producer --interval 0.05 --batch 5 --max-events 300
```

Encerre com **Ctrl+C**: o shutdown é gracioso e imprime o resumo da sessão (total, falhas, latência média e duração).

### 4.3. Terminal 2 — API (processamento)

```powershell
uvicorn app.main:app --reload
```

- Swagger UI: <http://127.0.0.1:8000/docs>
- ReDoc: <http://127.0.0.1:8000/redoc>
- OpenAPI JSON: <http://127.0.0.1:8000/openapi.json>

| Método | Endpoint | Descrição |
|---|---|---|
| `GET` | `/health` | Status da API e do banco. `200` operacional, `503` degradado. |
| `GET` | `/metrics/summary?minutes=5` | Latência média, latência máxima, total de requisições, contagem e taxa de erro (%) na janela. |
| `GET` | `/metrics/recent?limit=50` | Últimos registros de telemetria (padrão 50, máximo 1000). |
| `GET` | `/metrics/alerts?limit=20` | Apenas eventos com `error_flag = 1`. |

Exemplos:

```powershell
curl "http://127.0.0.1:8000/health"
curl "http://127.0.0.1:8000/metrics/summary?minutes=15"
curl "http://127.0.0.1:8000/metrics/recent?limit=10"
```

Resposta de `/metrics/summary`:

```json
{
  "window_minutes": 15,
  "total_requests": 428,
  "avg_latency_ms": 243.87,
  "max_latency_ms": 2871.44,
  "error_count": 89,
  "error_rate_percent": 20.79,
  "generated_at": "2026-09-28 12:34:56.789"
}
```

> **Nota de segurança:** a API não possui autenticação nem autorização — é um serviço de observabilidade local, pensado para rodar em `127.0.0.1`. Antes de expor em rede, adicione autenticação (API key, OAuth2/JWT), rate limiting e TLS.

### 4.4. Terminal 3 — Dashboard (visualização)

```powershell
streamlit run app/dashboard.py
```

Painel em <http://localhost:8501> com:

- **KPIs no topo:** Total de Requisições, Latência Média (ms), Taxa de Erro (%) e Latência Máxima (ms).
- **Gráfico de linha** da latência ao longo do tempo, com média móvel e marcação em vermelho (`x`) dos eventos com falha.
- **Tabela de alertas** com as linhas de `error_flag == 1` destacadas em vermelho, e filtro opcional "apenas falhas".
- **Sidebar:** fonte de dados (API ou banco), janela de análise, volume de registros, atualização automática e intervalo de refresh.

Se a API estiver fora do ar, o dashboard avisa e passa a ler o SQLite diretamente. Para apontar para outro host da API:

```powershell
$env:TELEMETRY_API_URL = "http://127.0.0.1:9000"
streamlit run app/dashboard.py
```

---

## 5. Tracing distribuído (OpenTelemetry + Jaeger)

Os três processos são instrumentados com **OpenTelemetry** e exportam spans por **OTLP/gRPC** para um **Jaeger** local. É o que permite responder "por que essa requisição do painel demorou?" olhando um trace único que atravessa dashboard → API → SQLite.

### 5.1. Subir o Jaeger

Um único container resolve coletor + armazenamento + UI. Comando em **uma linha** (o PowerShell não aceita as barras invertidas de continuação usadas nos exemplos em bash):

```powershell
docker run -d --name jaeger --restart unless-stopped -e COLLECTOR_OTLP_ENABLED=true -p 16686:16686 -p 4317:4317 -p 4318:4318 jaegertracing/all-in-one:latest
```

| Porta | Protocolo | Para quê |
|---|---|---|
| `4317` | OTLP gRPC | Recebe os spans dos três processos (é o endpoint padrão deste projeto). |
| `4318` | OTLP HTTP | Alternativa HTTP/protobuf. Este projeto exporta por gRPC; usar o `4318` exigiria instalar o pacote `opentelemetry-exporter-otlp-proto-http` (não fixado no `requirements.txt`) e trocar o exporter em `app/tracing.py`. |
| `16686` | HTTP | UI de traces e query API (<http://127.0.0.1:16686>). |

> **Nota sobre `COLLECTOR_OTLP_ENABLED`:** na versão **1.76** (a `all-in-one:latest` atual) essa variável é **redundante** — o OTLP vem habilitado por padrão desde a 1.49. Ela é mantida no comando por compatibilidade com imagens antigas e porque é o que circula na maioria dos tutoriais. A imagem `jaegertracing/all-in-one` é a linha **v1**; `jaegertracing/jaeger:latest` é a linha **v2**, que tem configuração própria (baseada em OpenTelemetry Collector) e não aceita as mesmas variáveis.

> **Atenção:** o `all-in-one` usa armazenamento **em memória**. Reiniciar o container apaga os traces já coletados — gere carga nova depois de qualquer `docker restart`.

### 5.2. Rodar o pipeline com tracing

Nada muda na forma de executar: suba o Jaeger e rode os três processos como de costume (seção 4). Cada um se registra no Jaeger com um `service.name` próprio:

| Processo | `service.name` | O que você vê no Jaeger |
|---|---|---|
| `app/producer.py` | `telemetry-producer` | Span `producer.gerar_evento` por evento, com os atributos `telemetry.service_name`, `telemetry.latency_ms`, `telemetry.status_code`, `telemetry.error_flag` e `telemetry.event_id`, e o `INSERT` do SQLite como span filho. |
| `app/main.py` | `telemetry-api` | Span por requisição (`GET /metrics/summary`, `GET /health`, ...) com o `SELECT` do SQLite como span filho. |
| `app/dashboard.py` | `telemetry-dashboard` | Span `dashboard.atualizar_painel` envolvendo as duas cargas de dados, com os spans `GET` do cliente HTTP abaixo dele. |

O painel usa `RequestsInstrumentor`, que injeta o cabeçalho `traceparent` (W3C Trace Context) nas chamadas à API. A API, instrumentada com `FastAPIInstrumentor`, aceita esse contexto como pai — por isso um refresh do painel aparece como **um trace com dois serviços** e os `SELECT` do banco dentro dele.

### 5.3. Consultar via query API

Útil para validar sem abrir o navegador:

```powershell
# Serviços registrados
(Invoke-RestMethod 'http://127.0.0.1:16686/api/services').data

# Últimos traces de um serviço
(Invoke-RestMethod 'http://127.0.0.1:16686/api/traces?service=telemetry-api&limit=5').data
```

### 5.4. Desligar o tracing

O kill switch vale para qualquer um dos processos e para a suíte de testes:

```powershell
$env:TELEMETRY_TRACING_ENABLED = "false"   # kill switch do projeto
$env:OTEL_SDK_DISABLED = "true"            # kill switch padrão do OpenTelemetry
```

Com qualquer um dos dois ligados, `app/tracing.py` não constrói provider nem exporter: nenhum socket é aberto e os spans viram no-op. A suíte de testes faz exatamente isso em `tests/conftest.py`.

Para apontar para outro coletor (um OpenTelemetry Collector, por exemplo):

```powershell
$env:OTEL_EXPORTER_OTLP_ENDPOINT = "http://10.0.0.20:4317"
```

> **Nota sobre as versões das instrumentações:** os pacotes `opentelemetry-instrumentation-*` seguem um **ciclo de versão próprio** (`0.66b0`, ainda em *beta*), diferente do SDK (`1.45.0`). Os dois conjuntos precisam ser atualizados juntos: a série `0.66bN` é a que casa com o SDK `1.45.x`. Misturar gerações causa erro de importação ou instrumentação silenciosamente inativa — por isso `requirements.txt` fixa as duas famílias.

---

## 6. Testes

Suíte com Pytest: testes unitários da camada de dados e testes de integração dos endpoints. Todos rodam contra bancos SQLite **temporários**, sem tocar o `telemetry.db` de desenvolvimento.

```powershell
# Suíte completa
pytest

# Com detalhes
pytest -v

# Arquivo específico
pytest tests/test_database.py -v

# Um teste específico
pytest tests/test_api.py::test_health_retorna_ok -v
```

Cobertura funcional dos testes:

| Arquivo | O que valida |
|---|---|
| `tests/test_database.py` | Criação e idempotência do schema, contrato de colunas, inserção e leitura, derivação de `error_flag`, rejeição de dados inválidos, ordenação, limites, agregações, janela temporal, alertas, health check e encapsulamento de erros em `DatabaseError`. |
| `tests/test_api.py` | `/health` (200 e 503 degradado), `/metrics/summary` (cálculo, banco vazio, janela temporal, validação 422), `/metrics/recent` (contrato, ordenação, limite padrão de 50, validação), `/metrics/alerts` e a documentação OpenAPI/Swagger. |
| `tests/test_tracing.py` | Com o kill switch ligado, o setup de tracing é no-op: nenhum provider é criado, os spans não gravam e `flush`/`shutdown` não levantam exceção. |

A suíte **não emite spans pela rede**: `tests/conftest.py` liga o kill switch antes de qualquer `import app`, então nenhum teste depende do Jaeger estar no ar.

---

## 7. Variáveis de ambiente

| Variável | Padrão | Usada por | Descrição |
|---|---|---|---|
| `TELEMETRY_DB_PATH` | `./telemetry.db` | producer, API, dashboard | Caminho do arquivo SQLite. |
| `TELEMETRY_API_URL` | `http://127.0.0.1:8000` | dashboard | URL base da API de observabilidade. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://127.0.0.1:4317` | producer, API, dashboard | Endpoint OTLP/gRPC do coletor de traces (Jaeger). |
| `TELEMETRY_TRACING_ENABLED` | `true` | producer, API, dashboard, testes | Kill switch do projeto. Com `false`/`0`/`no`/`off` o setup de tracing é no-op. |
| `OTEL_SDK_DISABLED` | *(vazio)* | producer, API, dashboard, testes | Kill switch padrão do OpenTelemetry, respeitado como alternativa: `true` também desliga o tracing. |
| `OTEL_EXPORTER_OTLP_TIMEOUT` | `5` | producer, API, dashboard | Timeout do exporter, em segundos. Curto de propósito, para que o `flush` do shutdown não prenda o processo quando o Jaeger está fora. |

---

## 8. Estrutura de arquivos

```
telemetry-observability-pipeline/
├── app/
│   ├── __init__.py
│   ├── database.py       # Conexão SQLite, schema e consultas agregadas
│   ├── producer.py       # Gerador contínuo de eventos (2s), shutdown gracioso
│   ├── main.py           # API FastAPI: /metrics/summary, /metrics/recent, /health
│   ├── dashboard.py      # Painel Streamlit com KPIs, gráfico e alertas
│   └── tracing.py        # Setup do OpenTelemetry (OTLP -> Jaeger), kill switch
├── tests/
│   ├── __init__.py
│   ├── conftest.py       # Desliga o tracing na suíte (nenhum span pela rede)
│   ├── test_database.py  # Testes unitários de inserção e consulta
│   ├── test_api.py       # Testes dos endpoints da API
│   └── test_tracing.py   # Garante que o setup de tracing é no-op desligado
├── .gitignore
├── requirements.txt
└── README.md
```

---

## 9. Troubleshooting

| Sintoma | Causa provável | Solução |
|---|---|---|
| Dashboard/API sem dados | Producer não está rodando | Execute `python -m app.producer` em outro terminal. |
| `/health` retorna `503` | Banco inacessível ou sem schema | Rode `python -m app.database` e confira permissões de escrita no diretório. |
| Aviso "API indisponível" no painel | Uvicorn parado ou porta diferente | Suba a API ou ajuste `TELEMETRY_API_URL`. |
| `ModuleNotFoundError: app` | Execução fora da raiz do projeto | Rode os comandos a partir de `telemetry-observability-pipeline/`. |
| `database is locked` | Escrita concorrente intensa | O WAL já está habilitado; aumente `--interval` no producer. |
| Dashboard consumindo CPU | Refresh muito agressivo | Aumente o intervalo de atualização na sidebar ou desligue o auto-refresh. |
| Jaeger sem traces | Container parado, endpoint errado, kill switch ligado ou processo encerrado antes do flush | Confira `docker ps --filter name=jaeger`; valide `OTEL_EXPORTER_OTLP_ENDPOINT` (padrão `http://127.0.0.1:4317`); confira se `TELEMETRY_TRACING_ENABLED`/`OTEL_SDK_DISABLED` não estão desligando o tracing; encerre o producer com **Ctrl+C** (o shutdown gracioso faz o flush) em vez de matar o processo. Lembre-se de que o `all-in-one` guarda os traces **em memória**: após um restart é preciso gerar carga nova. |
| Container do Jaeger em `Exited (255)` | O container caiu (conflito de porta ou reinício do Docker Desktop) | `docker start jaeger`, aguarde ~10 s, confirme com `docker ps --filter name=jaeger` (precisa aparecer `Up`) e só então consulte `http://127.0.0.1:16686/api/services`. Se voltar a cair, veja `docker logs jaeger` e confira se as portas `4317`/`16686` não estão ocupadas. |
| Log `Failed to export traces ... DEADLINE_EXCEEDED` | Jaeger fora do ar | Esperado e inofensivo: a aplicação continua funcionando sem traces. Suba o Jaeger ou desligue o tracing com `TELEMETRY_TRACING_ENABLED=false`. |
