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
└──────────────────┘                      └─────────┬──────────┘
                                                    │ SQL agregado
                                                    ▼
                                          ┌────────────────────┐
                                          │    app/main.py     │
                                          │  FastAPI + Swagger │
                                          │  /metrics/*        │
                                          │  /health           │
                                          └─────────┬──────────┘
                                                    │ HTTP JSON
                                                    ▼
                                          ┌────────────────────┐
                                          │  app/dashboard.py  │
                                          │     Streamlit      │
                                          │ KPIs · gráfico ·   │
                                          │ tabela de alertas  │
                                          └────────────────────┘
```

### Camadas e responsabilidades

| Camada | Arquivo | Responsabilidade |
|---|---|---|
| Ingestão | `app/producer.py` | Gera eventos sintéticos realistas (perfil log-normal para tráfego saudável, cauda longa para degradado) e grava continuamente. |
| Persistência | `app/database.py` | Única porta de acesso ao SQLite: schema, índices, validação de entrada, consultas agregadas e health check. |
| Processamento / API | `app/main.py` | Expõe as agregações via REST com contratos Pydantic e documentação OpenAPI automática. |
| Visualização | `app/dashboard.py` | Painel em tempo real com auto-refresh, consumindo a API (com fallback para leitura direta do banco). |
| Qualidade | `tests/` | Testes unitários da persistência e testes de integração dos endpoints. |

### Decisões técnicas relevantes

- **Uma conexão por operação.** O SQLite não compartilha conexões entre threads com segurança e três processos distintos (producer, API, dashboard) acessam o mesmo arquivo. O context manager `get_connection()` garante `commit`/`rollback`/`close`.
- **WAL (Write-Ahead Logging).** Permite leituras concorrentes enquanto o producer escreve, evitando `database is locked`.
- **Timestamps em UTC** no formato `YYYY-MM-DD HH:MM:SS.mmm`. A ordenação lexicográfica coincide com a cronológica e as comparações usam funções nativas (`datetime('now', '-N minutes')`).
- **Erros encapsulados.** Qualquer `sqlite3.Error` é convertido em `DatabaseError`; as camadas superiores nunca lidam com o driver. Validações de negócio levantam `ValueError`, traduzido para HTTP 422 pela API.
- **Banco configurável** por `TELEMETRY_DB_PATH` ou por `database.configure_database()`, o que permite testes isolados em arquivos temporários.

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
- Nenhum serviço externo: o banco é o arquivo local `telemetry.db`, criado automaticamente.

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

## 5. Testes

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

---

## 6. Variáveis de ambiente

| Variável | Padrão | Usada por | Descrição |
|---|---|---|---|
| `TELEMETRY_DB_PATH` | `./telemetry.db` | producer, API, dashboard | Caminho do arquivo SQLite. |
| `TELEMETRY_API_URL` | `http://127.0.0.1:8000` | dashboard | URL base da API de observabilidade. |

---

## 7. Estrutura de arquivos

```
telemetry-observability-pipeline/
├── app/
│   ├── __init__.py
│   ├── database.py       # Conexão SQLite, schema e consultas agregadas
│   ├── producer.py       # Gerador contínuo de eventos (2s), shutdown gracioso
│   ├── main.py           # API FastAPI: /metrics/summary, /metrics/recent, /health
│   └── dashboard.py      # Painel Streamlit com KPIs, gráfico e alertas
├── tests/
│   ├── __init__.py
│   ├── test_database.py  # Testes unitários de inserção e consulta
│   └── test_api.py       # Testes dos endpoints da API
├── .gitignore
├── requirements.txt
└── README.md
```

---

## 8. Troubleshooting

| Sintoma | Causa provável | Solução |
|---|---|---|
| Dashboard/API sem dados | Producer não está rodando | Execute `python -m app.producer` em outro terminal. |
| `/health` retorna `503` | Banco inacessível ou sem schema | Rode `python -m app.database` e confira permissões de escrita no diretório. |
| Aviso "API indisponível" no painel | Uvicorn parado ou porta diferente | Suba a API ou ajuste `TELEMETRY_API_URL`. |
| `ModuleNotFoundError: app` | Execução fora da raiz do projeto | Rode os comandos a partir de `telemetry-observability-pipeline/`. |
| `database is locked` | Escrita concorrente intensa | O WAL já está habilitado; aumente `--interval` no producer. |
| Dashboard consumindo CPU | Refresh muito agressivo | Aumente o intervalo de atualização na sidebar ou desligue o auto-refresh. |
