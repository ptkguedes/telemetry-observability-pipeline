"""
Pacote `app` - Pipeline de Observabilidade e Telemetria em Tempo Real.

Modulos:
    database  -> camada de persistencia (SQLite) e consultas agregadas.
    producer  -> gerador continuo de eventos sinteticos de telemetria.
    main      -> API REST (FastAPI) que expoe metricas e health check.
    dashboard -> painel Streamlit de visualizacao em tempo real.

Observacao: `dashboard` nao e exportado em `__all__` porque depende do runtime
do Streamlit e deve ser executado via `streamlit run app/dashboard.py`.
"""

__version__ = "1.0.0"
__all__ = ["database"]
