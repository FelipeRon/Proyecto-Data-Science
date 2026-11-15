# Revisión contra el enunciado del PSet 2

Revisión: 15 de noviembre de 2026. **Cumplimiento parcial: implementación y validación local disponibles; entrega cloud todavía incompleta.** No se asigna una nota estimada: los pesos no sustituyen evidencia de funcionamiento.

| Requisito del enunciado | Archivo / evidencia | Veredicto |
|---|---|---|
| Fuente → Kestra → Raw → Bronze → Silver → Gold → Spark → OBT | `kestra/flows/pipeline.yml`, scripts, modelos, job | Implementado; falta corrida completa contra Snowflake |
| Compose con Kestra y Spark, dbt en Docker | `docker-compose.yml`, `Dockerfile`, `requirements.lock` | Cumple; build y lanzamiento Docker comprobados |
| Diagrama de arquitectura | `docs/architecture.png` y `.dot`, memo §1 | Cumple |
| Ingesta automática hacia Snowflake Raw | `kestra/scripts/ingest.py`, flow principal | Pendiente validar con usuario/llave de Snowflake |
| Trigger y frecuencia | Schedule semanal, cron y zona horaria; memo §2 | Configurado; deshabilitado hasta validar credenciales y primera carga |
| Manejo de errores y retries | Timeouts, retry HTTP y Kestra; `kestra_local_results.json` | Retry local comprobado (2 intentos, SUCCESS); retry de ingesta cloud pendiente |
| Backfill histórico | Snapshot completo y parámetro `historical_snapshot`, README | Implementado como replay apropiado para datos sin fechas; falta demostrar idempotencia cloud |
| Completitud, precisión, consistencia y validez | `quality_results.json`, tabla del memo §3 | Cumple para copia pública perfilada; equivalencia con UCI pendiente |
| Evidencia/acción/justificación por problema | Memo §3 y modelos Silver/cuarentena | Cumple; 27 discrepancias conservadas y señaladas, no corregidas arbitrariamente |
| Limpieza dbt y capas Bronze/Silver/Gold | `dbt/models/` | Cumple localmente; rama Snowflake pendiente |
| Star schema y grano | `fct_reading`, `dim_product_type`, `docs/star.png`, memo §4 | Cumple: estrella mínima de una dimensión, sin inventar entidades ausentes |
| `source()`, `ref()`, `not_null`, `unique`, `relationships` y reglas de negocio | Modelos, `schema.yml`, tests SQL | Cumple localmente: 6 modelos y 14 tests aprobados |
| Spark construye OBT desde Gold | `spark/build_obt.py` | Cumple localmente: 10.000 filas e IDs únicos; escritura Snowflake pendiente |
| Validación de joins y conservación de observaciones | Comprobaciones antes/después de join y persistencia; tests negativos Spark | Cumple localmente; comprobar también contra Gold/OBT cloud |
| OBT almacenada nuevamente en Snowflake | Rama Snowflake del job, `docs/snowflake_setup.sql` | **Pendiente de ejecución** |
| Cuándo usar estrella y OBT | Memo §4 | Cumple |
| Batch justificado; condiciones para streaming sin implementarlo | Memo §6 | Cumple |
| Memo técnico ≤6 páginas, 7 temas requeridos | `docs/PSet2_memo_Ron_Andrade.pdf` | Cumple extensión: 6 páginas; incluye arquitectura, ingesta, calidad, transformaciones, OBT, batch y limitaciones |
| Nombre `PSet2_memo_<apellido_1>_<apellido_2>.pdf` | `PSet2_memo_Ron_Andrade.pdf` | Cumple: apellidos Ron Andrade |
| Estructura, README reproducible y plantilla de entorno | Raíz, `kestra/`, `dbt/`, `spark/`, `docs/`, `.env.example` | Cumple estructura; reproducción cloud requiere cuenta/llave y acceso de red |
| No publicar credenciales | `.gitignore`, `.dockerignore`; solo plantilla versionada | `.env`, secretos, datos y entorno virtual excluidos del commit |

## Pendientes que impiden declarar cumplimiento total

1. Configurar usuario, llave pública registrada/privada local segura, rol y warehouse en la cuenta indicada. El identificador de cuenta no autentica por sí solo.
2. Permitir UCI y el hostname exacto Snowflake en la red; preservar verificación TLS en clientes Python/Java.
3. Ejecutar el flow principal de Kestra y obtener Raw, Bronze, Silver, Gold y OBT reales en Snowflake.
4. Repetir mismo snapshot/replay sin duplicados y documentar un retry de ingesta cloud.
5. Conciliar conteos, claves y etiquetas por capa, conservar evidencia sin secretos y habilitar el schedule después de validar.

Ver `validation.md` para separar resultados ejecutados de pendientes. La prueba local de Kestra utiliza Raw DuckDB previamente preparado y no demuestra carga Raw en Snowflake. Subir el repositorio a GitHub tampoco constituye esa validación.
