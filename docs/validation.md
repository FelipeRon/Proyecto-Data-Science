# Evidencia de validación — 15 de noviembre de 2026

## Datos y alcance

Copia pública AI4I de GitHub, commit y SHA-256 en `references.md`. 10.000 filas; 339 fallas. La equivalencia con UCI está pendiente (403 del proxy al dominio oficial). No se ejecutó una sesión autenticada contra Snowflake; ningún conteo se atribuye a esa cuenta.

## Resultado comprobado

| Verificación | Resultado |
|---|---|
| `uv pip sync ... requirements.lock` + `uv pip check` | 85 paquetes compatibles; Python 3.11.18 |
| `python scripts/validate_local.py <csv>` | 6 modelos dbt 1.9.1 + 14 tests aprobados, 0 errores, 0 omitidos; adaptador DuckDB |
| `spark-submit --master local[2] ... --local` | 10.000 filas e IDs únicos, mismo target; Parquet escrito/releído; Spark 3.5.3 |
| `pytest -q` | 4 passed; incluye rechazo de dimensión duplicada y clave huérfana |
| `docker compose config --quiet` | Configuración válida |
| Build normal | Falló por CA TLS del proxy en pip; no se usaron los paquetes de un build inseguro |
| Build con CA temporal BuildKit | Aprobado, ver `cloud-build.md`; no se desactivó TLS ni se incorporó la CA a la imagen |
| `docker run --rm ai4i-runner:1 python -m pip check` | Sin dependencias incompatibles |
| Compose Postgres/Kestra | Postgres healthy, Kestra accesible por API autenticada |
| API Kestra validate/import | Flow principal y flow de validación aceptados por Kestra v1.3.44 |
| `ai4i_local_validation` | SUCCESS con 2 intentos; primero falla deliberadamente con exit 75, segundo ejecuta perfilado, dbt y Spark local |
| Memo | PDF de 6 páginas, texto extraído y páginas revisadas visualmente |

La prueba Kestra usa Raw local previamente preparado por `validate_local.py`; el perfilado del CSV en esa prueba no carga Raw de nuevo. No demuestra extracción/carga Snowflake. La prueba en contenedor ejecutó Java 17.0.21; en host Spark también pasó con Java 21.0.13. Advertencia no bloqueante en contenedor: `ps` no instalado y biblioteca Hadoop nativa no disponible; el job completó y releyó su salida.

Resumen dbt de la corrida host: `dbt_results.json`. ID, estados y dos intentos de Kestra: `kestra_local_results.json`. El estado final fue SUCCESS, no WARNING; el fallo inicial es intencional. `local_validation.yml` permite repetir la prueba por ejecución sin reutilizar el marcador de otra ejecución.

Una invocación adicional de pytest sin activar el entorno mezcló Python 3.11 del driver y 3.12 del worker. Se corrigió el fixture para fijar `PYSPARK_PYTHON` al intérprete de la prueba; la repetición aprobó las cuatro pruebas. No fue un defecto de datos.

## Pendiente para completar el PSet cloud

- Usuario Snowflake, llave pública registrada/privada local segura, rol y warehouse autorizados en la cuenta propia.
- Autorizar red a UCI y al hostname exacto de Snowflake; configurar confianza/proxy de clientes Python y Java si corresponde.
- Ejecutar ingesta oficial, dbt-Snowflake y Spark-Snowflake; demostrar Raw/Bronze/Silver/Gold/OBT con SQL real.
- Probar idempotencia/replay del hash y retry de la ingesta cloud; el retry local solo valida el mecanismo de orquestación.
- Habilitar el trigger semanal después de esa validación. Se entrega deshabilitado para evitar ejecuciones con credenciales de ejemplo.
- Guardar capturas/logs reales sin secretos. No reemplazar estas pruebas con capturas del repositorio de referencia.

## Configuración del entorno guardada

Se guardaron `install_script`, `start_skill` y adiciones de dominios UCI/GitHub API en el borrador de configuración del entorno. La selección de repositorios no se modificó. Revisar y guardar en ajustes, y publicar el entorno para activar la nueva configuración/snapshot. Guardar el borrador no aplica la política ni demuestra restauración en una tarea nueva.

## Repetir la validación local

### Preparar datos y ejecutar pruebas

Python 3.11 y Java 17 recomendados; la sesión también validó Spark en Java 21. No sustituye la entrega cloud.

```bash
python3.11 -m venv .venv
. .venv/bin/activate
pip install -r requirements.lock
# Conseguir CSV oficial; si UCI está bloqueado, copia pública explícita:
git clone https://github.com/BilalSikander/ai4i2020.git /tmp/ai4i-review
git -C /tmp/ai4i-review checkout 65331f7f004e8627d1d5ca908fce0702738b80c2
python scripts/validate_local.py /tmp/ai4i-review/ai4i2020.csv
SPARK_LOCAL_IP=127.0.0.1 spark-submit --master 'local[2]' spark/build_obt.py --local --path "$PWD/artifacts"
SPARK_LOCAL_IP=127.0.0.1 pytest -q
python scripts/cost_sensitivity.py
```

`validate_local.py` usa el mismo SQL dbt con un adaptador JSON distinto y exporta Gold a Parquet. La rama Snowflake y su autenticación todavía requieren prueba real. Los tests negativos rechazan dimensiones duplicadas y joins huérfanos. No usar los subtipos TWF/HDF/PWF/OSF/RNF como predictoras; tampoco UDI, Product ID, etiqueta, hash o alerta base. La lista autorizada está en `spark/build_obt.py:FEATURES`; la OBT incluye target y campos de auditoría separados conceptualmente.

### Prueba Kestra local con retry

Después de la validación local descrita arriba, copiar el CSV validado a `artifacts/validation_ai4i.csv` e importar `kestra/flows/local_validation.yml` en Kestra. Ejecutar `ai4i_local_validation`: el primer intento falla deliberadamente (exit 75); el segundo perfila el CSV y ejecuta dbt y Spark sobre Raw local ya preparado. No prueba carga Raw Snowflake. Su estado final comprobado fue SUCCESS con dos intentos; ver `docs/kestra_local_results.json`.
