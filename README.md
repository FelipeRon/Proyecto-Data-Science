# PSet 2 — Pipeline de mantenimiento: AI4I

Autor: Felipe Ron Andrade. Entrega: [documento técnico](docs/PSet2_memo_Ron_Andrade.pdf).
Implementación para **detectar falla en la lectura actual**. AI4I no permite predecir el siguiente ciclo, medir anticipación, estimar RUL ni identificar series de una misma máquina.

`UCI → Kestra → Snowflake RAW → dbt BRONZE → SILVER → GOLD → Spark → Snowflake OBT`

![Arquitectura](docs/architecture.png)

Consulta la [revisión requisito por requisito](docs/cumplimiento.md): la entrega cloud sigue pendiente de ejecución autenticada.

## Estado verificado y pendientes

- Validación local con copia pública de 10.000 registros: 6 modelos dbt + 14 tests pasan sobre DuckDB. Spark conserva 10.000 filas/IDs y persiste Parquet. Ver [evidencia](docs/validation.md).
- Snowflake **pendiente**: configurar la cuenta propia, el usuario de servicio y la llave autorizada siguiendo la guía. La ejecución cloud sigue pendiente de validación.
- Kestra arrancado; flow principal validado/importado. Una prueba local aprobó dbt/Spark y retry real en contenedores. Conectores y ejecución end-to-end Snowflake pendientes. Añadir la evidencia real antes de entregar.
- Descarga oficial UCI bloqueada por red en esta sesión. Se guardó un borrador para permitir `archive.ics.uci.edu` y `api.github.com` en configuración del entorno; revisar, guardar y publicar para usarla. Agregar también el hostname exacto de la cuenta Snowflake si se ejecuta con red restringida. Guardar el borrador no aplica la política a la máquina actual.

## 1. Configuración y credenciales

Seguir la [guía de conexión a Snowflake en Snowsight](docs/snowflake_connection.md): monitor de créditos, usuario `PSET2_SVC`, par de llaves y comprobaciones.

Docker con Compose, 6 GB RAM y salida HTTPS a UCI, Snowflake, Docker Hub, PyPI, Debian y Maven Central. El job Spark usa Java 17 en su imagen, 2 cores y Spark 3.5.3/Scala 2.12; no hace falta un clúster para 10.000 filas.

Completar los pasos 1–3 de la [guía de conexión](docs/snowflake_connection.md): preparar o reutilizar llaves, ejecutar `docs/snowflake_setup.sql` en Snowsight y completar `.env`. Sustituir `organization-account` por el identificador obtenido en tu cuenta. No sobrescribir archivos `.env` ni llaves existentes. Registrar solo la pública; mantener la privada fuera de Git. Completar también las contraseñas locales y `HOST_PROJECT_DIR` con la ruta absoluta del checkout en el host Docker.

## 2. Construir y arrancar

```bash
docker compose config --quiet
docker compose build runner  # En el cloud con proxy: usar docs/cloud-build.md
docker compose up -d postgres kestra
docker compose logs --tail=50 kestra
```

Kestra escucha en el puerto local 8080. Autenticar con el usuario configurado y cargar `kestra/flows/pipeline.yml` en el editor de flows (namespace `pset2`). Validar con el editor de la versión fijada antes de habilitar el schedule. Ejecutar `ai4i_pipeline`: por defecto descarga el snapshot completo sin intervención manual y encadena ingesta → `dbt build` → `spark-submit`. Solo continúa si la etapa anterior termina bien.

El trigger está deshabilitado hasta configurar/verificar Snowflake. Habilitarlo después de la primera carga validada; corre lunes a las 06:00, America/Guayaquil. Es una comprobación semanal del archivo estático, no una frecuencia inventada de sensores. Concurrencia 1 evita carreras entre cargas; 3 intentos exponenciales cubren fallos transitorios. Un error definitivo deja la ejecución fallida y conserva logs.

También se puede validar e importar por API (con Python del entorno activado):

```bash
set -a; . ./.env; set +a
python scripts/deploy_kestra.py
```

### Recorrido completo del PSet 2

1. Completar la guía de conexión, construir la imagen y arrancar los servicios con los comandos anteriores.
2. Ejecutar la consulta autenticada y `dbt debug` del paso 4 de la guía. Confirmar `PSET2_SVC`, `PSET2_ROLE`, `PSET2_WH` y `PSET2`.
3. Importar `kestra/flows/pipeline.yml` en Kestra y ejecutar `ai4i_pipeline` con `historical_snapshot` vacío. Esperar `SUCCESS`: descarga UCI → Raw → dbt Bronze/Silver/Gold y tests → Spark OBT en Snowflake.
4. Ejecutar las consultas del paso 4 de este README; comparar hash, conteos y claves en las cinco capas con la fuente de esa corrida. No usar una OBT anterior como evidencia de una corrida fallida.
5. Repetir el flow con el mismo archivo y comprobar que no acumule duplicados. Probar el backfill completo/replay según el paso 3 de este README: CSV histórico en `artifacts/` y parámetro `historical_snapshot`.
6. Probar el mecanismo de retry con `kestra/flows/local_validation.yml` según la sección de prueba local. Conservar sus dos intentos; esa prueba no reemplaza la validación cloud. Documentar también los reintentos reales de ingesta cloud cuando ocurran.
7. Habilitar el schedule solo tras validar la carga: cambiar `disabled: true` a `disabled: false` en `weekly_snapshot_check`, reimportar y confirmar cron/zona horaria en Kestra.
8. Guardar estados/logs de Kestra, resultados de dbt y consultas Snowflake sin secretos en `docs/`. Actualizar `validation.md` y `cumplimiento.md` con los resultados reales antes de entregar el memo.

La configuración permite seguir este recorrido con tu cuenta; no acredita una ejecución cloud previa. Los diagramas, el memo y la justificación batch se encuentran en `docs/`.

## 3. Ejecutar las piezas por separado

```bash
docker compose run --rm runner python kestra/scripts/ingest.py
docker compose run --rm runner sh -c 'cd dbt && dbt build --profiles-dir .'
docker compose run --rm spark
```

**Backfill/replay:** en la primera ejecución se carga toda la historia disponible (un snapshot sin fechas). Para reproducir una versión anterior, guardar su CSV como `artifacts/ai4i_historico.csv` y ejecutar el flow con `historical_snapshot=/app/artifacts/ai4i_historico.csv`. Esto promueve explícitamente ese snapshot a actual. No fabricar intervalos de fechas ni duplicar filas como si fuesen observaciones nuevas. Reintentar un hash reemplaza transaccionalmente sus filas; otros hashes se conservan en Raw. Bronze escoge el último snapshot cargado. No ejecutar los comandos manuales a la vez que el flow.

Una carga fallida no borra Raw anterior. Un fallo dbt bloquea Spark; Gold y OBT previos pueden seguir existiendo: **no consumirlos como resultado de la corrida fallida**. La publicación atómica entre Gold y OBT es una mejora pendiente.

## 4. Verificar en Snowflake

```sql
SELECT FILE_SHA256, COUNT(*), MAX(LOADED_AT) FROM PSET2.RAW.AI4I GROUP BY 1;
SELECT COUNT(*) FROM PSET2.BRONZE.STG_READINGS;
SELECT COUNT(*) FROM PSET2.SILVER.INT_READINGS;
SELECT COUNT(*) FROM PSET2.SILVER.INT_QUARANTINE;
SELECT COUNT(*), COUNT(DISTINCT READING_ID), SUM(MACHINE_FAILURE) FROM PSET2.GOLD.FCT_READING;
SELECT COUNT(*), COUNT(DISTINCT READING_ID), SUM(MACHINE_FAILURE) FROM PSET2.OBT.OBT_READINGS;
```

Esperado para el CSV perfilado: 10.000 filas, 10.000 IDs distintos, 339 fallas, 0 cuarentena. Confirmar primero que su hash coincide; no imponer esos conteos a futuras versiones diferentes. Guardar ejecución Kestra, logs dbt, conteos y auditoría Spark en `docs/` con credenciales ocultas. Probar retry con un fallo transitorio controlado y replay del mismo hash: mismo conteo, sin duplicados.

## 5. Validación local reproducible (sin Snowflake)

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

## Prueba Kestra local con retry (sin Snowflake)

Después de la validación local del paso 5, copiar el CSV validado a `artifacts/validation_ai4i.csv` e importar `kestra/flows/local_validation.yml` en Kestra. Ejecutar `ai4i_local_validation`: el primer intento falla deliberadamente (exit 75); el segundo perfila el CSV y ejecuta dbt y Spark sobre Raw local ya preparado. No prueba carga Raw Snowflake. Su estado final comprobado fue SUCCESS con dos intentos; ver `docs/kestra_local_results.json`.

## Documento y decisiones

- [Memo editable, seis páginas](docs/PSet2_memo_Ron_Andrade.md)
- [Respuesta a comentarios del PSet 1](docs/feedback.md)
- [Métricas de calidad](docs/quality_results.json), [regla base](docs/baseline_results.json), [sensibilidad de costos](docs/cost_sensitivity.csv), [capacidad](docs/capacity_sensitivity.csv)
- [Fuentes y atribución](docs/references.md)
- [Modelo dimensional](docs/star.png)

Regenerar PDF desde `docs/`: `pandoc PSet2_memo_Ron_Andrade.md -o PSet2_memo_Ron_Andrade.pdf --pdf-engine=xelatex`.
