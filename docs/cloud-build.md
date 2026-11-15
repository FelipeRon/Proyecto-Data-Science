# Build en el entorno cloud con proxy TLS

El build normal conserva la verificación TLS. En esta máquina falló porque el contenedor no hereda la CA del proxy de la sesión. Se verificó el siguiente procedimiento con el bundle de confianza del host, montado como secreto BuildKit. No usar `--trusted-host`, `curl -k` ni desactivar verificaciones. No versionar certificados de la sesión ni copiarlos a una capa.

Desde la raíz:

```bash
python - <<'PY'
from pathlib import Path
s=Path('Dockerfile').read_text().replace(
    'RUN pip install --no-cache-dir -r requirements.lock',
    'RUN --mount=type=secret,id=proxy_ca,required=true PIP_CERT=/run/secrets/proxy_ca pip install --no-cache-dir -r requirements.lock')
Path('/tmp/ai4i-session.Dockerfile').write_text('# syntax=docker/dockerfile:1\n'+s)
PY
docker build --secret id=proxy_ca,src=/etc/ssl/certs/ca-certificates.crt \
  -f /tmp/ai4i-session.Dockerfile -t ai4i-runner:1 .
docker run --rm ai4i-runner:1 python -m pip check
```

Esto resuelve la instalación, no los permisos de red de UCI/Snowflake ni la confianza de todos los clientes en runtime. Para solicitudes Python tras un proxy, montar el bundle de la sesión en modo lectura y establecer `REQUESTS_CA_BUNDLE`; para Spark/JDBC, configurar un truststore Java con esa CA y el proxy soportado, preservando certificados raíz. Validar esos caminos con la cuenta real antes de declarar listo el pipeline cloud. En redes sin proxy interceptante, usar el Dockerfile original.

Las imágenes de Kestra con plugins pesan varios GB. El primer pull tardará varios minutos; reutilizar el caché. En esta sesión se completó la descarga y se validó el servicio por API.
