# Conectar el proyecto a Snowflake desde cero

Esta guía prepara la cuenta y verifica la conexión; no implica que ya se haya ejecutado contra Snowflake. Obtener el identificador de tu propia cuenta con la consulta del paso 2.

## 1. Preparar el par de llaves en tu equipo

Si ya tienes una llave privada y registraste su pública, conserva ese par: no lo reemplaces por uno nuevo. Una llave pública puede usarse **solo si tienes su privada correspondiente**. Conocer una pública no permite conectarse ni reconstruir la privada.

Si todavía no tienes un par, desde la raíz, con rutas nuevas (no sobrescribir archivos existentes):

```bash
mkdir -p secrets artifacts
# Comprobar que estos archivos aún no existen antes de ejecutar OpenSSL.
openssl genrsa -out secrets/private.pem 2048
openssl pkcs8 -topk8 -inform PEM -outform PEM -nocrypt \
  -in secrets/private.pem -out secrets/snowflake_key.p8
openssl rsa -in secrets/private.pem -pubout -out secrets/snowflake_key.pub
chmod 600 secrets/private.pem secrets/snowflake_key.p8
# Imprimir únicamente la PÚBLICA, sin cabeceras ni saltos:
sed '/^-----/d' secrets/snowflake_key.pub | tr -d '\n'
```

El pipeline actual admite una privada PKCS#8 PEM sin passphrase; proteger el archivo mediante permisos y montaje de solo lectura. Nunca subirlo ni pegarlo en chat. `.env` y `secrets/` están ignorados por Git y Docker.

## 2. Configuración única en Snowsight

Entrar con tu usuario humano administrador, abrir un worksheet y seleccionar `ACCOUNTADMIN`. Abrir [snowflake_setup.sql](snowflake_setup.sql), reemplazar `<RSA_PUBLIC_KEY_BASE64>` por la pública del paso 1 y ejecutar el script. No ejecutar el marcador literal.

El script crea monitor de 20 créditos, warehouse XSMALL, cinco esquemas y un rol dedicado. Crea `PSET2_SVC` de tipo SERVICE sin contraseña y le asigna el rol. `SYSADMIN` hereda el rol para inspeccionar objetos. No usar `ACCOUNTADMIN` en `.env` ni en el pipeline.

Si `PSET2_SVC` ya existe, revisar su tipo y usos antes de ejecutar: `IF NOT EXISTS` no convierte un usuario anterior en SERVICE. El `ALTER USER` sí reemplaza su llave principal; no hacerlo si está en uso con otra llave. Para rotación sin interrupción, registrar la nueva en `RSA_PUBLIC_KEY_2`, probarla y retirar la anterior en un cambio separado.

Los permisos abarcan solo los cinco esquemas, sin `ALL PRIVILEGES` ni propiedad de la base. El pipeline debe crear sus propias tablas bajo `PSET2_ROLE`: no obtiene automáticamente acceso a tablas creadas por otros roles. Si hay objetos previos ajenos, el propietario debe revisar concesiones/propiedad para esos objetos, sin ampliar permisos globalmente.

Raw **no es append-only estricto** en esta implementación: conserva snapshots diferentes y reemplaza transaccionalmente las filas de un hash al reintentarlo. La OBT se sobrescribe. No se declara una tabla de auditoría Snowflake que todavía no existe.

### Monitor de créditos

`FREQUENCY=NEVER` hace la cuota acumulada; no reinicia el contador cada mes. Al 80% se genera notificación para destinatarios habilitados en Snowflake; configurar/verificar sus correos y preferencias. Al 100%, `SUSPEND` suspende el warehouse después de permitir terminar las consultas activas: puede haber sobreconsumo. No cubre todos los cargos de almacenamiento/servicios serverless ni equivale a un límite estricto de dólares. No conceder al servicio permisos para modificar el monitor o reanudar un warehouse bloqueado por cuota.

`IF NOT EXISTS` conserva parámetros de objetos existentes: inspeccionar tamaño, suspensión, cuota y frecuencia si se reutilizan. No reiniciar cuotas automáticamente para eludir el monitor.

## 3. Configurar el proyecto

Copiar `.env.example` a `.env` solo si no existe; editar sin publicar valores privados:

```dotenv
SNOWFLAKE_ACCOUNT=organization-account
SNOWFLAKE_USER=PSET2_SVC
SNOWFLAKE_ROLE=PSET2_ROLE
SNOWFLAKE_WAREHOUSE=PSET2_WH
SNOWFLAKE_DATABASE=PSET2
SNOWFLAKE_PRIVATE_KEY_FILE=/run/secrets/snowflake_key.p8
```

Usar el identificador devuelto por el SQL, sin `https://` ni `.snowflakecomputing.com`. La ruta de llave es **dentro del contenedor**; Compose monta `./secrets` en `/run/secrets`. Completar también contraseñas locales de Kestra y ruta absoluta `HOST_PROJECT_DIR`. La cuenta y usuario no son credenciales suficientes: la privada debe corresponder a la pública registrada.

## 4. Verificar autenticación y dbt

Desde la raíz, después de construir la imagen (ver `cloud-build.md` si hay proxy):

```bash
docker compose run --rm runner python -c 'from common import connect; c=connect(); print(c.cursor().execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE()").fetchone()); c.close()'
docker compose run --rm runner sh -c 'cd /app/dbt && dbt debug --profiles-dir .'
```

Esperar usuario `PSET2_SVC`, rol `PSET2_ROLE`, warehouse `PSET2_WH` y base `PSET2`. Esas consultas comprueban conexión; no crean las capas. Luego ejecutar el flow principal según README y comprobar conteos en las cinco capas. Solo después habilitar el trigger semanal y actualizar la evidencia de cumplimiento.

## 5. Diagnóstico

- Error de red/403: permitir el hostname exacto de la cuenta y UCI; comprobar proxy y certificados sin desactivar TLS.
- JWT/llave rechazada: revisar correspondencia de llaves, usuario, cuenta, formato PKCS#8 y reloj del sistema. No publicar la privada para diagnosticarlo.
- Objeto no autorizado: comprobar el rol activo y propietario del objeto. No resolver con `ACCOUNTADMIN` en el proceso.
- Warehouse suspendido por monitor: revisar consumo con el administrador. No aumentar la cuota automáticamente.
- `dbt debug` correcto pero ingesta fallida: revisar descarga UCI y logs de la ejecución; autenticar no demuestra el pipeline completo.
