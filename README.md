# vigilante-ingestion

## Objetivo

Primer slice funcional de ingestion: leer un MP4 local como cámara virtual,
muestrear frames a un FPS configurable, guardar evidencia mínima y emitir
eventos `frame.ingested` consumibles por `vigilante-recognition`.

Este slice prioriza replay local determinístico. RTSP productivo, cámaras reales
y streaming sostenido quedan preparados conceptualmente, pero fuera del alcance
de esta entrega.

## Requisitos

- Python 3.11+
- FFmpeg y ffprobe disponibles en `PATH`
- Entorno virtual local `.venv`

El código de la app corre localmente, sin Docker. MinIO puede usarse si ya está
levantado en el stack de soporte, pero el modo por defecto usa filesystem local.
RabbitMQ se usa solo cuando `INGESTION_PUBLISH_MODE=rabbitmq|both`.

## Instalación

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuración

Copia `.env.example` a `.env` y ajusta:

- `INGESTION_SOURCE_FILE`: MP4 local usado como cámara virtual.
- `INGESTION_CAMERA_ID`: UUID canónico de `api.camera.camera_id`.
- `INGESTION_FPS`: frecuencia de captura, por ejemplo `1`, `2` o `5`.
- `INGESTION_STORAGE_BACKEND`: `local`, `minio` o `s3`.
- `INGESTION_LOCAL_STORAGE_DIR`: raíz de almacenamiento local.
- `INGESTION_PUBLISH_MODE`: `jsonl`, `rabbitmq` o `both`.
- `INGESTION_OUTBOX_PATH`: archivo JSONL donde se publican eventos.
- `INGESTION_MAX_FRAMES`: límite opcional para demos y tests.
- `INGESTION_REPLAY`: `true` para timestamps determinísticos.
- `INGESTION_REPLAY_START_AT`: base temporal del replay.
- `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USER`, `RABBITMQ_PASSWORD`,
  `RABBITMQ_VHOST`: conexión AMQP local.

`camera_id` debe ser un UUID real existente en `api.camera`. No uses claves
lógicas como FK operativa; si necesitas conservar una clave externa, usa
`INGESTION_EXTERNAL_CAMERA_KEY`.

## Sample local

El repo incluye `samples/cameras.example.json` y espera un MP4 en:

```text
samples/cam01.mp4
```

Si no tienes un video real liviano, genera uno reproducible:

```bash
mkdir -p samples
ffmpeg -hide_banner -loglevel error -y -f lavfi -i testsrc=size=320x180:rate=10 -t 12 -pix_fmt yuv420p samples/cam01.mp4
```

## Ejecución

Comando local mínimo:

```bash
PYTHONPATH=. python3 -m app.main --source-file samples/cam01.mp4 --camera-id <UUID_REAL> --fps 1 --max-frames 10
```

Ejemplo con UUID de desarrollo:

```bash
PYTHONPATH=. python3 -m app.main --source-file samples/cam01.mp4 --camera-id 11111111-1111-1111-1111-111111111111 --fps 1 --max-frames 3
```

Salida esperada:

- frames JPEG bajo `storage/frames/<camera_id>/YYYY/MM/DD/...jpg`
- metadata JSON por frame junto al JPEG
- eventos JSONL en `outbox/frame_ingested.jsonl`

## Contrato emitido

Por cada frame se emite `frame.ingested` con:

- `event_id`
- `event_type = frame.ingested`
- `event_version`
- `occurred_at`
- `source.component = vigilante-ingestion`
- `payload.camera_id`
- `payload.frame_ref`
- `payload.frame_uri`
- `payload.captured_at`
- `payload.width`
- `payload.height`
- `payload.content_type`
- `payload.source_type`
- `payload.quality_metadata`
- `context.idempotency_key`

`payload.frame_ref` es el campo canónico que hoy consume
`vigilante-recognition`. En modo local se guarda como path absoluto para que el
worker de recognition pueda resolverlo aunque corra desde otro directorio.

## Storage

### Local

Es el backend por defecto. Guarda:

- imagen JPEG
- metadata JSON

El naming es estable:

```text
frames/<camera_id>/YYYY/MM/DD/HH-MM-SS-mmm_s<sample_index>_f<source_frame_index>.jpg
```

### MinIO

Configura:

```bash
export INGESTION_STORAGE_BACKEND=minio
export INGESTION_MINIO_ENDPOINT=localhost:9000
export INGESTION_MINIO_ACCESS_KEY=minio
export INGESTION_MINIO_SECRET_KEY=minio123
export INGESTION_MINIO_BUCKET=vigilante-frames
```

El objeto usa el mismo `object_key` que el backend local y publica
`frame_ref=s3://<bucket>/<object_key>` y `frame_uri` con el mismo valor. Ese URI
lo resuelve `vigilante-recognition` leyendo bucket y key desde el esquema
`s3://bucket/key` y descargando el frame a su cache local antes del pipeline de
visión.

Para un endpoint S3-compatible que no sea MinIO local puedes usar
`INGESTION_STORAGE_BACKEND=s3` y los aliases `INGESTION_S3_ENDPOINT`,
`INGESTION_S3_ACCESS_KEY`, `INGESTION_S3_SECRET_KEY`, `INGESTION_S3_BUCKET` y
`INGESTION_S3_SECURE`. Con `storage_backend=s3`, esos aliases tienen prioridad;
con `storage_backend=minio`, tienen prioridad las variables `INGESTION_MINIO_*`.

El backend remoto guarda además un JSON de metadata con la misma key base del
frame (`.json`) y metadata de objeto mínima: `camera-id`, `captured-at`,
`object-key`, índices de sample/frame, `source-type` y `storage-backend`.

## Publicación

El modo por defecto sigue publicando a un outbox local JSONL. Para Slice 3
también existe publicación real a RabbitMQ y un modo combinado:

- `jsonl`: escribe `outbox/frame_ingested.jsonl`
- `rabbitmq`: publica directo al broker
- `both`: escribe JSONL y publica al broker en la misma corrida

Por defecto el outbox se resetea al iniciar (`INGESTION_OUTBOX_RESET=true`) para
que el replay sea reproducible. Usa `--append-outbox` si necesitas acumular
eventos.

Topología RabbitMQ declarada por el publisher:

- exchange principal: `vigilante.frames`
- routing key: `frame.ingested`
- cola de recognition: `vigilante.recognition.frame_ingested`
- DLX: `vigilante.frames.dlx`
- DLQ: `vigilante.recognition.frame_ingested.dlq`
- routing key DLQ: `frame.ingested.dlq`

Publicación local a RabbitMQ:

```bash
docker compose -f ../vigilante-docs/docker/docker-compose.support.yml up -d rabbitmq

PYTHONPATH=. python -m app.main \
  --source-file samples/cam01.mp4 \
  --camera-id 11111111-1111-1111-1111-111111111111 \
  --fps 1 \
  --max-frames 10 \
  --publish-mode rabbitmq
```

Para conservar el artefacto reproducible y además probar el broker:

```bash
PYTHONPATH=. python -m app.main \
  --source-file samples/cam01.mp4 \
  --camera-id 11111111-1111-1111-1111-111111111111 \
  --fps 1 \
  --max-frames 10 \
  --publish-mode both
```

## Flujo local con MinIO/S3 compartido

Levanta MinIO y RabbitMQ desde el stack de soporte:

```bash
docker compose -f ../vigilante-docs/docker/docker-compose.support.yml up -d minio rabbitmq
```

Publica frames en MinIO y eventos en JSONL:

```bash
PYTHONPATH=. python -m app.main \
  --source-file samples/cam01.mp4 \
  --camera-id 11111111-1111-1111-1111-111111111111 \
  --fps 1 \
  --max-frames 10 \
  --storage-backend minio \
  --minio-endpoint localhost:9000 \
  --minio-access-key minio \
  --minio-secret-key minio123 \
  --minio-bucket vigilante-frames \
  --publish-mode jsonl
```

El outbox resultante conserva el contrato `frame.ingested`: `payload.frame_ref`
es el campo canónico y apunta a `s3://vigilante-frames/<object_key>`;
`payload.frame_uri` queda como alias práctico resoluble con el mismo URI.

## Replay determinístico

Con `INGESTION_REPLAY=true`, la misma combinación de:

- source file
- camera_id
- FPS
- `INGESTION_REPLAY_START_AT`
- `max_frames`

produce el mismo orden de samples, los mismos object keys, los mismos
`event_id` y las mismas claves de idempotencia.

## Tests

```bash
PYTHONPATH=. pytest
```

La suite genera videos MP4 temporales con FFmpeg, valida metadata, extracción,
storage local, construcción del evento `frame.ingested`, errores básicos y
replay determinístico.

## Pendiente próximos slices

- entrada RTSP real con proceso FFmpeg persistente
- webcam local formal
- integración con `vigilante-media` encima de `frame_ref` / `frame_uri`
- health events (`camera_offline`, `stream_frozen`, `stream_recovered`)
- control de backpressure y métricas operativas
