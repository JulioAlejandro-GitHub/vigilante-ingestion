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
- `INGESTION_STORAGE_BACKEND`: `local` o `minio`.
- `INGESTION_LOCAL_STORAGE_DIR`: raíz de almacenamiento local.
- `INGESTION_OUTBOX_PATH`: archivo JSONL donde se publican eventos.
- `INGESTION_MAX_FRAMES`: límite opcional para demos y tests.
- `INGESTION_REPLAY`: `true` para timestamps determinísticos.
- `INGESTION_REPLAY_START_AT`: base temporal del replay.

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
export INGESTION_MINIO_ACCESS_KEY=minioadmin
export INGESTION_MINIO_SECRET_KEY=minioadmin
export INGESTION_MINIO_BUCKET=vigilante-frames
```

El objeto usa el mismo `object_key` que el backend local y publica
`frame_ref=s3://<bucket>/<object_key>`. La resolución de objetos MinIO desde
recognition queda para el siguiente slice de integración.

## Publicación

Slice 1 publica a un outbox local JSONL. Esto evita bloquear la captura real por
RabbitMQ y deja un artefacto reproducible para tests, demos y futura conexión
al broker `vigilante.frames`.

Por defecto el outbox se resetea al iniciar (`INGESTION_OUTBOX_RESET=true`) para
que el replay sea reproducible. Usa `--append-outbox` si necesitas acumular
eventos.

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

## Pendiente Slice 2

- entrada RTSP real con proceso FFmpeg persistente
- webcam local formal
- publicación RabbitMQ en `vigilante.frames`
- resolución de `s3://` desde recognition o integración con `vigilante-media`
- health events (`camera_offline`, `stream_frozen`, `stream_recovered`)
- control de backpressure y métricas operativas
