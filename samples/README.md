# Samples

Coloca aquí videos MP4 livianos para replay local.

El demo por defecto espera:

```text
samples/cam01.mp4
```

Puedes generarlo con FFmpeg:

```bash
ffmpeg -hide_banner -loglevel error -y -f lavfi -i testsrc=size=320x180:rate=10 -t 12 -pix_fmt yuv420p samples/cam01.mp4
```

`cameras.example.json` incluye un perfil `file_replay` y un perfil `rtsp` de
referencia. El perfil RTSP espera una fuente local en
`rtsp://127.0.0.1:8554/cam01`.
