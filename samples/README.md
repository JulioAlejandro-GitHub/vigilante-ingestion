# Samples Dev/Test Only

Coloca aquí videos MP4 livianos solo para tests y replay local acotado.

El fixture de replay usado por tests es:

```text
samples/cam01.mp4
```

Puedes generarlo con FFmpeg:

```bash
ffmpeg -hide_banner -loglevel error -y -f lavfi -i testsrc=size=320x180:rate=10 -t 12 -pix_fmt yuv420p samples/cam01.mp4
```

`cameras.example.json` incluye perfiles de referencia para replay y para una
cámara RTSP real. No se publica una fuente RTSP local desde el stack.
