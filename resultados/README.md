# resultados/

Esta es la salida esperada de nuestra corrida del 6 de noviembre de 2026, generada por [`../scripts/ejecutar.sh`](../scripts/ejecutar.sh) con 500,000 registros en Hadoop 3.3.6 standalone (Debian 12, OpenJDK 17.0.21).

Los archivos `part-r-*` (≈ 48 MB binarios) **no se suben**: se regeneran idénticos, y su checksum lo demuestra. En su lugar guardamos la evidencia de que están completos y en orden.

| Archivo | Qué contiene |
|---|---|
| `resumen.txt` | Los números clave de toda la corrida. Es lo primero que hay que comparar. |
| `teravalidate-1r.txt` | Salida de `teravalidate` con 1 reducer: `checksum 3d1575c41c3d8` |
| `teravalidate-2r.txt` | Salida de `teravalidate` con 2 reducers: `checksum 3d1575c41c3d8` |
| `listado-salidas.txt` | `ls -l` de `/tmp/tera-input`, `/tmp/tera-out-1r` y `/tmp/tera-out-2r` |
| `hex-entrada-primeros-3.txt` | Primeros 3 registros de la entrada (desordenados) |
| `hex-salida-primeros-3.txt` | Primeros 3 registros de la salida ordenada (claves `00 00 3c…`, `00 00 61…`, `00 00 66…`) |
| `hex-partition-lst-2r.txt` | El punto de corte elegido por el muestreo para 2 reducers |
| `tiempos.tsv` | Duración de cada job |
| `version.txt` | Versión de Hadoop y Java |
| `logs/*.log` | Log completo de cada job, con todos los contadores |

## Cómo comparar tu corrida con la nuestra

💻 Después de correr el script dentro del contenedor:

```bash
mkdir -p mis-resultados
docker cp hadoop:/tmp/resultados-terasort/. mis-resultados/
diff <(sed '/^Tiempos/,$d' resultados/resumen.txt) <(sed '/^Tiempos/,$d' mis-resultados/resumen.txt) && echo "✅ Igual que el equipo 6"
```

Lo único que puede cambiar son los tiempos, las fechas de los logs y los identificadores `job_local…`. El contador `FILE: Number of bytes written` de los logs también puede variar por unos bytes, porque incluye archivos internos del job; por eso no está en `resumen.txt`. Los registros, los checksums, el reparto 250,312 / 249,688 y el punto de corte deben ser idénticos.
