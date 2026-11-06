# datos/

Esta carpeta **no trae archivos de entrada a propósito**. Hay dos razones:

1. **`teragen` genera los datos dentro del contenedor y siempre son los mismos.** Su generador pseudoaleatorio parte del número de fila, así que `teragen 500000` produce exactamente el mismo archivo en cualquier computadora. Lo comprueba el checksum: `1074598070305752` en decimal, o `3d1575c41c3d8` en hexadecimal.
2. **Subir el archivo sería inútil y pesado.** Mide 50,000,000 bytes de datos binarios, casi en el límite de 50 MB de la entrega, y se regenera en 2 segundos.

## Comando para generar los datos

🐳 Dentro del contenedor `hadoop`:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 HADOOP_HOME=/opt/hadoop
export PATH=$PATH:$HADOOP_HOME/bin && cd $HADOOP_HOME/share/hadoop/mapreduce
hadoop jar hadoop-mapreduce-examples-3.3.6.jar teragen 500000 /tmp/tera-input
```

| Dato | Valor |
|---|---|
| Carpeta en el contenedor | `/tmp/tera-input/` |
| Archivo | `part-m-00000` (1 mapper) |
| Tamaño | 50,000,000 bytes (≈ 48 MB); con su `.crc`, 50,390,636 bytes |
| Registros | 500,000 × 100 bytes |
| CHECKSUM del job | `1074598070305752` |

Para el mini ejercicio se generan 200,000 registros en `/tmp/ejercicio-input` (ver [`../ejercicio/ejercicio.md`](../ejercicio/ejercicio.md)).

## ¿Cómo se ven los datos?

Los primeros 3 registros de la entrada, en hexadecimal, están en [`../resultados/hex-entrada-primeros-3.txt`](../resultados/hex-entrada-primeros-3.txt). Así se ve el primer registro:

```
0000000 4a 69 6d 47 72 61 79 52 49 50 00 11 30 30 30 30   ← clave: "JimGrayRIP" | 00 11 | número de fila…
0000016 30 30 30 30 30 30 30 30 30 30 30 30 30 30 30 30   ← …número de fila en hex (ASCII "000…0")
0000032 30 30 30 30 30 30 30 30 30 30 30 30 88 99 aa bb   ← … | separador 88 99 aa bb
0000048 32 32 32 32 30 30 30 30 32 32 32 32 30 30 30 30   ← relleno (48 bytes)
...
0000096 cc dd ee ff                                       ← fin del registro
```
