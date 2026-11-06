# Solución · Tu jefe dice que el job tarda demasiado

Valores obtenidos el 6 de noviembre de 2026 en Hadoop 3.3.6 standalone (Debian 12, OpenJDK 17.0.21). Como `teragen` es determinista, tus registros, checksums y repartos deben salir idénticos. Solo cambian los tiempos.

## Parte A · Comandos

🐳 Dentro del contenedor, después de preparar el entorno:

```bash
rm -rf /tmp/ejercicio-input /tmp/ejercicio-out-1r /tmp/ejercicio-out-3r /tmp/ejercicio-val-1r /tmp/ejercicio-val-3r

# 1. Generar 200,000 registros (20,000,000 bytes)
hadoop jar hadoop-mapreduce-examples-3.3.6.jar teragen 200000 /tmp/ejercicio-input

# 2. Ordenar con 1 reducer
hadoop jar hadoop-mapreduce-examples-3.3.6.jar terasort -Dmapreduce.job.reduces=1 /tmp/ejercicio-input /tmp/ejercicio-out-1r

# 3. Ordenar con 3 reducers
hadoop jar hadoop-mapreduce-examples-3.3.6.jar terasort -Dmapreduce.job.reduces=3 /tmp/ejercicio-input /tmp/ejercicio-out-3r

# 4. Validar ambas salidas
hadoop jar hadoop-mapreduce-examples-3.3.6.jar teravalidate /tmp/ejercicio-out-1r /tmp/ejercicio-val-1r
hadoop jar hadoop-mapreduce-examples-3.3.6.jar teravalidate /tmp/ejercicio-out-3r /tmp/ejercicio-val-3r
cat /tmp/ejercicio-val-1r/part-r-00000 /tmp/ejercicio-val-3r/part-r-00000
```

### Resultados esperados

| Dato | 1 reducer | 3 reducers |
|---|---|---|
| CHECKSUM de `teragen` | `429115762551593` (decimal) | ← mismo input |
| `number of splits` (mappers) | 1 | 1 |
| `Shuffled Maps` | **1** | **3** |
| Archivos de salida | `part-r-00000` | `part-r-00000`, `part-r-00001`, `part-r-00002` |
| Registros por archivo (bytes ÷ 100) | 200,000 | 66,553 · 66,614 · 66,833 |
| `_partition.lst` | 0 bytes | 22 bytes |
| Checksum `teravalidate` | `186474c982b29` | `186474c982b29` |
| Tiempo de `terasort` | ≈ 3.2 s | ≈ 3.2 s |

```
$ ls -l /tmp/ejercicio-out-3r
-rw-r--r-- 1 root root       0  _SUCCESS
-rw-r--r-- 1 root root      22  _partition.lst
-rw-r--r-- 1 root root 6655300  part-r-00000
-rw-r--r-- 1 root root 6661400  part-r-00001
-rw-r--r-- 1 root root 6683300  part-r-00002
```

## Parte B · Respuestas

**1. ¿Cuántos archivos `part-r-*` genera cada configuración?**
Uno por reducer: 1 archivo con 1 reducer y 3 archivos con 3 reducers. Cada reducer escribe exactamente un archivo con su rango de claves, y al concatenarlos en orden (`00000`, `00001`, `00002`) se obtiene el orden global.

**2. ¿Cuántos `Shuffled Maps` reporta el job?**
**1 con 1 reducer y 3 con 3 reducers.** El contador vale *mappers × reducers*, porque cada reducer jala su parte de **todos** los mappers. Aquí solo hay **1 mapper**: en standalone, Hadoop divide los archivos locales en bloques de 32 MB, y la entrada de 20 MB cabe en un solo split (`number of splits:1`).

Compáralo con la demo de 500,000 registros. Ese archivo de 48 MB se divide en 2 splits (32 MB + 16 MB), así que hay 2 mappers y `Shuffled Maps` vale 2 con 1 reducer y 4 con 2 reducers.

**3. ¿Qué contiene `_partition.lst` con 3 reducers?**
Los **N − 1 = 2 puntos de corte** que calculó el muestreo, guardados como dos claves binarias. Cada una ocupa 11 bytes, 1 de longitud (`0a` = 10) más 10 de la clave, y por eso el archivo mide 22 bytes:

```
$ od -A d -t x1 /tmp/ejercicio-out-3r/_partition.lst
0000000 0a 55 0e fb 88 3a d3 90 c9 f0 b3 0a aa 23 05 72
0000016 b4 f4 bd 81 5f 40
```

- Corte 1: `550efb883ad390c9f0b3`. Las claves menores van al reducer 0.
- Corte 2: `aa230572b4f4bd815f40`. Las claves entre el corte 1 y este van al reducer 1, y el resto al reducer 2.

Los cortes quedan cerca de 1/3 (`55…`) y 2/3 (`aa…`) del espacio de claves, porque las claves son uniformes. Por eso cada reducer recibió casi 66,666 registros. Con 1 reducer no hay cortes y el archivo mide 0 bytes.

**4. En standalone, ¿3 reducers son más rápidos que 1?**
**No.** Ambos tardaron ≈ 3.2 s. En modo standalone todo corre en **una sola JVM** (`LocalJobRunner`) y los reducers se ejecutan **uno tras otro**, así que no hay paralelismo real. Puede incluso tardar un poco más por el costo de arrancar más tareas y escribir más archivos. La ventaja de más reducers solo aparece en un clúster, donde corren al mismo tiempo en máquinas distintas.

**5. Clúster de 10 nodos con 4 contenedores de reduce por nodo, para 10 TB: ¿cuántos reducers?**
La documentación oficial de Hadoop (*MapReduce Tutorial*, sección "How Many Reduces?") recomienda:

> reducers = **0.95** o **1.75** × (número de nodos × contenedores máximos por nodo)

Con 10 × 4 = 40 contenedores:

- **0.95 × 40 ≈ 38 reducers.** Todos arrancan en una sola oleada en cuanto terminan los maps. El 5 % de margen deja lugar para reintentos si algo falla.
- **1.75 × 40 = 70 reducers.** Los nodos más rápidos terminan una primera oleada y toman una segunda. Así se balancea mejor la carga, a cambio de un poco más de overhead.

Con 10 TB, cada reducer escribiría unos 263 GB (con 38) o unos 143 GB (con 70). Esto está muy por encima del bloque de 128 MB de HDFS, así que el límite real no es el tamaño de los archivos sino el número de contenedores.

**Bonus: ¿el checksum de 1 reducer es igual al de 3?**
Sí: `186474c982b29` en ambos. El checksum es la suma de un CRC32 por registro, así que no depende del orden ni de en qué archivo quedó cada registro. Si es igual, la salida tiene **exactamente los mismos registros** que la entrada, sin perder ni duplicar ninguno. Además coincide con el CHECKSUM de `teragen` convertido a hexadecimal:

```bash
printf '%x\n' 429115762551593    # 186474c982b29
```

Las dos validaciones no imprimieron ninguna otra línea, así que también están en orden global.
