# bda-equipo-6-terasort · La suite TeraSort

**Equipo 6** · Kevin, Leon e Isac
Manejo Masivo de Datos · IDIA 224 · Prof. José Javier Moya Moya · Universidad Politécnica de Querétaro

Guía paso a paso para reproducir los ejemplos **`teragen` → `terasort` → `teravalidate`** de `hadoop-mapreduce-examples-3.3.6.jar` en el contenedor de la práctica *Hadoop Standalone en contenedor Debian*.

> Los bloques de código están marcados así:
> 💻 **en tu computadora** (terminal normal, donde está este repositorio) ·
> 🐳 **dentro del contenedor** `hadoop` (prompt `root@hadoop` o `hadoop@hadoop`)

---

## Contenido

1. [¿Qué hace?](#1-qué-hace)
2. [¿Cómo funciona? (fases Map y Reduce)](#2-cómo-funciona-fases-map-y-reduce)
3. [Requisitos](#3-requisitos)
4. [Reproducción rápida con el script](#4-reproducción-rápida-con-el-script)
5. [Reproducción manual, comando por comando](#5-reproducción-manual-comando-por-comando)
6. [Resultados esperados](#6-resultados-esperados)
7. [Problemas comunes](#7-problemas-comunes)
8. [Mini ejercicio](#8-mini-ejercicio)
9. [Estructura del repositorio](#9-estructura-del-repositorio)
10. [Fuentes](#10-fuentes)

---

## 1. ¿Qué hace?

TeraSort es el *benchmark* clásico de Hadoop: **ordenar registros de 100 bytes** (clave de 10 bytes + valor de 90) para que el resultado quede en **orden global**, aunque el trabajo se reparta entre varios reducers. Se compone de tres programas que se corren siempre en este orden:

| Programa | Qué hace | Fases |
|---|---|---|
| `teragen` | Genera N registros pseudoaleatorios de 100 bytes. Es determinista: el mismo N produce siempre los mismos datos. | Solo Map |
| `terasort` | Muestrea las claves, reparte por **rangos** y ordena. | Map + Shuffle + Reduce |
| `teravalidate` | Comprueba que la salida esté en orden global y calcula un checksum. | Map + Reduce |

Un terabyte equivale a 10,000,000,000 registros. En esta práctica usamos **500,000 registros (≈ 48 MB)** para que corra en segundos en modo standalone.

**¿Dónde se usa en la vida real?**

- **Motores de búsqueda:** construir un índice invertido es ordenar miles de millones de pares (palabra, documento).
- **Bases de datos:** los *joins* y `GROUP BY` masivos usan *sort-merge*: ordenan ambas tablas por la llave y luego las recorren juntas.
- **Ingeniería de datos e IA:** deduplicar y particionar datasets de entrenamiento, o compactar logs por fecha antes de cargarlos a un *data lake*.
- **Infraestructura:** antes de poner un clúster en producción se corre TeraSort porque estresa disco, red y CPU al mismo tiempo.

## 2. ¿Cómo funciona? (fases Map y Reduce)

### El problema: ordenar en paralelo sin perder el orden

WordCount reparte las claves con un **hash** (`HashPartitioner`). Así, cada reducer queda ordenado por dentro, pero al juntar los archivos el orden global se rompe. TeraSort usa `TotalOrderPartitioner`: cada reducer recibe un **rango** de claves, así que concatenar `part-r-00000`, `part-r-00001`, … da el orden global.

### Diagrama (nuestra corrida: 500,000 registros, 2 mappers, 2 reducers)

```
                    ┌──────────────────────────────┐
   0. Muestreo      │ lee 100,000 claves del input,│
   (antes del job)  │ las ordena y elige N−1 = 1   │──► _partition.lst
                    │ punto de corte               │    (corte: 7ff2ca13…)
                    └──────────────────────────────┘

  /tmp/tera-input/part-m-00000  (50,000,000 bytes)
        │
        ├── split 0 (32 MB) ──► mapper 0 ─┐  Map: lee (clave, valor) y lo emite
        │                    335,545 reg. │  SIN transformarlo.
        └── split 1 (16 MB) ──► mapper 1 ─┤  Partición: TotalOrderPartitioner busca
                             164,455 reg. │  la clave en un trie y la manda al
                                          │  reducer dueño de su rango (nunca hash).
                       ┌──────────────────┴──────────────────┐
                       │ SHUFFLE: cada reducer jala su rango  │
                       │ de TODOS los mappers (2 × 2 = 4      │
                       │ "Shuffled Maps") y se ordena por     │
                       │ clave. Es la fase más cara.          │
                       └─────────┬───────────────────┬────────┘
                    clave < corte│                   │clave ≥ corte
                                 ▼                   ▼
                            reducer 0           reducer 1      Reduce: identidad,
                                 │                   │         escribe tal cual llega
                                 ▼                   ▼
                          part-r-00000         part-r-00001
                          250,312 reg.         249,688 reg.    (diferencia 0.12 %)
                                 └─────────┬─────────┘
                                           ▼
                    teravalidate: cada archivo en orden + último de uno < primero
                    del siguiente + checksum  ──►  checksum 3d1575c41c3d8
```

| Fase | Entra | Sale |
|---|---|---|
| **Muestreo** | Una muestra de claves del input (100,000 en nuestra corrida) | N−1 puntos de corte en `_partition.lst` |
| **Map** | Un *split* del archivo de entrada | Pares (clave de 10 bytes, valor de 90 bytes) sin cambios |
| **Partición** | Cada clave | El número de reducer según su rango |
| **Shuffle/Sort** | Las salidas de todos los mappers | Por cada reducer, sus claves ordenadas |
| **Reduce** | Claves ya ordenadas | Un archivo `part-r-0000N` por reducer (identidad) |
| **Validación** | Los `part-r-*` | Checksum, más una línea por cada error de orden (0 = correcto) |

### Anatomía de un registro (100 bytes)

```
| 10 bytes: clave binaria aleatoria | 00 11 | 32 bytes: número de fila en hex (ASCII) | 88 99 aa bb | 48 bytes de relleno | cc dd ee ff |
|<-------------- clave ------------>|<----------------------------------- valor (90 bytes) -------------------------------------------->|
```

Por eso `head part-r-00000` muestra caracteres raros: la clave no es texto. Curiosidad: la clave de la fila 0 es `4a 69 6d 47 72 61 79 52 49 50`, que en ASCII dice `JimGrayRIP`, un homenaje a Jim Gray, creador del *sort benchmark* (ver `resultados/hex-entrada-primeros-3.txt`).

## 3. Requisitos

- El contenedor **`hadoop`** de la práctica *Hadoop Standalone en contenedor Debian*: Debian 12, Java 17 y Hadoop 3.3.6 en `/opt/hadoop`.
- Git en tu computadora.
- Unos 300 MB libres dentro del contenedor.

No hay que copiar datos de entrada: `teragen` los genera dentro del contenedor (ver [`datos/README.md`](datos/README.md)).

> 🚫 **Nunca ejecutes `docker rm hadoop`.** Borra el contenedor con Hadoop, Java y toda la configuración.

## 4. Reproducción rápida con el script

💻 Clona el repositorio y entra a la carpeta:

```bash
git clone https://github.com/XxRaliugaRdzxX/bda-equipo-6-terasort.git
cd bda-equipo-6-terasort
```

💻 Enciende el contenedor (si ya está encendido no pasa nada) y copia el script a **`/tmp/ejecutar.sh`** dentro de él:

```bash
docker start hadoop
docker cp scripts/ejecutar.sh hadoop:/tmp/ejecutar.sh
```

💻 Entra al contenedor:

```bash
docker exec -it hadoop bash
```

🐳 Ya dentro, corre el script. Tarda unos 15 segundos:

```bash
bash /tmp/ejecutar.sh
```

El script exporta las variables de entorno, borra salidas anteriores, corre `teragen`, `terasort` con 1 y con 2 reducers y `teravalidate` sobre ambas salidas. Al final imprime un resumen y un veredicto:

```
✅ Todo en orden: mismo checksum (3d1575c41c3d8) con 1 y 2 reducers, 0 errores.
```

🐳 Sal del contenedor:

```bash
exit
```

💻 Copia tu evidencia a la computadora y compárala con la nuestra. Solo deben cambiar los tiempos y las fechas:

```bash
mkdir -p mis-resultados
docker cp hadoop:/tmp/resultados-terasort/. mis-resultados/
diff <(sed '/^Tiempos/,$d' resultados/resumen.txt) <(sed '/^Tiempos/,$d' mis-resultados/resumen.txt) && echo "✅ Igual que el equipo 6"
```

## 5. Reproducción manual, comando por comando

Son los mismos pasos que hace el script; los usamos en la demo en vivo.

💻 Entra al contenedor:

```bash
docker start hadoop
docker exec -it hadoop bash
```

🐳 **Paso 1. Preparar el entorno.** Al reiniciar el contenedor las variables se pierden, así que hay que exportarlas otra vez:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 HADOOP_HOME=/opt/hadoop
export PATH=$PATH:$HADOOP_HOME/bin && cd $HADOOP_HOME/share/hadoop/mapreduce
hadoop version        # debe decir Hadoop 3.3.6
```

🐳 **Paso 2. Limpiar.** Hadoop se niega a escribir en una carpeta de salida que ya existe:

```bash
rm -rf /tmp/tera-input /tmp/tera-out-1r /tmp/tera-out-2r /tmp/tera-val-1r /tmp/tera-val-2r
```

🐳 **Paso 3. Generar 500,000 registros (≈ 48 MB):**

```bash
hadoop jar hadoop-mapreduce-examples-3.3.6.jar teragen 500000 /tmp/tera-input
ls -l /tmp/tera-input/                       # part-m-00000 de 50000000 bytes
echo $(( $(stat -c %s /tmp/tera-input/part-m-00000) / 100 ))   # 500000 registros
```

En los contadores del job busca `CHECKSUM=1074598070305752`.

🐳 **Paso 4. Ordenar con 1 reducer:**

```bash
hadoop jar hadoop-mapreduce-examples-3.3.6.jar terasort -Dmapreduce.job.reduces=1 /tmp/tera-input /tmp/tera-out-1r
```

🐳 **Paso 5. Ordenar con 2 reducers.** Fíjate en el contador `Shuffled Maps =4`, que es 2 mappers × 2 reducers:

```bash
hadoop jar hadoop-mapreduce-examples-3.3.6.jar terasort -Dmapreduce.job.reduces=2 /tmp/tera-input /tmp/tera-out-2r
ls -lh /tmp/tera-out-2r/
```

Esperado: `_partition.lst` de 11 bytes, más `part-r-00000` y `part-r-00001` de unos 24 MB cada uno.

🐳 **Paso 6. Validar ambas salidas:**

```bash
hadoop jar hadoop-mapreduce-examples-3.3.6.jar teravalidate /tmp/tera-out-1r /tmp/tera-val-1r
hadoop jar hadoop-mapreduce-examples-3.3.6.jar teravalidate /tmp/tera-out-2r /tmp/tera-val-2r
cat /tmp/tera-val-1r/part-r-00000 /tmp/tera-val-2r/part-r-00000
```

Esperado: `checksum	3d1575c41c3d8` en las dos y **ninguna otra línea**. Cualquier línea extra sería un error de orden.

🐳 **Paso 7 (opcional). Comprobar el checksum.** `teragen` lo imprime en decimal y `teravalidate` en hexadecimal; son el mismo número:

```bash
printf '%x\n' 1074598070305752    # 3d1575c41c3d8
```

🐳 **Paso 8 (opcional). Ver el punto de corte y los primeros registros ordenados:**

```bash
od -A d -t x1 /tmp/tera-out-2r/_partition.lst            # 0a + 10 bytes de la clave de corte
od -A d -t x1 -N 300 /tmp/tera-out-2r/part-r-00000        # primeros 3 registros (claves 00 00 3c…, 00 00 61…)
```

## 6. Resultados esperados

La evidencia completa está en [`resultados/`](resultados/) (ver [`resultados/README.md`](resultados/README.md)). Resumen de nuestra corrida del 6 de noviembre de 2026 (Hadoop 3.3.6 standalone, Debian 12, OpenJDK 17.0.21):

| Dato | 1 reducer | 2 reducers |
|---|---|---|
| Registros de entrada | 500,000 | 500,000 |
| Splits (mappers) en `terasort` | 2 (335,545 + 164,455) | 2 (335,545 + 164,455) |
| `Shuffled Maps` | 2 | **4** |
| Archivos de salida | `part-r-00000` (500,000) | `part-r-00000` (250,312) + `part-r-00001` (249,688) |
| `_partition.lst` | 0 bytes (no hay cortes) | 11 bytes (1 corte: `7ff2ca13634a6b2ac574`) |
| Checksum `teravalidate` | `3d1575c41c3d8` | `3d1575c41c3d8` |
| Errores de orden | 0 | 0 |
| Tiempo de `terasort` | ≈ 3.2 s | ≈ 3.2 s |

Qué nos dicen estos números:

- **Mismo checksum con 1 y con 2 reducers:** el contenido es idéntico y no se perdió ni se duplicó ningún registro, solo cambió cómo se repartió.
- **El reparto quedó casi perfecto:** 250,312 frente a 249,688 registros, apenas 0.12 % de diferencia, gracias al muestreo.
- **La frontera es correcta:** la última clave de `part-r-00000` (`7ff2bba4…`) es menor que el punto de corte, y la primera de `part-r-00001` es exactamente el corte (`7ff2ca13…`).
- **2 reducers no fue más rápido.** En standalone todo corre en una sola JVM y los reducers van uno tras otro. La ganancia solo aparece en un clúster real.
- **El input da 2 splits** porque el sistema de archivos local usa bloques de 32 MB, y 48 MB se dividen en uno de 32 MB y otro de 16 MB.

## 7. Problemas comunes

| Síntoma | Causa | Solución |
|---|---|---|
| `bash: hadoop: command not found` | Al reiniciar el contenedor se pierden `JAVA_HOME`, `HADOOP_HOME` y `PATH` (sobre todo como root) | Exportarlas otra vez (paso 1). El script lo hace solo. |
| `Output directory file:/tmp/tera-out-2r already exists` | Hadoop no sobrescribe salidas | `rm -rf` de esa carpeta (paso 2) |
| `head part-r-00000` muestra basura | La clave es binaria | Usar `od -t x1` |
| `wc -l` da un número raro | Los registros son de 100 bytes fijos, no líneas | `tamaño ÷ 100` (`stat -c %s`) |
| `docker: command not found` o "no such container" dentro del contenedor | Estás corriendo `docker` dentro del contenedor | Abre otra terminal en tu computadora 💻 |
| `teravalidate` imprime líneas además de `checksum` | La salida no está en orden global (por ejemplo, la ordenaste con otro particionador) | Volver a correr `terasort` |

## 8. Mini ejercicio

*Tu jefe dice que el job tarda demasiado*: simula con TeraSort el efecto de cambiar el número de reducers.

- Instrucciones: [`ejercicio/ejercicio.md`](ejercicio/ejercicio.md)
- Solución con los valores reales: [`ejercicio/solucion.md`](ejercicio/solucion.md)

## 9. Estructura del repositorio

```
bda-equipo-6-terasort/
├── README.md              ← esta guía
├── datos/
│   └── README.md          ← cómo se generan los datos (teragen) y por qué no se suben
├── scripts/
│   └── ejecutar.sh        ← 🐳 todos los comandos en orden + evidencia + veredicto
├── resultados/            ← salida esperada de nuestra corrida
│   ├── README.md          ← qué es cada archivo
│   ├── resumen.txt
│   ├── teravalidate-1r.txt, teravalidate-2r.txt
│   ├── listado-salidas.txt
│   ├── hex-*.txt
│   ├── tiempos.tsv, version.txt
│   └── logs/              ← log completo de cada job
└── ejercicio/
    ├── ejercicio.md       ← mini ejercicio para compañeros
    └── solucion.md        ← la solución
```

## 10. Fuentes

- Sort Benchmark: <https://sortbenchmark.org/>
- Apache Hadoop, paquete `terasort`: <https://hadoop.apache.org/docs/r3.4.2/api/org/apache/hadoop/examples/terasort/package-summary.html>. El código fuente de la versión que usamos está dentro del contenedor, en `/opt/hadoop/share/hadoop/mapreduce/sources/hadoop-mapreduce-examples-3.3.6-sources.jar`.
- Apache Spark, Daytona GraySort 2014: <https://spark.apache.org/news/spark-wins-daytona-gray-sort-100tb-benchmark>
