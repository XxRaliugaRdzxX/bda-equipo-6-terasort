#!/usr/bin/env bash
# =============================================================================
#  🐳 ejecutar.sh  ·  Equipo 6 · La suite TeraSort (teragen → terasort → teravalidate)
#
#  Este script se ejecuta DENTRO del contenedor "hadoop" (Hadoop 3.3.6 standalone).
#  Corre la suite completa con 1 y con 2 reducers y deja toda la evidencia en
#  /tmp/resultados-terasort, lista para copiarla a tu computadora con docker cp.
#
#  Uso (dentro del contenedor):
#      bash /tmp/ejecutar.sh            # 500,000 registros (lo que usamos nosotros)
#      bash /tmp/ejecutar.sh 200000     # otro número de registros
# =============================================================================
set -euo pipefail

# --- 1. Entorno ---------------------------------------------------------------
# Al reiniciar el contenedor estas variables se pierden (sobre todo como root),
# así que el script las exporta siempre.
export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
export HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
export PATH="$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin"

JAR="$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar"
FILAS="${1:-500000}"
CHECKSUM_ESPERADO="3d1575c41c3d8"   # solo aplica para 500,000 registros

IN=/tmp/tera-input
OUT1=/tmp/tera-out-1r
OUT2=/tmp/tera-out-2r
VAL1=/tmp/tera-val-1r
VAL2=/tmp/tera-val-2r
RES=/tmp/resultados-terasort

if [ ! -f "$JAR" ]; then
  echo "❌ No encuentro $JAR. ¿Estás dentro del contenedor hadoop?" >&2
  exit 1
fi

titulo() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

# Corre un job de Hadoop, guarda su log y mide cuánto tarda.
correr() {
  local nombre="$1"; shift
  titulo "$nombre: hadoop jar ... $*"
  local t0 t1
  t0=$(date +%s.%N)
  hadoop jar "$JAR" "$@" > "$RES/logs/$nombre.log" 2>&1 || {
    echo "❌ Falló $nombre. Últimas líneas del log:" >&2
    tail -n 30 "$RES/logs/$nombre.log" >&2
    exit 1
  }
  t1=$(date +%s.%N)
  awk -v n="$nombre" -v a="$t0" -v b="$t1" 'BEGIN{printf "%s\t%.1f s\n", n, b-a}' >> "$RES/tiempos.tsv"
  echo "   ✓ terminado (log en $RES/logs/$nombre.log)"
}

# Lee un contador del log de un job (por ejemplo "Shuffled Maps").
# En modo local Hadoop imprime primero los contadores de cada tarea y al final
# los del job completo; por eso nos quedamos con la ÚLTIMA aparición.
contador() { { grep -E "^\s+$2=" "$RES/logs/$1.log" || true; } | tail -n 1 | sed -E 's/.*=//' ; }

# --- 2. Limpieza -------------------------------------------------------------
# Hadoop se niega a escribir en una carpeta de salida que ya existe.
titulo "Limpiando salidas anteriores"
rm -rf "$IN" "$OUT1" "$OUT2" "$VAL1" "$VAL2" "$RES"
mkdir -p "$RES/logs"
: > "$RES/tiempos.tsv"
hadoop version | head -n 1 | tee "$RES/version.txt"
java -version 2>&1 | head -n 1 | tee -a "$RES/version.txt"

# --- 3. teragen: generar los datos -------------------------------------------
correr teragen teragen "$FILAS" "$IN"

# --- 4. terasort con 1 reducer y con 2 reducers ------------------------------
correr terasort-1r terasort -Dmapreduce.job.reduces=1 "$IN" "$OUT1"
correr terasort-2r terasort -Dmapreduce.job.reduces=2 "$IN" "$OUT2"

# --- 5. teravalidate sobre cada salida ---------------------------------------
correr teravalidate-1r teravalidate "$OUT1" "$VAL1"
correr teravalidate-2r teravalidate "$OUT2" "$VAL2"

# --- 6. Evidencia ------------------------------------------------------------
titulo "Guardando evidencia en $RES"
{
  echo "### ls -l $IN";   ls -l "$IN"
  echo; echo "### ls -l $OUT1"; ls -l "$OUT1"
  echo; echo "### ls -l $OUT2"; ls -l "$OUT2"
} > "$RES/listado-salidas.txt"

cp "$VAL1/part-r-00000" "$RES/teravalidate-1r.txt"
cp "$VAL2/part-r-00000" "$RES/teravalidate-2r.txt"

# Los registros son de 100 bytes fijos (no son líneas): registros = bytes / 100
registros() { echo $(( $(stat -c %s "$1") / 100 )); }

# Primeros 3 registros (300 bytes) de la entrada y de la salida ordenada, en hexadecimal.
od -A d -t x1 -N 300 "$IN/part-m-00000"   > "$RES/hex-entrada-primeros-3.txt"
od -A d -t x1 -N 300 "$OUT2/part-r-00000" > "$RES/hex-salida-primeros-3.txt"
# Punto de corte que eligió el muestreo para 2 reducers (1 byte de longitud + 10 de clave).
od -A d -t x1 "$OUT2/_partition.lst" > "$RES/hex-partition-lst-2r.txt"

# Frontera entre los dos archivos: la última clave de part-r-00000 debe ser
# menor que la primera de part-r-00001.
TAM0=$(stat -c %s "$OUT2/part-r-00000")
ULTIMA_R0=$(od -A n -t x1 -j $((TAM0 - 100)) -N 10 "$OUT2/part-r-00000" | tr -d ' \n')
PRIMERA_R1=$(od -A n -t x1 -N 10 "$OUT2/part-r-00001" | tr -d ' \n')
CORTE=$(od -A n -t x1 -j 1 -N 10 "$OUT2/_partition.lst" | tr -d ' \n')

CK_DEC=$(contador teragen CHECKSUM)
CK_HEX=$(printf '%x' "$CK_DEC")
CK_VAL1=$(cut -f2 "$VAL1/part-r-00000" | head -n1)
CK_VAL2=$(cut -f2 "$VAL2/part-r-00000" | head -n1)
# teravalidate solo escribe la línea "checksum" si todo está bien; cualquier otra línea es un error.
ERRORES=$(cat "$VAL1/part-r-00000" "$VAL2/part-r-00000" | { grep -vc '^checksum' || true; })

{
  echo "RESUMEN · Equipo 6 · La suite TeraSort"
  echo "======================================"
  cat "$RES/version.txt"
  echo
  echo "teragen"
  echo "  Registros generados (Map output records): $(contador teragen 'Map output records')"
  echo "  Bytes del archivo part-m-00000:           $(stat -c %s "$IN/part-m-00000")"
  echo "  Bytes en disco (part-m-00000 + su .crc):  $(( $(stat -c %s "$IN/part-m-00000") + $(stat -c %s "$IN/.part-m-00000.crc") ))"
  echo "  Archivos part-m-*:                        $(ls "$IN"/part-m-* | wc -l)"
  echo "  CHECKSUM (decimal):                       $CK_DEC"
  echo "  CHECKSUM (hexadecimal):                   $CK_HEX"
  echo
  for r in 1r 2r; do
    echo "terasort-$r"
    echo "  Splits (mappers):        $(grep -m1 -oE 'number of splits:[0-9]+' "$RES/logs/terasort-$r.log" | cut -d: -f2)"
    echo "  Map output por mapper:   $(grep -E '^\s+Map output records=' "$RES/logs/terasort-$r.log" | sed -E 's/.*=//' | head -n -1 | paste -sd' ')"
    echo "  Map input records:       $(contador terasort-$r 'Map input records')"
    echo "  Shuffled Maps:           $(contador terasort-$r 'Shuffled Maps ')"
    echo "  Reduce output records:   $(contador terasort-$r 'Reduce output records')"
  done
  echo
  echo "Registros por archivo de salida (bytes / 100)"
  for f in "$OUT1"/part-r-* "$OUT2"/part-r-*; do
    echo "  $f: $(registros "$f")"
  done
  echo
  echo "Frontera entre reducers (2r)"
  echo "  Punto de corte (_partition.lst):    $CORTE"
  echo "  Última clave de part-r-00000:       $ULTIMA_R0"
  echo "  Primera clave de part-r-00001:      $PRIMERA_R1"
  echo
  echo "teravalidate"
  echo "  checksum 1 reducer:  $CK_VAL1"
  echo "  checksum 2 reducers: $CK_VAL2"
  echo "  líneas de error:     $ERRORES"
  echo
  echo "Tiempos (standalone, una sola JVM)"
  sed 's/^/  /' "$RES/tiempos.tsv"
} > "$RES/resumen.txt"

cat "$RES/resumen.txt"

# --- 7. Veredicto ------------------------------------------------------------
titulo "Veredicto"
OK=1
[ "$CK_VAL1" = "$CK_HEX" ] && [ "$CK_VAL2" = "$CK_HEX" ] || { echo "❌ Los checksums no coinciden"; OK=0; }
[ "$ERRORES" -eq 0 ] || { echo "❌ teravalidate reportó errores de orden"; OK=0; }
[[ "$ULTIMA_R0" < "$PRIMERA_R1" ]] || { echo "❌ La frontera entre archivos no está en orden"; OK=0; }
if [ "$FILAS" = "500000" ] && [ "$CK_HEX" != "$CHECKSUM_ESPERADO" ]; then
  echo "❌ Esperábamos el checksum $CHECKSUM_ESPERADO y salió $CK_HEX"; OK=0
fi
if [ "$OK" -eq 1 ]; then
  echo "✅ Todo en orden: mismo checksum ($CK_HEX) con 1 y 2 reducers, 0 errores."
  echo "   Copia la evidencia a tu computadora con (💻, en otra terminal):"
  echo "   mkdir -p mis-resultados && docker cp hadoop:$RES/. mis-resultados/"
else
  exit 1
fi
