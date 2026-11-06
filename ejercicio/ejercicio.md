# Mini ejercicio · Tu jefe dice que el job tarda demasiado

Eres ingeniero de datos en una empresa de sensores IoT. Cada noche se ordenan millones de lecturas por su clave y tu jefe te pide **acelerar el ordenamiento ajustando el número de reducers**. Antes de tocar producción, lo vas a simular con TeraSort en el contenedor.

> 💻 = en tu computadora · 🐳 = dentro del contenedor `hadoop`
> 🚫 Nunca ejecutes `docker rm hadoop`.

## Preparación

💻 Enciende el contenedor y entra:

```bash
docker start hadoop
docker exec -it hadoop bash
```

🐳 Prepara el entorno:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 HADOOP_HOME=/opt/hadoop
export PATH=$PATH:$HADOOP_HOME/bin && cd $HADOOP_HOME/share/hadoop/mapreduce
```

## Parte A · Práctica

Usa el jar `hadoop-mapreduce-examples-3.3.6.jar`. Si alguna carpeta de salida ya existe, bórrala primero con `rm -rf`.

1. Genera **200,000** registros en `/tmp/ejercicio-input`.
2. Ordena con **1 reducer** en `/tmp/ejercicio-out-1r`.
3. Ordena con **3 reducers** en `/tmp/ejercicio-out-3r`. Pista: `-Dmapreduce.job.reduces=3`
4. Valida ambas salidas con `teravalidate`, en `/tmp/ejercicio-val-1r` y `/tmp/ejercicio-val-3r`.

Anota para cada configuración el checksum, el contador `Shuffled Maps` del job (el último que aparece) y el tiempo que tardó.

## Parte B · Análisis

Responde con base en lo que viste en la terminal:

1. ¿Cuántos archivos `part-r-*` genera cada configuración? ¿Por qué?
2. ¿Cuántos `Shuffled Maps` reporta el job en cada caso? ¿De dónde sale ese número? Pista: busca `number of splits` en la salida de `terasort`.
3. ¿Qué contiene `_partition.lst` con 3 reducers? ¿Cuánto mide y por qué? Pista: `od -A d -t x1 /tmp/ejercicio-out-3r/_partition.lst`
4. En standalone, ¿3 reducers son más rápidos que 1? ¿Por qué?
5. En un clúster de 10 nodos con 4 contenedores de reduce por nodo, ¿cuántos reducers usarías para ordenar 10 TB?

**Bonus:** ¿el checksum de 1 reducer es igual al de 3? ¿Qué demuestra eso?

La solución está en [`solucion.md`](solucion.md). Intenta resolverlo antes de abrirla.
