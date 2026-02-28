"""
02_silver_clean.py — Limpieza Silver: Bronze Parquet → Silver Parquet tipado

Pipeline:  Bronze (StringType, 111 cols, 3.174.135.934 filas)
        → O1: Drop 39 columnas CAS/CIRT (N/A para SF)
        → O2: Sentinelas → NULL (XX activo; numericos = no-op defensivo)
        → O3+O4: Harmonizar BAP (Y→F, no-op) + trim + empty strings → NULL
        → O5+O6+O7: Cast tipos: 9 fechas, 20 doubles, 14 ints (select unico)
        → O8: Validar rangos: 13 columnas → NULL si fuera de rango
        → O9: Silver Parquet particionado por acquisition_quarter (72 cols)

Input:  abfss://synapse-fs@stsynapsemetadata.dfs.core.windows.net/bronze/
Output: abfss://synapse-fs@stsynapsemetadata.dfs.core.windows.net/silver/

Ejecucion: Copiar cada CELDA en una celda separada del notebook Synapse.
           CELDA 0 (%%configure) DEBE ser la primera celda.
           Estimado: ~30-45 min, ~$2-4 USD.

Prerequisitos:
    - Bronze completado (01_bronze_ingest.py): 3.174.135.934 filas, 101 particiones
    - schema.py subido a ADLS: synapse-fs/scripts/schema.py
    - EDA ejecutado (02_eda_bronze_quality.py): validaciones pre-Silver OK

Validado por: EDA sobre Bronze completo (2026-02-20). Ver context/VACIOS_SILVER.md.
Especificacion: context/VACIOS_SILVER.md §1 + §5
"""


# ══════════════════════════════════════════════════════════════════════════════
# CELDA 0 — Configuracion Spark (%%configure)
#
# Crear una celda PySpark al inicio del notebook y pegar SOLO lo siguiente:
#
# %%configure -f
# {
#     "executorMemory": "24g",
#     "executorCores": 3,
#     "numExecutors": 2,
#     "driverMemory": "12g",
#     "driverCores": 2,
#     "conf": {
#         "spark.executor.memoryOverhead": "4g",
#         "spark.sql.shuffle.partitions": "48",
#         "spark.sql.adaptive.enabled": "true",
#         "spark.sql.adaptive.coalescePartitions.enabled": "true",
#         "spark.sql.adaptive.skewJoin.enabled": "true",
#         "spark.sql.adaptive.advisoryPartitionSizeInBytes": "134217728",
#         "spark.sql.files.maxPartitionBytes": "134217728",
#         "spark.sql.parquet.compression.codec": "snappy",
#         "spark.sql.parquet.filterPushdown": "true",
#         "spark.sql.legacy.timeParserPolicy": "EXCEPTION"
#     }
# }
#
# Cambios vs Bronze: driverMemory 6g→12g, driverCores 2, memoryOverhead 3g→4g.
# timeParserPolicy=EXCEPTION: garantiza que to_date() usa DateTimeFormatter
# (no SimpleDateFormat) para parsear "MMyyyy" correctamente en Spark 3.5.
# El driver mas grande es necesario para el plan Catalyst mas complejo (72 cols
# con transformaciones) y para materializar resultados de la auditoria de cast.
# ══════════════════════════════════════════════════════════════════════════════


# ══════════════════════════════════════════════════════════════════════════════
# CELDA 1 — Imports y constantes
# ══════════════════════════════════════════════════════════════════════════════

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, IntegerType, DateType
import time

# ── Cargar schema.py desde ADLS ──────────────────────────────────────────────
# Prerequisito: subir notebooks/etl/scripts/schema.py a ADLS una vez:
#   Azure Portal → stsynapsemetadata → synapse-fs → crear carpeta scripts/ →
#   Upload schema.py
# Alternativa: ejecutar el contenido de schema.py como celda separada antes.
SCHEMA_PATH = "abfss://synapse-fs@stsynapsemetadata.dfs.core.windows.net/scripts/schema.py"
sc.addPyFile(SCHEMA_PATH)

from schema import (
    SF_NA_COLUMNS,
    SENTINEL_VALUES,
    BAP_HARMONIZATION,
    DATE_COLUMNS,
    DATE_COLUMNS_SPARSE,
    NUMERIC_DOUBLE_COLUMNS,
    NUMERIC_INT_COLUMNS,
    STRING_COLUMNS,
    SILVER_RANGE_VALIDATIONS,
    ADLS_PATHS,
)

# ── Constantes derivadas ─────────────────────────────────────────────────────
ALL_DATE_COLUMNS = DATE_COLUMNS + DATE_COLUMNS_SPARSE  # 8 + 1 = 9
EXPECTED_BRONZE_COUNT = 3_174_135_934
EXPECTED_SILVER_COLS = 72  # 110 - 39 CAS/CIRT + 1 acquisition_quarter

# Sets para O(1) lookup en expresiones select()
_SENTINEL_COLS = set(SENTINEL_VALUES.keys())
_DATE_SET = set(ALL_DATE_COLUMNS)
_DOUBLE_SET = set(NUMERIC_DOUBLE_COLUMNS)
_INT_SET = set(NUMERIC_INT_COLUMNS)
_RANGE_SET = set(SILVER_RANGE_VALIDATIONS.keys())

# ── Validar consistencia de las listas de tipos ──────────────────────────────
all_typed = ALL_DATE_COLUMNS + NUMERIC_DOUBLE_COLUMNS + NUMERIC_INT_COLUMNS + STRING_COLUMNS
dupes = [x for x in all_typed if all_typed.count(x) > 1]
assert len(dupes) == 0, f"Columnas duplicadas en listas de tipos: {set(dupes)}"
assert len(set(all_typed)) == EXPECTED_SILVER_COLS, \
    f"Listas definen {len(set(all_typed))} cols, esperado {EXPECTED_SILVER_COLS}"

print(f"Schema cargado OK:")
print(f"  Drop CAS/CIRT:  {len(SF_NA_COLUMNS)} columnas")
print(f"  Sentinelas:     {len(SENTINEL_VALUES)} columnas")
print(f"  Fechas:         {len(ALL_DATE_COLUMNS)} → DateType")
print(f"  Doubles:        {len(NUMERIC_DOUBLE_COLUMNS)} → DoubleType")
print(f"  Integers:       {len(NUMERIC_INT_COLUMNS)} → IntegerType")
print(f"  Strings:        {len(STRING_COLUMNS)} → StringType (sin cambio)")
print(f"  Rangos:         {len(SILVER_RANGE_VALIDATIONS)} columnas validadas")
print(f"  Total Silver:   {EXPECTED_SILVER_COLS} columnas")


# ══════════════════════════════════════════════════════════════════════════════
# CELDA 2 — Leer Bronze + validar conteo
# ══════════════════════════════════════════════════════════════════════════════

t0 = time.time()

df = spark.read.parquet(ADLS_PATHS["bronze"])
bronze_count = df.count()

t_read = time.time() - t0

print(f"Bronze leido en {t_read / 60:.1f} min")
print(f"  Filas:    {bronze_count:,}  (esperado: {EXPECTED_BRONZE_COUNT:,})")
print(f"  Columnas: {len(df.columns)}  (esperado: 111)")

if bronze_count != EXPECTED_BRONZE_COUNT:
    print(f"  WARNING: Diferencia de {bronze_count - EXPECTED_BRONZE_COUNT:,} filas")
if len(df.columns) != 111:
    print(f"  WARNING: Se esperaban 111 columnas, hay {len(df.columns)}")


# ══════════════════════════════════════════════════════════════════════════════
# CELDA 3 — O1: Drop 39 columnas CAS/CIRT (N/A para SF)
#
# Estas 39 columnas son exclusivas de los programas CAS/CIRT (transferencia de
# riesgo crediticio) y estan 100% vacias en el dataset SF Loan Performance.
# Validado por EDA Cell 3: todas 100% null.
# Lista: schema.py:SF_NA_COLUMNS
# ══════════════════════════════════════════════════════════════════════════════

cols_before = len(df.columns)
df = df.drop(*SF_NA_COLUMNS)
cols_after = len(df.columns)

print(f"O1 — Drop CAS/CIRT: {cols_before} → {cols_after} columnas "
      f"(eliminadas: {cols_before - cols_after})")

if cols_after != EXPECTED_SILVER_COLS:
    print(f"  WARNING: Se esperaban {EXPECTED_SILVER_COLS} columnas, quedaron {cols_after}")
else:
    print(f"  OK: {EXPECTED_SILVER_COLS} columnas activas para SF")


# ══════════════════════════════════════════════════════════════════════════════
# CELDA 4 — O2: Sentinelas → NULL (ANTES del cast numerico)
#
# Valores centinela que parecen datos validos pero significan "desconocido":
#   - borrower_credit_score:      "9999" → NULL  (0 filas en EDA — no-op)
#   - co_borrower_credit_score:   "9999" → NULL  (0 filas — no-op)
#   - dti:                        "999"  → NULL  (0 filas — no-op)
#   - orig_ltv:                   "999"  → NULL  (0 filas — no-op)
#   - orig_cltv:                  "999"  → NULL  (0 filas — no-op)
#   - mi_percentage:              "999"  → NULL  (0 filas — no-op)
#   - num_borrowers:              "99"   → NULL  (0 filas — no-op)
#   - current_delinquency_status: "XX"   → NULL  (30,4M filas — ACTIVO)
#
# Se usa un unico select() con expresiones agrupadas (no withColumn en loop)
# para generar un solo nodo Project en el plan Catalyst.
# ══════════════════════════════════════════════════════════════════════════════

sentinel_exprs = []
sentinel_log = []

for c in df.columns:
    if c in _SENTINEL_COLS:
        sentinel_exprs.append(
            F.when(F.col(c).isin(SENTINEL_VALUES[c]), None)
            .otherwise(F.col(c))
            .alias(c)
        )
        sentinel_log.append(f"  {c}: {SENTINEL_VALUES[c]} → NULL")
    else:
        sentinel_exprs.append(F.col(c))

df = df.select(*sentinel_exprs)

print(f"O2 — Sentinelas → NULL ({len(SENTINEL_VALUES)} columnas):")
for entry in sentinel_log:
    print(entry)
print("  Numericos son no-op defensivo (EDA: 0 filas).")
print("  Solo XX en current_delinquency_status es activo (30,4M filas).")


# ══════════════════════════════════════════════════════════════════════════════
# CELDA 5 — O3+O4: Harmonizar BAP + trim + empty strings → NULL
#
# O3 — borrower_assistance_plan:
#   Pre-jul 2020: "Y"=forbearance, "N"=no. Post-jul 2020: F/R/T/O/N/7/9.
#   Harmonizacion: Y→F. EDA confirmo que "Y" NO existe (datos desde 2019+).
#   Se mantiene como codigo defensivo.
#
# O4 — Trim + empty strings → NULL:
#   - Remueve whitespace padding residual del CSV pipe-delimited original
#   - Strings vacios explicitos ("") → NULL
#   - Aplica a TODAS las 72 columnas (aun StringType en este punto)
#   - Critico: limpiar ANTES del cast para que la auditoria de cast (Celda 6)
#     solo detecte valores genuinamente no-parseables, no strings vacios.
# ══════════════════════════════════════════════════════════════════════════════

string_exprs = []

for c in df.columns:
    if c == "borrower_assistance_plan":
        # O3: Harmonizar Y→F (no-op defense), luego O4: trim + empty → null
        harmonized = F.when(F.col(c) == "Y", F.lit("F")).otherwise(F.col(c))
        trimmed = F.trim(harmonized)
        string_exprs.append(
            F.when(trimmed == "", None).otherwise(trimmed).alias(c)
        )
    else:
        # O4: Trim + empty string → null
        trimmed = F.trim(F.col(c))
        string_exprs.append(
            F.when(trimmed == "", None).otherwise(trimmed).alias(c)
        )

df = df.select(*string_exprs)

print("O3+O4 aplicados a 72 columnas (un solo select):")
print("  O3 — BAP harmonizado: Y→F (no-op defensivo, Y no existe en datos)")
print("  O4 — Trim whitespace + empty string → NULL en todas las columnas")


# ══════════════════════════════════════════════════════════════════════════════
# CELDA 6 — Auditoria de cast: deteccion de cast silencioso en 1 scan
#
# PySpark castea valores no-parseables a NULL silenciosamente (sin error).
# Esta celda ejecuta "trial casts" dentro de un .agg() para detectar:
#   - Fechas que no parsean con "MMyyyy" → NULL inesperado
#   - Strings que no castean a double/int → NULL silencioso
#
# Metodo: count(col) vs count(col.cast(type)) en el MISMO scan.
#   Pre  = count(col)           → non-nulls antes del cast (StringType)
#   Post = count(col.cast(...)) → non-nulls despues del trial cast
#   Delta = Pre - Post          → valores destruidos por el cast
#
# 43 columnas × 2 metrics + 1 total = 87 expresiones en un .agg().
# Bajo el limite de 100 expresiones del codegen JVM (SPARK-21870).
# ══════════════════════════════════════════════════════════════════════════════

t0 = time.time()

audit_exprs = []

# Fechas: trial to_date(col, "MMyyyy")
for c in ALL_DATE_COLUMNS:
    audit_exprs.append(F.count(c).alias(f"pre_{c}"))
    audit_exprs.append(F.count(F.to_date(F.col(c), "MMyyyy")).alias(f"post_{c}"))

# Doubles: trial cast("double")
for c in NUMERIC_DOUBLE_COLUMNS:
    audit_exprs.append(F.count(c).alias(f"pre_{c}"))
    audit_exprs.append(F.count(F.col(c).cast("double")).alias(f"post_{c}"))

# Ints: trial cast("integer")
for c in NUMERIC_INT_COLUMNS:
    audit_exprs.append(F.count(c).alias(f"pre_{c}"))
    audit_exprs.append(F.count(F.col(c).cast("integer")).alias(f"post_{c}"))

# Total filas (reutiliza el mismo scan)
audit_exprs.append(F.count(F.lit(1)).alias("total_rows"))

# UN UNICO SCAN: Bronze → O1 → O2 → O3+O4 → trial casts → agg
audit_row = df.agg(*audit_exprs).first()

t_audit = time.time() - t0
total_rows = audit_row["total_rows"]
all_cast_cols = ALL_DATE_COLUMNS + NUMERIC_DOUBLE_COLUMNS + NUMERIC_INT_COLUMNS

print(f"Auditoria de cast completada en {t_audit / 60:.1f} min")
print(f"Total filas: {total_rows:,}")
print(f"\n{'Columna':<45} {'Pre':>14} {'Post':>14} {'Delta':>10} {'%':>8}")
print("─" * 91)

cast_failures_total = 0
for c in all_cast_cols:
    pre = audit_row[f"pre_{c}"]
    post = audit_row[f"post_{c}"]
    delta = pre - post
    pct = (delta / total_rows * 100) if total_rows > 0 else 0
    flag = ""
    if delta > 0 and pct > 0.01:
        flag = " ← WARNING"
    elif delta > 0:
        flag = " ← info"
    print(f"  {c:<43} {pre:>14,} {post:>14,} {delta:>10,} {pct:>7.4f}%{flag}")
    cast_failures_total += delta

print(f"\nTotal cast failures: {cast_failures_total:,} "
      f"({cast_failures_total / total_rows * 100:.6f}%)")
if cast_failures_total == 0:
    print("OK: Cero cast failures — todos los valores non-null castearon correctamente.")
else:
    print("REVISAR: Hay valores que el cast convierte a NULL silenciosamente.")
    print("  Causa probable: strings no-numericos en columnas financieras.")
    print("  Accion: inspeccionar las columnas con delta > 0 antes de continuar.")

print("\n>>> REVISAR resultados antes de ejecutar Celda 7 (cast real). <<<")
print(">>> Si hay cast failures inesperados, investigar ANTES de continuar. <<<")


# ══════════════════════════════════════════════════════════════════════════════
# CELDA 7 — O5+O6+O7: Cast de todos los tipos en un unico select()
#
# Un unico select() con todas las expresiones genera UN SOLO nodo Project
# en el plan Catalyst. Esto evita el anti-patron de withColumn() en loop,
# que genera N nodos de proyeccion encadenados y puede causar:
#   - Planes excesivamente complejos (>100 proyecciones)
#   - Degradacion de rendimiento del optimizador Catalyst
#   - En casos extremos, StackOverflowException
#
# Tipos aplicados:
#   - 9 fechas MMYYYY → DateType via to_date(col, "MMyyyy")
#     Spark infiere dia=1 cuando no hay componente de dia en el formato.
#   - 20 doubles (monetarios/porcentuales) → DoubleType
#   - 14 ints (conteos/scores/ratios enteros) → IntegerType
#   - 29 strings (categoricas/flags) → pasan sin cambio
# ══════════════════════════════════════════════════════════════════════════════

cast_exprs = []
cast_counts = {"date": 0, "double": 0, "int": 0, "string": 0}

for c in df.columns:
    if c in _DATE_SET:
        cast_exprs.append(F.to_date(F.col(c), "MMyyyy").alias(c))
        cast_counts["date"] += 1
    elif c in _DOUBLE_SET:
        cast_exprs.append(F.col(c).cast("double").alias(c))
        cast_counts["double"] += 1
    elif c in _INT_SET:
        cast_exprs.append(F.col(c).cast("integer").alias(c))
        cast_counts["int"] += 1
    else:
        cast_exprs.append(F.col(c))
        cast_counts["string"] += 1

df = df.select(*cast_exprs)

print(f"O5+O6+O7 — Cast aplicado en un unico select() ({len(df.columns)} columnas):")
print(f"  {cast_counts['date']} fechas → DateType (to_date MMyyyy)")
print(f"  {cast_counts['double']} doubles → DoubleType")
print(f"  {cast_counts['int']} ints → IntegerType")
print(f"  {cast_counts['string']} strings → sin cambio")


# ══════════════════════════════════════════════════════════════════════════════
# CELDA 8 — O8: Validacion de rangos (13 columnas)
#
# Valores fuera de los rangos definidos en SILVER_RANGE_VALIDATIONS → NULL.
# Se aplica POST-CAST porque las comparaciones requieren tipo numerico.
#
# Rangos expandidos post-EDA (2026-02-20):
#   orig_interest_rate:       [0, 20]   (EDA: max=16,50 legit)
#   current_interest_rate:    [0, 20]   (EDA: min=0,00 post-mod legit)
#   loan_age:                 [-5, 600] (EDA: min=-3, FAQ #30)
#   current_delinquency_status: [0, 99] (00-99 meses, XX ya limpiado)
#
# EDA mostro <500 outliers totales en los 3 rangos originales que se
# expandieron. Con los rangos corregidos, los OOR son ~0.
# ══════════════════════════════════════════════════════════════════════════════

range_exprs = []
range_log = []

for c in df.columns:
    if c in _RANGE_SET:
        lo, hi = SILVER_RANGE_VALIDATIONS[c]
        range_exprs.append(
            F.when((F.col(c) < lo) | (F.col(c) > hi), None)
            .otherwise(F.col(c))
            .alias(c)
        )
        range_log.append(f"  {c:<43} [{lo}, {hi}]")
    else:
        range_exprs.append(F.col(c))

df = df.select(*range_exprs)

print(f"O8 — Rangos validados ({len(SILVER_RANGE_VALIDATIONS)} columnas):")
print(f"  {'Columna':<43} {'Rango'}")
print(f"  {'─' * 43} {'─' * 15}")
for entry in range_log:
    print(entry)
print("  Valores fuera de rango → NULL")


# ══════════════════════════════════════════════════════════════════════════════
# CELDA 9 — Schema enforcement
#
# Verifica que el DataFrame transformado tiene exactamente los tipos esperados.
# Solo inspecciona metadata (df.schema) — NO requiere scan de datos.
# ══════════════════════════════════════════════════════════════════════════════

# Construir mapa de tipos esperados desde las listas de schema.py
expected_types = {}
for c in STRING_COLUMNS:         expected_types[c] = "string"
for c in NUMERIC_DOUBLE_COLUMNS: expected_types[c] = "double"
for c in NUMERIC_INT_COLUMNS:    expected_types[c] = "int"
for c in ALL_DATE_COLUMNS:       expected_types[c] = "date"

# Comparar con schema real
mismatches = []
extra_cols = []
actual_col_names = set()

for field in df.schema.fields:
    actual_col_names.add(field.name)
    actual_type = field.dataType.simpleString()
    expected = expected_types.get(field.name)
    if expected is None:
        extra_cols.append((field.name, actual_type))
    elif actual_type != expected:
        mismatches.append((field.name, actual_type, expected))

# Columnas faltantes
missing_cols = [c for c in expected_types if c not in actual_col_names]

print(f"Schema enforcement — {len(df.schema.fields)} columnas:")

if mismatches:
    print(f"\n  ERRORES de tipo ({len(mismatches)}):")
    for name, actual, expected in mismatches:
        print(f"    {name}: actual={actual}, esperado={expected}")
else:
    print(f"  OK: Todos los tipos coinciden con lo esperado")

if missing_cols:
    print(f"\n  FALTANTES ({len(missing_cols)}):")
    for c in missing_cols:
        print(f"    {c} (tipo esperado: {expected_types[c]})")

if extra_cols:
    print(f"\n  EXTRA no definidas en listas ({len(extra_cols)}):")
    for name, dtype in extra_cols:
        print(f"    {name}: {dtype}")

if not mismatches and not missing_cols:
    print(f"  {len(expected_types)} columnas tipadas correctamente")

# Resumen de tipos
type_summary = {}
for field in df.schema.fields:
    t = field.dataType.simpleString()
    type_summary[t] = type_summary.get(t, 0) + 1
print(f"\n  Distribucion de tipos:")
for t, cnt in sorted(type_summary.items()):
    print(f"    {t:<10} {cnt:>3} columnas")


# ══════════════════════════════════════════════════════════════════════════════
# CELDA 10 — O9: Escribir Silver Parquet
#
# - Particionado por acquisition_quarter (misma estructura que Bronze)
# - Compresion snappy (configurada en %%configure)
# - Modo overwrite: Silver se regenera completo desde Bronze
#
# No se hace reparticion explicita porque:
#   - Bronze ya tiene 1 archivo por particion (101 archivos)
#   - Las transformaciones Silver son map-only (sin shuffle)
#   - El paralelismo de escritura hereda el de lectura (~101 tasks)
#   - Resultado: ~1 archivo por directorio de particion
# ══════════════════════════════════════════════════════════════════════════════

print("Escribiendo Silver Parquet...")
print(f"  Destino: {ADLS_PATHS['silver']}")
t0 = time.time()

df.write \
    .mode("overwrite") \
    .partitionBy("acquisition_quarter") \
    .parquet(ADLS_PATHS["silver"])

t_write = time.time() - t0

print(f"\nSilver escrito en {t_write / 60:.1f} min ({t_write / 3600:.1f} h)")
print(f"  Compresion: snappy")
print(f"  Particionado por: acquisition_quarter")


# ══════════════════════════════════════════════════════════════════════════════
# CELDA 11 — Validacion post-escritura
#
# Re-lee el Silver Parquet y verifica:
#   1. Conteo de filas == Bronze count (0 filas perdidas ni ganadas)
#   2. 101 particiones (una por trimestre de adquisicion)
#   3. Schema correcto (tipos, cantidad de columnas)
#   4. Muestra visual para inspeccion rapida
# ══════════════════════════════════════════════════════════════════════════════

print("Validando Silver escrito...")
t0 = time.time()

df_silver = spark.read.parquet(ADLS_PATHS["silver"])
silver_count = df_silver.count()

t_val = time.time() - t0

# ── 1. Conteo ────────────────────────────────────────────────────────────────
print(f"\nValidacion completada en {t_val / 60:.1f} min")
print(f"\n1. Conteo de filas:")
print(f"   Silver: {silver_count:,}")
print(f"   Bronze: {bronze_count:,}")

if silver_count == bronze_count:
    print(f"   OK: Silver == Bronze (0 filas perdidas)")
else:
    delta = bronze_count - silver_count
    print(f"   WARNING: Diferencia de {delta:,} filas ({delta / bronze_count * 100:.4f}%)")

# ── 2. Particiones ───────────────────────────────────────────────────────────
partitions = (
    df_silver.select("acquisition_quarter")
    .distinct()
    .orderBy("acquisition_quarter")
    .collect()
)
partition_values = [row["acquisition_quarter"] for row in partitions]

print(f"\n2. Particiones: {len(partition_values)}")
print(f"   Primera: {partition_values[0]}  |  Ultima: {partition_values[-1]}")
if len(partition_values) != 101:
    print(f"   WARNING: Se esperaban 101 particiones")
else:
    print(f"   OK: 101 particiones (2000Q1 — 2025Q1)")

# ── 3. Schema ────────────────────────────────────────────────────────────────
print(f"\n3. Schema: {len(df_silver.columns)} columnas")
type_counts = {}
for f_field in df_silver.schema.fields:
    t = f_field.dataType.simpleString()
    type_counts[t] = type_counts.get(t, 0) + 1
for t, cnt in sorted(type_counts.items()):
    print(f"   {t:<10} {cnt:>3} columnas")

if len(df_silver.columns) != EXPECTED_SILVER_COLS:
    print(f"   WARNING: Se esperaban {EXPECTED_SILVER_COLS} columnas")
else:
    print(f"   OK: {EXPECTED_SILVER_COLS} columnas")

# ── 4. Muestra visual ────────────────────────────────────────────────────────
print(f"\n4. Muestra Silver ({partition_values[0]}):")
df_silver.filter(
    df_silver.acquisition_quarter == partition_values[0]
).select(
    "loan_id",
    "monthly_reporting_period",
    "channel",
    "borrower_credit_score",
    "orig_interest_rate",
    "current_delinquency_status",
    "origination_date",
    "zero_balance_code",
    "acquisition_quarter",
).show(5, truncate=False)

# ── 5. Schema completo ──────────────────────────────────────────────────────
print("Schema Silver completo:")
df_silver.printSchema()

# ── Resumen final ────────────────────────────────────────────────────────────
print("=" * 65)
print("  SILVER COMPLETADO")
print(f"  Filas:       {silver_count:,}")
print(f"  Columnas:    {len(df_silver.columns)}")
print(f"  Particiones: {len(partition_values)}")
print(f"  Ubicacion:   {ADLS_PATHS['silver']}")
print("=" * 65)
