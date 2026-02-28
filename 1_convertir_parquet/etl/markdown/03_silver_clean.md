## **Imports y constantes**


```python
%%configure -f
{
    "executorMemory": "24g",
    "executorCores": 3,
    "numExecutors": 2,
    "driverMemory": "12g",
    "driverCores": 2,
    "conf": {
        "spark.executor.memoryOverhead": "4g",
        "spark.sql.shuffle.partitions": "48",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "134217728",
        "spark.sql.files.maxPartitionBytes": "134217728",
        "spark.sql.parquet.compression.codec": "snappy",
        "spark.sql.parquet.filterPushdown": "true",
        "spark.sql.legacy.timeParserPolicy": "EXCEPTION"
    }
}
```


    StatementMeta(sparkpool01, 11, -1, Finished, Available, Finished)


    Warning: When setting executor and driver size using %%configure the requested size will be mapped to closest available size which may be bigger than requested. Please use "configure session" panel to select directly from available sizes.
    See https://go.microsoft.com/fwlink/?linkid=2170827
    


```python
from pyspark.sql import functions as F
from pyspark.sql.types port StringType, DoubleType, IntegerType, DateType
import time
```


    StatementMeta(sparkpool01, 11, 2, Finished, Available, Finished)



```python
SCHEMA_PATH = "abfss://synapse-fs@stsynapsemetadata.dfs.core.windows.net/scripts/schema.py"
sc.addPyFile(SCHEMA_PATH)
```


    StatementMeta(sparkpool01, 11, 4, Finished, Available, Finished)



```python
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
```


    StatementMeta(sparkpool01, 11, 5, Finished, Available, Finished)



```python
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
```


    StatementMeta(sparkpool01, 11, 6, Finished, Available, Finished)


    Schema cargado OK:
      Drop CAS/CIRT:  39 columnas
      Sentinelas:     8 columnas
      Fechas:         9 → DateType
      Doubles:        20 → DoubleType
      Integers:       14 → IntegerType
      Strings:        29 → StringType (sin cambio)
      Rangos:         13 columnas validadas
      Total Silver:   72 columnas
    

## **Leer Bronze + validar conteo**


```python
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
```


    StatementMeta(sparkpool01, 11, 8, Finished, Available, Finished)


    Bronze leido en 2.6 min
      Filas:    3,174,135,934  (esperado: 3,174,135,934)
      Columnas: 111  (esperado: 111)
    

## **Drop 39 columnas CAS/CIRT (N/A para SF)**


```python
cols_before = len(df.columns)
df = df.drop(*SF_NA_COLUMNS)
cols_after = len(df.columns)

print(f"O1 — Drop CAS/CIRT: {cols_before} → {cols_after} columnas "
      f"(eliminadas: {cols_before - cols_after})")

if cols_after != EXPECTED_SILVER_COLS:
    print(f"  WARNING: Se esperaban {EXPECTED_SILVER_COLS} columnas, quedaron {cols_after}")
else:
    print(f"  OK: {EXPECTED_SILVER_COLS} columnas activas para SF")
```


    StatementMeta(sparkpool01, 9, 12, Finished, Available, Finished)


    O1 — Drop CAS/CIRT: 111 → 72 columnas (eliminadas: 39)
      OK: 72 columnas activas para SF
    

## **Sentinelas → NULL (ANTES del cast numerico)**


```python
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
```


    StatementMeta(sparkpool01, 9, 13, Finished, Available, Finished)


    O2 — Sentinelas → NULL (8 columnas):
      orig_ltv: ['999'] → NULL
      orig_cltv: ['999'] → NULL
      num_borrowers: ['99'] → NULL
      dti: ['999'] → NULL
      borrower_credit_score: ['9999'] → NULL
      co_borrower_credit_score: ['9999'] → NULL
      mi_percentage: ['999'] → NULL
      current_delinquency_status: ['XX'] → NULL
      Numericos son no-op defensivo (EDA: 0 filas).
      Solo XX en current_delinquency_status es activo (30,4M filas).
    

## **Harmonizar BAP + trim + empty strings → NULL**


```python
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
```


    StatementMeta(sparkpool01, 9, 14, Finished, Available, Finished)


    O3+O4 aplicados a 72 columnas (un solo select):
      O3 — BAP harmonizado: Y→F (no-op defensivo, Y no existe en datos)
      O4 — Trim whitespace + empty string → NULL en todas las columnas
    

## **Auditoria de cast: deteccion de cast silencioso en 1 scan**


```python
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
```


    StatementMeta(sparkpool01, 9, 15, Finished, Available, Finished)


    Auditoria de cast completada en 55.1 min
    Total filas: 3,174,135,934
    
    Columna                                                  Pre           Post      Delta        %
    ───────────────────────────────────────────────────────────────────────────────────────────
      monthly_reporting_period                     3,174,135,934  3,174,135,934          0  0.0000%
      origination_date                             3,174,135,933  3,174,135,933          0  0.0000%
      first_payment_date                           3,174,135,933  3,174,135,933          0  0.0000%
      maturity_date                                3,132,707,157  3,132,707,157          0  0.0000%
      zero_balance_effective_date                     41,256,543     41,256,543          0  0.0000%
      last_paid_installment_date                         670,507        670,507          0  0.0000%
      foreclosure_date                                   670,581        670,581          0  0.0000%
      disposition_date                                   664,433        664,433          0  0.0000%
      io_first_pi_payment_date                               206            206          0  0.0000%
      orig_interest_rate                           3,174,135,402  3,174,135,402          0  0.0000%
      current_interest_rate                        3,132,915,707  3,132,915,707          0  0.0000%
      orig_upb                                     3,174,135,933  3,174,135,933          0  0.0000%
      current_actual_upb                           3,174,135,851  3,174,135,851          0  0.0000%
      mi_percentage                                  592,150,658    592,150,658          0  0.0000%
      upb_at_time_of_removal                          41,254,981     41,254,981          0  0.0000%
      total_principal_current                        902,460,893    902,460,893          0  0.0000%
      foreclosure_costs                                  652,773        652,773          0  0.0000%
      property_preservation_repair_costs                 596,000        596,000          0  0.0000%
      asset_recovery_costs                               516,053        516,053          0  0.0000%
      misc_holding_expenses_credits                      628,134        628,134          0  0.0000%
      associated_taxes_holding_property                  615,281        615,281          0  0.0000%
      net_sales_proceeds                                 652,014        652,014          0  0.0000%
      credit_enhancement_proceeds                        473,403        473,403          0  0.0000%
      repurchase_make_whole_proceeds                     414,123        414,123          0  0.0000%
      other_foreclosure_proceeds                         524,311        524,311          0  0.0000%
      modification_related_ni_upb                     45,645,180     45,645,180          0  0.0000%
      principal_forgiveness_amount                    27,133,318     27,133,318          0  0.0000%
      foreclosure_principal_writeoff                  27,142,390     27,142,390          0  0.0000%
      total_deferral_amount                           15,908,121     15,908,121          0  0.0000%
      orig_loan_term                               3,174,135,933  3,174,135,933          0  0.0000%
      loan_age                                     3,132,879,391  3,132,879,391          0  0.0000%
      remaining_months_legal_maturity              3,132,707,158  3,132,707,158          0  0.0000%
      remaining_months_to_maturity                 3,085,951,146  3,085,951,146          0  0.0000%
      orig_ltv                                     3,174,135,675  3,174,135,675          0  0.0000%
      orig_cltv                                    3,163,007,139  3,163,007,139          0  0.0000%
      num_borrowers                                3,173,741,311  3,173,741,311          0  0.0000%
      dti                                          3,127,477,999  3,127,477,999          0  0.0000%
      borrower_credit_score                        3,164,828,030  3,164,828,030          0  0.0000%
      co_borrower_credit_score                     1,540,659,887  1,540,659,887          0  0.0000%
      current_delinquency_status                   3,143,731,847  3,143,731,847          0  0.0000%
      num_units                                    3,174,135,933  3,174,135,933          0  0.0000%
      months_to_amortization                                   0              0          0  0.0000%
      alternative_delinquency_resolution_count        16,321,568     16,321,568          0  0.0000%
    
    Total cast failures: 0 (0.000000%)
    OK: Cero cast failures — todos los valores non-null castearon correctamente.
    
    >>> REVISAR resultados antes de ejecutar Celda 7 (cast real). <<<
    >>> Si hay cast failures inesperados, investigar ANTES de continuar. <<<
    

## **Cast de todos los tipos en un unico select()**


```python
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
```


    StatementMeta(sparkpool01, 9, 16, Finished, Available, Finished)


    O5+O6+O7 — Cast aplicado en un unico select() (72 columnas):
      9 fechas → DateType (to_date MMyyyy)
      20 doubles → DoubleType
      14 ints → IntegerType
      29 strings → sin cambio
    

## **Validacion de rangos (13 columnas)**


```python
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
```


    StatementMeta(sparkpool01, 9, 17, Finished, Available, Finished)


    O8 — Rangos validados (13 columnas):
      Columna                                     Rango
      ─────────────────────────────────────────── ───────────────
      orig_interest_rate                          [0, 20]
      current_interest_rate                       [0, 20]
      orig_loan_term                              [1, 480]
      loan_age                                    [-5, 600]
      orig_ltv                                    [1, 200]
      orig_cltv                                   [1, 200]
      num_borrowers                               [1, 10]
      dti                                         [0, 65]
      borrower_credit_score                       [300, 850]
      co_borrower_credit_score                    [300, 850]
      num_units                                   [1, 4]
      mi_percentage                               [0, 60]
      current_delinquency_status                  [0, 99]
      Valores fuera de rango → NULL
    

## **Schema enforcement**


```python
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
```


    StatementMeta(sparkpool01, 9, 18, Finished, Available, Finished)


    Schema enforcement — 72 columnas:
      OK: Todos los tipos coinciden con lo esperado
      72 columnas tipadas correctamente
    
      Distribucion de tipos:
        date         9 columnas
        double      20 columnas
        int         14 columnas
        string      29 columnas
    

## **Escribir Silver Parquet**


```python
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
```


    StatementMeta(sparkpool01, 9, 19, Submitted, Running, Running)


    Escribiendo Silver Parquet...
      Destino: abfss://synapse-fs@stsynapsemetadata.dfs.core.windows.net/silver/
    

## **Validacion después de la escritura**


```python
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
```


    StatementMeta(sparkpool01, 11, 9, Finished, Available, Finished)


    Validando Silver escrito...
    
    Validacion completada en 0.6 min
    
    1. Conteo de filas:
       Silver: 3,174,135,934
       Bronze: 3,174,135,934
       OK: Silver == Bronze (0 filas perdidas)
    
    2. Particiones: 101
       Primera: 2000Q1  |  Ultima: 2025Q1
       OK: 101 particiones (2000Q1 — 2025Q1)
    
    3. Schema: 72 columnas
       date         9 columnas
       double      20 columnas
       int         14 columnas
       string      29 columnas
       OK: 72 columnas
    
    4. Muestra Silver (2000Q1):
    +------------+------------------------+-------+---------------------+------------------+--------------------------+----------------+-----------------+-------------------+
    |loan_id     |monthly_reporting_period|channel|borrower_credit_score|orig_interest_rate|current_delinquency_status|origination_date|zero_balance_code|acquisition_quarter|
    +------------+------------------------+-------+---------------------+------------------+--------------------------+----------------+-----------------+-------------------+
    |412075630226|2007-11-01              |B      |660                  |8.75              |1                         |2000-01-01      |NULL             |2000Q1             |
    |874355421363|2016-04-01              |R      |618                  |8.625             |0                         |2000-01-01      |NULL             |2000Q1             |
    |412075630226|2007-12-01              |B      |660                  |8.75              |1                         |2000-01-01      |NULL             |2000Q1             |
    |874355421363|2016-05-01              |R      |618                  |8.625             |0                         |2000-01-01      |NULL             |2000Q1             |
    |412075630226|2008-01-01              |B      |660                  |8.75              |2                         |2000-01-01      |NULL             |2000Q1             |
    +------------+------------------------+-------+---------------------+------------------+--------------------------+----------------+-----------------+-------------------+
    only showing top 5 rows
    
    Schema Silver completo:
    root
     |-- loan_id: string (nullable = true)
     |-- monthly_reporting_period: date (nullable = true)
     |-- channel: string (nullable = true)
     |-- seller_name: string (nullable = true)
     |-- servicer_name: string (nullable = true)
     |-- orig_interest_rate: double (nullable = true)
     |-- current_interest_rate: double (nullable = true)
     |-- orig_upb: double (nullable = true)
     |-- current_actual_upb: double (nullable = true)
     |-- orig_loan_term: integer (nullable = true)
     |-- origination_date: date (nullable = true)
     |-- first_payment_date: date (nullable = true)
     |-- loan_age: integer (nullable = true)
     |-- remaining_months_legal_maturity: integer (nullable = true)
     |-- remaining_months_to_maturity: integer (nullable = true)
     |-- maturity_date: date (nullable = true)
     |-- orig_ltv: integer (nullable = true)
     |-- orig_cltv: integer (nullable = true)
     |-- num_borrowers: integer (nullable = true)
     |-- dti: integer (nullable = true)
     |-- borrower_credit_score: integer (nullable = true)
     |-- co_borrower_credit_score: integer (nullable = true)
     |-- first_time_buyer: string (nullable = true)
     |-- loan_purpose: string (nullable = true)
     |-- property_type: string (nullable = true)
     |-- num_units: integer (nullable = true)
     |-- occupancy_status: string (nullable = true)
     |-- property_state: string (nullable = true)
     |-- msa: string (nullable = true)
     |-- zip_code_short: string (nullable = true)
     |-- mi_percentage: double (nullable = true)
     |-- amortization_type: string (nullable = true)
     |-- prepayment_penalty_indicator: string (nullable = true)
     |-- interest_only_indicator: string (nullable = true)
     |-- io_first_pi_payment_date: date (nullable = true)
     |-- months_to_amortization: integer (nullable = true)
     |-- current_delinquency_status: integer (nullable = true)
     |-- loan_payment_history: string (nullable = true)
     |-- modification_flag: string (nullable = true)
     |-- zero_balance_code: string (nullable = true)
     |-- zero_balance_effective_date: date (nullable = true)
     |-- upb_at_time_of_removal: double (nullable = true)
     |-- total_principal_current: double (nullable = true)
     |-- last_paid_installment_date: date (nullable = true)
     |-- foreclosure_date: date (nullable = true)
     |-- disposition_date: date (nullable = true)
     |-- foreclosure_costs: double (nullable = true)
     |-- property_preservation_repair_costs: double (nullable = true)
     |-- asset_recovery_costs: double (nullable = true)
     |-- misc_holding_expenses_credits: double (nullable = true)
     |-- associated_taxes_holding_property: double (nullable = true)
     |-- net_sales_proceeds: double (nullable = true)
     |-- credit_enhancement_proceeds: double (nullable = true)
     |-- repurchase_make_whole_proceeds: double (nullable = true)
     |-- other_foreclosure_proceeds: double (nullable = true)
     |-- modification_related_ni_upb: double (nullable = true)
     |-- principal_forgiveness_amount: double (nullable = true)
     |-- mi_type: string (nullable = true)
     |-- servicing_activity_indicator: string (nullable = true)
     |-- special_eligibility_program: string (nullable = true)
     |-- foreclosure_principal_writeoff: double (nullable = true)
     |-- relocation_mortgage_indicator: string (nullable = true)
     |-- property_valuation_method: string (nullable = true)
     |-- high_balance_loan_indicator: string (nullable = true)
     |-- borrower_assistance_plan: string (nullable = true)
     |-- hltv_refi_option_indicator: string (nullable = true)
     |-- repurchase_make_whole_proceeds_flag: string (nullable = true)
     |-- alternative_delinquency_resolution: string (nullable = true)
     |-- alternative_delinquency_resolution_count: integer (nullable = true)
     |-- total_deferral_amount: double (nullable = true)
     |-- payment_deferral_mod_event_indicator: string (nullable = true)
     |-- acquisition_quarter: string (nullable = true)
    
    =================================================================
      SILVER COMPLETADO
      Filas:       3,174,135,934
      Columnas:    72
      Particiones: 101
      Ubicacion:   abfss://synapse-fs@stsynapsemetadata.dfs.core.windows.net/silver/
    =================================================================
    
