"""
03_gold_subsets.py — Gold Virtual + Subsets: Silver Parquet → Subsets A + B

Pipeline:  Silver (72 cols tipadas, 3.174.135.934 loan-months, 101 particiones)
        → G3: Pre-indicadores (SAI clean, DLQ flags, forbearance, default_age)
        → G4: groupBy("loan_id").agg() — colapso temporal UNICO (~60 expresiones)
        → G5: Features derivadas (is_default, net_loss, vintage_bin, stratum, etc.)
        → G6: Parseo loan_payment_history (4 features de 24 pares × substring)
        → G7: persist(MEMORY_AND_DISK) + validacion Gold virtual
        → G8: Muestreo estratificado Subset A (~50k loans, CSV)
        → G9: Muestreo estratificado Subset B (~500k loans, Parquet)
        → G10: Validacion subsets + SMD + unpersist + resumen

Input:  abfss://synapse-fs@stsynapsemetadata.dfs.core.windows.net/silver/
Output: Subset A (CSV, ~50k loans, 41 cols) → gold/subsets/subset_a/
        Subset B (Parquet, ~500k loans, 51 cols) → gold/subsets/subset_b/

Ejecucion: Copiar cada CELDA en una celda separada del notebook Synapse.
           CELDA 0 (%%configure) DEBE ser la primera celda del notebook.
           Estimado: ~30-60 min, ~$2-4 USD.

Prerequisitos:
    - Silver completado (02_silver_clean.py): 3.174.135.934 filas, 72 cols, 0 cast failures
    - schema.py ACTUALIZADO subido a ADLS: synapse-fs/scripts/schema.py
      (debe incluir constantes Gold: SUBSET_A_COLUMNS, SUBSET_B_EXTRA_COLUMNS, etc.)
    - Sesion Spark disponible (cerrar sesiones previas si 12 vCores ocupados)

Especificacion: context/VACIOS_GOLD.md §1+§5 (actualizada post-auditoria 2026-02-20)
Auditoria:      context/HALLAZGOS_AUDITORIA_GOLD.md (13 secciones, 0 bloqueantes)
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
#         "spark.sql.shuffle.partitions": "600",
#         "spark.sql.adaptive.enabled": "true",
#         "spark.sql.adaptive.coalescePartitions.enabled": "true",
#         "spark.sql.adaptive.skewJoin.enabled": "true",
#         "spark.sql.adaptive.advisoryPartitionSizeInBytes": "134217728",
#         "spark.sql.adaptive.coalescePartitions.parallelismFirst": "false",
#         "spark.sql.files.maxPartitionBytes": "134217728",
#         "spark.sql.parquet.compression.codec": "snappy",
#         "spark.sql.parquet.filterPushdown": "true",
#         "spark.memory.fraction": "0.7",
#         "spark.memory.storageFraction": "0.3"
#     }
# }
#
# Diferencias vs Silver:
#   shuffle.partitions: 48 → 600 (shuffle pesado del groupBy sobre ~57M grupos)
#   memory.fraction: default → 0.7 (mas espacio para ejecucion del shuffle)
#   storageFraction: default → 0.3 (menos storage, no hay broadcast)
#   parallelismFirst: false (respetar tamano de particion, no crear micro-particiones)
# ══════════════════════════════════════════════════════════════════════════════


# ══════════════════════════════════════════════════════════════════════════════
# CELDA 1 — Imports, helpers y constantes
# ══════════════════════════════════════════════════════════════════════════════

from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, substring, greatest
from pyspark import StorageLevel
from functools import reduce
from operator import add
import time

# ── Cargar schema.py desde ADLS ──────────────────────────────────────────────
SCHEMA_PATH = "abfss://synapse-fs@stsynapsemetadata.dfs.core.windows.net/scripts/schema.py"
sc.addPyFile(SCHEMA_PATH)

from schema import (
    DEFAULT_ZBC_CODES,
    VINTAGE_BINS,
    STRATUM_TARGETS_A,
    STRATUM_TARGETS_B,
    SUBSET_A_COLUMNS,
    SUBSET_B_EXTRA_COLUMNS,
    NULL_MEANS_ZERO_COLUMNS,
    ADLS_PATHS,
)

# ── Constantes ────────────────────────────────────────────────────────────────
EXPECTED_SILVER_COUNT = 3_174_135_934
EXPECTED_SILVER_COLS = 72

# Columnas estaticas: constantes por loan_id → first(ignorenulls=True) es seguro
# Excluye loan_id (groupBy key) y columnas sin uso en subsets
STATIC_FIRST_COLS = [
    "acquisition_quarter", "channel", "orig_interest_rate", "orig_upb",
    "orig_loan_term", "origination_date", "orig_ltv", "orig_cltv",
    "num_borrowers", "dti", "borrower_credit_score", "co_borrower_credit_score",
    "first_time_buyer", "loan_purpose", "property_type", "num_units",
    "occupancy_status", "property_state", "mi_percentage", "mi_type",
    "high_balance_loan_indicator",
]

# Expenses (pos 54-58) y Proceeds (pos 59-62) — NULL = $0 (FAQ #46)
_EXPENSE_COLS = NULL_MEANS_ZERO_COLUMNS[:5]
_PROCEED_COLS = NULL_MEANS_ZERO_COLUMNS[5:]


# ── Helpers ───────────────────────────────────────────────────────────────────
def struct_last(col_name, alias_name=None, condition=None):
    """Deterministic 'last' via struct trick: max(struct(date, col))[col].

    Reemplaza F.last() que es NO determinista post-shuffle (SPARK docs:
    "The function is non-deterministic because its results depends on the
    order of the rows which may be non-deterministic after a shuffle.").

    El struct trick compara por fecha (campo 0) y retorna el valor (campo 1)
    de la fila con la fecha mas alta. Completamente determinista.

    Ref: VACIOS_GOLD.md §4.6, HALLAZGOS_AUDITORIA_GOLD.md §1.
    """
    alias = alias_name or col_name
    cond = condition if condition is not None else F.col(col_name).isNotNull()
    return F.max(
        F.when(cond, F.struct("monthly_reporting_period", col_name))
    )[col_name].alias(alias)


def compute_fractions(targets, counts_by_key, counts_by_stratum, vintage_labels):
    """Compute sampleBy fractions for 2-variable stratification (stratum × vintage).

    Distribuye el target de cada estrato proporcionalmente entre las celdas
    de vintage. Todas las fracciones son <= 1.0 (no hay oversampling absoluto).
    Ref: VACIOS_GOLD.md P7+P14.
    """
    fractions = {}
    for stratum, target_n in targets.items():
        stratum_total = counts_by_stratum.get(stratum, 0)
        if stratum_total == 0:
            continue
        for vbin in vintage_labels:
            key = f"{stratum}|{vbin}"
            n_real = counts_by_key.get(key, 0)
            if n_real == 0:
                continue
            frac_vintage = n_real / stratum_total
            target_cell = int(target_n * frac_vintage)
            frac = min(target_cell / n_real, 1.0)
            if frac > 0:
                fractions[key] = frac
    return fractions


# ── Validar imports ───────────────────────────────────────────────────────────
print("Schema cargado OK:")
print(f"  DEFAULT_ZBC_CODES:      {DEFAULT_ZBC_CODES}")
print(f"  VINTAGE_BINS:           {list(VINTAGE_BINS.keys())}")
print(f"  STRATUM_TARGETS_A:      {STRATUM_TARGETS_A}")
print(f"  SUBSET_A_COLUMNS:       {len(SUBSET_A_COLUMNS)} columnas")
print(f"  SUBSET_B_EXTRA_COLUMNS: {len(SUBSET_B_EXTRA_COLUMNS)} columnas")
print(f"  Total Subset B:         {len(SUBSET_A_COLUMNS) + len(SUBSET_B_EXTRA_COLUMNS)} columnas")
print(f"  Expenses (NULL=$0):     {len(_EXPENSE_COLS)} columnas")
print(f"  Proceeds (NULL=$0):     {len(_PROCEED_COLS)} columnas")
print(f"  Static first() cols:    {len(STATIC_FIRST_COLS)}")


# ══════════════════════════════════════════════════════════════════════════════
# CELDA 2 — Leer Silver + validar conteo
# ══════════════════════════════════════════════════════════════════════════════

t0 = time.time()

df = spark.read.parquet(ADLS_PATHS["silver"])
silver_count = df.count()

t_read = time.time() - t0

print(f"Silver leido en {t_read / 60:.1f} min")
print(f"  Filas:    {silver_count:,}  (esperado: {EXPECTED_SILVER_COUNT:,})")
print(f"  Columnas: {len(df.columns)}  (esperado: {EXPECTED_SILVER_COLS})")

if silver_count != EXPECTED_SILVER_COUNT:
    print(f"  WARNING: Diferencia de {silver_count - EXPECTED_SILVER_COUNT:,} filas")
if len(df.columns) != EXPECTED_SILVER_COLS:
    print(f"  WARNING: Se esperaban {EXPECTED_SILVER_COLS} columnas, hay {len(df.columns)}")


# ══════════════════════════════════════════════════════════════════════════════
# CELDA 3 — Pre-computar indicadores (ANTES del groupBy)
#
# Estas columnas row-level se crean sobre el Silver (3,17B filas) como
# transformaciones lazy (sin materializar). Se consumen en el groupBy (G4)
# mediante sum(), max(), min().
#
# dlq_clean: Limpia contaminacion de DLQ por cambio de servicer (SAI='Y').
#   Ref: FAQ #33, HALLAZGOS_AUDITORIA_GOLD.md §5.
#   Cuando SAI='Y', el servicer reporta DLQ artificial porque el pago esta
#   "en transito" entre servicers. dlq_clean neutraliza este ruido.
# ══════════════════════════════════════════════════════════════════════════════

# --- SAI: limpiar contaminacion de DLQ por cambio de servicer ---
df = df.withColumn("dlq_clean",
    when((col("servicing_activity_indicator") == "Y")
         & (col("current_delinquency_status") > 0), 0
    ).otherwise(col("current_delinquency_status")))

# --- Indicadores row-level para agregacion en groupBy ---
df = (df
    .withColumn("had_servicing_transfer",
        when(col("servicing_activity_indicator") == "Y", 1).otherwise(0))
    .withColumn("is_d30plus", when(col("dlq_clean") >= 1, 1).otherwise(0))
    .withColumn("is_d60plus", when(col("dlq_clean") >= 2, 1).otherwise(0))
    .withColumn("is_d90plus", when(col("dlq_clean") >= 3, 1).otherwise(0))
    .withColumn("d1_age",
        when(col("dlq_clean") >= 1, col("loan_age")))
    .withColumn("has_forbearance",
        when(col("borrower_assistance_plan").isin("F", "R", "T", "O"), 1).otherwise(0))
    .withColumn("default_age",
        when(col("zero_balance_code").isin(*DEFAULT_ZBC_CODES), col("loan_age")))
)

print("G3 — Pre-indicadores computados (lazy, sin materializar):")
print("  dlq_clean:               DLQ limpio de contaminacion SAI")
print("  had_servicing_transfer:  1 si SAI='Y' en este mes")
print("  is_d30/d60/d90plus:      Flags DLQ >= 1/2/3 (sobre dlq_clean)")
print("  d1_age:                  loan_age cuando dlq_clean >= 1 (para time_to_first_dlq)")
print("  has_forbearance:         1 si BAP in (F,R,T,O) en este mes")
print("  default_age:             loan_age cuando ZBC in default codes")


# ══════════════════════════════════════════════════════════════════════════════
# CELDA 4 — groupBy("loan_id").agg(...) — colapso temporal UNICO
#
# Un unico groupBy sobre 3,17B loan-months → ~57M loans.
# ~49 expresiones de agregacion, bien dentro del limite seguro de 80.
#
# Patrones usados:
#   - first(col, ignorenulls=True): atributos estaticos constantes por loan_id
#   - max/sum/min: agregaciones comportamentales (deterministicas por naturaleza)
#   - struct trick: campos temporalmente ultimos (determinista, reemplaza last())
#
# NO se usa F.last() — es no determinista post-shuffle. Ver VACIOS_GOLD.md §4.6.
# NO se usa sortWithinPartitions — el groupBy shuffle destruye el orden.
# ══════════════════════════════════════════════════════════════════════════════

agg_exprs = []

# ── 1. Atributos estaticos: first(ignorenulls=True) — 21 expresiones ─────────
# Estos campos son constantes por loan_id (atributos de originacion).
# first() retorna cualquier valor non-null del grupo, lo cual es correcto
# porque todos los valores son identicos para un mismo loan.
for c in STATIC_FIRST_COLS:
    agg_exprs.append(F.first(c, ignorenulls=True).alias(c))

# ── 2. Agregaciones comportamentales — 12 expresiones ────────────────────────
# Todas usan dlq_clean (limpio de SAI) en lugar de current_delinquency_status.
agg_exprs.extend([
    F.max("dlq_clean").alias("max_delinquency_status"),
    F.sum("is_d30plus").alias("months_delinquent_30plus"),
    F.sum("is_d60plus").alias("months_delinquent_60plus"),
    F.sum("is_d90plus").alias("months_delinquent_90plus"),
    F.max(when(col("modification_flag") == "Y", 1).otherwise(0)).alias("ever_modified"),
    F.max("has_forbearance").alias("had_forbearance"),
    F.max("loan_age").alias("loan_duration_months"),
    F.min("d1_age").alias("time_to_first_delinquency"),
    F.max("had_servicing_transfer").alias("had_servicing_transfer"),
    F.max(when(col("dlq_clean") >= 3, 1).otherwise(0)).alias("ever_d90"),
    F.max(when(col("dlq_clean") >= 6, 1).otherwise(0)).alias("ever_d180"),
    F.min("default_age").alias("time_to_default"),
])

# ── 3. Struct trick: campos de liquidacion + temporal — 7 expresiones ────────
# El struct trick captura el valor de la fila con la fecha mas reciente.
agg_exprs.extend([
    struct_last("zero_balance_code", "final_zero_balance_code"),
    struct_last("disposition_date", "final_disposition_date"),
    struct_last("foreclosure_date", "final_foreclosure_date"),
    struct_last("current_interest_rate", "last_interest_rate"),
    struct_last("current_actual_upb", "last_active_upb",
                condition=col("current_actual_upb") > 0),
    struct_last("loan_payment_history", "last_loan_payment_history"),
    struct_last("upb_at_time_of_removal", "final_upb_at_removal"),
])

# ── 4. Expenses/proceeds via struct trick — 9 expresiones ────────────────────
# FAQ #47: se actualizan progresivamente post-disposition.
# Struct trick captura el valor acumulativo mas reciente (correcto).
for c in NULL_MEANS_ZERO_COLUMNS:
    agg_exprs.append(struct_last(c))

print(f"G4 — groupBy con {len(agg_exprs)} expresiones de agregacion:")
print(f"  {len(STATIC_FIRST_COLS)} first() para atributos estaticos")
print(f"  12 agregaciones comportamentales (dlq_clean)")
print(f"  7 struct trick (liquidacion + temporal)")
print(f"  {len(NULL_MEANS_ZERO_COLUMNS)} struct trick (expenses/proceeds)")

t0 = time.time()

gold = df.groupBy("loan_id").agg(*agg_exprs)

print(f"  Plan Catalyst generado (lazy). Total: {len(agg_exprs)} expresiones.")


# ══════════════════════════════════════════════════════════════════════════════
# CELDA 5 — Features derivadas post-colapso
#
# Estas columnas se computan sobre el Gold virtual (~57M filas).
# Son transformaciones lazy que no materializan datos.
# ══════════════════════════════════════════════════════════════════════════════

# --- Metadata derivada ---
gold = gold.withColumn("origination_year", F.year("origination_date"))

# vintage_bin: clasificacion temporal por epoca del mercado hipotecario
vintage_expr = None
for label, (lo, hi) in VINTAGE_BINS.items():
    cond = (col("origination_year") >= lo) & (col("origination_year") <= hi)
    if vintage_expr is None:
        vintage_expr = when(cond, label)
    else:
        vintage_expr = vintage_expr.when(cond, label)
gold = gold.withColumn("vintage_bin", vintage_expr)

# --- Target: is_default ---
# Default = ZBC IN (02, 03, 09, 15) AND disposition_date IS NOT NULL
# Ref: HALLAZGOS_DATASET.md §4.1
gold = gold.withColumn("is_default",
    when((col("final_zero_balance_code").isin(*DEFAULT_ZBC_CODES))
         & (col("final_disposition_date").isNotNull()), 1
    ).otherwise(0))

# --- Variables de validacion ---
gold = gold.withColumn("ever_foreclosed",
    when(col("final_foreclosure_date").isNotNull(), 1).otherwise(0))

gold = gold.withColumn("is_clean_liquidation",
    when(col("final_zero_balance_code").isin("01", "06"), 1).otherwise(0))

# --- Net Loss + Net Severity ---
# Formula: Net Loss = UPB_at_removal + Total_Expenses - Total_Proceeds
# Solo para defaults con UPB_at_removal valido. NULL = $0 para expenses/proceeds.
# Limitacion: excluye foregone interest (pos 85 N/A para SF). Es cota inferior.
total_expenses = reduce(add,
    [F.coalesce(col(c), F.lit(0.0)) for c in _EXPENSE_COLS])
total_proceeds = reduce(add,
    [F.coalesce(col(c), F.lit(0.0)) for c in _PROCEED_COLS])

gold = gold.withColumn("net_loss",
    when((col("is_default") == 1) & col("final_upb_at_removal").isNotNull(),
         col("final_upb_at_removal") + total_expenses - total_proceeds))

gold = gold.withColumn("net_severity",
    when((col("net_loss").isNotNull()) & (col("final_upb_at_removal") > 0),
         col("net_loss") / col("final_upb_at_removal")))

# --- Features de prestatario ---
# has_coborrower: reemplaza co_borrower_credit_score (51% NULL) en AFE
gold = gold.withColumn("has_coborrower",
    when(col("co_borrower_credit_score").isNotNull(), 1).otherwise(0))

# lph_available: flag de disponibilidad LPH (~55-65% NULL, solo post-abr 2020)
gold = gold.withColumn("lph_available",
    when(col("last_loan_payment_history").isNotNull(), 1).otherwise(0))

# --- Features financieras derivadas (solo Subset B) ---
gold = gold.withColumn("rate_spread",
    col("orig_interest_rate") - col("last_interest_rate"))

gold = gold.withColumn("upb_paydown_pct",
    when(col("orig_upb") > 0,
         (col("orig_upb") - F.coalesce(col("last_active_upb"), col("orig_upb")))
         / col("orig_upb")))

# --- Estrato de riesgo (para muestreo) ---
# Orden de evaluacion: default > serious_dlq > early_dlq > performing
gold = gold.withColumn("stratum",
    when(col("is_default") == 1, "default")
    .when(col("max_delinquency_status") >= 3, "serious_dlq")
    .when(col("max_delinquency_status").between(1, 2), "early_dlq")
    .otherwise("performing"))

# --- Clave compuesta para sampleBy (estrato × vintage, 16 celdas) ---
# Separador "|" evita ambiguedad con guiones bajos en nombres de estrato
gold = gold.withColumn("sample_key",
    F.concat_ws("|", col("stratum"), col("vintage_bin")))

print("G5 — Features derivadas computadas (lazy):")
print("  Metadata:    origination_year, vintage_bin")
print("  Target:      is_default")
print("  Validacion:  ever_foreclosed, is_clean_liquidation")
print("  Financiero:  net_loss, net_severity")
print("  Prestatario: has_coborrower, lph_available")
print("  Derivado SB: rate_spread, upb_paydown_pct")
print("  Muestreo:    stratum, sample_key")


# ══════════════════════════════════════════════════════════════════════════════
# CELDA 6 — Parseo loan_payment_history (sobre ~57M filas, NO sobre 3,17B)
#
# loan_payment_history es un string de 48 chars (24 pares de 2 digitos).
# Orientacion: posiciones 1-2 = mes mas antiguo, 47-48 = mes mas reciente.
# Ref: Glossary PDF pos 41 — "most recent month is located to the right"
#
# Codigos: "00"=current, "01"-"99"=meses delinquent, "XX"=unknown, " "=N/A.
# El struct trick ya capturo el ultimo LPH no-null (last_loan_payment_history).
#
# Se parsean 4 features resumidas en un select() unico (un nodo de proyeccion):
#   ph_months_current_24:      COUNT de meses al corriente en los 24 meses
#   ph_months_delinquent_24:   COUNT de meses delinquent (01-99)
#   ph_max_delinquency_24:     MAX delinquency numerico en los 24 meses
#   ph_recent_delinquency_3m:  1 si hubo delinquency en los 3 meses mas recientes
#
# Las 4 features son NULL cuando last_loan_payment_history es NULL (~55-65%).
# ══════════════════════════════════════════════════════════════════════════════

lph = col("last_loan_payment_history")


def is_delinquent_pair(pair_expr):
    """True si el par LPH es delinquent numerico (01-99). Excluye 00, XX, espacios."""
    return pair_expr.rlike("^0[1-9]$|^[1-9][0-9]$")


# ph_months_current_24: count de pares == "00" en 24 meses
pairs_current = [
    when(substring(lph, 2 * i - 1, 2) == "00", F.lit(1)).otherwise(F.lit(0))
    for i in range(1, 25)
]
ph_months_current_24 = reduce(add, pairs_current)

# ph_months_delinquent_24: count de pares delinquent (01-99)
pairs_delinquent = [
    when(is_delinquent_pair(substring(lph, 2 * i - 1, 2)), F.lit(1)).otherwise(F.lit(0))
    for i in range(1, 25)
]
ph_months_delinquent_24 = reduce(add, pairs_delinquent)

# ph_max_delinquency_24: max numerico de los 24 pares
# greatest() ignora nulls → pares XX/espacios que castean a null se ignoran
pair_ints = [
    when(substring(lph, 2 * i - 1, 2).rlike("^[0-9]{2}$"),
         substring(lph, 2 * i - 1, 2).cast("int"))
    for i in range(1, 25)
]
ph_max_delinquency_24 = greatest(*pair_ints)

# ph_recent_delinquency_3m: any delinquent en 3 pares mas recientes
# Par 22 = pos 43-44, Par 23 = pos 45-46, Par 24 = pos 47-48
ph_recent_delinquency_3m = when(
    is_delinquent_pair(substring(lph, 43, 2))
    | is_delinquent_pair(substring(lph, 45, 2))
    | is_delinquent_pair(substring(lph, 47, 2)),
    1
).otherwise(0)

# Agregar las 4 features en un select unico
# when(lph.isNotNull(), expr) garantiza NULL cuando LPH no disponible
gold = gold.select(
    "*",
    when(lph.isNotNull(), ph_months_current_24).alias("ph_months_current_24"),
    when(lph.isNotNull(), ph_months_delinquent_24).alias("ph_months_delinquent_24"),
    when(lph.isNotNull(), ph_max_delinquency_24).alias("ph_max_delinquency_24"),
    when(lph.isNotNull(), ph_recent_delinquency_3m).alias("ph_recent_delinquency_3m"),
)

print("G6 — LPH parse configurado (lazy):")
print("  4 features de 24 pares × substring()")
print("  Orientacion: derecha = mes mas reciente (pos 47-48)")
print("  NULL cuando last_loan_payment_history es NULL (~55-65% loans)")
print(f"  Columnas Gold virtual: {len(gold.columns)}")


# ══════════════════════════════════════════════════════════════════════════════
# CELDA 7 — persist(MEMORY_AND_DISK) + validacion Gold virtual
#
# Esta celda MATERIALIZA el Gold virtual. Es el momento mas costoso del job:
# lee 3,17B loan-months, ejecuta el shuffle del groupBy (~57M grupos),
# computa todas las features derivadas y el parseo LPH, y cachea el resultado.
#
# Despues del persist, las acciones subsiguientes (counts, distribuciones,
# sampling, escritura) operan sobre datos cacheados = rapido.
#
# Esperado: ~30-50 min para la materializacion.
# ══════════════════════════════════════════════════════════════════════════════

print("Persistiendo Gold virtual (MEMORY_AND_DISK)...")
print("  Esto materializa el pipeline completo: Silver → groupBy → features → LPH")
print("  Estimado: ~30-50 min (shuffle de 3,17B → ~57M loans)")

t0 = time.time()

gold = gold.persist(StorageLevel.MEMORY_AND_DISK)
gold_count = gold.count()  # Trigger materialization

t_gold = time.time() - t0

print(f"\nGold virtual persistido en {t_gold / 60:.1f} min ({t_gold / 3600:.2f} h)")
print(f"  Loans:    {gold_count:,}")
print(f"  Columnas: {len(gold.columns)}")

# ── Distribuciones basicas (rapido sobre cache) ──────────────────────────────

# Distribucion is_default
default_dist = (gold.groupBy("is_default").count()
    .orderBy("is_default").collect())
print(f"\n  Distribucion is_default:")
for row in default_dist:
    pct = row["count"] / gold_count * 100
    print(f"    {row['is_default']}: {row['count']:>12,} ({pct:.2f}%)")

# Distribucion ZBC (top 10)
zbc_dist = (gold.groupBy("final_zero_balance_code").count()
    .orderBy(F.desc("count")).limit(10).collect())
print(f"\n  Distribucion ZBC (top 10):")
for row in zbc_dist:
    pct = row["count"] / gold_count * 100
    label = row["final_zero_balance_code"] or "NULL (activo)"
    print(f"    {label:<20} {row['count']:>12,} ({pct:.2f}%)")

# Distribucion estrato
strat_dist = (gold.groupBy("stratum").count()
    .orderBy(F.desc("count")).collect())
print(f"\n  Distribucion estrato:")
for row in strat_dist:
    pct = row["count"] / gold_count * 100
    print(f"    {row['stratum']:<15} {row['count']:>12,} ({pct:.2f}%)")

# ── Conteos por sample_key (para fracciones de muestreo en G8/G9) ────────────
key_counts_rows = gold.groupBy("sample_key").count().collect()
counts_by_key = {row["sample_key"]: row["count"] for row in key_counts_rows}

counts_by_stratum = {}
for key, n in counts_by_key.items():
    stratum = key.split("|")[0]
    counts_by_stratum[stratum] = counts_by_stratum.get(stratum, 0) + n

print(f"\n  Celdas sample_key: {len(counts_by_key)} (estrato × vintage)")
print(f"  Distribucion estrato × vintage:")
for key in sorted(counts_by_key.keys()):
    n = counts_by_key[key]
    pct = n / gold_count * 100
    print(f"    {key:<30} {n:>10,} ({pct:.2f}%)")


# ══════════════════════════════════════════════════════════════════════════════
# CELDA 8 — Muestreo estratificado Subset A (~50k loans, CSV)
#
# Muestreo con sampleBy() usando 2 variables: estrato × vintage (16 celdas).
# Target: performing=25k, early_dlq=10k, serious_dlq=10k, default=5k.
# Todas las fracciones son <= 1.0 (no hay oversampling absoluto).
# sampleBy es probabilistico: conteos reales pueden diferir ±5-10% del target.
#
# Output: CSV con header, 1 archivo (coalesce), 41 columnas.
# Ref: VACIOS_GOLD.md P7+P14, schema.py:SUBSET_A_COLUMNS
# ══════════════════════════════════════════════════════════════════════════════

t0 = time.time()

vintage_labels = list(VINTAGE_BINS.keys())
fractions_a = compute_fractions(
    STRATUM_TARGETS_A, counts_by_key, counts_by_stratum, vintage_labels)

print(f"G8 — Subset A: {len(fractions_a)} celdas con fraccion > 0")
print(f"  Targets: {STRATUM_TARGETS_A}")
print(f"\n  {'Celda':<30} {'N real':>10} {'Fraccion':>10} {'N esperado':>10}")
print(f"  {'─' * 30} {'─' * 10} {'─' * 10} {'─' * 10}")
for key in sorted(fractions_a.keys()):
    n_real = counts_by_key[key]
    frac = fractions_a[key]
    n_exp = int(n_real * frac)
    print(f"  {key:<30} {n_real:>10,} {frac:>10.6f} {n_exp:>10,}")

# Muestreo
subset_a = gold.sampleBy("sample_key", fractions_a, seed=42)

# Seleccionar columnas de Subset A (41 cols)
subset_a = subset_a.select(*SUBSET_A_COLUMNS)

# Escribir CSV (1 archivo con header)
print(f"\n  Escribiendo Subset A como CSV...")
subset_a.coalesce(1) \
    .write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(ADLS_PATHS["subset_a"])

t_sa = time.time() - t0
print(f"  Subset A escrito en {t_sa / 60:.1f} min")
print(f"  Destino: {ADLS_PATHS['subset_a']}")
print(f"  Columnas: {len(SUBSET_A_COLUMNS)}")


# ══════════════════════════════════════════════════════════════════════════════
# CELDA 9 — Muestreo estratificado Subset B (~500k loans, Parquet)
#
# Misma logica de muestreo que SA pero con targets ×10.
# Seed distinto (123) para independencia muestral respecto a SA.
#
# Output: Parquet (4 archivos, snappy), 51 columnas.
# Ref: VACIOS_GOLD.md P8 §8.5, schema.py:SUBSET_B_EXTRA_COLUMNS
# ══════════════════════════════════════════════════════════════════════════════

t0 = time.time()

fractions_b = compute_fractions(
    STRATUM_TARGETS_B, counts_by_key, counts_by_stratum, vintage_labels)

print(f"G9 — Subset B: {len(fractions_b)} celdas con fraccion > 0")
print(f"  Targets: {STRATUM_TARGETS_B}")

# Muestreo
subset_b = gold.sampleBy("sample_key", fractions_b, seed=123)

# Seleccionar columnas de Subset B (41 base + 10 extra = 51 cols)
subset_b_cols = SUBSET_A_COLUMNS + SUBSET_B_EXTRA_COLUMNS
subset_b = subset_b.select(*subset_b_cols)

# Escribir Parquet (4 archivos)
print(f"\n  Escribiendo Subset B como Parquet (4 particiones)...")
subset_b.repartition(4) \
    .write \
    .mode("overwrite") \
    .parquet(ADLS_PATHS["subset_b"])

t_sb = time.time() - t0
print(f"  Subset B escrito en {t_sb / 60:.1f} min")
print(f"  Destino: {ADLS_PATHS['subset_b']}")
print(f"  Columnas: {len(subset_b_cols)}")


# ══════════════════════════════════════════════════════════════════════════════
# CELDA 10 — Validacion subsets + SMD + unpersist + resumen
#
# Re-lee subsets desde ADLS para validacion round-trip.
# Calcula SMD (Standardized Mean Difference) para verificar que la muestra
# no diverge significativamente de la poblacion Gold (umbral: SMD < 0,1).
# Ref: VACIOS_GOLD.md P14 §14.3
# ══════════════════════════════════════════════════════════════════════════════

t0 = time.time()

# ── Re-leer subsets desde ADLS ────────────────────────────────────────────────
sa_check = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(ADLS_PATHS["subset_a"]))
sb_check = spark.read.parquet(ADLS_PATHS["subset_b"])

sa_count = sa_check.count()
sb_count = sb_check.count()

print(f"Validacion Subsets (re-lectura desde ADLS):")
print(f"  Subset A: {sa_count:,} loans, {len(sa_check.columns)} columnas")
print(f"  Subset B: {sb_count:,} loans, {len(sb_check.columns)} columnas")

# Verificar columnas
expected_sa = len(SUBSET_A_COLUMNS)
expected_sb = len(SUBSET_A_COLUMNS) + len(SUBSET_B_EXTRA_COLUMNS)
if len(sa_check.columns) != expected_sa:
    print(f"  WARNING SA: esperadas {expected_sa}, tiene {len(sa_check.columns)}")
if len(sb_check.columns) != expected_sb:
    print(f"  WARNING SB: esperadas {expected_sb}, tiene {len(sb_check.columns)}")

# ── Distribucion por estrato (verificacion de oversampling) ──────────────────
print(f"\n  Distribucion por estrato en Subset A:")
sa_strat = sa_check.groupBy("stratum").count().orderBy(F.desc("count")).collect()
for row in sa_strat:
    pct = row["count"] / sa_count * 100
    print(f"    {row['stratum']:<15} {row['count']:>8,} ({pct:.1f}%)")

print(f"\n  Distribucion por estrato en Subset B:")
sb_strat = sb_check.groupBy("stratum").count().orderBy(F.desc("count")).collect()
for row in sb_strat:
    pct = row["count"] / sb_count * 100
    print(f"    {row['stratum']:<15} {row['count']:>8,} ({pct:.1f}%)")

# ── SMD: Standardized Mean Difference (Subset A vs Gold) ─────────────────────
# Umbral: SMD < 0,1 = OK (Cohen's d negligible).
# Verifica que el muestreo no distorsiono las distribuciones clave.
smd_vars = ["borrower_credit_score", "orig_ltv", "dti", "orig_upb"]

print(f"\n  Verificacion SMD (Subset A vs Gold virtual):")
print(f"  {'Variable':<30} {'Pop mean':>10} {'SA mean':>10} {'SMD':>8} {'Status':>8}")
print(f"  {'─' * 30} {'─' * 10} {'─' * 10} {'─' * 8} {'─' * 8}")

for var in smd_vars:
    pop_stats = gold.agg(
        F.avg(var).alias("pop_mean"),
        F.stddev(var).alias("pop_std"),
    ).first()
    sample_mean = sa_check.agg(F.avg(F.col(var).cast("double"))).first()[0]

    pop_mean = pop_stats["pop_mean"]
    pop_std = pop_stats["pop_std"]

    if pop_std and pop_std > 0 and sample_mean is not None:
        smd = abs(sample_mean - pop_mean) / pop_std
    else:
        smd = 0.0
    status = "OK" if smd < 0.1 else "REVISAR"
    print(f"  {var:<30} {pop_mean:>10.2f} {sample_mean:>10.2f} {smd:>8.4f} {status:>8}")

# ── Distribucion is_default por subset ────────────────────────────────────────
print(f"\n  is_default en Subset A:")
sa_def = sa_check.groupBy("is_default").count().collect()
for row in sorted(sa_def, key=lambda r: r["is_default"]):
    val = row["is_default"]
    cnt = row["count"]
    pct = cnt / sa_count * 100
    print(f"    {val}: {cnt:>8,} ({pct:.1f}%)")

# ── Muestra visual ────────────────────────────────────────────────────────────
print(f"\n  Muestra Subset A (5 filas):")
sa_check.select(
    "loan_id", "borrower_credit_score", "orig_ltv", "is_default",
    "stratum", "vintage_bin", "max_delinquency_status"
).show(5, truncate=False)

# ── Unpersist Gold ────────────────────────────────────────────────────────────
gold.unpersist()
print("Gold virtual unpersisted.")

t_val = time.time() - t0

# ── Resumen final ─────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("  GOLD SUBSETS COMPLETADOS")
print("=" * 70)
print(f"  Gold virtual:  {gold_count:,} loans (materializado en {t_gold / 60:.1f} min)")
print(f"  Subset A:      {sa_count:,} loans, {len(sa_check.columns)} cols, CSV")
print(f"  Subset B:      {sb_count:,} loans, {len(sb_check.columns)} cols, Parquet")
print(f"  Subset A ruta: {ADLS_PATHS['subset_a']}")
print(f"  Subset B ruta: {ADLS_PATHS['subset_b']}")
print(f"  Validacion:    {t_val / 60:.1f} min")
print("=" * 70)
print("\n  Siguiente paso:")
print("  1. Descargar Subset A (CSV ~10MB) para R (AFE/AFC)")
print("  2. Subir Subset B (Parquet ~100MB) a Google Drive para Colab (VAE)")
print("  3. Subir schema.py actualizado si no se hizo antes de esta ejecucion")
