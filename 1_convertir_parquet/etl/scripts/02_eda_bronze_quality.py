"""
EDA de Calidad de Datos — Sobre Bronze Completo (Synapse Spark)
================================================================
Ejecuta sobre el Bronze Parquet completo (3.174.135.934 loan-months, 101 particiones)
para obtener una vista INTEGRAL del dataset antes de definir transformaciones Silver.

Proposito: Descubrir vacios, centinelas, inconsistencias y patrones de calidad
que una muestra local podria ocultar.

Ejecucion: Copiar cada seccion como celda de un notebook en Synapse Studio.
           La primera celda DEBE ser %%configure.

Costo estimado: ~$3-5 USD (12 vCores x ~120-180 min)
Calibracion: ~3-4 min por scan completo (medido en Cell 2 original)

v2 (2026-02-19):
  - Cell 0: driverMemory 6g->12g, driverCores=2 (evita OOM en planificacion)
  - Cell 2: REESCRITA — descubrimiento de centinelas via min/max + conteo de patrones
  - Cell 3: REESCRITA — batched approx_count_distinct (elimina operador EXPAND)
  - Cell 7: Mejorada — agrupacion por anio de reporte (no acquisition_quarter)
  - Cell 10: Batched — 1 query en vez de 12, ampliada con loan_age/mi_percentage
"""

# ==============================================================================
# CELDA 0: Configuracion Spark (%%configure — DEBE ser la primera celda)
# ==============================================================================
# %%configure
# {
#     "driverMemory": "12g",
#     "driverCores": 2,
#     "executorMemory": "24g",
#     "executorCores": 3,
#     "numExecutors": 2,
#     "conf": {
#         "spark.sql.adaptive.enabled": "true",
#         "spark.sql.adaptive.coalescePartitions.enabled": "true",
#         "spark.sql.parquet.filterPushdown": "true",
#         "spark.sql.ui.retainedExecutions": "5"
#     }
# }


# ==============================================================================
# CELDA 1: Imports y lectura de Bronze
# ==============================================================================

from pyspark.sql.functions import (
    col, when, trim, length, lit, count, countDistinct,
    min as spark_min, max as spark_max, avg as spark_avg,
    sum as spark_sum, approx_count_distinct, substring
)
import math

BRONZE_PATH = "abfss://synapse-fs@stsynapsemetadata.dfs.core.windows.net/bronze/"

df = spark.read.parquet(BRONZE_PATH)
total_rows = df.count()
total_partitions = df.select("acquisition_quarter").distinct().count()

print(f"Bronze cargado: {total_rows:,} filas, {total_partitions} particiones")
print(f"Columnas: {len(df.columns)}")
print(f"Schema fields: {[f.name for f in df.schema.fields[:10]]}...")


# ==============================================================================
# CELDA 2: DESCUBRIMIENTO DE CENTINELAS
# En vez de verificar solo centinelas conocidos, computa min/max/avg para
# CADA columna numerico-casteable y cuenta patrones "todos nueves" (99, 999,
# 9999). Descubre centinelas que no estan documentados.
#
# Las 33 columnas se procesan en batches de 10 (7 expresiones/col = 70/batch).
# Sin countDistinct → sin operador EXPAND → sin hang.
# ==============================================================================

NUMERIC_COLUMNS = [
    # --- Se castearan a DoubleType en Silver (20 cols) ---
    "orig_interest_rate", "current_interest_rate", "orig_upb",
    "current_actual_upb", "mi_percentage", "upb_at_time_of_removal",
    "total_principal_current", "foreclosure_costs",
    "property_preservation_repair_costs", "asset_recovery_costs",
    "misc_holding_expenses_credits", "associated_taxes_holding_property",
    "net_sales_proceeds", "credit_enhancement_proceeds",
    "repurchase_make_whole_proceeds", "other_foreclosure_proceeds",
    "modification_related_ni_upb", "principal_forgiveness_amount",
    "foreclosure_principal_writeoff", "total_deferral_amount",
    # --- Se castearan a IntegerType en Silver (13 cols) ---
    "orig_loan_term", "loan_age", "remaining_months_legal_maturity",
    "remaining_months_to_maturity", "orig_ltv", "orig_cltv",
    "num_borrowers", "dti", "borrower_credit_score",
    "co_borrower_credit_score", "num_units", "months_to_amortization",
    "alternative_delinquency_resolution_count",
]

NINE_PATTERNS = [99.0, 999.0, 9999.0]

print("=" * 80)
print("DESCUBRIMIENTO DE CENTINELAS — MIN/MAX/AVG + PATRONES 'TODOS NUEVES'")
print("=" * 80)
print(f"Columnas numericas: {len(NUMERIC_COLUMNS)}")
print(f"Patrones buscados (cast double): {[int(p) for p in NINE_PATTERNS]}")
print()

BATCH_SIZE = 10
discovery_results = []

for i in range(0, len(NUMERIC_COLUMNS), BATCH_SIZE):
    batch = NUMERIC_COLUMNS[i:i + BATCH_SIZE]
    batch_num = (i // BATCH_SIZE) + 1
    total_batches = math.ceil(len(NUMERIC_COLUMNS) / BATCH_SIZE)
    print(f"--- Batch {batch_num}/{total_batches}: {', '.join(batch)} ---")

    exprs = []
    for c in batch:
        vc = col(c).cast("double")
        exprs.append(spark_min(vc).alias(f"min__{c}"))
        exprs.append(spark_max(vc).alias(f"max__{c}"))
        exprs.append(spark_avg(vc).alias(f"avg__{c}"))
        exprs.append(
            spark_sum(when(col(c).isNotNull() & (trim(col(c)) != ""), 1).otherwise(0))
            .alias(f"cnt__{c}")
        )
        for pat in NINE_PATTERNS:
            exprs.append(
                spark_sum(when(vc == pat, 1).otherwise(0))
                .alias(f"p{int(pat)}__{c}")
            )

    row = df.agg(*exprs).collect()[0]

    for c in batch:
        mn = row[f"min__{c}"]
        mx = row[f"max__{c}"]
        av = row[f"avg__{c}"]
        cnt = row[f"cnt__{c}"]

        pattern_hits = {}
        for pat in NINE_PATTERNS:
            pc = row[f"p{int(pat)}__{c}"]
            if pc and pc > 0:
                pattern_hits[int(pat)] = pc

        max_is_sentinel = False
        if mx is not None:
            try:
                mx_int_str = str(int(mx))
                if all(ch == '9' for ch in mx_int_str) and len(mx_int_str) >= 2:
                    max_is_sentinel = True
            except (ValueError, OverflowError):
                pass

        discovery_results.append({
            "col": c, "min": mn, "max": mx, "avg": av, "count": cnt,
            "pattern_hits": pattern_hits, "max_is_sentinel": max_is_sentinel,
        })

        min_s = f"{mn:,.2f}" if mn is not None else "N/A"
        max_s = f"{mx:,.2f}" if mx is not None else "N/A"
        avg_s = f"{av:,.2f}" if av is not None else "N/A"

        flags = []
        if max_is_sentinel:
            flags.append(f"MAX={int(mx)} CENTINELA")
        for pat_val, pc in sorted(pattern_hits.items()):
            pct = (pc / total_rows) * 100
            flags.append(f"'{pat_val}'={pc:,}({pct:.3f}%)")

        flag_str = "  << " + " | ".join(flags) if flags else ""
        print(f"  {c:45s} [{min_s:>12s}, {max_s:>12s}]  avg={avg_s:>12s}  n={cnt:>13,}{flag_str}")
    print()

# Resumen de descubrimientos
print("=" * 80)
print("RESUMEN: COLUMNAS CON CENTINELAS DESCUBIERTOS")
print("=" * 80)

found_any = False
for r in discovery_results:
    if r["max_is_sentinel"] or r["pattern_hits"]:
        found_any = True
        print(f"\n  {r['col']}:")
        if r["max_is_sentinel"]:
            print(f"    MAX = {int(r['max'])} — patron 'todos nueves' (centinela probable)")
        for pat_val, pc in sorted(r["pattern_hits"].items()):
            pct = (pc / total_rows) * 100
            pct_of_col = (pc / max(r["count"], 1)) * 100
            print(f"    Valor {pat_val}: {pc:>15,} ({pct:.4f}% total, {pct_of_col:.2f}% de no-null)")

if not found_any:
    print("  Ningun patron 'todos nueves' detectado (inesperado — verificar)")

# Centinela string: current_delinquency_status = "XX"
xx_count = df.filter(trim(col("current_delinquency_status")) == "XX").count()
xx_pct = (xx_count / total_rows) * 100
print(f"\n  current_delinquency_status='XX': {xx_count:>15,} ({xx_pct:.4f}%) — centinela string")


# ==============================================================================
# CELDA 3: PERFIL DE NULLS — Todas las columnas activas SF
# REESCRITA: usa batches + approx_count_distinct para evitar el operador EXPAND
# que causaba hang con countDistinct sobre 72+ columnas.
#
# Paso 1: Null counts en un solo pass (72 sum(when...) — sin countDistinct)
# Paso 2: Distinct counts en batches de 15 con approx_count_distinct (HLL++)
# ==============================================================================

SF_NA_COLUMNS = [
    "reference_pool_id", "master_servicer", "upb_at_issuance",
    "mi_cancellation_indicator", "repurchase_date",
    "scheduled_principal_current", "unscheduled_principal_current",
    "original_list_start_date", "original_list_price",
    "current_list_start_date", "current_list_price",
    "borrower_credit_score_at_issuance", "co_borrower_credit_score_at_issuance",
    "borrower_credit_score_current", "co_borrower_credit_score_current",
    "current_period_modification_loss", "cumulative_modification_loss",
    "current_period_credit_event_net_gain_loss",
    "cumulative_credit_event_net_gain_loss",
    "zero_balance_code_change_date", "loan_holdback_indicator",
    "loan_holdback_effective_date", "delinquent_accrued_interest",
    "arm_initial_fixed_rate_5yr_indicator", "arm_product_type",
    "initial_fixed_rate_period", "interest_rate_adjustment_frequency",
    "next_interest_rate_adjustment_date", "next_payment_change_date",
    "index_description", "arm_cap_structure",
    "initial_interest_rate_cap_up_pct", "periodic_interest_rate_cap_up_pct",
    "lifetime_interest_rate_cap_up_pct", "mortgage_margin",
    "arm_balloon_indicator", "arm_plan_number",
    "deal_name", "interest_bearing_upb",
]

active_cols = [c for c in df.columns if c not in SF_NA_COLUMNS]

print("=" * 80)
print(f"PERFIL DE NULLS/VACIOS — {len(active_cols)} columnas activas SF")
print("=" * 80)

# PASO 1: Contar nulls/vacios para TODAS las columnas en un solo pass
# Solo sum(when(...)) sin countDistinct → sin EXPAND → sin hang
print("Paso 1: Contando nulls+vacios (1 pass, sin EXPAND)...")

null_exprs = []
for c in active_cols:
    null_exprs.append(
        spark_sum(
            when((col(c).isNull()) | (trim(col(c)) == ""), 1).otherwise(0)
        ).alias(f"null_{c}")
    )

null_row = df.agg(*null_exprs).collect()[0]
print("  Paso 1 completado.")

# PASO 2: Contar distintos con approx_count_distinct en batches de 15
# HyperLogLog++ — sin EXPAND, error relativo ~5%
print("Paso 2: Contando valores distintos (approx, batches de 15)...")

DIST_BATCH = 15
distinct_map = {}

for i in range(0, len(active_cols), DIST_BATCH):
    batch = active_cols[i:i + DIST_BATCH]
    bn = (i // DIST_BATCH) + 1
    tb = math.ceil(len(active_cols) / DIST_BATCH)
    print(f"  Batch distintos {bn}/{tb}...")

    dist_exprs = [
        approx_count_distinct(col(c), rsd=0.05).alias(f"dist_{c}")
        for c in batch
    ]
    dist_row = df.agg(*dist_exprs).collect()[0]
    for c in batch:
        distinct_map[c] = dist_row[f"dist_{c}"]

print("  Paso 2 completado.\n")

# Compilar resultados y ordenar por % null descendente
null_profile = []
for c in active_cols:
    nc = null_row[f"null_{c}"]
    nd = distinct_map.get(c, -1)
    pct = round((nc / total_rows) * 100, 2)
    if pct > 99:
        status = "VACIO (>99%)"
    elif pct > 95:
        status = "RARO (>95%)"
    elif pct > 50:
        status = "ALTO (>50%)"
    elif pct > 10:
        status = "MODERADO (>10%)"
    elif pct > 1:
        status = "BAJO (>1%)"
    else:
        status = "OK"
    null_profile.append({
        "campo": c, "null_count": nc, "pct_null": pct,
        "n_distinct": nd, "status": status,
    })

null_profile.sort(key=lambda x: x["pct_null"], reverse=True)

for r in null_profile:
    nd_str = f"~{r['n_distinct']:,}" if r['n_distinct'] >= 0 else "?"
    print(f"  {r['campo']:50s} {r['pct_null']:>8.2f}% null  ({nd_str:>12s} distintos)  [{r['status']}]")


# ==============================================================================
# CELDA 4: Validacion de unicidad de PK (loan_id + monthly_reporting_period)
# ==============================================================================

print("=" * 80)
print("VERIFICACION DE UNICIDAD DE PK")
print("=" * 80)

total_distinct_pk = df.select("loan_id", "monthly_reporting_period").distinct().count()
total_loans = df.select("loan_id").distinct().count()
duplicates = total_rows - total_distinct_pk

print(f"  Total filas:                 {total_rows:>15,}")
print(f"  PKs distintas:               {total_distinct_pk:>15,}")
print(f"  Filas duplicadas:            {duplicates:>15,}")
print(f"  Tasa de duplicados:          {(duplicates/total_rows)*100:.6f}%")
print(f"  Total loans unicos:          {total_loans:>15,}")

if duplicates > 0:
    print("\n  Muestra de duplicados (top 10):")
    dup_sample = (
        df.groupBy("loan_id", "monthly_reporting_period")
        .count()
        .filter(col("count") > 1)
        .orderBy(col("count").desc())
        .limit(10)
    )
    dup_sample.show(truncate=False)
else:
    print("  [OK] Sin duplicados de PK")


# ==============================================================================
# CELDA 5: Validacion de formatos de fecha MMYYYY
# Batched: 1 solo .agg() para las 9 columnas de fecha (18 exprs, 1 scan)
# en vez de 18 scans secuenciales (~4 min vs ~70 min)
# ==============================================================================

DATE_COLUMNS = [
    "monthly_reporting_period", "origination_date", "first_payment_date",
    "maturity_date", "zero_balance_effective_date", "last_paid_installment_date",
    "foreclosure_date", "disposition_date", "io_first_pi_payment_date",
]

print("=" * 80)
print("VALIDACION DE FORMATO DE FECHAS MMYYYY")
print("=" * 80)

# Todas las validaciones en un solo pass (9 cols × 2 exprs = 18 exprs)
date_exprs = []
for dc in DATE_COLUMNS:
    date_exprs.extend([
        spark_sum(when(
            (col(dc).isNotNull()) & (trim(col(dc)) != "")
            & (~col(dc).rlike("^\\d{6}$")), 1
        ).otherwise(0)).alias(f"malformed_{dc}"),
        spark_sum(when(
            (col(dc).isNotNull()) & (trim(col(dc)) != ""), 1
        ).otherwise(0)).alias(f"nonnull_{dc}"),
    ])

date_row = df.agg(*date_exprs).collect()[0]

for dc in DATE_COLUMNS:
    malformed = date_row[f"malformed_{dc}"]
    total_non_null = date_row[f"nonnull_{dc}"]
    status = "OK" if malformed == 0 else f"PROBLEMA: {malformed:,} malformadas"
    print(f"  {dc:40s} {total_non_null:>15,} no-null  {status}")

# Solo buscar ejemplos si hay malformadas (queries adicionales puntuales)
for dc in DATE_COLUMNS:
    if date_row[f"malformed_{dc}"] > 0:
        print(f"\n  Ejemplos malformados en {dc}:")
        df.filter(
            (col(dc).isNotNull())
            & (trim(col(dc)) != "")
            & (~col(dc).rlike("^\\d{6}$"))
        ).select("loan_id", dc).show(5, truncate=False)


# ==============================================================================
# CELDA 6: Consistencia de columnas estaticas
# Las columnas de originacion NO deben cambiar entre reporting periods
# para un mismo loan_id. Si cambian, hay error de datos o correccion retroactiva.
#
# Optimizado: 1 groupBy con 18 countDistinct sobre muestra cacheada
# en vez de 18 groupBys secuenciales (~15 min vs ~70 min)
# El EXPAND (18 cols × ~5M filas = ~95M filas) es manejable en la muestra.
# ==============================================================================

STATIC_COLUMNS = [
    "channel", "orig_interest_rate", "orig_upb", "orig_loan_term",
    "origination_date", "first_payment_date", "orig_ltv", "orig_cltv",
    "dti", "borrower_credit_score", "co_borrower_credit_score",
    "first_time_buyer", "loan_purpose", "property_type", "num_units",
    "occupancy_status", "property_state", "amortization_type",
]

print("=" * 80)
print("CONSISTENCIA DE COLUMNAS ESTATICAS POR LOAN")
print("=" * 80)
print("(Columnas que deberian ser constantes por loan_id)")
print("(Muestra: 100k loans)")
print()

# Crear y cachear muestra
sample_loans = df.select("loan_id").distinct().limit(100_000).cache()
n_sample_loans = sample_loans.count()
df_sample = df.join(sample_loans, on="loan_id", how="inner").cache()
sample_rows = df_sample.count()
print(f"  Muestra materializada: {n_sample_loans:,} loans, {sample_rows:,} filas")

# Un solo groupBy con 18 countDistinct (en vez de 18 queries separadas)
static_exprs = [
    countDistinct(when(trim(col(sc)) != "", col(sc))).alias(f"ndist_{sc}")
    for sc in STATIC_COLUMNS
]
inconsistency_df = df_sample.groupBy("loan_id").agg(*static_exprs).cache()
inconsistency_df.count()  # forzar materializacion

for sc in STATIC_COLUMNS:
    inconsistent = inconsistency_df.filter(col(f"ndist_{sc}") > 1).count()
    pct = (inconsistent / n_sample_loans) * 100
    status = "OK" if inconsistent == 0 else f"INCONSISTENTE: {inconsistent:,} loans ({pct:.4f}%)"
    print(f"  {sc:35s} {status}")

inconsistency_df.unpersist()
df_sample.unpersist()
sample_loans.unpersist()


# ==============================================================================
# CELDA 7: Cambio de codificacion borrower_assistance_plan
# Pre-julio 2020: Y/N. Post-julio 2020: F/R/T/O/N/7/9
# Agrupado por ANO DE REPORTE (no acquisition_quarter) para ver el corte real.
# ==============================================================================

print("=" * 80)
print("CAMBIO DE CODIFICACION: borrower_assistance_plan")
print("=" * 80)

# Vista 1: Rango temporal de cada valor BAP (primera/ultima aparicion)
print("\n--- Rango temporal de cada valor BAP ---")
bap_range = (
    df.filter(col("borrower_assistance_plan").isNotNull() & (trim(col("borrower_assistance_plan")) != ""))
    .groupBy("borrower_assistance_plan")
    .agg(
        spark_min("monthly_reporting_period").alias("primera_aparicion"),
        spark_max("monthly_reporting_period").alias("ultima_aparicion"),
        count("*").alias("n_filas"),
    )
    .orderBy("borrower_assistance_plan")
)
bap_range.show(truncate=False)

# Vista 2: Distribucion BAP por anio de reporte (MMYYYY → chars 3-6 = YYYY)
print("\n--- Distribucion BAP por anio de reporte ---")
bap_by_year = (
    df.filter(col("borrower_assistance_plan").isNotNull() & (trim(col("borrower_assistance_plan")) != ""))
    .withColumn("rpt_year", substring("monthly_reporting_period", 3, 4))
    .groupBy("rpt_year", "borrower_assistance_plan")
    .count()
    .orderBy("rpt_year", "borrower_assistance_plan")
)
bap_by_year.show(200, truncate=False)

# Vista 3: Anios donde aun aparece "Y" (formato antiguo)
print("\nAnios de reporte con 'Y' (formato pre-2020):")
bap_y = (
    df.filter(trim(col("borrower_assistance_plan")) == "Y")
    .withColumn("rpt_year", substring("monthly_reporting_period", 3, 4))
    .groupBy("rpt_year")
    .count()
    .orderBy("rpt_year")
)
bap_y.show(50, truncate=False)


# ==============================================================================
# CELDA 8: Impacto de servicing_activity_indicator
# Cuando SAI='Y', la delinquency puede ser artificial (cambio de servicer).
# FAQ #33: puede reportar mora aunque el deudor haya pagado.
# ==============================================================================

print("=" * 80)
print("IMPACTO DE servicing_activity_indicator = 'Y'")
print("=" * 80)

sai_total = df.filter(trim(col("servicing_activity_indicator")) == "Y").count()
sai_pct = (sai_total / total_rows) * 100
print(f"  Filas con SAI='Y': {sai_total:,} ({sai_pct:.4f}%)")

sai_delinquent = (
    df.filter(
        (trim(col("servicing_activity_indicator")) == "Y")
        & (col("current_delinquency_status").isNotNull())
        & (trim(col("current_delinquency_status")) != "")
        & (trim(col("current_delinquency_status")) != "00")
        & (trim(col("current_delinquency_status")) != "XX")
    ).count()
)
print(f"  SAI='Y' + delinquency > 0: {sai_delinquent:,}")
if sai_total > 0:
    print(f"  Tasa de mora artificial: {(sai_delinquent/sai_total)*100:.2f}% de las filas SAI='Y'")


# ==============================================================================
# CELDA 9: Analisis de UPB masking en primeros 6 meses
# FAQ #38: current_actual_upb masked durante los primeros 6 meses de vida
# ==============================================================================

print("=" * 80)
print("UPB MASKING EN PRIMEROS 6 MESES DE VIDA DEL PRESTAMO")
print("=" * 80)

df_early = df.filter(
    (col("loan_age").isNotNull())
    & (trim(col("loan_age")) != "")
    & (col("loan_age").cast("int") <= 6)
    & (col("loan_age").cast("int") >= 0)
)
early_count = df_early.count()
early_pct = (early_count / total_rows) * 100

early_upb_null = df_early.filter(
    (col("current_actual_upb").isNull())
    | (trim(col("current_actual_upb")) == "")
    | (trim(col("current_actual_upb")) == "0")
    | (trim(col("current_actual_upb")) == "0.00")
).count()

print(f"  Filas con loan_age 0-6:     {early_count:>15,} ({early_pct:.2f}%)")
print(f"  De esas, UPB null/0:        {early_upb_null:>15,} ({(early_upb_null/max(early_count,1))*100:.2f}%)")
print(f"  Filas con loan_age > 6:     {total_rows - early_count:>15,}")


# ==============================================================================
# CELDA 10: Validacion de rangos numericos (post-centinela)
# Computa min/max/avg/count y conteo fuera-de-rango para 12 columnas en un
# solo .agg() (60 expresiones, sin EXPAND). Excluye centinelas conocidos
# + patrones "todos nueves" que esten por encima del rango valido.
# ==============================================================================

SENTINEL_VALUES = {
    "borrower_credit_score":      ["9999"],
    "co_borrower_credit_score":   ["9999"],
    "dti":                        ["999"],
    "orig_ltv":                   ["999"],
    "orig_cltv":                  ["999"],
    "mi_percentage":              ["999"],
    "num_borrowers":              ["99"],
}

RANGE_CHECKS = {
    "borrower_credit_score":    (300, 850),
    "co_borrower_credit_score": (300, 850),
    "dti":                      (0, 65),
    "orig_ltv":                 (1, 200),
    "orig_cltv":                (1, 200),
    "orig_interest_rate":       (0.001, 15),
    "current_interest_rate":    (0.001, 15),
    "num_borrowers":            (1, 10),
    "num_units":                (1, 4),
    "orig_loan_term":           (1, 480),
    "loan_age":                 (-1, 600),
    "mi_percentage":            (0, 60),
}

print("=" * 80)
print("VALIDACION DE RANGOS NUMERICOS (excluyendo centinelas y nulls)")
print("=" * 80)

# Construir todas las expresiones en un solo .agg() (60 exprs, sin EXPAND)
range_exprs = []
range_col_list = list(RANGE_CHECKS.keys())

for col_name in range_col_list:
    lo, hi = RANGE_CHECKS[col_name]
    sentinels = SENTINEL_VALUES.get(col_name, [])
    vc = col(col_name).cast("double")

    # Condicion de validez: no null, no vacio, no centinela, cast exitoso
    valid_cond = (col(col_name).isNotNull()) & (trim(col(col_name)) != "")
    for s in sentinels:
        valid_cond = valid_cond & (trim(col(col_name)) != s)
    valid_cond = valid_cond & vc.isNotNull()
    # Excluir patrones "todos nueves" solo si estan por encima del rango valido
    for nine_val in [99.0, 999.0, 9999.0]:
        if nine_val > hi and str(int(nine_val)) not in sentinels:
            valid_cond = valid_cond & (vc != nine_val)

    valid_val = when(valid_cond, vc)

    range_exprs.extend([
        spark_min(valid_val).alias(f"min_{col_name}"),
        spark_max(valid_val).alias(f"max_{col_name}"),
        spark_avg(valid_val).alias(f"avg_{col_name}"),
        count(valid_val).alias(f"n_{col_name}"),
        spark_sum(
            when(valid_cond & ((vc < lo) | (vc > hi)), 1).otherwise(0)
        ).alias(f"oor_{col_name}"),
    ])

range_row = df.agg(*range_exprs).collect()[0]

for col_name in range_col_list:
    lo, hi = RANGE_CHECKS[col_name]
    mn = range_row[f"min_{col_name}"]
    mx = range_row[f"max_{col_name}"]
    av = range_row[f"avg_{col_name}"]
    n = range_row[f"n_{col_name}"]
    oor = range_row[f"oor_{col_name}"]

    status = "OK" if oor == 0 else f"FUERA DE RANGO: {oor:,}"
    min_s = f"{mn:.2f}" if mn is not None else "N/A"
    max_s = f"{mx:.2f}" if mx is not None else "N/A"
    avg_s = f"{av:.2f}" if av is not None else "N/A"
    print(f"  {col_name:35s} rango [{lo}, {hi}]")
    print(f"    limpio: [{min_s}, {max_s}]  avg={avg_s}  n={n:,}  {status}")


# ==============================================================================
# CELDA 11: Distribucion de zero_balance_code (outcomes del prestamo)
# Critico para definir is_default, validar Gold y subsets
# ==============================================================================

print("=" * 80)
print("DISTRIBUCION DE ZERO BALANCE CODE")
print("=" * 80)

ZBC_LABELS = {
    "01": "Prepaid/Matured",
    "02": "Third Party Sale (DEFAULT)",
    "03": "Short Sale (DEFAULT)",
    "06": "Repurchased",
    "09": "REO Disposition (DEFAULT)",
    "15": "NPL Sale (DEFAULT)",
    "16": "RPL Sale",
    "96": "Removal (admin)",
}

zbc_dist = (
    df.filter(
        (col("zero_balance_code").isNotNull())
        & (trim(col("zero_balance_code")) != "")
    )
    .groupBy("zero_balance_code")
    .count()
    .orderBy(col("count").desc())
)

zbc_rows = zbc_dist.collect()
total_zbc = sum(row["count"] for row in zbc_rows)

print(f"  Total loans unicos: {total_loans:,}")
print(f"  Filas con ZBC no-null: {total_zbc:,}")
print()

for row in zbc_rows:
    code = row["zero_balance_code"].strip()
    cnt = row["count"]
    label = ZBC_LABELS.get(code, "DESCONOCIDO")
    print(f"  ZBC={code}: {cnt:>15,} ({(cnt/total_zbc)*100:.2f}%)  — {label}")


# ==============================================================================
# CELDA 12: Distribucion de delinquency_status (excluyendo sentinela "XX")
# Para entender la composicion de mora en el dataset completo
# ==============================================================================

print("=" * 80)
print("DISTRIBUCION DE DELINQUENCY STATUS")
print("=" * 80)

dlq_dist = (
    df.filter(
        (col("current_delinquency_status").isNotNull())
        & (trim(col("current_delinquency_status")) != "")
    )
    .groupBy("current_delinquency_status")
    .count()
    .orderBy(col("count").desc())
)

dlq_rows = dlq_dist.collect()
total_dlq = sum(row["count"] for row in dlq_rows)

for row in dlq_rows[:20]:
    status = row["current_delinquency_status"].strip()
    cnt = row["count"]
    label = ""
    if status == "00":
        label = "Current (al dia)"
    elif status == "XX":
        label = "CENTINELA — limpiar a null"
    elif status.isdigit():
        n = int(status)
        if n == 1:
            label = "30-59 dias"
        elif n == 2:
            label = "60-89 dias"
        elif n == 3:
            label = "90-119 dias (trigger mitigacion)"
        elif n >= 6:
            label = f"{n*30}+ dias (D180+)"
    print(f"  DLQ={status}: {cnt:>15,} ({(cnt/total_dlq)*100:.4f}%)  {label}")


# ==============================================================================
# CELDA 13: Distribucion por acquisition_quarter
# Para verificar que todas las particiones estan presentes y balanceadas
# ==============================================================================

print("=" * 80)
print("DISTRIBUCION POR ACQUISITION_QUARTER")
print("=" * 80)

quarter_dist = (
    df.groupBy("acquisition_quarter")
    .agg(
        count("*").alias("n_rows"),
        countDistinct("loan_id").alias("n_loans"),
    )
    .orderBy("acquisition_quarter")
)

quarter_rows = quarter_dist.collect()
for row in quarter_rows:
    q = row["acquisition_quarter"]
    n = row["n_rows"]
    loans = row["n_loans"]
    avg_months = n / max(loans, 1)
    print(f"  {q}: {n:>12,} rows, {loans:>10,} loans, {avg_months:.1f} meses/loan promedio")


# ==============================================================================
# CELDA 14: Resumen ejecutivo de hallazgos
# ==============================================================================

print("=" * 80)
print("RESUMEN EJECUTIVO — EDA DE CALIDAD DE DATOS BRONZE")
print("=" * 80)

cols_vacio = sum(1 for r in null_profile if r["pct_null"] > 99)
cols_alto = sum(1 for r in null_profile if 50 < r["pct_null"] <= 99)
cols_sentinel = sum(1 for r in discovery_results if r["max_is_sentinel"] or r["pattern_hits"])

print(f"""
Dataset: Fannie Mae SF Loan Performance (Bronze Parquet)
  Total filas:       {total_rows:,}
  Total particiones: {total_partitions}
  Total loans:       {total_loans:,}

CALIDAD DE DATOS:
  Columnas >99% null:                   {cols_vacio}
  Columnas >50% null:                   {cols_alto}
  Columnas con centinelas descubiertos: {cols_sentinel}

CENTINELAS DESCUBIERTOS (Cell 2):""")

for r in discovery_results:
    if r["max_is_sentinel"] or r["pattern_hits"]:
        hits = ", ".join(f"{k}={v:,}" for k, v in sorted(r["pattern_hits"].items()))
        print(f"  {r['col']}: {hits}")

print(f"\n  current_delinquency_status='XX': centinela string")

print("""
PROXIMOS PASOS:
  1. Revisar columnas con >50% null — decidir si conservar o descartar
  2. Actualizar SENTINEL_VALUES en schema.py con centinelas descubiertos
  3. Implementar 02_silver_clean.py con reglas validadas
  4. Implementar 03_gold_subsets.py (colapso temporal + muestreo)
""")
