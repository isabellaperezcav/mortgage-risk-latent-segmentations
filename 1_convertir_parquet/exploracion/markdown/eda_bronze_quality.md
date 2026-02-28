```python
%%configure
{
    "driverMemory": "12g",
    "driverCores": 2,
    "executorMemory": "24g",
    "executorCores": 3,
    "numExecutors": 2,
    "conf": {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.parquet.filterPushdown": "true",
        "spark.sql.ui.retainedExecutions": "5"
    }
}
```


    StatementMeta(sparkpool01, 8, -1, Finished, Available, Finished)


    Warning: When setting executor and driver size using %%configure the requested size will be mapped to closest available size which may be bigger than requested. Please use "configure session" panel to select directly from available sizes.
    See https://go.microsoft.com/fwlink/?linkid=2170827
    


```python
from pyspark.sql.functions import (
    col, when, trim, length, lit, count, countDistinct,
    min as spark_min, max as spark_max, avg as spark_avg,
    sum as spark_sum, approx_count_distinct, substring
)
import math
```


    StatementMeta(sparkpool01, 8, 2, Finished, Available, Finished)



```python
BRONZE_PATH = "abfss://synapse-fs@stsynapsemetadata.dfs.core.windows.net/bronze/"

df = spark.read.parquet(BRONZE_PATH)
```


    StatementMeta(sparkpool01, 8, 3, Finished, Available, Finished)



```python
total_rows = df.count()
total_partitions = df.select("acquisition_quarter").distinct().count()

print(f"Bronze cargado: {total_rows:,} filas, {total_partitions} particiones")
print(f"Columnas: {len(df.columns)}")
print(f"Schema fields: {[f.name for f in df.schema.fields[:10]]}...")
```


    StatementMeta(sparkpool01, 8, 4, Finished, Available, Finished)


    Bronze cargado: 3,174,135,934 filas, 101 particiones
    Columnas: 111
    Schema fields: ['reference_pool_id', 'loan_id', 'monthly_reporting_period', 'channel', 'seller_name', 'servicer_name', 'master_servicer', 'orig_interest_rate', 'current_interest_rate', 'orig_upb']...
    

## **Deteccion de valores centinela**


```python
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
```


    StatementMeta(sparkpool01, 8, 5, Finished, Available, Finished)


    ================================================================================
    DESCUBRIMIENTO DE CENTINELAS — MIN/MAX/AVG + PATRONES 'TODOS NUEVES'
    ================================================================================
    Columnas numericas: 33
    Patrones buscados (cast double): [99, 999, 9999]
    
    --- Batch 1/4: orig_interest_rate, current_interest_rate, orig_upb, current_actual_upb, mi_percentage, upb_at_time_of_removal, total_principal_current, foreclosure_costs, property_preservation_repair_costs, asset_recovery_costs ---
      orig_interest_rate                            [        1.50,        16.50]  avg=        4.75  n=3,174,135,402
      current_interest_rate                         [        0.00,        13.50]  avg=        4.72  n=3,132,915,707
      orig_upb                                      [    1,000.00, 2,212,000.00]  avg=  196,828.21  n=3,174,135,933
      current_actual_upb                            [        0.00, 2,860,003.29]  avg=  157,044.72  n=3,174,135,851  << '99'=10(0.000%) | '999'=34(0.000%) | '9999'=202(0.000%)
      mi_percentage                                 [        0.12,        50.00]  avg=       24.07  n=  592,150,658
      upb_at_time_of_removal                        [        0.00, 2,074,295.25]  avg=  172,370.22  n=   41,254,981  << '99'=7(0.000%) | '999'=23(0.000%) | '9999'=31(0.000%)
      total_principal_current                       [ -863,906.82, 2,074,295.25]  avg=    3,124.88  n=  902,460,893  << '99'=3,176(0.000%) | '999'=3,536(0.000%) | '9999'=366(0.000%)
      foreclosure_costs                             [        0.00,   708,413.23]  avg=    5,779.18  n=      652,773  << '99'=116(0.000%) | '999'=3(0.000%)
      property_preservation_repair_costs            [        0.00, 5,759,891.39]  avg=    5,539.91  n=      596,000  << '99'=2(0.000%) | '999'=5(0.000%) | '9999'=2(0.000%)
      asset_recovery_costs                          [        0.00,   236,732.61]  avg=    1,007.27  n=      516,053  << '99'=1(0.000%) | '999'=13(0.000%)
    
    --- Batch 2/4: misc_holding_expenses_credits, associated_taxes_holding_property, net_sales_proceeds, credit_enhancement_proceeds, repurchase_make_whole_proceeds, other_foreclosure_proceeds, modification_related_ni_upb, principal_forgiveness_amount, foreclosure_principal_writeoff, total_deferral_amount ---
      misc_holding_expenses_credits                 [ -197,309.69,   227,296.93]  avg=    1,793.98  n=      628,134  << '99'=9(0.000%) | '999'=4(0.000%)
      associated_taxes_holding_property             [        0.00,   298,469.68]  avg=    5,350.90  n=      615,281  << '99'=2(0.000%) | '999'=10(0.000%)
      net_sales_proceeds                            [  -61,443.92, 1,281,303.18]  avg=  109,982.85  n=      652,014  << '9999'=1(0.000%)
      credit_enhancement_proceeds                   [        0.00,   655,087.03]  avg=   15,073.46  n=      473,403  << '99'=3(0.000%)
      repurchase_make_whole_proceeds                [ -107,481.37, 1,539,963.49]  avg=   10,883.79  n=      414,123
      other_foreclosure_proceeds                    [  -43,063.83,   835,403.87]  avg=    4,357.88  n=      524,311  << '99'=91(0.000%) | '999'=9(0.000%)
      modification_related_ni_upb                   [        0.00,   473,844.50]  avg=    7,052.84  n=   45,645,180
      principal_forgiveness_amount                  [        0.00,     1,415.52]  avg=        0.00  n=   27,133,318
      foreclosure_principal_writeoff                [        0.00,   951,249.24]  avg=       19.53  n=   27,142,390
      total_deferral_amount                         [  -62,753.31,   228,230.26]  avg=   13,034.31  n=   15,908,121
    
    --- Batch 3/4: orig_loan_term, loan_age, remaining_months_legal_maturity, remaining_months_to_maturity, orig_ltv, orig_cltv, num_borrowers, dti, borrower_credit_score, co_borrower_credit_score ---
      orig_loan_term                                [       36.00,       360.00]  avg=      305.94  n=3,174,135,933  << '99'=3,935(0.000%)
      loan_age                                      [       -3.00,       313.00]  avg=       45.85  n=3,132,879,391  << '99'=9,004,412(0.284%)
      remaining_months_legal_maturity               [        0.00,       692.00]  avg=      262.16  n=3,132,707,158  << '99'=4,928,062(0.155%)
      remaining_months_to_maturity                  [        0.00,       480.00]  avg=      253.17  n=3,085,951,146  << '99'=5,166,971(0.163%)
      orig_ltv                                      [        1.00,       105.00]  avg=       69.69  n=3,174,135,675
      orig_cltv                                     [        1.00,       199.00]  avg=       70.39  n=3,163,007,139  << '99'=1,023,047(0.032%)
      num_borrowers                                 [        1.00,        10.00]  avg=        1.55  n=3,173,741,311
      dti                                           [        0.00,        64.00]  avg=       33.29  n=3,127,477,999
      borrower_credit_score                         [      300.00,       850.00]  avg=      746.17  n=3,164,828,030
      co_borrower_credit_score                      [      300.00,       850.00]  avg=      754.28  n=1,540,659,887
    
    --- Batch 4/4: num_units, months_to_amortization, alternative_delinquency_resolution_count ---
      num_units                                     [        1.00,         4.00]  avg=        1.04  n=3,174,135,933
      months_to_amortization                        [         N/A,          N/A]  avg=         N/A  n=            0
      alternative_delinquency_resolution_count      [        1.00,        11.00]  avg=        1.07  n=   16,321,568
    
    ================================================================================
    RESUMEN: COLUMNAS CON CENTINELAS DESCUBIERTOS
    ================================================================================
    
      current_actual_upb:
        Valor 99:              10 (0.0000% total, 0.00% de no-null)
        Valor 999:              34 (0.0000% total, 0.00% de no-null)
        Valor 9999:             202 (0.0000% total, 0.00% de no-null)
    
      upb_at_time_of_removal:
        Valor 99:               7 (0.0000% total, 0.00% de no-null)
        Valor 999:              23 (0.0000% total, 0.00% de no-null)
        Valor 9999:              31 (0.0000% total, 0.00% de no-null)
    
      total_principal_current:
        Valor 99:           3,176 (0.0001% total, 0.00% de no-null)
        Valor 999:           3,536 (0.0001% total, 0.00% de no-null)
        Valor 9999:             366 (0.0000% total, 0.00% de no-null)
    
      foreclosure_costs:
        Valor 99:             116 (0.0000% total, 0.02% de no-null)
        Valor 999:               3 (0.0000% total, 0.00% de no-null)
    
      property_preservation_repair_costs:
        Valor 99:               2 (0.0000% total, 0.00% de no-null)
        Valor 999:               5 (0.0000% total, 0.00% de no-null)
        Valor 9999:               2 (0.0000% total, 0.00% de no-null)
    
      asset_recovery_costs:
        Valor 99:               1 (0.0000% total, 0.00% de no-null)
        Valor 999:              13 (0.0000% total, 0.00% de no-null)
    
      misc_holding_expenses_credits:
        Valor 99:               9 (0.0000% total, 0.00% de no-null)
        Valor 999:               4 (0.0000% total, 0.00% de no-null)
    
      associated_taxes_holding_property:
        Valor 99:               2 (0.0000% total, 0.00% de no-null)
        Valor 999:              10 (0.0000% total, 0.00% de no-null)
    
      net_sales_proceeds:
        Valor 9999:               1 (0.0000% total, 0.00% de no-null)
    
      credit_enhancement_proceeds:
        Valor 99:               3 (0.0000% total, 0.00% de no-null)
    
      other_foreclosure_proceeds:
        Valor 99:              91 (0.0000% total, 0.02% de no-null)
        Valor 999:               9 (0.0000% total, 0.00% de no-null)
    
      orig_loan_term:
        Valor 99:           3,935 (0.0001% total, 0.00% de no-null)
    
      loan_age:
        Valor 99:       9,004,412 (0.2837% total, 0.29% de no-null)
    
      remaining_months_legal_maturity:
        Valor 99:       4,928,062 (0.1553% total, 0.16% de no-null)
    
      remaining_months_to_maturity:
        Valor 99:       5,166,971 (0.1628% total, 0.17% de no-null)
    
      orig_cltv:
        Valor 99:       1,023,047 (0.0322% total, 0.03% de no-null)
    
      current_delinquency_status='XX':      30,404,087 (0.9579%) — centinela string
    

## **Perfil de nulls/vacios para las 71 columnas activas SF**


```python
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
```


    StatementMeta(sparkpool01, 8, 6, Finished, Available, Finished)


    ================================================================================
    PERFIL DE NULLS/VACIOS — 72 columnas activas SF
    ================================================================================
    Paso 1: Contando nulls+vacios (1 pass, sin EXPAND)...
      Paso 1 completado.
    Paso 2: Contando valores distintos (approx, batches de 15)...
      Batch distintos 1/5...
      Batch distintos 2/5...
      Batch distintos 3/5...
      Batch distintos 4/5...
      Batch distintos 5/5...
      Paso 2 completado.
    
      io_first_pi_payment_date                             100.00% null  (          ~1 distintos)  [VACIO (>99%)]
      months_to_amortization                               100.00% null  (          ~0 distintos)  [VACIO (>99%)]
      credit_enhancement_proceeds                           99.99% null  (    ~166,725 distintos)  [VACIO (>99%)]
      repurchase_make_whole_proceeds                        99.99% null  (     ~37,376 distintos)  [VACIO (>99%)]
      last_paid_installment_date                            99.98% null  (        ~311 distintos)  [VACIO (>99%)]
      foreclosure_date                                      99.98% null  (        ~305 distintos)  [VACIO (>99%)]
      disposition_date                                      99.98% null  (        ~304 distintos)  [VACIO (>99%)]
      foreclosure_costs                                     99.98% null  (    ~453,957 distintos)  [VACIO (>99%)]
      property_preservation_repair_costs                    99.98% null  (    ~234,221 distintos)  [VACIO (>99%)]
      asset_recovery_costs                                  99.98% null  (     ~42,394 distintos)  [VACIO (>99%)]
      misc_holding_expenses_credits                         99.98% null  (    ~373,338 distintos)  [VACIO (>99%)]
      associated_taxes_holding_property                     99.98% null  (    ~417,544 distintos)  [VACIO (>99%)]
      net_sales_proceeds                                    99.98% null  (    ~622,842 distintos)  [VACIO (>99%)]
      other_foreclosure_proceeds                            99.98% null  (    ~239,082 distintos)  [VACIO (>99%)]
      total_deferral_amount                                 99.50% null  (    ~447,602 distintos)  [VACIO (>99%)]
      alternative_delinquency_resolution_count              99.49% null  (         ~11 distintos)  [VACIO (>99%)]
      principal_forgiveness_amount                          99.15% null  (          ~2 distintos)  [VACIO (>99%)]
      foreclosure_principal_writeoff                        99.14% null  (        ~743 distintos)  [VACIO (>99%)]
      zero_balance_code                                     98.70% null  (          ~7 distintos)  [RARO (>95%)]
      zero_balance_effective_date                           98.70% null  (        ~309 distintos)  [RARO (>95%)]
      upb_at_time_of_removal                                98.70% null  ( ~22,381,027 distintos)  [RARO (>95%)]
      repurchase_make_whole_proceeds_flag                   98.70% null  (          ~2 distintos)  [RARO (>95%)]
      modification_related_ni_upb                           98.56% null  (    ~128,891 distintos)  [RARO (>95%)]
      mi_percentage                                         81.34% null  (         ~54 distintos)  [ALTO (>50%)]
      mi_type                                               81.34% null  (          ~3 distintos)  [ALTO (>50%)]
      property_valuation_method                             75.98% null  (          ~5 distintos)  [ALTO (>50%)]
      total_principal_current                               71.57% null  ( ~11,855,440 distintos)  [ALTO (>50%)]
      loan_payment_history                                  71.08% null  ( ~10,410,412 distintos)  [ALTO (>50%)]
      borrower_assistance_plan                              70.74% null  (          ~5 distintos)  [ALTO (>50%)]
      alternative_delinquency_resolution                    70.74% null  (          ~4 distintos)  [ALTO (>50%)]
      co_borrower_credit_score                              51.46% null  (        ~505 distintos)  [ALTO (>50%)]
      interest_only_indicator                               11.04% null  (          ~2 distintos)  [MODERADO (>10%)]
      remaining_months_to_maturity                           2.78% null  (        ~474 distintos)  [BAJO (>1%)]
      servicer_name                                          2.31% null  (      ~1,181 distintos)  [BAJO (>1%)]
      dti                                                    1.47% null  (         ~65 distintos)  [BAJO (>1%)]
      remaining_months_legal_maturity                        1.31% null  (        ~559 distintos)  [BAJO (>1%)]
      maturity_date                                          1.31% null  (        ~774 distintos)  [BAJO (>1%)]
      current_interest_rate                                  1.30% null  (      ~6,574 distintos)  [BAJO (>1%)]
      loan_age                                               1.30% null  (        ~315 distintos)  [BAJO (>1%)]
      modification_flag                                      1.30% null  (          ~2 distintos)  [BAJO (>1%)]
      servicing_activity_indicator                           1.30% null  (          ~2 distintos)  [BAJO (>1%)]
      orig_cltv                                              0.35% null  (        ~195 distintos)  [OK]
      borrower_credit_score                                  0.29% null  (        ~518 distintos)  [OK]
      first_time_buyer                                       0.03% null  (          ~2 distintos)  [OK]
      num_borrowers                                          0.01% null  (         ~10 distintos)  [OK]
      prepayment_penalty_indicator                           0.01% null  (          ~1 distintos)  [OK]
      loan_id                                                0.00% null  ( ~56,185,893 distintos)  [OK]
      monthly_reporting_period                               0.00% null  (        ~311 distintos)  [OK]
      channel                                                0.00% null  (          ~3 distintos)  [OK]
      seller_name                                            0.00% null  (        ~190 distintos)  [OK]
      orig_interest_rate                                     0.00% null  (      ~6,581 distintos)  [OK]
      orig_upb                                               0.00% null  (      ~1,551 distintos)  [OK]
      current_actual_upb                                     0.00% null  ( ~65,127,572 distintos)  [OK]
      orig_loan_term                                         0.00% null  (        ~289 distintos)  [OK]
      origination_date                                       0.00% null  (        ~326 distintos)  [OK]
      first_payment_date                                     0.00% null  (        ~326 distintos)  [OK]
      orig_ltv                                               0.00% null  (        ~103 distintos)  [OK]
      loan_purpose                                           0.00% null  (          ~4 distintos)  [OK]
      property_type                                          0.00% null  (          ~5 distintos)  [OK]
      num_units                                              0.00% null  (          ~4 distintos)  [OK]
      occupancy_status                                       0.00% null  (          ~4 distintos)  [OK]
      property_state                                         0.00% null  (         ~58 distintos)  [OK]
      msa                                                    0.00% null  (        ~428 distintos)  [OK]
      zip_code_short                                         0.00% null  (      ~1,049 distintos)  [OK]
      amortization_type                                      0.00% null  (          ~2 distintos)  [OK]
      current_delinquency_status                             0.00% null  (        ~105 distintos)  [OK]
      special_eligibility_program                            0.00% null  (          ~4 distintos)  [OK]
      relocation_mortgage_indicator                          0.00% null  (          ~2 distintos)  [OK]
      high_balance_loan_indicator                            0.00% null  (          ~2 distintos)  [OK]
      hltv_refi_option_indicator                             0.00% null  (          ~2 distintos)  [OK]
      payment_deferral_mod_event_indicator                   0.00% null  (          ~1 distintos)  [OK]
      acquisition_quarter                                    0.00% null  (        ~106 distintos)  [OK]
    

## **Validacion de unicidad de PK (loan_id + monthly_reporting_period)**


```python
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
```


    StatementMeta(sparkpool01, 8, 7, Finished, Available, Finished)


    ================================================================================
    VERIFICACION DE UNICIDAD DE PK
    ================================================================================
      Total filas:                   3,174,135,934
      PKs distintas:                 3,174,135,934
      Filas duplicadas:                          0
      Tasa de duplicados:          0.000000%
      Total loans unicos:               56,822,017
      [OK] Sin duplicados de PK
    

## **Validacion de formatos de fecha MMYYYY**


```python
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
```


    StatementMeta(sparkpool01, 8, 8, Finished, Available, Finished)


    ================================================================================
    VALIDACION DE FORMATO DE FECHAS MMYYYY
    ================================================================================
      monthly_reporting_period                   3,174,135,934 no-null  OK
      origination_date                           3,174,135,933 no-null  OK
      first_payment_date                         3,174,135,933 no-null  OK
      maturity_date                              3,132,707,157 no-null  OK
      zero_balance_effective_date                   41,256,543 no-null  OK
      last_paid_installment_date                       670,507 no-null  OK
      foreclosure_date                                 670,581 no-null  OK
      disposition_date                                 664,433 no-null  OK
      io_first_pi_payment_date                             206 no-null  OK
    

## **Consistencia de columnas estaticas**


```python
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
```


    StatementMeta(sparkpool01, 8, 9, Finished, Available, Finished)


    ================================================================================
    CONSISTENCIA DE COLUMNAS ESTATICAS POR LOAN
    ================================================================================
    (Columnas que deberian ser constantes por loan_id)
    (Muestra: 100k loans)
    
      Muestra materializada: 100,000 loans, 5,112,596 filas
      channel                             INCONSISTENTE: 71 loans (0.0710%)
      orig_interest_rate                  INCONSISTENTE: 1 loans (0.0010%)
      orig_upb                            INCONSISTENTE: 3 loans (0.0030%)
      orig_loan_term                      OK
      origination_date                    INCONSISTENTE: 36 loans (0.0360%)
      first_payment_date                  INCONSISTENTE: 7 loans (0.0070%)
      orig_ltv                            INCONSISTENTE: 8 loans (0.0080%)
      orig_cltv                           INCONSISTENTE: 9 loans (0.0090%)
      dti                                 OK
      borrower_credit_score               INCONSISTENTE: 6 loans (0.0060%)
      co_borrower_credit_score            INCONSISTENTE: 3 loans (0.0030%)
      first_time_buyer                    INCONSISTENTE: 2 loans (0.0020%)
      loan_purpose                        INCONSISTENTE: 2 loans (0.0020%)
      property_type                       INCONSISTENTE: 1 loans (0.0010%)
      num_units                           OK
      occupancy_status                    OK
      property_state                      OK
      amortization_type                   OK
    




    DataFrame[loan_id: string]



## **Cambio de codificacion borrower_assistance_plan**


```python
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
```


    StatementMeta(sparkpool01, 8, 10, Finished, Available, Finished)


    ================================================================================
    CAMBIO DE CODIFICACION: borrower_assistance_plan
    ================================================================================
    
    --- Rango temporal de cada valor BAP ---
    +------------------------+-----------------+----------------+---------+
    |borrower_assistance_plan|primera_aparicion|ultima_aparicion|n_filas  |
    +------------------------+-----------------+----------------+---------+
    |7                       |012020           |122024          |905203283|
    |F                       |012020           |122024          |9059898  |
    |N                       |012020           |122024          |13182472 |
    |R                       |012020           |122024          |248031   |
    |T                       |012020           |122024          |1187784  |
    +------------------------+-----------------+----------------+---------+
    
    
    --- Distribucion BAP por anio de reporte ---
    +--------+------------------------+---------+
    |rpt_year|borrower_assistance_plan|count    |
    +--------+------------------------+---------+
    |2019    |7                       |4700247  |
    |2019    |F                       |85       |
    |2019    |N                       |18671    |
    |2019    |R                       |89       |
    |2019    |T                       |8        |
    |2020    |7                       |125906403|
    |2020    |F                       |5076129  |
    |2020    |N                       |1625005  |
    |2020    |R                       |50603    |
    |2020    |T                       |103313   |
    |2021    |7                       |175267456|
    |2021    |F                       |2650103  |
    |2021    |N                       |2367359  |
    |2021    |R                       |36538    |
    |2021    |T                       |271464   |
    |2022    |7                       |183635425|
    |2022    |F                       |625785   |
    |2022    |N                       |2587097  |
    |2022    |R                       |29530    |
    |2022    |T                       |284594   |
    |2023    |7                       |184904803|
    |2023    |F                       |362384   |
    |2023    |N                       |2703369  |
    |2023    |R                       |49167    |
    |2023    |T                       |211847   |
    |2024    |7                       |184750657|
    |2024    |F                       |272713   |
    |2024    |N                       |3098308  |
    |2024    |R                       |64689    |
    |2024    |T                       |244493   |
    |2025    |7                       |46038292 |
    |2025    |F                       |72699    |
    |2025    |N                       |782663   |
    |2025    |R                       |17415    |
    |2025    |T                       |72065    |
    +--------+------------------------+---------+
    
    
    Anios de reporte con 'Y' (formato pre-2020):
    +--------+-----+
    |rpt_year|count|
    +--------+-----+
    +--------+-----+
    
    

## **Impacto de servicing_activity_indicator**


```python
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
```


    StatementMeta(sparkpool01, 8, 11, Finished, Available, Finished)


    ================================================================================
    IMPACTO DE servicing_activity_indicator = 'Y'
    ================================================================================
      Filas con SAI='Y': 36,247,197 (1.1420%)
      SAI='Y' + delinquency > 0: 1,637,161
      Tasa de mora artificial: 4.52% de las filas SAI='Y'
    

## **Analisis de UPB masking en primeros 6 meses**


```python
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
```


    StatementMeta(sparkpool01, 8, 12, Finished, Available, Finished)


    ================================================================================
    UPB MASKING EN PRIMEROS 6 MESES DE VIDA DEL PRESTAMO
    ================================================================================
      Filas con loan_age 0-6:         368,774,466 (11.62%)
      De esas, UPB null/0:            237,433,169 (64.38%)
      Filas con loan_age > 6:       2,805,361,468
    

## **Validacion de rangos numericos (usando strings, pre-cast)**


```python
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
```


    StatementMeta(sparkpool01, 8, 13, Finished, Available, Finished)


    ================================================================================
    VALIDACION DE RANGOS NUMERICOS (excluyendo centinelas y nulls)
    ================================================================================
      borrower_credit_score               rango [300, 850]
        limpio: [300.00, 850.00]  avg=746.17  n=3,164,828,030  OK
      co_borrower_credit_score            rango [300, 850]
        limpio: [300.00, 850.00]  avg=754.28  n=1,540,659,887  OK
      dti                                 rango [0, 65]
        limpio: [0.00, 64.00]  avg=33.29  n=3,127,477,999  OK
      orig_ltv                            rango [1, 200]
        limpio: [1.00, 105.00]  avg=69.69  n=3,174,135,675  OK
      orig_cltv                           rango [1, 200]
        limpio: [1.00, 199.00]  avg=70.39  n=3,163,007,139  OK
      orig_interest_rate                  rango [0.001, 15]
        limpio: [1.50, 16.50]  avg=4.75  n=3,174,135,402  FUERA DE RANGO: 60
      current_interest_rate               rango [0.001, 15]
        limpio: [0.00, 13.50]  avg=4.72  n=3,132,915,707  FUERA DE RANGO: 383
      num_borrowers                       rango [1, 10]
        limpio: [1.00, 10.00]  avg=1.55  n=3,173,741,311  OK
      num_units                           rango [1, 4]
        limpio: [1.00, 4.00]  avg=1.04  n=3,174,135,933  OK
      orig_loan_term                      rango [1, 480]
        limpio: [36.00, 360.00]  avg=305.94  n=3,174,135,933  OK
      loan_age                            rango [-1, 600]
        limpio: [-3.00, 313.00]  avg=45.85  n=3,132,879,391  FUERA DE RANGO: 29
      mi_percentage                       rango [0, 60]
        limpio: [0.12, 50.00]  avg=24.07  n=592,150,658  OK
    

## **Distribucion de zero_balance_code (outcomes del prestamo)**


```python
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
```


    StatementMeta(sparkpool01, 8, 14, Finished, Available, Finished)


    ================================================================================
    DISTRIBUCION DE ZERO BALANCE CODE
    ================================================================================
      Total loans unicos: 56,822,017
      Filas con ZBC no-null: 41,256,543
    
      ZBC=01:      40,347,990 (97.80%)  — Prepaid/Matured
      ZBC=09:         451,848 (1.10%)  — REO Disposition (DEFAULT)
      ZBC=16:         142,202 (0.34%)  — RPL Sale
      ZBC=03:         107,707 (0.26%)  — Short Sale (DEFAULT)
      ZBC=06:          95,758 (0.23%)  — Repurchased
      ZBC=02:          66,591 (0.16%)  — Third Party Sale (DEFAULT)
      ZBC=15:          44,447 (0.11%)  — NPL Sale (DEFAULT)
    

## **Distribucion de delinquency_status (excluyendo "XX" y "00")**


```python
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
```


    StatementMeta(sparkpool01, 8, 15, Finished, Available, Finished)


    ================================================================================
    DISTRIBUCION DE DELINQUENCY STATUS
    ================================================================================
      DLQ=00:   3,064,917,650 (96.5591%)  Current (al dia)
      DLQ=01:      36,864,288 (1.1614%)  30-59 dias
      DLQ=XX:      30,404,087 (0.9579%)  CENTINELA — limpiar a null
      DLQ=02:      10,069,774 (0.3172%)  60-89 dias
      DLQ=03:       4,972,486 (0.1567%)  90-119 dias (trigger mitigacion)
      DLQ=04:       3,541,903 (0.1116%)  
      DLQ=05:       2,916,672 (0.0919%)  
      DLQ=06:       2,365,917 (0.0745%)  180+ dias (D180+)
      DLQ=07:       1,970,515 (0.0621%)  210+ dias (D180+)
      DLQ=08:       1,688,498 (0.0532%)  240+ dias (D180+)
      DLQ=09:       1,465,161 (0.0462%)  270+ dias (D180+)
      DLQ=10:       1,282,168 (0.0404%)  300+ dias (D180+)
      DLQ=11:       1,130,224 (0.0356%)  330+ dias (D180+)
      DLQ=12:         991,415 (0.0312%)  360+ dias (D180+)
      DLQ=13:         867,637 (0.0273%)  390+ dias (D180+)
      DLQ=14:         770,238 (0.0243%)  420+ dias (D180+)
      DLQ=15:         682,736 (0.0215%)  450+ dias (D180+)
      DLQ=16:         606,437 (0.0191%)  480+ dias (D180+)
      DLQ=17:         549,181 (0.0173%)  510+ dias (D180+)
      DLQ=18:         486,727 (0.0153%)  540+ dias (D180+)
    

## **Distribucion por acquisition_quarter**


```python
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
```


    StatementMeta(sparkpool01, 8, 16, Finished, Available, Finished)


    ================================================================================
    DISTRIBUCION POR ACQUISITION_QUARTER
    ================================================================================
      2000Q1:    9,229,283 rows,    246,862 loans, 37.4 meses/loan promedio
      2000Q2:    8,337,215 rows,    274,339 loans, 30.4 meses/loan promedio
      2000Q3:    8,862,186 rows,    333,498 loans, 26.6 meses/loan promedio
      2000Q4:   10,348,845 rows,    363,892 loans, 28.4 meses/loan promedio
      2001Q1:   15,822,944 rows,    471,277 loans, 33.6 meses/loan promedio
      2001Q2:   32,085,226 rows,    847,128 loans, 37.9 meses/loan promedio
      2001Q3:   28,497,878 rows,    790,095 loans, 36.1 meses/loan promedio
      2001Q4:   38,091,352 rows,    896,643 loans, 42.5 meses/loan promedio
      2002Q1:   42,613,246 rows,    968,761 loans, 44.0 meses/loan promedio
      2002Q2:   27,051,016 rows,    669,494 loans, 40.4 meses/loan promedio
      2002Q3:   33,594,606 rows,    751,084 loans, 44.7 meses/loan promedio
      2002Q4:   72,743,738 rows,  1,248,940 loans, 58.2 meses/loan promedio
      2003Q1:   94,866,429 rows,  1,427,858 loans, 66.4 meses/loan promedio
      2003Q2:  126,832,419 rows,  1,652,957 loans, 76.7 meses/loan promedio
      2003Q3:  150,039,551 rows,  1,738,612 loans, 86.3 meses/loan promedio
      2003Q4:   67,783,438 rows,    840,388 loans, 80.7 meses/loan promedio
      2004Q1:   35,855,570 rows,    452,471 loans, 79.2 meses/loan promedio
      2004Q2:   51,045,668 rows,    614,487 loans, 83.1 meses/loan promedio
      2004Q3:   28,624,514 rows,    389,487 loans, 73.5 meses/loan promedio
      2004Q4:   28,605,388 rows,    361,659 loans, 79.1 meses/loan promedio
      2005Q1:   24,619,814 rows,    303,611 loans, 81.1 meses/loan promedio
      2005Q2:   26,993,372 rows,    339,372 loans, 79.5 meses/loan promedio
      2005Q3:   35,556,059 rows,    440,521 loans, 80.7 meses/loan promedio
      2005Q4:   29,539,407 rows,    378,311 loans, 78.1 meses/loan promedio
      2006Q1:   18,117,253 rows,    253,043 loans, 71.6 meses/loan promedio
      2006Q2:   19,649,645 rows,    291,165 loans, 67.5 meses/loan promedio
      2006Q3:   16,483,479 rows,    271,373 loans, 60.7 meses/loan promedio
      2006Q4:   17,971,993 rows,    280,979 loans, 64.0 meses/loan promedio
      2007Q1:   16,689,500 rows,    253,279 loans, 65.9 meses/loan promedio
      2007Q2:   18,509,014 rows,    287,263 loans, 64.4 meses/loan promedio
      2007Q3:   18,214,457 rows,    314,703 loans, 57.9 meses/loan promedio
      2007Q4:   22,564,316 rows,    391,181 loans, 57.7 meses/loan promedio
      2008Q1:   22,285,870 rows,    380,832 loans, 58.5 meses/loan promedio
      2008Q2:   25,127,120 rows,    444,374 loans, 56.5 meses/loan promedio
      2008Q3:   16,916,305 rows,    353,265 loans, 47.9 meses/loan promedio
      2008Q4:   14,800,701 rows,    342,122 loans, 43.3 meses/loan promedio
      2009Q1:   34,551,598 rows,    617,655 loans, 55.9 meses/loan promedio
      2009Q2:   47,502,495 rows,    744,708 loans, 63.8 meses/loan promedio
      2009Q3:   38,058,659 rows,    563,158 loans, 67.6 meses/loan promedio
      2009Q4:   25,216,851 rows,    393,046 loans, 64.2 meses/loan promedio
      2010Q1:   20,775,501 rows,    323,174 loans, 64.3 meses/loan promedio
      2010Q2:   20,276,608 rows,    334,202 loans, 60.7 meses/loan promedio
      2010Q3:   31,107,475 rows,    496,355 loans, 62.7 meses/loan promedio
      2010Q4:   48,119,146 rows,    672,768 loans, 71.5 meses/loan promedio
      2011Q1:   36,018,145 rows,    505,196 loans, 71.3 meses/loan promedio
      2011Q2:   17,840,193 rows,    282,034 loans, 63.3 meses/loan promedio
      2011Q3:   21,335,311 rows,    328,627 loans, 64.9 meses/loan promedio
      2011Q4:   41,697,023 rows,    587,765 loans, 70.9 meses/loan promedio
      2012Q1:   49,315,852 rows,    637,558 loans, 77.4 meses/loan promedio
      2012Q2:   43,912,933 rows,    542,027 loans, 81.0 meses/loan promedio
      2012Q3:   61,640,524 rows,    715,316 loans, 86.2 meses/loan promedio
      2012Q4:   65,799,936 rows,    721,612 loans, 91.2 meses/loan promedio
      2013Q1:   62,239,315 rows,    681,364 loans, 91.3 meses/loan promedio
      2013Q2:   57,890,247 rows,    662,920 loans, 87.3 meses/loan promedio
      2013Q3:   48,208,298 rows,    623,648 loans, 77.3 meses/loan promedio
      2013Q4:   27,896,350 rows,    421,733 loans, 66.1 meses/loan promedio
      2014Q1:   17,506,720 rows,    274,821 loans, 63.7 meses/loan promedio
      2014Q2:   20,147,731 rows,    326,157 loans, 61.8 meses/loan promedio
      2014Q3:   24,794,979 rows,    394,546 loans, 62.8 meses/loan promedio
      2014Q4:   25,610,178 rows,    404,165 loans, 63.4 meses/loan promedio
      2015Q1:   28,805,996 rows,    437,261 loans, 65.9 meses/loan promedio
      2015Q2:   33,921,746 rows,    498,516 loans, 68.0 meses/loan promedio
      2015Q3:   32,552,738 rows,    506,852 loans, 64.2 meses/loan promedio
      2015Q4:   27,212,330 rows,    430,015 loans, 63.3 meses/loan promedio
      2016Q1:   25,069,121 rows,    405,925 loans, 61.8 meses/loan promedio
      2016Q2:   34,065,508 rows,    535,506 loans, 63.6 meses/loan promedio
      2016Q3:   42,603,669 rows,    655,131 loans, 65.0 meses/loan promedio
      2016Q4:   44,934,034 rows,    692,673 loans, 64.9 meses/loan promedio
      2017Q1:   28,541,011 rows,    487,789 loans, 58.5 meses/loan promedio
      2017Q2:   26,174,996 rows,    492,517 loans, 53.1 meses/loan promedio
      2017Q3:   29,006,359 rows,    546,663 loans, 53.1 meses/loan promedio
      2017Q4:   26,881,935 rows,    519,882 loans, 51.7 meses/loan promedio
      2018Q1:   22,143,275 rows,    458,830 loans, 48.3 meses/loan promedio
      2018Q2:   18,903,352 rows,    454,581 loans, 41.6 meses/loan promedio
      2018Q3:   18,886,546 rows,    500,018 loans, 37.8 meses/loan promedio
      2018Q4:   14,649,360 rows,    421,040 loans, 34.8 meses/loan promedio
      2019Q1:   10,988,084 rows,    341,865 loans, 32.1 meses/loan promedio
      2019Q2:   13,282,677 rows,    407,389 loans, 32.6 meses/loan promedio
      2019Q3:   23,758,234 rows,    673,662 loans, 35.3 meses/loan promedio
      2019Q4:   25,275,383 rows,    690,018 loans, 36.6 meses/loan promedio
      2020Q1:   24,343,796 rows,    683,742 loans, 35.6 meses/loan promedio
      2020Q2:   51,364,328 rows,  1,235,767 loans, 41.6 meses/loan promedio
      2020Q3:   62,587,272 rows,  1,384,277 loans, 45.2 meses/loan promedio
      2020Q4:   69,693,523 rows,  1,513,861 loans, 46.0 meses/loan promedio
      2021Q1:   63,273,590 rows,  1,413,390 loans, 44.8 meses/loan promedio
      2021Q2:   56,173,202 rows,  1,316,215 loans, 42.7 meses/loan promedio
      2021Q3:   42,289,552 rows,  1,039,051 loans, 40.7 meses/loan promedio
      2021Q4:   39,038,823 rows,  1,013,436 loans, 38.5 meses/loan promedio
      2022Q1:   28,587,098 rows,    793,818 loans, 36.0 meses/loan promedio
      2022Q2:   18,891,807 rows,    567,613 loans, 33.3 meses/loan promedio
      2022Q3:   11,438,822 rows,    377,913 loans, 30.3 meses/loan promedio
      2022Q4:    7,490,618 rows,    273,662 loans, 27.4 meses/loan promedio
      2023Q1:    5,152,700 rows,    210,810 loans, 24.4 meses/loan promedio
      2023Q2:    6,062,927 rows,    276,714 loans, 21.9 meses/loan promedio
      2023Q3:    5,108,344 rows,    266,728 loans, 19.2 meses/loan promedio
      2023Q4:    3,460,457 rows,    215,934 loans, 16.0 meses/loan promedio
      2024Q1:    2,535,876 rows,    188,502 loans, 13.5 meses/loan promedio
      2024Q2:    2,704,635 rows,    254,648 loans, 10.6 meses/loan promedio
      2024Q3:    2,206,431 rows,    277,280 loans, 8.0 meses/loan promedio
      2024Q4:    1,256,272 rows,    250,546 loans, 5.0 meses/loan promedio
      2025Q1:      388,622 rows,    192,096 loans, 2.0 meses/loan promedio
    

## **Resumen**


```python
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
```


    StatementMeta(sparkpool01, 8, 17, Finished, Available, Finished)


    ================================================================================
    RESUMEN EJECUTIVO — EDA DE CALIDAD DE DATOS BRONZE
    ================================================================================
    
    Dataset: Fannie Mae SF Loan Performance (Bronze Parquet)
      Total filas:       3,174,135,934
      Total particiones: 101
      Total loans:       56,822,017
    
    CALIDAD DE DATOS:
      Columnas >99% null:                   18
      Columnas >50% null:                   13
      Columnas con centinelas descubiertos: 16
    
    CENTINELAS DESCUBIERTOS (Cell 2):
      current_actual_upb: 99=10, 999=34, 9999=202
      upb_at_time_of_removal: 99=7, 999=23, 9999=31
      total_principal_current: 99=3,176, 999=3,536, 9999=366
      foreclosure_costs: 99=116, 999=3
      property_preservation_repair_costs: 99=2, 999=5, 9999=2
      asset_recovery_costs: 99=1, 999=13
      misc_holding_expenses_credits: 99=9, 999=4
      associated_taxes_holding_property: 99=2, 999=10
      net_sales_proceeds: 9999=1
      credit_enhancement_proceeds: 99=3
      other_foreclosure_proceeds: 99=91, 999=9
      orig_loan_term: 99=3,935
      loan_age: 99=9,004,412
      remaining_months_legal_maturity: 99=4,928,062
      remaining_months_to_maturity: 99=5,166,971
      orig_cltv: 99=1,023,047
    
      current_delinquency_status='XX': centinela string
    
    PROXIMOS PASOS:
      1. Revisar columnas con >50% null — decidir si conservar o descartar
      2. Actualizar SENTINEL_VALUES en schema.py con centinelas descubiertos
      3. Implementar 02_silver_clean.py con reglas validadas
      4. Implementar 03_gold_subsets.py (colapso temporal + muestreo)
    
    
