```python
%%configure -f
{
    "executorMemory": "24g",
    "executorCores": 3,
    "numExecutors": 2,
    "driverMemory": "6g",
    "conf": {
        "spark.executor.memoryOverhead": "3g",
        "spark.sql.shuffle.partitions": "48",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "134217728",
        "spark.sql.files.maxPartitionBytes": "134217728",
        "spark.sql.parquet.compression.codec": "snappy"
    }
}
```


    StatementMeta(sparkpool01, 5, -1, Finished, Available, Finished)


    Warning: When setting executor and driver size using %%configure the requested size will be mapped to closest available size which may be bigger than requested. Please use "configure session" panel to select directly from available sizes.
    See https://go.microsoft.com/fwlink/?linkid=2170827
    


```python
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import input_file_name, regexp_extract
import time
import re

INPUT_PATH  = "abfss://raw-csvs@stsynapsemetadata.dfs.core.windows.net/"
OUTPUT_PATH = "abfss://synapse-fs@stsynapsemetadata.dfs.core.windows.net/bronze/"

BATCH_SIZE = 5  # archivos por lote (ajustar si hay OOM; 1 = mas seguro, 10 = mas rapido)
```


    StatementMeta(sparkpool01, 5, 2, Finished, Available, Finished)



```python
COLUMN_NAMES = [
    "reference_pool_id",                          #   1 — N/A SF
    "loan_id",                                    #   2 — PK
    "monthly_reporting_period",                   #   3 — PK, MMYYYY
    "channel",                                    #   4 — R/C/B
    "seller_name",                                #   5
    "servicer_name",                              #   6
    "master_servicer",                            #   7 — N/A SF
    "orig_interest_rate",                         #   8
    "current_interest_rate",                      #   9
    "orig_upb",                                   #  10
    "upb_at_issuance",                            #  11 — N/A SF
    "current_actual_upb",                         #  12 — masked 6 meses
    "orig_loan_term",                             #  13
    "origination_date",                           #  14 — MMYYYY
    "first_payment_date",                         #  15 — MMYYYY
    "loan_age",                                   #  16
    "remaining_months_legal_maturity",            #  17
    "remaining_months_to_maturity",               #  18
    "maturity_date",                              #  19 — MMYYYY
    "orig_ltv",                                   #  20
    "orig_cltv",                                  #  21
    "num_borrowers",                              #  22
    "dti",                                        #  23
    "borrower_credit_score",                      #  24
    "co_borrower_credit_score",                   #  25 — ~51% null
    "first_time_buyer",                           #  26 — Y/N
    "loan_purpose",                               #  27 — C/R/P/U
    "property_type",                              #  28
    "num_units",                                  #  29
    "occupancy_status",                           #  30 — P/S/I/U
    "property_state",                             #  31
    "msa",                                        #  32
    "zip_code_short",                             #  33
    "mi_percentage",                              #  34
    "amortization_type",                          #  35 — FRM/ARM
    "prepayment_penalty_indicator",               #  36 — Y/N
    "interest_only_indicator",                    #  37 — Y/N
    "io_first_pi_payment_date",                   #  38 — MMYYYY
    "months_to_amortization",                     #  39
    "current_delinquency_status",                 #  40 — 00-99, XX
    "loan_payment_history",                       #  41 — 48 chars
    "modification_flag",                          #  42 — Y/N
    "mi_cancellation_indicator",                  #  43 — N/A SF
    "zero_balance_code",                          #  44
    "zero_balance_effective_date",                #  45 — MMYYYY
    "upb_at_time_of_removal",                     #  46
    "repurchase_date",                            #  47 — N/A SF
    "scheduled_principal_current",                #  48 — N/A SF
    "total_principal_current",                    #  49
    "unscheduled_principal_current",              #  50 — N/A SF
    "last_paid_installment_date",                 #  51 — MMYYYY
    "foreclosure_date",                           #  52 — MMYYYY
    "disposition_date",                           #  53 — MMYYYY
    "foreclosure_costs",                          #  54
    "property_preservation_repair_costs",         #  55
    "asset_recovery_costs",                       #  56
    "misc_holding_expenses_credits",              #  57
    "associated_taxes_holding_property",          #  58
    "net_sales_proceeds",                         #  59
    "credit_enhancement_proceeds",                #  60
    "repurchase_make_whole_proceeds",             #  61
    "other_foreclosure_proceeds",                 #  62
    "modification_related_ni_upb",                #  63
    "principal_forgiveness_amount",               #  64
    "original_list_start_date",                   #  65 — N/A SF
    "original_list_price",                        #  66 — N/A SF
    "current_list_start_date",                    #  67 — N/A SF
    "current_list_price",                         #  68 — N/A SF
    "borrower_credit_score_at_issuance",          #  69 — N/A SF
    "co_borrower_credit_score_at_issuance",       #  70 — N/A SF
    "borrower_credit_score_current",              #  71 — N/A SF
    "co_borrower_credit_score_current",           #  72 — N/A SF
    "mi_type",                                    #  73
    "servicing_activity_indicator",               #  74
    "current_period_modification_loss",           #  75 — N/A SF
    "cumulative_modification_loss",               #  76 — N/A SF
    "current_period_credit_event_net_gain_loss",  #  77 — N/A SF
    "cumulative_credit_event_net_gain_loss",      #  78 — N/A SF
    "special_eligibility_program",                #  79
    "foreclosure_principal_writeoff",             #  80
    "relocation_mortgage_indicator",              #  81 — Y/N
    "zero_balance_code_change_date",              #  82 — N/A SF
    "loan_holdback_indicator",                    #  83 — N/A SF
    "loan_holdback_effective_date",               #  84 — N/A SF
    "delinquent_accrued_interest",                #  85 — N/A SF
    "property_valuation_method",                  #  86
    "high_balance_loan_indicator",                #  87
    "arm_initial_fixed_rate_5yr_indicator",       #  88 — N/A SF
    "arm_product_type",                           #  89 — N/A SF
    "initial_fixed_rate_period",                  #  90 — N/A SF
    "interest_rate_adjustment_frequency",         #  91 — N/A SF
    "next_interest_rate_adjustment_date",         #  92 — N/A SF
    "next_payment_change_date",                   #  93 — N/A SF
    "index_description",                          #  94 — N/A SF
    "arm_cap_structure",                          #  95 — N/A SF
    "initial_interest_rate_cap_up_pct",           #  96 — N/A SF
    "periodic_interest_rate_cap_up_pct",          #  97 — N/A SF
    "lifetime_interest_rate_cap_up_pct",          #  98 — N/A SF
    "mortgage_margin",                            #  99 — N/A SF
    "arm_balloon_indicator",                      # 100 — N/A SF
    "arm_plan_number",                            # 101 — N/A SF
    "borrower_assistance_plan",                   # 102 — desde abr 2020
    "hltv_refi_option_indicator",                 # 103
    "deal_name",                                  # 104 — N/A SF
    "repurchase_make_whole_proceeds_flag",        # 105
    "alternative_delinquency_resolution",         # 106
    "alternative_delinquency_resolution_count",   # 107
    "total_deferral_amount",                      # 108
    "payment_deferral_mod_event_indicator",       # 109 — SF: siempre "7"
    "interest_bearing_upb",                       # 110 — N/A SF
]

assert len(COLUMN_NAMES) == 110, f"Expected 110, got {len(COLUMN_NAMES)}"

BRONZE_SCHEMA = StructType([
    StructField(name, StringType(), True) for name in COLUMN_NAMES
])

print(f"Schema definido: {len(COLUMN_NAMES)} columnas, todas StringType")
```


    StatementMeta(sparkpool01, 5, 3, Finished, Available, Finished)


    Schema definido: 110 columnas, todas StringType
    


```python
# Listar CSVs en el container de entrada
all_files = mssparkutils.fs.ls(INPUT_PATH)
csv_files = sorted([f.name for f in all_files if f.name.endswith(".csv")])

# Validar que los nombres siguen el patron esperado (YYYYQn.csv)
quarter_pattern = re.compile(r"^\d{4}Q\d\.csv$")
invalid_files = [f for f in csv_files if not quarter_pattern.match(f)]
if invalid_files:
    print(f"ADVERTENCIA: {len(invalid_files)} archivos con nombre inesperado:")
    for f in invalid_files:
        print(f"  - {f}")
    print("Estos archivos se OMITEN del procesamiento.")
    csv_files = [f for f in csv_files if quarter_pattern.match(f)]

print(f"Archivos CSV encontrados: {len(csv_files)}")

# Verificar particiones ya escritas en Bronze (para reanudacion)
existing_partitions = set()
try:
    bronze_contents = mssparkutils.fs.ls(OUTPUT_PATH)
    for item in bronze_contents:
        if item.isDir and "acquisition_quarter=" in item.name:
            partition_value = item.name.split("=")[-1].rstrip("/")
            existing_partitions.add(partition_value)
    print(f"Particiones Bronze existentes: {len(existing_partitions)}")
except Exception:
    print("No hay datos Bronze previos — inicio desde cero.")

# Determinar archivos pendientes
files_to_process = []
files_skipped = []
for f in csv_files:
    quarter = f.replace(".csv", "")
    if quarter in existing_partitions:
        files_skipped.append(f)
    else:
        files_to_process.append(f)

if files_skipped:
    print(f"Archivos ya procesados (se omiten): {len(files_skipped)}")
    for f in files_skipped:
        print(f"  [SKIP] {f}")

print(f"\nArchivos pendientes de procesar: {len(files_to_process)}")
for f in files_to_process:
    print(f"  [TODO] {f}")

total_batches = (len(files_to_process) + BATCH_SIZE - 1) // BATCH_SIZE if files_to_process else 0
print(f"\nLotes de {BATCH_SIZE} archivos: {total_batches}")
```


    StatementMeta(sparkpool01, 5, 4, Finished, Available, Finished)


    Archivos CSV encontrados: 101
    Particiones Bronze existentes: 101
    Archivos ya procesados (se omiten): 101
      [SKIP] 2000Q1.csv
      [SKIP] 2000Q2.csv
      [SKIP] 2000Q3.csv
      [SKIP] 2000Q4.csv
      [SKIP] 2001Q1.csv
      [SKIP] 2001Q2.csv
      [SKIP] 2001Q3.csv
      [SKIP] 2001Q4.csv
      [SKIP] 2002Q1.csv
      [SKIP] 2002Q2.csv
      [SKIP] 2002Q3.csv
      [SKIP] 2002Q4.csv
      [SKIP] 2003Q1.csv
      [SKIP] 2003Q2.csv
      [SKIP] 2003Q3.csv
      [SKIP] 2003Q4.csv
      [SKIP] 2004Q1.csv
      [SKIP] 2004Q2.csv
      [SKIP] 2004Q3.csv
      [SKIP] 2004Q4.csv
      [SKIP] 2005Q1.csv
      [SKIP] 2005Q2.csv
      [SKIP] 2005Q3.csv
      [SKIP] 2005Q4.csv
      [SKIP] 2006Q1.csv
      [SKIP] 2006Q2.csv
      [SKIP] 2006Q3.csv
      [SKIP] 2006Q4.csv
      [SKIP] 2007Q1.csv
      [SKIP] 2007Q2.csv
      [SKIP] 2007Q3.csv
      [SKIP] 2007Q4.csv
      [SKIP] 2008Q1.csv
      [SKIP] 2008Q2.csv
      [SKIP] 2008Q3.csv
      [SKIP] 2008Q4.csv
      [SKIP] 2009Q1.csv
      [SKIP] 2009Q2.csv
      [SKIP] 2009Q3.csv
      [SKIP] 2009Q4.csv
      [SKIP] 2010Q1.csv
      [SKIP] 2010Q2.csv
      [SKIP] 2010Q3.csv
      [SKIP] 2010Q4.csv
      [SKIP] 2011Q1.csv
      [SKIP] 2011Q2.csv
      [SKIP] 2011Q3.csv
      [SKIP] 2011Q4.csv
      [SKIP] 2012Q1.csv
      [SKIP] 2012Q2.csv
      [SKIP] 2012Q3.csv
      [SKIP] 2012Q4.csv
      [SKIP] 2013Q1.csv
      [SKIP] 2013Q2.csv
      [SKIP] 2013Q3.csv
      [SKIP] 2013Q4.csv
      [SKIP] 2014Q1.csv
      [SKIP] 2014Q2.csv
      [SKIP] 2014Q3.csv
      [SKIP] 2014Q4.csv
      [SKIP] 2015Q1.csv
      [SKIP] 2015Q2.csv
      [SKIP] 2015Q3.csv
      [SKIP] 2015Q4.csv
      [SKIP] 2016Q1.csv
      [SKIP] 2016Q2.csv
      [SKIP] 2016Q3.csv
      [SKIP] 2016Q4.csv
      [SKIP] 2017Q1.csv
      [SKIP] 2017Q2.csv
      [SKIP] 2017Q3.csv
      [SKIP] 2017Q4.csv
      [SKIP] 2018Q1.csv
      [SKIP] 2018Q2.csv
      [SKIP] 2018Q3.csv
      [SKIP] 2018Q4.csv
      [SKIP] 2019Q1.csv
      [SKIP] 2019Q2.csv
      [SKIP] 2019Q3.csv
      [SKIP] 2019Q4.csv
      [SKIP] 2020Q1.csv
      [SKIP] 2020Q2.csv
      [SKIP] 2020Q3.csv
      [SKIP] 2020Q4.csv
      [SKIP] 2021Q1.csv
      [SKIP] 2021Q2.csv
      [SKIP] 2021Q3.csv
      [SKIP] 2021Q4.csv
      [SKIP] 2022Q1.csv
      [SKIP] 2022Q2.csv
      [SKIP] 2022Q3.csv
      [SKIP] 2022Q4.csv
      [SKIP] 2023Q1.csv
      [SKIP] 2023Q2.csv
      [SKIP] 2023Q3.csv
      [SKIP] 2023Q4.csv
      [SKIP] 2024Q1.csv
      [SKIP] 2024Q2.csv
      [SKIP] 2024Q3.csv
      [SKIP] 2024Q4.csv
      [SKIP] 2025Q1.csv
    
    Archivos pendientes de procesar: 0
    
    Lotes de 5 archivos: 0
    


```python
if not files_to_process:
    print("No hay archivos pendientes. Bronze ya esta completo.")
else:
    start_time = time.time()

    for i in range(0, len(files_to_process), BATCH_SIZE):
        batch_files = files_to_process[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        batch_quarters = [f.replace(".csv", "") for f in batch_files]

        print(f"\n{'=' * 65}")
        print(f"  LOTE {batch_num}/{total_batches}  |  {batch_quarters}")
        print(f"{'=' * 65}")

        batch_start = time.time()

        # Rutas completas ADLS
        paths = [INPUT_PATH + f for f in batch_files]

        # Leer CSVs con schema explicito
        df = spark.read.csv(
            paths,
            schema=BRONZE_SCHEMA,
            sep="|",
            header=False,
        )

        # Extraer acquisition_quarter del nombre del archivo fuente
        # input_file_name() retorna la ruta ADLS completa, ej:
        #   abfss://raw-csvs@stsynapsemetadata.dfs.core.windows.net/2019Q4.csv
        # El regex captura "2019Q4" (patron YYYYQn)
        df = (
            df.withColumn("_source_file", input_file_name())
            .withColumn(
                "acquisition_quarter",
                regexp_extract("_source_file", r"(\d{4}Q\d)", 1),
            )
            .drop("_source_file")
        )

        # Escribir Parquet particionado (append para soportar reanudacion)
        df.write.mode("append").partitionBy("acquisition_quarter").parquet(OUTPUT_PATH)

        elapsed_batch = time.time() - batch_start
        elapsed_total = time.time() - start_time
        remaining = (total_batches - batch_num) * (elapsed_total / batch_num)

        print(f"  Completado en {elapsed_batch / 60:.1f} min")
        print(f"  Acumulado: {elapsed_total / 60:.1f} min  |  Restante estimado: {remaining / 60:.1f} min")

    total_elapsed = time.time() - start_time
    print(f"\n{'=' * 65}")
    print(f"  ESCRITURA BRONZE COMPLETADA")
    print(f"  Tiempo total: {total_elapsed / 60:.1f} min ({total_elapsed / 3600:.1f} h)")
    print(f"{'=' * 65}")
```


    StatementMeta(sparkpool01, 5, 6, Finished, Available, Finished)


    No hay archivos pendientes. Bronze ya esta completo.
    


```python
print("Leyendo Bronze para validacion...")
df_bronze = spark.read.parquet(OUTPUT_PATH)

# Conteo de particiones
partitions = (
    df_bronze.select("acquisition_quarter")
    .distinct()
    .orderBy("acquisition_quarter")
    .collect()
)
partition_values = [row["acquisition_quarter"] for row in partitions]
print(f"\nParticiones: {len(partition_values)}")
print(f"  Primera: {partition_values[0]}  |  Ultima: {partition_values[-1]}")

# Conteo total de filas
total_rows = df_bronze.count()
print(f"\nFilas totales: {total_rows:,}")
print(f"  (Esperado: ~1-2 billion loan-months para dataset completo)")

# Verificar que no haya acquisition_quarter vacio
empty_aq = df_bronze.filter(df_bronze.acquisition_quarter == "").count()
if empty_aq > 0:
    print(f"\n  ADVERTENCIA: {empty_aq:,} filas con acquisition_quarter vacio!")
else:
    print(f"\n  OK: Todas las filas tienen acquisition_quarter asignado.")

# Conteo por particion (top 5 mas grandes y 5 mas pequenas)
print("\nFilas por particion (muestra):")
counts = (
    df_bronze.groupBy("acquisition_quarter")
    .count()
    .orderBy("acquisition_quarter")
    .collect()
)
print(f"  {'Particion':<12} {'Filas':>15}")
print(f"  {'-'*12} {'-'*15}")
for row in counts[:5]:
    print(f"  {row['acquisition_quarter']:<12} {row['count']:>15,}")
if len(counts) > 10:
    print(f"  {'...':^27}")
for row in counts[-5:]:
    print(f"  {row['acquisition_quarter']:<12} {row['count']:>15,}")

# Muestra visual (5 filas del primer trimestre)
print(f"\nMuestra de datos ({partition_values[0]}):")
df_bronze.filter(
    df_bronze.acquisition_quarter == partition_values[0]
).select("loan_id", "monthly_reporting_period", "channel", "orig_upb", "borrower_credit_score", "acquisition_quarter").show(5, truncate=False)

# Schema
print("\nSchema Bronze:")
df_bronze.printSchema()
```


    StatementMeta(sparkpool01, 5, 5, Finished, Available, Finished)


    Leyendo Bronze para validacion...
    
    Particiones: 101
      Primera: 2000Q1  |  Ultima: 2025Q1
    
    Filas totales: 3,174,135,934
      (Esperado: ~1-2 billion loan-months para dataset completo)
    
      OK: Todas las filas tienen acquisition_quarter asignado.
    
    Filas por particion (muestra):
      Particion              Filas
      ------------ ---------------
      2000Q1             9,229,283
      2000Q2             8,337,215
      2000Q3             8,862,186
      2000Q4            10,348,845
      2001Q1            15,822,944
                  ...            
      2024Q1             2,535,876
      2024Q2             2,704,635
      2024Q3             2,206,431
      2024Q4             1,256,272
      2025Q1               388,622
    
    Muestra de datos (2000Q1):
    +------------+------------------------+-------+--------+---------------------+-------------------+
    |loan_id     |monthly_reporting_period|channel|orig_upb|borrower_credit_score|acquisition_quarter|
    +------------+------------------------+-------+--------+---------------------+-------------------+
    |100007365142|012000                  |R      |75000.00|763                  |2000Q1             |
    |100007365142|022000                  |R      |75000.00|763                  |2000Q1             |
    |100007365142|032000                  |R      |75000.00|763                  |2000Q1             |
    |100007365142|042000                  |R      |75000.00|763                  |2000Q1             |
    |100007365142|052000                  |R      |75000.00|763                  |2000Q1             |
    +------------+------------------------+-------+--------+---------------------+-------------------+
    only showing top 5 rows
    
    
    Schema Bronze:
    root
     |-- reference_pool_id: string (nullable = true)
     |-- loan_id: string (nullable = true)
     |-- monthly_reporting_period: string (nullable = true)
     |-- channel: string (nullable = true)
     |-- seller_name: string (nullable = true)
     |-- servicer_name: string (nullable = true)
     |-- master_servicer: string (nullable = true)
     |-- orig_interest_rate: string (nullable = true)
     |-- current_interest_rate: string (nullable = true)
     |-- orig_upb: string (nullable = true)
     |-- upb_at_issuance: string (nullable = true)
     |-- current_actual_upb: string (nullable = true)
     |-- orig_loan_term: string (nullable = true)
     |-- origination_date: string (nullable = true)
     |-- first_payment_date: string (nullable = true)
     |-- loan_age: string (nullable = true)
     |-- remaining_months_legal_maturity: string (nullable = true)
     |-- remaining_months_to_maturity: string (nullable = true)
     |-- maturity_date: string (nullable = true)
     |-- orig_ltv: string (nullable = true)
     |-- orig_cltv: string (nullable = true)
     |-- num_borrowers: string (nullable = true)
     |-- dti: string (nullable = true)
     |-- borrower_credit_score: string (nullable = true)
     |-- co_borrower_credit_score: string (nullable = true)
     |-- first_time_buyer: string (nullable = true)
     |-- loan_purpose: string (nullable = true)
     |-- property_type: string (nullable = true)
     |-- num_units: string (nullable = true)
     |-- occupancy_status: string (nullable = true)
     |-- property_state: string (nullable = true)
     |-- msa: string (nullable = true)
     |-- zip_code_short: string (nullable = true)
     |-- mi_percentage: string (nullable = true)
     |-- amortization_type: string (nullable = true)
     |-- prepayment_penalty_indicator: string (nullable = true)
     |-- interest_only_indicator: string (nullable = true)
     |-- io_first_pi_payment_date: string (nullable = true)
     |-- months_to_amortization: string (nullable = true)
     |-- current_delinquency_status: string (nullable = true)
     |-- loan_payment_history: string (nullable = true)
     |-- modification_flag: string (nullable = true)
     |-- mi_cancellation_indicator: string (nullable = true)
     |-- zero_balance_code: string (nullable = true)
     |-- zero_balance_effective_date: string (nullable = true)
     |-- upb_at_time_of_removal: string (nullable = true)
     |-- repurchase_date: string (nullable = true)
     |-- scheduled_principal_current: string (nullable = true)
     |-- total_principal_current: string (nullable = true)
     |-- unscheduled_principal_current: string (nullable = true)
     |-- last_paid_installment_date: string (nullable = true)
     |-- foreclosure_date: string (nullable = true)
     |-- disposition_date: string (nullable = true)
     |-- foreclosure_costs: string (nullable = true)
     |-- property_preservation_repair_costs: string (nullable = true)
     |-- asset_recovery_costs: string (nullable = true)
     |-- misc_holding_expenses_credits: string (nullable = true)
     |-- associated_taxes_holding_property: string (nullable = true)
     |-- net_sales_proceeds: string (nullable = true)
     |-- credit_enhancement_proceeds: string (nullable = true)
     |-- repurchase_make_whole_proceeds: string (nullable = true)
     |-- other_foreclosure_proceeds: string (nullable = true)
     |-- modification_related_ni_upb: string (nullable = true)
     |-- principal_forgiveness_amount: string (nullable = true)
     |-- original_list_start_date: string (nullable = true)
     |-- original_list_price: string (nullable = true)
     |-- current_list_start_date: string (nullable = true)
     |-- current_list_price: string (nullable = true)
     |-- borrower_credit_score_at_issuance: string (nullable = true)
     |-- co_borrower_credit_score_at_issuance: string (nullable = true)
     |-- borrower_credit_score_current: string (nullable = true)
     |-- co_borrower_credit_score_current: string (nullable = true)
     |-- mi_type: string (nullable = true)
     |-- servicing_activity_indicator: string (nullable = true)
     |-- current_period_modification_loss: string (nullable = true)
     |-- cumulative_modification_loss: string (nullable = true)
     |-- current_period_credit_event_net_gain_loss: string (nullable = true)
     |-- cumulative_credit_event_net_gain_loss: string (nullable = true)
     |-- special_eligibility_program: string (nullable = true)
     |-- foreclosure_principal_writeoff: string (nullable = true)
     |-- relocation_mortgage_indicator: string (nullable = true)
     |-- zero_balance_code_change_date: string (nullable = true)
     |-- loan_holdback_indicator: string (nullable = true)
     |-- loan_holdback_effective_date: string (nullable = true)
     |-- delinquent_accrued_interest: string (nullable = true)
     |-- property_valuation_method: string (nullable = true)
     |-- high_balance_loan_indicator: string (nullable = true)
     |-- arm_initial_fixed_rate_5yr_indicator: string (nullable = true)
     |-- arm_product_type: string (nullable = true)
     |-- initial_fixed_rate_period: string (nullable = true)
     |-- interest_rate_adjustment_frequency: string (nullable = true)
     |-- next_interest_rate_adjustment_date: string (nullable = true)
     |-- next_payment_change_date: string (nullable = true)
     |-- index_description: string (nullable = true)
     |-- arm_cap_structure: string (nullable = true)
     |-- initial_interest_rate_cap_up_pct: string (nullable = true)
     |-- periodic_interest_rate_cap_up_pct: string (nullable = true)
     |-- lifetime_interest_rate_cap_up_pct: string (nullable = true)
     |-- mortgage_margin: string (nullable = true)
     |-- arm_balloon_indicator: string (nullable = true)
     |-- arm_plan_number: string (nullable = true)
     |-- borrower_assistance_plan: string (nullable = true)
     |-- hltv_refi_option_indicator: string (nullable = true)
     |-- deal_name: string (nullable = true)
     |-- repurchase_make_whole_proceeds_flag: string (nullable = true)
     |-- alternative_delinquency_resolution: string (nullable = true)
     |-- alternative_delinquency_resolution_count: string (nullable = true)
     |-- total_deferral_amount: string (nullable = true)
     |-- payment_deferral_mod_event_indicator: string (nullable = true)
     |-- interest_bearing_upb: string (nullable = true)
     |-- acquisition_quarter: string (nullable = true)
    
    
