"""
Schema del dataset Fannie Mae SF Loan Performance (110 columnas, Enhanced format).

Fuente:     docs/dataset/crt-file-layout-and-glossary.pdf
Referencia: context/SCHEMA_110_COLUMNAS.md
Validado:   1) EDA con DuckDB sobre 3 samples (2007Q3, 2019Q4, 2021Q2)
               - 110 columnas exactas en todas las filas
               - 0 filas malformadas
               - 39 columnas CAS/CIRT completamente vacias
            2) EDA sobre Bronze completo (3.174.135.934 filas, 2026-02-20)
               - 0 duplicados PK, 0 fechas malformadas
               - Centinelas numericos NO presentes (solo XX=30,4M filas)
               - 3 rangos expandidos post-validacion (interest rates, loan_age)
               - current_delinquency_status: confirmado cast a IntegerType

Uso:
    - Bronze:  COLUMN_NAMES + BRONZE_SCHEMA (todo StringType)
    - EDA:     SENTINEL_VALUES, SILVER_RANGE_VALIDATIONS (verificacion pre-Silver)
    - Silver:  SF_NA_COLUMNS (drop), DATE_COLUMNS, NUMERIC_DOUBLE_COLUMNS,
               NUMERIC_INT_COLUMNS, SENTINEL_VALUES (limpieza)
    - Gold:    Derivaciones a nivel prestamo (colapso loan-month -> loan-level)
"""

from pyspark.sql.types import StructType, StructField, StringType


# ─────────────────────────────────────────────────────────────────────────────
# 110 nombres de columnas (orden posicional 1-110 del glossary oficial)
# ─────────────────────────────────────────────────────────────────────────────

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

assert len(COLUMN_NAMES) == 110, f"Expected 110 columns, got {len(COLUMN_NAMES)}"


# ─────────────────────────────────────────────────────────────────────────────
# Bronze Schema: todas las columnas como StringType (sin transformaciones)
# ─────────────────────────────────────────────────────────────────────────────

BRONZE_SCHEMA = StructType([
    StructField(name, StringType(), True) for name in COLUMN_NAMES
])


# ─────────────────────────────────────────────────────────────────────────────
# 39 columnas N/A para SF (CAS/CIRT-only) — se eliminan en Silver
# Posiciones 1-indexed del glossary oficial
# ─────────────────────────────────────────────────────────────────────────────

SF_NA_POSITIONS = [
    1, 7, 11, 43, 47, 48, 50,
    65, 66, 67, 68, 69, 70, 71, 72,
    75, 76, 77, 78,
    82, 83, 84, 85,
    88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101,
    104, 110,
]

SF_NA_COLUMNS = [
    "reference_pool_id",
    "master_servicer",
    "upb_at_issuance",
    "mi_cancellation_indicator",
    "repurchase_date",
    "scheduled_principal_current",
    "unscheduled_principal_current",
    "original_list_start_date",
    "original_list_price",
    "current_list_start_date",
    "current_list_price",
    "borrower_credit_score_at_issuance",
    "co_borrower_credit_score_at_issuance",
    "borrower_credit_score_current",
    "co_borrower_credit_score_current",
    "current_period_modification_loss",
    "cumulative_modification_loss",
    "current_period_credit_event_net_gain_loss",
    "cumulative_credit_event_net_gain_loss",
    "zero_balance_code_change_date",
    "loan_holdback_indicator",
    "loan_holdback_effective_date",
    "delinquent_accrued_interest",
    "arm_initial_fixed_rate_5yr_indicator",
    "arm_product_type",
    "initial_fixed_rate_period",
    "interest_rate_adjustment_frequency",
    "next_interest_rate_adjustment_date",
    "next_payment_change_date",
    "index_description",
    "arm_cap_structure",
    "initial_interest_rate_cap_up_pct",
    "periodic_interest_rate_cap_up_pct",
    "lifetime_interest_rate_cap_up_pct",
    "mortgage_margin",
    "arm_balloon_indicator",
    "arm_plan_number",
    "deal_name",
    "interest_bearing_upb",
]

assert len(SF_NA_COLUMNS) == 39, f"Expected 39 N/A columns, got {len(SF_NA_COLUMNS)}"


# ─────────────────────────────────────────────────────────────────────────────
# Valores centinela: parecen datos validos pero son "dato desconocido"
# Se deben reemplazar por null ANTES de castear a tipo numerico en Silver.
# Fuente: FAQs oficiales + validacion en repo mrsasayoo/proyecto_deep_statistics
# ─────────────────────────────────────────────────────────────────────────────

SENTINEL_VALUES = {
    "borrower_credit_score":      ["9999"],   # FICO desconocido (pos 24)
    "co_borrower_credit_score":   ["9999"],   # FICO co-prestatario desconocido (pos 25)
    "dti":                        ["999"],    # DTI desconocido (pos 23)
    "orig_ltv":                   ["999"],    # LTV desconocido (pos 20)
    "orig_cltv":                  ["999"],    # CLTV desconocido (pos 21)
    "mi_percentage":              ["999"],    # MI% desconocido (pos 34)
    "num_borrowers":              ["99"],     # Num. prestatarios desconocido (pos 22)
    "current_delinquency_status": ["XX"],     # Estado morosidad no disponible (pos 40)
}


# ─────────────────────────────────────────────────────────────────────────────
# Columnas de fecha (formato MMYYYY, 6 chars sin separador)
# Solo las que aplican a SF (excluye CAS/CIRT-only)
# Cast en Silver: to_date(col, "MMyyyy")
# ─────────────────────────────────────────────────────────────────────────────

DATE_COLUMNS = [
    "monthly_reporting_period",       #  3 — PK parte 2
    "origination_date",               # 14
    "first_payment_date",             # 15
    "maturity_date",                  # 19
    "zero_balance_effective_date",    # 45
    "last_paid_installment_date",     # 51
    "foreclosure_date",              # 52
    "disposition_date",              # 53
]

# Fechas SF adicionales con baja poblacion (solo aplican a subconjuntos)
DATE_COLUMNS_SPARSE = [
    "io_first_pi_payment_date",      # 38 — solo loans IO (raros en SF FRM)
]


# ─────────────────────────────────────────────────────────────────────────────
# Columnas numericas: cast a DoubleType en Silver
# Incluye todas las que tienen decimales o valores monetarios
# ─────────────────────────────────────────────────────────────────────────────

NUMERIC_DOUBLE_COLUMNS = [
    "orig_interest_rate",                #  8
    "current_interest_rate",             #  9
    "orig_upb",                          # 10
    "current_actual_upb",                # 12 — masked primeros 6 meses
    "mi_percentage",                     # 34
    "upb_at_time_of_removal",            # 46
    "total_principal_current",           # 49
    "foreclosure_costs",                 # 54 — NULL = $0 (FAQ #46)
    "property_preservation_repair_costs", # 55 — NULL = $0
    "asset_recovery_costs",              # 56 — NULL = $0
    "misc_holding_expenses_credits",     # 57 — NULL = $0
    "associated_taxes_holding_property", # 58 — NULL = $0
    "net_sales_proceeds",                # 59 — NULL = $0
    "credit_enhancement_proceeds",       # 60 — NULL = $0
    "repurchase_make_whole_proceeds",    # 61 — NULL = $0
    "other_foreclosure_proceeds",        # 62 — NULL = $0
    "modification_related_ni_upb",       # 63
    "principal_forgiveness_amount",      # 64
    "foreclosure_principal_writeoff",    # 80
    "total_deferral_amount",             # 108
]


# ─────────────────────────────────────────────────────────────────────────────
# Columnas numericas: cast a IntegerType en Silver
# Valores enteros (conteos, scores, ratios sin decimales)
# ─────────────────────────────────────────────────────────────────────────────

NUMERIC_INT_COLUMNS = [
    "orig_loan_term",                            # 13
    "loan_age",                                  # 16 — puede ser -1 (FAQ #24)
    "remaining_months_legal_maturity",           # 17
    "remaining_months_to_maturity",              # 18 — blank si modificado (FAQ #26)
    "orig_ltv",                                  # 20
    "orig_cltv",                                 # 21
    "num_borrowers",                             # 22
    "dti",                                       # 23
    "borrower_credit_score",                     # 24
    "co_borrower_credit_score",                  # 25
    "current_delinquency_status",                # 40 — 00-99; XX limpiado por SENTINEL_VALUES antes de cast
    "num_units",                                 # 29
    "months_to_amortization",                    # 39 — solo loans IO
    "alternative_delinquency_resolution_count",  # 107
]


# ─────────────────────────────────────────────────────────────────────────────
# Columnas que permanecen como StringType en Silver
# Categoricas, flags, campos de texto, y campos especiales
# ─────────────────────────────────────────────────────────────────────────────

STRING_COLUMNS = [
    "loan_id",                          #  2 — PK
    "channel",                          #  4 — R/C/B
    "seller_name",                      #  5 — "Other" si <1% volumen
    "servicer_name",                    #  6 — "Other" si <1% UPB
    "amortization_type",                # 35 — FRM/ARM (SF: solo FRM)
    "prepayment_penalty_indicator",     # 36 — Y/N
    "interest_only_indicator",          # 37 — Y/N
    # current_delinquency_status movido a NUMERIC_INT_COLUMNS (cast a int, EDA 2026-02-20)
    "loan_payment_history",             # 41 — 48 chars, NO castear
    "modification_flag",                # 42 — Y/N
    "zero_balance_code",                # 44 — 01/02/03/06/09/15/16/96
    "first_time_buyer",                 # 26 — Y/N
    "loan_purpose",                     # 27 — C/R/P/U
    "property_type",                    # 28 — CO/CP/PU/MH/SF
    "occupancy_status",                 # 30 — P/S/I/U
    "property_state",                   # 31 — 2 letras
    "msa",                              # 32 — 5 digitos o "00000"
    "zip_code_short",                   # 33 — 3 digitos
    "mi_type",                          # 73 — 1/2/3/null
    "servicing_activity_indicator",     # 74 — Y/N
    "special_eligibility_program",      # 79 — F/H/R/O/7/9
    "relocation_mortgage_indicator",    # 81 — Y/N
    "property_valuation_method",        # 86 — A/C/P/R/W/O (solo >=ene 2017)
    "high_balance_loan_indicator",      # 87 — Y/N
    "borrower_assistance_plan",         # 102 — F/R/T/O/N/7/9 (post-2020); Y/N (pre-2020)
    "hltv_refi_option_indicator",       # 103 — Y/N
    "repurchase_make_whole_proceeds_flag",  # 105 — Y/N
    "alternative_delinquency_resolution",   # 106 — P/C/D/7/9
    "payment_deferral_mod_event_indicator", # 109 — SF: siempre "7"
    "acquisition_quarter",              # columna derivada en Bronze (metadata)
]


# ─────────────────────────────────────────────────────────────────────────────
# Validaciones de rango para Silver
# Valores fuera de estos rangos → null (probablemente error de datos)
# ─────────────────────────────────────────────────────────────────────────────

SILVER_RANGE_VALIDATIONS = {
    "borrower_credit_score":    (300, 850),   # FICO valido
    "co_borrower_credit_score": (300, 850),   # FICO valido
    "dti":                      (0, 65),      # Limite Fannie Mae ~50, post-mod hasta 65
    "orig_ltv":                 (1, 200),     # >100 posible (underwater), >200 corrupto
    "orig_cltv":                (1, 200),
    "orig_interest_rate":       (0, 20),      # EDA: max=16.50 legit; 0% post-mod posible
    "current_interest_rate":    (0, 20),      # EDA: min=0.00 legit post-modificacion
    "num_borrowers":            (1, 10),      # Razonable: 1-4 en practica
    "num_units":                (1, 4),       # Solo SF 1-4 unidades
    "orig_loan_term":           (1, 480),     # 1-40 anios en meses
    "loan_age":                 (-5, 600),    # EDA: min=-3 (FAQ #30: adquirido antes de 1er pago)
    "mi_percentage":            (0, 60),      # Rango razonable de MI
    "current_delinquency_status": (0, 99),    # 00-99 meses; XX limpiado por SENTINEL_VALUES
}


# ─────────────────────────────────────────────────────────────────────────────
# Armonizacion de borrower_assistance_plan (cambio de codificacion jul 2020)
# Pre-julio 2020: "Y" (si, en forbearance) / "N" (no)
# Post-julio 2020: F/R/T/O/N/7/9
# Regla: mapear "Y" → "F" (forbearance), mantener "N" como "N"
# ─────────────────────────────────────────────────────────────────────────────

BAP_HARMONIZATION = {
    "Y": "F",   # Yes (forbearance) → F (Forbearance Plan)
    "N": "N",   # No → No
    # F, R, T, O, 7, 9 se mantienen tal cual (post-2020)
}


# ─────────────────────────────────────────────────────────────────────────────
# Columnas donde NULL semantico = $0 (FAQ #46)
# En Silver: NO imputar con 0. En Gold/subsets: usar COALESCE(col, 0).
# ─────────────────────────────────────────────────────────────────────────────

NULL_MEANS_ZERO_COLUMNS = [
    "foreclosure_costs",                 # 54
    "property_preservation_repair_costs", # 55
    "asset_recovery_costs",              # 56
    "misc_holding_expenses_credits",     # 57
    "associated_taxes_holding_property", # 58
    "net_sales_proceeds",                # 59
    "credit_enhancement_proceeds",       # 60
    "repurchase_make_whole_proceeds",    # 61
    "other_foreclosure_proceeds",        # 62
]


# ─────────────────────────────────────────────────────────────────────────────
# Gold Virtual — Constantes para 03_gold_subsets.py
# Ref: context/VACIOS_GOLD.md §8.2, HALLAZGOS_AUDITORIA_GOLD.md §§1-8
# ─────────────────────────────────────────────────────────────────────────────

# Atributos estaticos: constantes por loan_id → first(col, ignorenulls=True) es seguro
GOLD_STATIC_COLUMNS = [
    "loan_id",                        # PK
    "acquisition_quarter",            # metadata (de Bronze)
    "channel",                        #  4
    "seller_name",                    #  5 — no en subsets, solo para groupBy
    "servicer_name",                  #  6 — no en subsets
    "orig_interest_rate",             #  8
    "orig_upb",                       # 10
    "orig_loan_term",                 # 13
    "origination_date",               # 14
    "first_payment_date",             # 15
    "maturity_date",                  # 19
    "orig_ltv",                       # 20
    "orig_cltv",                      # 21
    "num_borrowers",                  # 22
    "dti",                            # 23
    "borrower_credit_score",          # 24
    "co_borrower_credit_score",       # 25
    "first_time_buyer",               # 26
    "loan_purpose",                   # 27
    "property_type",                  # 28
    "num_units",                      # 29
    "occupancy_status",               # 30
    "property_state",                 # 31
    "mi_percentage",                  # 34
    "prepayment_penalty_indicator",   # 36 — no en subsets (varianza ~0)
    "interest_only_indicator",        # 37 — no en subsets (varianza ~0)
    "mi_type",                        # 73
    "high_balance_loan_indicator",    # 87
]

# Codigos ZBC que definen default
DEFAULT_ZBC_CODES = ["02", "03", "09", "15"]

# Bins de vintage para estratificacion temporal
VINTAGE_BINS = {
    "Pre-crisis":  (2000, 2008),   # 2000-2008
    "Recovery":    (2009, 2013),   # 2009-2013
    "Stable":      (2014, 2019),   # 2014-2019
    "COVID-era":   (2020, 2025),   # 2020-2025
}

# Targets de muestreo por estrato para Subset A (~50k loans)
STRATUM_TARGETS_A = {
    "performing":   25000,
    "early_dlq":    10000,
    "serious_dlq":  10000,
    "default":       5000,
}

# Targets de muestreo para Subset B (~500k loans, proporcional ×10)
STRATUM_TARGETS_B = {
    "performing":  250000,
    "early_dlq":   100000,
    "serious_dlq": 100000,
    "default":      50000,
}

# Columnas de Subset A (AFE/AFC en R, ~50k loans, CSV)
# 26 features AFE + 1 indicador + 5 metadata + 9 target/validacion = 41
SUBSET_A_COLUMNS = [
    # --- PK + Metadata (5) ---
    "loan_id",
    "acquisition_quarter",
    "stratum",
    "origination_year",
    "property_state",
    # --- V1: Prestatario (5) ---
    "borrower_credit_score",
    "dti",
    "num_borrowers",
    "first_time_buyer",
    "has_coborrower",
    # --- V2: Prestamo (7) ---
    "orig_interest_rate",
    "orig_upb",
    "orig_loan_term",
    "orig_ltv",
    "orig_cltv",
    "mi_percentage",
    "high_balance_loan_indicator",
    # --- V3: Comportamiento (9) ---
    "max_delinquency_status",
    "months_delinquent_30plus",
    "months_delinquent_90plus",
    "ever_modified",
    "had_forbearance",
    "loan_duration_months",
    "time_to_first_delinquency",
    "ph_months_delinquent_24",
    "ph_max_delinquency_24",
    # --- V4: Originacion (5) ---
    "channel",
    "loan_purpose",
    "property_type",
    "occupancy_status",
    "vintage_bin",
    # --- Indicador (1) ---
    "lph_available",
    # --- Target + Validacion (9) ---
    "is_default",
    "net_loss",
    "net_severity",
    "ever_d90",
    "ever_d180",
    "ever_foreclosed",
    "final_zero_balance_code",
    "is_clean_liquidation",
    "time_to_default",
]

# Columnas adicionales de Subset B (VAE en Colab, ~500k loans, Parquet)
# Subset B = SUBSET_A_COLUMNS + SUBSET_B_EXTRA_COLUMNS = 41 + 10 = 51
SUBSET_B_EXTRA_COLUMNS = [
    # --- V1+ (1) ---
    "co_borrower_credit_score",
    # --- V2+ (2) ---
    "mi_type",
    "num_units",
    # --- V3+ (4) ---
    "months_delinquent_60plus",
    "had_servicing_transfer",
    "ph_months_current_24",
    "ph_recent_delinquency_3m",
    # --- Financiero derivado (3) ---
    "rate_spread",
    "upb_paydown_pct",
    "last_active_upb",
]


# ─────────────────────────────────────────────────────────────────────────────
# Rutas ADLS Gen2 (Azure Synapse)
# ─────────────────────────────────────────────────────────────────────────────

ADLS_PATHS = {
    "raw_csvs":  "abfss://raw-csvs@stsynapsemetadata.dfs.core.windows.net/",
    "bronze":    "abfss://synapse-fs@stsynapsemetadata.dfs.core.windows.net/bronze/",
    "silver":    "abfss://synapse-fs@stsynapsemetadata.dfs.core.windows.net/silver/",
    "subsets":   "abfss://synapse-fs@stsynapsemetadata.dfs.core.windows.net/gold/subsets/",
    "subset_a":  "abfss://synapse-fs@stsynapsemetadata.dfs.core.windows.net/gold/subsets/subset_a/",
    "subset_b":  "abfss://synapse-fs@stsynapsemetadata.dfs.core.windows.net/gold/subsets/subset_b/",
}
