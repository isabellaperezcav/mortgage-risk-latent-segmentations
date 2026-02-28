# Guia de Subsets para Fases Downstream (AFE/AFC + VAE)

> **Generado:** 2026-02-20
> **Para:** Equipo de Analitica de Datos I — Situacion 3
> **Prerequisito:** `03_gold_subsets.py` ejecutado sobre Azure Synapse Spark
> **Fuentes:** Fannie Mae SF Loan Performance Dataset (Enhanced, post-Oct 2020)

---

## 0. Resumen Ejecutivo

Este documento describe los dos datasets de salida del pipeline ETL (Bronze → Silver → Gold Virtual → Subsets). Estan disenados para alimentar las fases downstream del proyecto:

| Subset | Destino | Herramienta | Filas aprox. | Columnas | Formato | Tamano est. |
|--------|---------|-------------|-------------|----------|---------|-------------|
| **A** | AFE/AFC multi-vista | R (psych + lavaan) | ~50.000 loans | 41 | CSV (con header) | ~10-20 MB |
| **B** | VAE + Clustering | Python/PyTorch (Colab T4) | ~500.000 loans | 51 | Parquet (snappy) | ~80-120 MB |

**Cada fila representa UN prestamo hipotecario** (no un mes). El pipeline colapso ~55 meses promedio de historial mensual por prestamo en un unico registro con metricas agregadas de comportamiento.

**Poblacion original:** ~57 millones de prestamos hipotecarios unifamiliares de Fannie Mae, originados entre 2000 y 2025. Se aplico muestreo estratificado deliberado para sobre-representar eventos de riesgo (ver §3).

---

## 1. Contexto del Dataset

### 1.1 Que es Fannie Mae

Fannie Mae (Federal National Mortgage Association) es una empresa respaldada por el gobierno de EE.UU. que compra hipotecas a los bancos. Cuando un banco da un prestamo hipotecario, puede venderselo a Fannie Mae, quien entonces "garantiza" ese prestamo.

El dataset SF Loan Performance rastrea que pasa con esos prestamos **mes a mes**: el deudor paga a tiempo? Se atrasa? Paga anticipadamente? Cae en default?

### 1.2 Que contiene

- **Solo hipotecas unifamiliares** (1-4 unidades) de tasa fija (FRM)
- **Documentacion completa**, completamente amortizantes
- **Adquiridas por Fannie Mae desde enero 2000**
- **Excluye:** ARMs, balloon, interest-only, HARP, FHA/VA, LTV >97%

### 1.3 Pipeline ETL

```
820 GB CSVs (101 archivos, 110 columnas, pipe-delimited)
    │
    ▼  Bronze: CSV → Parquet (3.174.135.934 loan-months)
    │
    ▼  Silver: Limpieza de tipos, centinelas → NULL, schema enforcement (72 columnas tipadas)
    │
    ▼  Gold Virtual: groupBy("loan_id") — colapso temporal → ~57M loans × ~65 columnas
    │
    ├── Subset A: Muestreo estratificado → ~50k loans × 41 cols (CSV)
    └── Subset B: Muestreo estratificado → ~500k loans × 51 cols (Parquet)
```

---

## 2. Glosario Hipotecario Rapido

| Termino | Que es | Por que importa |
|---------|--------|-----------------|
| **LTV (Loan-to-Value)** | Monto del prestamo / valor de la propiedad. Ej: prestamo $160K, casa $200K → LTV=80% | LTV alto = mas riesgo. >80% requiere seguro hipotecario (MI) |
| **CLTV (Combined LTV)** | Igual que LTV pero suma TODOS los prestamos sobre la propiedad | Captura exposicion total real |
| **DTI (Debt-to-Income)** | Deuda mensual total / ingreso bruto mensual del prestatario | DTI alto = deudor muy comprometido. Rango: 0-65 |
| **FICO Score** | Puntuacion 300-850 que resume historial crediticio | >740 muy bueno, <670 riesgoso. Predice fuertemente default |
| **UPB (Unpaid Principal Balance)** | Capital que falta por pagar (sin intereses) | Cifra central para calcular perdidas en default |
| **Delinquency** | Meses de atraso: 0=al dia, 1=30-59d, 2=60-89d, 3=90-119d, ..., 99=99+ meses | 90+ dias (DLQ >= 3) activa procesos de mitigacion |
| **Prepayment** | Pagar todo el prestamo antes de tiempo (venta de casa, refinanciacion) | ~71% de prestamos terminan asi; NO es default |
| **Default** | Prestamo liquidado por incumplimiento (ZBC IN 02, 03, 09, 15) | ~1,2% de todos los prestamos |
| **Foreclosure** | Proceso legal donde el banco recupera la propiedad | Puede durar meses a anos |
| **Short Sale** | Venta por menos del UPB con aprobacion del banco | Perdida parcial para el banco |
| **REO (Real Estate Owned)** | Propiedad que el banco no vendio en remate | Peor resultado para el prestamista |
| **Modification** | Cambiar los terminos del prestamo (tasa, plazo) para evitar default | Flag binaria en los subsets |
| **MI (Mortgage Insurance)** | Seguro que protege al prestamista si LTV >80% | `mi_percentage` indica cobertura; `mi_type` indica tipo |
| **Servicer** | Empresa que administra el prestamo dia a dia (cobra, gestiona moras) | Cambios de servicer contaminan datos de DLQ (ya mitigado) |
| **Net Severity (LGD)** | Perdida Dado Default = Net Loss / UPB. Benchmark: 14-22% post-2012, 30-45% pre-crisis | Metrica clave de validacion financiera |
| **Vintage** | Ano de originacion del prestamo. Prestamos del mismo ano comparten condiciones de mercado | Variable de control critica para heterogeneidad temporal |

---

## 3. Muestreo Estratificado

### 3.1 Por que NO es una muestra aleatoria simple

Los defaults representan <2% de la poblacion total. Una muestra aleatoria de 50k loans tendria solo ~1.000 defaults — insuficiente para detectar factores de riesgo en el AFE. Se aplico **oversampling deliberado de eventos de riesgo**.

### 3.2 Estratos de riesgo

| Estrato | Criterio | Target Subset A | Target Subset B | Logica |
|---------|----------|----------------|----------------|--------|
| `performing` | Nunca llego a DLQ >= 1, no default | ~25.000 | ~250.000 | Baseline mayoritario |
| `early_dlq` | Max DLQ entre 1-2 (30-89 dias), no default | ~10.000 | ~100.000 | Transicion temprana de riesgo |
| `serious_dlq` | Max DLQ >= 3 (90+ dias), no default | ~10.000 | ~100.000 | Eventos de riesgo severos sin liquidacion |
| `default` | ZBC IN (02, 03, 09, 15), disposition confirmada | ~5.000 | ~50.000 | Eventos terminales de default |

### 3.3 Estratificacion temporal (vintage)

Dentro de cada estrato, los loans se distribuyen proporcionalmente segun `vintage_bin`:

| Vintage Bin | Rango de originacion | Contexto economico |
|-------------|---------------------|-------------------|
| `Pre-crisis` | 2000-2008 | Burbuja inmobiliaria, estandares laxos, crisis subprime |
| `Recovery` | 2009-2013 | Post-crisis, estandares estrictos, volumen bajo |
| `Stable` | 2014-2019 | Mercado estable, estandares altos, tasas moderadas |
| `COVID-era` | 2020-2025 | Tasas ultra-bajas, boom refi, planes de asistencia |

### 3.4 Muestreo probabilistico

Se uso `sampleBy()` de PySpark con semilla fija (Subset A: `seed=42`, Subset B: `seed=123`). Este metodo es probabilistico — **los conteos reales pueden diferir ±5-10% del target**. La verificacion post-hoc (SMD < 0,1) confirma que la muestra no diverge significativamente de la poblacion.

> **IMPORTANTE para AFE:** Los pesos muestrales NO son necesarios para el analisis factorial. La estructura factorial depende de las correlaciones entre variables (invariante bajo muestreo), no de la proporcionalidad poblacional. El oversampling garantiza suficientes observaciones en cada perfil de riesgo para que las correlaciones sean estables.

> **IMPORTANTE para VAE:** El Subset B (500k) tiene oversampling similar. QuantileTransformer normaliza cada variable marginalmente, absorbiendo el efecto del oversampling. No se necesitan pesos muestrales.

---

## 4. Subset A — Detalle de Columnas (41 columnas, CSV)

**Destino:** R local (RStudio), paquetes `psych` y `lavaan`
**Uso:** AFE multi-vista (4 dominios) + AFC confirmatoria
**Formato:** CSV con header, 1 archivo. Separador: coma.

### 4.1 Roles de las columnas

Las columnas se dividen en **3 roles** estrictos:

| Rol | Uso en AFE/AFC | Columnas |
|-----|---------------|----------|
| **Features** (26) | Input del analisis factorial | Vistas 1-4 (ver §4.2-4.5) |
| **Indicador** (1) | Informativo, NO usar como feature | `lph_available` |
| **Metadata** (5) | Identificacion y control, NO input | `loan_id`, `acquisition_quarter`, `stratum`, `origination_year`, `property_state` |
| **Target/Validacion** (9) | Variable respuesta y metricas de verificacion, NO input | `is_default`, `net_loss`, `net_severity`, `ever_d90`, `ever_d180`, `ever_foreclosed`, `final_zero_balance_code`, `is_clean_liquidation`, `time_to_default` |

> **Regla de oro:** Solo las 26 features entran a la matriz de correlacion del AFE. Las 15 columnas restantes sirven para validar los factores contra resultados reales de riesgo.

### 4.2 Vista 1: Prestatario (5 features)

| Columna | Tipo | Rango | NULLs | Descripcion | Transformacion sugerida en R |
|---------|------|-------|-------|-------------|------------------------------|
| `borrower_credit_score` | int | 300-850 | ~0,3% | FICO score del prestatario principal al momento de originacion | Ninguna (escala intervalo) |
| `dti` | double | 0-65 | ~1,3% | Ratio deuda-ingreso al momento de originacion | Ninguna |
| `num_borrowers` | int | 1-4 | Raro | Numero de prestatarios en la hipoteca | Polychoric (pocos valores unicos) |
| `first_time_buyer` | string | Y/N | Raro | Indica si es primera compra de vivienda | Polychoric (binaria, convertir Y→1, N→0) |
| `has_coborrower` | int | 0/1 | 0% | 1 si existe co-prestatario (derivada de co_borrower_credit_score IS NOT NULL) | Polychoric (binaria) |

**Factores esperados (1-2):** "Capacidad crediticia del prestatario"

**Nota sobre `co_borrower_credit_score`:** Se excluyo del AFE porque tiene ~51% NULL (prestamos sin co-prestatario). En su lugar se usa `has_coborrower` como flag binaria. Si necesitan el score del co-prestatario, esta disponible en el Subset B.

**Imputacion sugerida:**
- `borrower_credit_score` (~0,3% NULL): mediana por vintage_bin
- `dti` (~1,3% NULL): mediana por vintage_bin × FICO bin (o usar FIML en lavaan)

### 4.3 Vista 2: Prestamo (7 features)

| Columna | Tipo | Rango | NULLs | Descripcion | Transformacion sugerida en R |
|---------|------|-------|-------|-------------|------------------------------|
| `orig_interest_rate` | double | 0-20 | Raro | Tasa de interes original del prestamo | Ninguna (proxy de riesgo percibido) |
| `orig_upb` | double | >0 | Raro | Saldo principal original (en USD) | **log()** para normalizar skewness |
| `orig_loan_term` | int | 180/360 | Raro | Plazo original en meses (mayoria 360 = 30 anos) | Polychoric (ordinal, pocos valores) |
| `orig_ltv` | int | 1-97 | Raro | Loan-to-Value ratio original | Ninguna |
| `orig_cltv` | int | ≥ LTV | Raro | Combined Loan-to-Value (incluye segunda hipoteca) | Ninguna |
| `mi_percentage` | double | 0-50 | Raro | Porcentaje de cobertura del seguro hipotecario. 0 = sin MI | Ninguna (muchos 0 — bimodal) |
| `high_balance_loan_indicator` | string | Y/N | Raro | Indica si el prestamo supera el limite conforming estandar | Polychoric (binaria, Y→1, N→0) |

**Factores esperados (2-3):** "Tamano/apalancamiento", "Costo del credito", "Cobertura de seguro"

### 4.4 Vista 3: Comportamiento (9 features)

| Columna | Tipo | Rango | NULLs | Descripcion | Transformacion sugerida en R |
|---------|------|-------|-------|-------------|------------------------------|
| `max_delinquency_status` | int | 0-99 | Raro | Maximo de meses delinquent durante la vida del prestamo | Polychoric (ordinal). **Cap en 12** para reducir outliers |
| `months_delinquent_30plus` | int | 0-N | 0% | Meses totales con DLQ >= 1 (30+ dias de atraso) | **sqrt()** o **log1p()** para corregir skewness |
| `months_delinquent_90plus` | int | 0-N | 0% | Meses totales con DLQ >= 3 (90+ dias) | **sqrt()** o **log1p()** (mas sesgada aun) |
| `ever_modified` | int | 0/1 | 0% | 1 si el prestamo fue reestructurado al menos una vez | Polychoric (binaria) |
| `had_forbearance` | int | 0/1 | 0% | 1 si el prestamo recibio plan de asistencia (F/R/T/O) | Polychoric (binaria) |
| `loan_duration_months` | int | 1-300+ | 0% | Meses totales de vida observada del prestamo | Ninguna |
| `time_to_first_delinquency` | int | ≥0 | Alto para performing | Meses desde originacion hasta primer DLQ >= 1. NULL si nunca fue delinquent | NULL = nunca delinquent. Excluir o imputar con max observado |
| `ph_months_delinquent_24` | int | 0-24 | ~55-65% | Meses delinquent en los ultimos 24 meses (de `loan_payment_history`) | Ninguna (ya es count). **Ver §6.3 sobre NULLs** |
| `ph_max_delinquency_24` | int | 0-99 | ~55-65% | Maximo DLQ numerico en los ultimos 24 meses | Polychoric (ordinal). **Ver §6.3** |

**Factores esperados (2-3):** "Severidad de mora", "Velocidad de deterioro", "Resiliencia"

**Nota sobre `cure_speed`:** Se decidio derivarla downstream en R. Definicion: meses desde primer DLQ >= 3 hasta retorno a DLQ = 0. Implementar sobre Subset A usando:
```r
# Conceptual — derivar sobre el Subset A ya cargado
# cure_speed = time_to_first_delinquency + alguna logica sobre meses...
# Se sugiere implementar como: si ever_d90==1 y max_delinquency < 3 en recientes → curado
# Alternativa simple: usar months_delinquent_90plus / loan_duration_months como proxy
```

### 4.5 Vista 4: Originacion/Canal (5 features)

| Columna | Tipo | Categorias | NULLs | Descripcion | Transformacion sugerida en R |
|---------|------|-----------|-------|-------------|------------------------------|
| `channel` | string | R=Retail, C=Correspondent, B=Broker | Raro | Canal de originacion del prestamo | Dummies → Polychoric |
| `loan_purpose` | string | C=Compra, R=Refinanciacion, P=Cash-out Refi, U=Refi-NE | Raro | Proposito del prestamo | Dummies → Polychoric |
| `property_type` | string | SF, CO, PU, MH, CP | Raro | SF=Unifamiliar, CO=Condo, PU=PUD, MH=Manufactured, CP=Cooperative | Dummies → Polychoric |
| `occupancy_status` | string | P=Principal, S=Segunda, I=Inversion, U=Unknown | Raro | Tipo de ocupacion de la propiedad | Dummies → Polychoric |
| `vintage_bin` | string | Pre-crisis, Recovery, Stable, COVID-era | 0% | Epoca del mercado al momento de originacion (ver §3.3) | Polychoric (ordinal) |

**Factores esperados (1-2):** "Canal de originacion", "Perfil de propiedad"

**Nota sobre `property_state`:** Esta incluida como **metadata**, no como feature AFE. Su cardinalidad (51 estados) es demasiado alta para el analisis factorial. Para el VAE (Subset B), se usa como entity embedding.

### 4.6 Columnas de Target y Validacion (9 columnas)

| Columna | Tipo | Descripcion | Uso |
|---------|------|-------------|-----|
| `is_default` | int (0/1) | **Variable target principal.** 1 si ZBC IN (02,03,09,15) y tiene disposition_date | Estratificacion, validacion de clusters |
| `net_loss` | double | Perdida neta = UPB_removal + expenses - proceeds. Solo para defaults | Validacion financiera clusters |
| `net_severity` | double | LGD = net_loss / UPB_removal. Benchmark: 14-45% segun vintage | Validacion financiera clusters |
| `ever_d90` | int (0/1) | 1 si DLQ alcanzo >= 3 (90+ dias) alguna vez | Validacion comportamental |
| `ever_d180` | int (0/1) | 1 si DLQ alcanzo >= 6 (180+ dias) alguna vez | Estres crediticio severo |
| `ever_foreclosed` | int (0/1) | 1 si tiene fecha de foreclosure | Validacion: proceso legal iniciado |
| `final_zero_balance_code` | string | Codigo de liquidacion final (01=prepago, 02=3rd party sale, 03=short sale, 06=recompra, 09=REO, 15=NPL, NULL=activo) | Interpretacion del resultado del prestamo |
| `is_clean_liquidation` | int (0/1) | 1 si ZBC IN (01, 06) — prepago o recompra, sin perdida crediticia | Discriminador de resultados positivos |
| `time_to_default` | int | Meses desde originacion hasta el evento de default. NULL si no defaulteo | Validacion de velocidad de deterioro |

### 4.7 Metadata (5 columnas)

| Columna | Tipo | Descripcion | Uso |
|---------|------|-------------|-----|
| `loan_id` | string | Identificador unico del prestamo (anonimizado por Fannie Mae) | **JOIN clave** para fusionar factor scores con embeddings en Fase IV |
| `acquisition_quarter` | string | Trimestre de adquisicion (ej: "2019Q4") | Control temporal |
| `stratum` | string | Estrato de riesgo asignado (performing/early_dlq/serious_dlq/default) | Control del muestreo |
| `origination_year` | int | Ano de originacion (extraido de origination_date) | Control temporal |
| `property_state` | string | Estado de la propiedad (codigo 2 letras, ej: "CA", "TX") | Control geografico. En Subset B se usa como feature (entity embedding) |

---

## 5. Subset B — Columnas Adicionales (10 extra = 51 total)

**Destino:** Google Colab (T4 GPU), PyTorch VAE
**Uso:** Entrenamiento de Variational Autoencoder + proyeccion de embeddings
**Formato:** Parquet (4 archivos, compresion snappy)

El Subset B contiene las **41 columnas del Subset A** mas **10 columnas adicionales**:

| Columna | Tipo | Rango | NULLs | Descripcion | Preprocesamiento VAE sugerido |
|---------|------|-------|-------|-------------|-------------------------------|
| `co_borrower_credit_score` | int | 300-850 | ~51% | FICO score del co-prestatario. NULL = no hay co-prestatario | Median impute + **missing indicator flag** (`co_borrower_missing`) |
| `mi_type` | string | 1/2/3 | Raro | 1=Borrower-paid, 2=Lender-paid, 3=Enterprise-paid | Entity embedding (dim=2) |
| `num_units` | int | 1-4 | Raro | Numero de unidades de la propiedad (1=unifamiliar) | Entity embedding (dim=2) |
| `months_delinquent_60plus` | int | 0-N | 0% | Meses totales con DLQ >= 2 (60+ dias) | QuantileTransformer |
| `had_servicing_transfer` | int | 0/1 | 0% | 1 si el prestamo cambio de servicer al menos una vez | Pass-through (binaria) |
| `ph_months_current_24` | int | 0-24 | ~55-65% | Meses al corriente en los ultimos 24 meses (de LPH) | QuantileTransformer. **Ver §6.3** |
| `ph_recent_delinquency_3m` | int | 0/1 | ~55-65% | 1 si hubo delinquency en los 3 meses mas recientes | Pass-through. **Ver §6.3** |
| `rate_spread` | double | Variable | ~bajo | Diferencia: orig_interest_rate - last_interest_rate. Positivo = tasa bajo (modificacion) | QuantileTransformer |
| `upb_paydown_pct` | double | 0-1+ | ~bajo | (orig_upb - last_active_upb) / orig_upb. Velocidad de amortizacion | QuantileTransformer |
| `last_active_upb` | double | >0 | ~bajo | UPB del ultimo mes ACTIVO (antes de liquidacion). Evita el UPB=0 post-liquidacion | QuantileTransformer |

### Dimension estimada de entrada al VAE (post-encoding)

| Tipo | N variables | Dims post-encoding | Detalle |
|------|------------|-------------------|---------|
| Continuas | 21 | 21 | FICO, co-FICO, DTI, rates, UPBs, LTV, CLTV, MI%, counts DLQ, durations, spreads |
| Binarias | 8 | 8 | first_time, has_coborrower, high_balance, ever_modified, had_forbearance, had_servicing_transfer, lph_available, ph_recent_3m |
| Categoricas → embeddings | 7 | ~21 | channel(3→2), purpose(4→2), property_type(5→3), occupancy(4→2), vintage_bin(4→2), mi_type(3→2), state(51→8) |
| Ordinal → embedding | 1 | ~2 | num_units(4→2) |
| Missing indicators | 1 | 1 | co_borrower_credit_score_missing |
| **Total** | **~37 vars** | **~53 dims** | |

---

## 6. Caveats y Limitaciones Criticas

### 6.1 Net Loss es cota inferior

La formula de Net Loss excluye `delinquent_accrued_interest` (posicion 85 del schema original), que es N/A para Single-Family. Esto significa que **la Net Severity reportada es una cota inferior** de la perdida real. Documentar esta limitacion en el informe.

### 6.2 DLQ contaminada por cambio de servicer (ya mitigada)

Cuando un prestamo cambia de servicer (`servicing_activity_indicator='Y'`), el nuevo servicer a veces reporta delinquency artificial porque el pago esta "en transito" entre servicers. El pipeline limpia esto con `dlq_clean` (DLQ=0 cuando SAI='Y' y DLQ>0). **Todas las metricas de delinquency en los subsets usan `dlq_clean`**, no el DLQ crudo.

### 6.3 LPH: ~55-65% NULL (critico)

`loan_payment_history` solo se reporta desde abril 2020. Las 4 features derivadas (`ph_months_current_24`, `ph_months_delinquent_24`, `ph_max_delinquency_24`, `ph_recent_delinquency_3m`) son NULL para prestamos cuyo ultimo reporte fue anterior a esa fecha.

**Columna de control:** `lph_available` = 1 si las features LPH estan disponibles, 0 si son NULL.

**Recomendaciones:**
- **En AFE (R):** Usar FIML (Full Information Maximum Likelihood) en lavaan, que maneja NULLs nativamente sin necesidad de imputacion. Alternativamente, excluir las variables LPH y correr el AFE con 24 features en lugar de 26 (las 2 LPH features de Vista 3).
- **En VAE (Python):** Aplicar mascara en la funcion de perdida: no penalizar reconstruccion de variables LPH cuando el original es NULL. Alternativa: imputar con mediana y agregar flag `lph_available` como input.

### 6.4 UPB=0 post-liquidacion (ya mitigada)

`current_actual_upb` reporta 0 despues de la liquidacion del prestamo (FAQ #56). En su lugar, `last_active_upb` captura el UPB del ultimo mes donde el prestamo estaba activo (antes de ZBC != NULL). Usar `last_active_upb` en lugar de `current_actual_upb` para cualquier calculo financiero.

### 6.5 Max DLQ limitada a 9 para loans pre-Oct 2020

Antes del formato Enhanced (octubre 2020), la delinquency se reportaba con 1 digito (max 9). Prestamos cuyo ultimo reporte fue antes de esta fecha tienen `max_delinquency_status` con techo artificial de 9. Para segmentacion, DLQ >= 3 (90+ dias) es el umbral critico, por lo que esta limitacion es menor.

### 6.6 `borrower_assistance_plan` solo desde abril 2020

Pre-2020, Fannie Mae reportaba un simple "Forbearance Indicator" (Y/N) que fue harmonizado a F (Forbearance) en el pipeline. `had_forbearance` es robusto para todo el periodo, pero el detalle granular (F=Forbearance, R=Repayment, T=Trial, O=Other) solo existe para prestamos con reportes post-abril 2020.

### 6.7 sampleBy() es probabilistico

Los conteos exactos por estrato pueden variar ±5-10% respecto al target. La verificacion SMD < 0,1 (Standardized Mean Difference) confirma que la muestra no diverge significativamente de la poblacion en las variables clave (FICO, LTV, DTI, UPB).

### 6.8 co_borrower_credit_score: ~51% NULL

Este valor es legitimamente NULL para prestamos con un solo prestatario (no es dato faltante). Se creo `has_coborrower` como flag binaria para el AFE. El score original se incluyo en Subset B para el VAE, donde se imputa con mediana + missing indicator.

### 6.9 Segmentacion vs. Prediccion

Este proyecto hace **segmentacion descriptiva** del ciclo de vida completo del prestamo, NO prediccion prospectiva. Variables como `ever_modified`, `had_forbearance`, y `max_delinquency_status` son **features validas** porque describen el comportamiento observado. En un modelo predictivo, estas serian leakage — pero aqui no aplica.

---

## 7. Guia de Uso por Fase

### 7.1 Fase II — AFE/AFC en R

**Input:** Subset A (CSV)

```r
# Cargar datos
library(readr)
sa <- read_csv("subset_a.csv")

# Verificar dimensiones
dim(sa)  # ~50k × 41

# Separar roles
features_v1 <- c("borrower_credit_score", "dti", "num_borrowers",
                  "first_time_buyer", "has_coborrower")
features_v2 <- c("orig_interest_rate", "orig_upb", "orig_loan_term",
                  "orig_ltv", "orig_cltv", "mi_percentage",
                  "high_balance_loan_indicator")
features_v3 <- c("max_delinquency_status", "months_delinquent_30plus",
                  "months_delinquent_90plus", "ever_modified",
                  "had_forbearance", "loan_duration_months",
                  "time_to_first_delinquency",
                  "ph_months_delinquent_24", "ph_max_delinquency_24")
features_v4 <- c("channel", "loan_purpose", "property_type",
                  "occupancy_status", "vintage_bin")
target_val  <- c("is_default", "net_loss", "net_severity", "ever_d90",
                  "ever_d180", "ever_foreclosed", "final_zero_balance_code",
                  "is_clean_liquidation", "time_to_default")
metadata    <- c("loan_id", "acquisition_quarter", "stratum",
                  "origination_year", "property_state")

# Transformaciones previas
sa$log_orig_upb <- log(sa$orig_upb)
sa$first_time_buyer <- ifelse(sa$first_time_buyer == "Y", 1, 0)
sa$high_balance_loan_indicator <- ifelse(
    sa$high_balance_loan_indicator == "Y", 1, 0)
sa$max_delinquency_capped <- pmin(sa$max_delinquency_status, 12)
sa$sqrt_months_dlq30 <- sqrt(sa$months_delinquent_30plus)
sa$sqrt_months_dlq90 <- sqrt(sa$months_delinquent_90plus)

# Dummies para categoricas (Vista 4)
# channel, loan_purpose, property_type, occupancy_status
# Pueden usar model.matrix() o libreria fastDummies

# Split 60/40: train_efa / test_cfa
set.seed(42)
idx <- sample(1:nrow(sa), size = 0.6 * nrow(sa))
train_efa <- sa[idx, ]
test_cfa  <- sa[-idx, ]
```

**Procedimiento AFE:**
1. Correlacion polychoric por Vista (mixto ordinal+continuo)
2. KMO >= 0,60, Bartlett p < 0,001
3. Analisis paralelo (determina K factores)
4. `psych::fa(cor=poly_matrix, nfactors=k, fm="ml", rotate="oblimin")`
5. Evaluar: loadings > 0,40, communalities > 0,30, no cross-loadings > 0,32

**Output esperado:** `factor_scores.csv` (~50k × 6-9 factores)

### 7.2 Fase III — VAE en Google Colab

**Input:** Subset B (Parquet)

```python
import pandas as pd
import numpy as np

# Cargar datos (subir Parquet a Google Drive primero)
sb = pd.read_parquet("/content/drive/MyDrive/subset_b/")
print(f"Shape: {sb.shape}")  # ~500k × 51

# Separar variables por tipo
continuous_cols = [
    "borrower_credit_score", "co_borrower_credit_score", "dti",
    "orig_interest_rate", "orig_upb", "orig_ltv", "orig_cltv",
    "mi_percentage", "max_delinquency_status",
    "months_delinquent_30plus", "months_delinquent_60plus",
    "months_delinquent_90plus", "loan_duration_months",
    "time_to_first_delinquency", "ph_months_current_24",
    "ph_months_delinquent_24", "ph_max_delinquency_24",
    "rate_spread", "upb_paydown_pct", "last_active_upb",
    "orig_loan_term",
]
binary_cols = [
    "first_time_buyer_enc", "has_coborrower",
    "high_balance_loan_indicator_enc", "ever_modified",
    "had_forbearance", "had_servicing_transfer",
    "lph_available", "ph_recent_delinquency_3m",
]
categorical_cols = [
    "channel", "loan_purpose", "property_type",
    "occupancy_status", "vintage_bin", "mi_type",
    "property_state", "num_units",
]
target_cols = [
    "is_default", "net_loss", "net_severity",
    "ever_d90", "ever_d180", "ever_foreclosed",
    "final_zero_balance_code", "is_clean_liquidation",
    "time_to_default",
]
metadata_cols = [
    "loan_id", "acquisition_quarter", "stratum", "origination_year",
]

# Preprocesamiento
# 1. Encodear binarias string → int
sb["first_time_buyer_enc"] = (sb["first_time_buyer"] == "Y").astype(int)
sb["high_balance_loan_indicator_enc"] = (
    sb["high_balance_loan_indicator"] == "Y").astype(int)

# 2. Missing indicator para co_borrower_credit_score
sb["co_borrower_missing"] = sb["co_borrower_credit_score"].isna().astype(int)
sb["co_borrower_credit_score"].fillna(
    sb["co_borrower_credit_score"].median(), inplace=True)

# 3. QuantileTransformer para continuas
from sklearn.preprocessing import QuantileTransformer
qt = QuantileTransformer(output_distribution='normal', random_state=42)
sb[continuous_cols] = qt.fit_transform(
    sb[continuous_cols].fillna(sb[continuous_cols].median()))

# 4. Entity Embeddings: definir en la arquitectura del VAE
# channel (3 cats → dim 2), loan_purpose (4 → 2),
# property_type (5 → 3), occupancy_status (4 → 2),
# vintage_bin (4 → 2), mi_type (3 → 2),
# property_state (51 → 8), num_units (4 → 2)
```

**Arquitectura VAE recomendada:**
```
Input (~53 dims post-encoding)
  → Linear(53 → 256) + BatchNorm + ReLU + Dropout(0.3)
  → Linear(256 → 128) + BatchNorm + ReLU + Dropout(0.2)
  → μ: Linear(128 → 16), log(σ²): Linear(128 → 16)
  → z = μ + σ·ε (reparametrization trick)
  → Linear(16 → 128) + BatchNorm + ReLU + Dropout(0.2)
  → Linear(128 → 256) + BatchNorm + ReLU + Dropout(0.3)
  → Reconstruccion: MSE (continuas) + CE (categoricas)
```

**Hiperparametros clave:** `z_dim=16`, `beta=2.0` (β-VAE), KL annealing 0→1 en 50 epochs, batch_size=512, lr=1e-3, early stopping patience=20.

**Output esperado:**
1. Modelo entrenado (`vae_model.pt`)
2. Proyectar los ~50k loans del Subset A por el encoder → `vae_embeddings_50k.csv` (50k × 16)

### 7.3 Fase IV — Clustering

**Input:** Factor scores (50k × 6-9) + VAE embeddings proyectados (50k × 16)

```python
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.mixture import GaussianMixture

# Cargar y concatenar
factor_scores = pd.read_csv("factor_scores.csv")   # 50k × m
vae_embeddings = pd.read_csv("vae_embeddings_50k.csv")  # 50k × 16
X_cluster = pd.concat([factor_scores, vae_embeddings], axis=1)

# Normalizar
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X_cluster)

# K-Means + GMM para K=3..8
# Seleccion de K: silhouette, CH, DB, BIC + validacion financiera
```

**Validacion financiera por cluster** (usar Subset A con labels):
- Default rate: % loans con is_default=1
- D180 rate: % loans con ever_d180=1
- Avg net severity: mean(net_severity) donde is_default=1
- Avg FICO: mean(borrower_credit_score)
- Avg LTV: mean(orig_ltv)

---

## 8. Rutas de Archivos

### 8.1 En Azure ADLS Gen2

| Archivo | Ruta ADLS |
|---------|-----------|
| Subset A (CSV) | `abfss://synapse-fs@stsynapsemetadata.dfs.core.windows.net/gold/subsets/subset_a/` |
| Subset B (Parquet) | `abfss://synapse-fs@stsynapsemetadata.dfs.core.windows.net/gold/subsets/subset_b/` |

### 8.2 Descarga

- **Subset A:** Descargar via CyberDuck, Azure Storage Explorer, o `az storage blob download-batch`. Buscar el archivo `*.csv` dentro de la carpeta (Spark escribe `part-00000-...csv`).
- **Subset B:** Descargar la carpeta completa (4 archivos `.parquet`). Pandas lee la carpeta directamente con `pd.read_parquet("carpeta/")`.

### 8.3 Flujo de archivos entre fases

```
ADLS
  ├── subset_a/ (CSV)
  │     ├── → Descargar a PC local → R (AFE/AFC)
  │     │      └── Output: factor_scores.csv (50k × m)
  │     └── → Subir a Colab → Proyectar por encoder VAE
  │            └── Output: vae_embeddings_50k.csv (50k × 16)
  │
  └── subset_b/ (Parquet)
        └── → Subir a Google Drive → Colab (VAE training)
               └── Output: vae_model.pt

Python local:
  factor_scores.csv + vae_embeddings_50k.csv
    → Clustering (K-Means + GMM)
    → tSNE/UMAP visualization
    → cluster_labels.csv, cluster_profiles.csv
```

---

## 9. Verificaciones Rapidas Post-Descarga

```r
# En R (Subset A)
sa <- read.csv("subset_a.csv")
cat("Filas:", nrow(sa), "\n")           # ~50k
cat("Columnas:", ncol(sa), "\n")        # 41
table(sa$stratum)                       # performing ~50%, early_dlq ~20%, serious_dlq ~20%, default ~10%
table(sa$is_default)                    # 0 ~90%, 1 ~10%
summary(sa$borrower_credit_score)       # Min ~300, Max ~850, Median ~740
```

```python
# En Python (Subset B)
import pandas as pd
sb = pd.read_parquet("subset_b/")
print(f"Shape: {sb.shape}")             # ~500k × 51
print(sb["stratum"].value_counts())
print(sb["is_default"].value_counts(normalize=True))
print(sb.describe())
```

---

## 10. Preguntas Frecuentes

**P: Por que hay tantos NULLs en las columnas `ph_*`?**
R: `loan_payment_history` solo se reporta desde abril 2020. Prestamos anteriores no tienen esta informacion. Usar `lph_available` para filtrar. Ver §6.3.

**P: Puedo usar `is_default` como variable predictora en el AFE?**
R: NO. `is_default` es una variable target/validacion. Solo las 26 features (Vistas 1-4) entran al AFE. Ver §4.1.

**P: Por que `property_state` es metadata en Subset A pero feature en Subset B?**
R: Su cardinalidad (51 categorias) es demasiado alta para el AFE (polychoric). En el VAE, se maneja con entity embedding (51→8 dims).

**P: Como fusiono los factor scores de R con los embeddings del VAE?**
R: Ambos comparten `loan_id`. Exportar factor scores desde R con `loan_id`, luego hacer JOIN en Python:
```python
merged = factor_scores.merge(vae_embeddings, on="loan_id")
```

**P: Que significan los valores de `final_zero_balance_code`?**
R: 01=Prepago/Vencimiento (no default), 02=Venta 3ra parte (default), 03=Short Sale (default), 06=Recompra (no default), 09=REO/Deed-in-Lieu (default), 15=NPL Sale (default), NULL=Activo.

**P: La muestra es representativa de la poblacion?**
R: No en proporciones (hay oversampling de defaults), pero si en distribuciones dentro de cada estrato. La verificacion SMD < 0,1 confirma que FICO, LTV, DTI y UPB no divergen significativamente de la poblacion para cada estrato.

**P: Que hago si una variable tiene demasiados NULLs para mi analisis?**
R: Opciones: (1) Excluir la variable, (2) Usar FIML en lavaan (R), (3) Imputar con mediana + missing indicator (Python), (4) Filtrar a `lph_available==1` si el NULL es solo en features LPH.
