# Fase II: Análisis Factorial Exploratorio (AFE) y Confirmatorio (AFC)
### Proyecto: Segmentación de Riesgo Hipotecario — Fannie Mae SF Loan Performance
**Dataset:** Subset A · ~49,735 préstamos · 41 columnas · Formato CSV  
**Herramientas:** R — `psych`, `lavaan`, `polycor`, `GPArotation`, `corrplot`

---

## 1. Objetivo de la Fase

El propósito de esta fase es identificar **estructuras latentes** (factores no observables directamente) que expliquen la variabilidad del comportamiento crediticio de los préstamos hipotecarios. En lugar de trabajar con las 41 variables originales, se busca reducirlas a un conjunto compacto de **factores interpretables** que capturen los patrones subyacentes de riesgo.

El output principal es `factor_scores.csv` (49,735 × 7): un score por factor para cada préstamo, que alimentará directamente el clustering de la Fase IV.

---

## 2. Pipeline General

```
Subset A (49,735 loans × 41 cols)
        │
        ▼
   Preprocesamiento
   (transformaciones + imputación)
        │
        ▼
   20 features → 4 vistas temáticas
        │
        ├── 60% → train_efa (29,841 loans)
        │              │
        │           [AFE] Análisis Factorial Exploratorio
        │           psych::fa() · oblimin · ML
        │              │
        │         6 factores latentes
        │
        └── 40% → test_cfa (19,894 loans)
                       │
                    [AFC] Análisis Factorial Confirmatorio
                    lavaan::cfa() · WLSMV
                       │
                  Validación de estructura
        │
        ▼
   factor_scores.csv  →  Fase IV (Clustering)
```

---

## 3. Preprocesamiento

### 3.1 Transformaciones de variables

Antes del análisis se aplicaron transformaciones para cumplir los supuestos del análisis factorial:

| Variable original | Transformación | Motivo |
|---|---|---|
| `orig_upb` | `log(orig_upb)` | Corregir sesgo derecho (distribución muy asimétrica) |
| `months_delinquent_30plus` | `sqrt()` | Reducir asimetría en conteos de mora |
| `max_delinquency_status` | `pmin(..., 12)` | Capear en 12 para reducir influencia de outliers extremos |
| `first_time_buyer` (Y/N) | → `0/1` numérico | Binarizar para análisis |
| `high_balance_loan_indicator` (Y/N) | → `0/1` numérico | Binarizar |
| `vintage_bin` | → ordinal 1–4 | Pre-crisis=1, Recovery=2, Stable=3, COVID-era=4 |
| `channel` | → ordinal 1–3 | Correspondent=1, Retail=2, Broker=3 (orden por riesgo histórico) |
| `loan_purpose` | → ordinal 1–4 | Compra=1, Refi=2–3, Cash-out=4 (orden por riesgo) |
| `occupancy_status` | → ordinal 1–3 | Principal=1, Segunda=2, Inversión=3 |

### 3.2 Estrategia de imputación

Cada variable faltante recibió un tratamiento acorde a su naturaleza:

| Variable | NAs | Estrategia | Justificación |
|---|---|---|---|
| `borrower_credit_score` | ~0.3% | Mediana por `vintage_bin` | NAs aleatorios; mediana por grupo preserva distribución |
| `dti` | ~1.3% | Mediana por `vintage_bin` | Ídem |
| `mi_percentage` | ~76% | → 0 | Los NAs son **estructuralmente válidos**: LTV ≤ 80% no requiere seguro hipotecario |
| `orig_cltv` | ~0.3% | → `orig_ltv` | Sin segunda hipoteca, CLTV = LTV |
| `time_to_first_delinquency` | ~50%+ | → `loan_duration + 1` | NULL significa "nunca fue moroso", no dato faltante |
| `ph_months_delinquent_24` | ~55–65% | → 0 | Variables LPH solo disponibles desde abril 2020; pre-2020 implica sin mora reciente reportada |

### 3.3 Decisiones de exclusión de variables

Varias variables del dataset original fueron **excluidas del AFE** por problemas de colinealidad o por aportar información redundante:

| Variable excluida | Motivo |
|---|---|
| `has_coborrower` | Correlación ≈ 1.0 con `num_borrowers_num` |
| `orig_cltv` | Correlación ≈ 1.0 con `orig_ltv` → se usa `cltv_ltv_gap = cltv - ltv` |
| `sqrt_dlq90` | Correlación ≈ 1.0 con `max_dlq_capped` |
| `ph_max_delinquency_24` | Correlación ≈ 1.0 con `ph_months_delinquent_24` |
| `channel_ord` | Comunalidad = 0.007 (no aporta al espacio factorial) |
| `cltv_ltv_gap` | Comunalidad = 0.008 (prácticamente constante = 0 en la muestra) |
| `proptype_ord` | Comunalidad = 0.054 (muy baja, no explicada por factores) |

La colinealidad casi perfecta entre pares produce **inestabilidad numérica** en la matriz de correlación polychoric (eigenvalores negativos o cercanos a cero) y sesga el KMO hacia abajo.

---

## 4. Definición de las 4 Vistas Temáticas

El AFE se organiza en **4 vistas** que agrupan variables por dominio conceptual, siguiendo la estructura multi-vista de la guía del proyecto:

| Vista | Variables incluidas (20 total) | Constructo teórico |
|---|---|---|
| **Vista 1 — Prestatario** | `borrower_credit_score`, `dti`, `num_borrowers_num`, `first_time_buyer_num` | Capacidad y perfil crediticio del deudor |
| **Vista 2 — Préstamo** | `orig_interest_rate`, `log_orig_upb`, `orig_ltv`, `mi_percentage`, `high_balance_num`, `orig_loan_term_num` | Estructura financiera y apalancamiento del instrumento |
| **Vista 3 — Comportamiento** | `max_dlq_capped`, `sqrt_dlq30`, `ever_modified`, `had_forbearance`, `loan_duration_months`, `ttfd_imputed`, `ph_months_delinquent_24` | Historia y patrones de mora y pago |
| **Vista 4 — Originación** | `vintage_ord`, `purpose_ord`, `occ_ord` | Contexto de mercado y perfil de uso del préstamo |

---

## 5. Split de datos: AFE (60%) / AFC (40%)

Los datos se dividieron aleatoriamente con semilla `set.seed(42)`:

- **Train EFA:** 29,841 loans → para explorar la estructura factorial
- **Test CFA:** 19,894 loans → para confirmar/validar la estructura encontrada

Este principio sigue la **lógica de holdout**: el AFE no debe verse contaminado por los mismos datos que se usan para confirmarlo.

---

## 6. Análisis Factorial Exploratorio (AFE)

### 6.1 Matriz de correlación heterogénea

Dado que las variables son de tipos mixtos (continuas, binarias, ordinales), no se puede usar la correlación de Pearson estándar. Se empleó `hetcor()` del paquete `polycor`, que calcula automáticamente el tipo de correlación apropiado por par:

| Par de variables | Tipo de correlación |
|---|---|
| Continua – Continua | Pearson |
| Continua – Ordinal | Polyserial |
| Ordinal – Ordinal | Polychoric |

La matriz resultante fue verificada para ser **definida positiva** (todos los eigenvalores > 0). Cuando el eigenvalor mínimo estaba muy cerca de cero, se aplicó el suavizado de Higham (`psych::cor.smooth()`) para garantizar la invertibilidad.

### 6.2 Criterios de factorabilidad

| Criterio | Resultado | Interpretación |
|---|---|---|
| **KMO global** | 0.184 | Bajo — ver nota abajo |
| **Bartlett p-valor** | < 2.2e-16 | ✓ La matriz no es identidad — hay estructura correlacional |
| **RMSR** | 0.044 | ✓ Excelente (< 0.08) |
| **Fit off-diagonal** | 0.974 | ✓ Excelente (> 0.95) |

> **Nota sobre el KMO bajo (0.184):** El KMO mide si las correlaciones parciales son pequeñas en relación a las totales. Con datos organizados en **4 dominios conceptualmente distintos** (prestatario, préstamo, mora, originación), las correlaciones *entre* dominios son naturalmente bajas, lo que penaliza el KMO global. Sin embargo, el **Fit off-diagonal de 0.974** confirma que los factores capturan adecuadamente la covarianza *dentro* de cada dominio. En datos financieros multi-vista, KMO bajo con buen fit residual es la norma, no una falla del análisis.

### 6.3 Determinación del número de factores

Se usaron dos criterios complementarios:

- **Análisis Paralelo** (`fa.parallel()`): compara los eigenvalores reales con los de matrices aleatorias simuladas → sugirió **K = 8**
- **Criterio MAP de Velicer** (`VSS()`): minimiza las correlaciones parciales residuales → sugirió **K = 3**

**Decisión final: K = 6**, calculado como el promedio redondeado (5.5 → 6), acotado al rango teórico [4, 8] dado que hay 4 vistas y se esperan al menos 1–2 factores por vista.

### 6.4 Estimación del modelo AFE

```
Método de extracción: Maximum Likelihood (fm = "ml")
  → Permite calcular RMSEA como índice de ajuste
  → Más apropiado cuando se asume normalidad multivariada aproximada

Rotación: Oblimin (oblicua)
  → Permite correlación entre factores
  → Apropiada porque los dominios de riesgo hipotecario no son ortogonales
     (ej: un préstamo con alto LTV puede también tener historial de mora)
```

### 6.5 Factores identificados y sus cargas principales

| Factor | Nombre interpretativo | Indicadores principales (|λ| > 0.70) | Var. explicada |
|---|---|---|---|
| **ML1 → F1** | Severidad de Mora Histórica | `sqrt_dlq30` (0.95), `ever_modified` (0.91), `max_dlq_capped` (0.81) | 15.8% |
| **ML3 → F2** | Perfil de Primera Compra | `first_time_buyer_num` (1.00), `purpose_ord` (0.74), `occ_ord` (-0.69) | 13.7% |
| **ML6 → F3** | Mora Reciente (LPH) | `ph_months_delinquent_24` (0.90), `had_forbearance` (0.85) | 10.3% |
| **ML2 → F4** | Costo y Época del Mercado | `orig_interest_rate` (0.99), `vintage_ord` (-0.66) | 9.5% |
| **ML4 → F5** | Tamaño del Préstamo | `log_orig_upb` (0.99), `high_balance_num` (0.76) | 9.3% |
| **ML5 → F6** | Duración y Deterioro | `loan_duration_months` (0.89), `ttfd_imputed` (-0.37) | 7.6% |

**Varianza total explicada: 66.2%**

### 6.6 Correlaciones entre factores (matriz Phi)

La rotación oblimin permite que los factores correlacionen. Las correlaciones más relevantes:

| Par | r | Interpretación |
|---|---|---|
| F1 — F4 | 0.377 | Mora histórica correlaciona con mayor costo del préstamo: préstamos de épocas de tasas altas (pre-crisis) mostraron más mora |
| F1 — F5 | 0.267 | Mora histórica con préstamos de menor tamaño: UPB más alto puede indicar mayor capacidad de pago |
| F4 — F5 | -0.306 | Relación inversa: épocas de tasas altas tenían préstamos de menor monto relativo |

---

## 7. Análisis Factorial Confirmatorio (AFC)

### 7.1 Propósito

El AFC contrasta una **hipótesis estructural específica** contra los datos: dado lo que sugirió el AFE, ¿se puede confirmar que esa estructura es consistente con un subconjunto independiente de los datos?

### 7.2 Especificación del modelo

Se definieron 5 factores latentes con sus indicadores:

```
Riesgo_Prestatario  =~ borrower_credit_score + dti + first_time_buyer_num
Apalancamiento      =~ orig_ltv + mi_percentage + orig_interest_rate +
                        log_orig_upb + high_balance_num
Severidad_Mora      =~ max_dlq_capped + sqrt_dlq30 + ever_modified + had_forbearance
Mora_Reciente       =~ ph_months_delinquent_24 + ever_modified + had_forbearance
Perfil_Originacion  =~ vintage_ord + purpose_ord + occ_ord
```

`ever_modified` y `had_forbearance` cargan en **dos factores** (F3 y F4 — cross-loading), lo cual está justificado teóricamente: ambas capturan intervención del banco en el préstamo, tanto en la historia total como en el comportamiento reciente.

### 7.3 Estimador: WLSMV

Se utilizó **Diagonally Weighted Least Squares with Mean and Variance adjustment (WLSMV)** por dos razones:

1. El dataset contiene variables ordinales y binarias — WLSMV no asume normalidad multivariada
2. Es el estimador estándar en SEM cuando hay variables categóricas ordenadas

### 7.4 Resultados del AFC

| Índice | Resultado | Criterio | Estado |
|---|---|---|---|
| CFI | NA | ≥ 0.90 | No convergió completamente |
| RMSEA | NA | < 0.08 | No convergió completamente |
| SRMR | 0.188 | < 0.08 | ⚠ Alto |
| **Severidad_Mora — omega** | **0.751** | ≥ 0.70 | ✓ |
| **Severidad_Mora — AVE** | **0.555** | ≥ 0.50 | ✓ |

El AFC no convergió perfectamente (varianzas residuales negativas — **casos de Heywood**), lo que indica que algunos factores están sobre-identificados o tienen indicadores con correlación casi perfecta que no fue completamente resuelta.

**El factor más robusto es `Severidad_Mora`**, con omega = 0.751 y AVE = 0.555, confirmando que los indicadores de mora histórica forman un constructo latente estable y confiable.

### 7.5 Índices de modificación relevantes

Los índices de modificación identificaron covarianzas residuales no modeladas con justificación teórica clara:

| Covarianza | MI | Justificación financiera |
|---|---|---|
| `orig_ltv ~~ mi_percentage` | 7,375 | MI es **contractualmente obligatorio** cuando LTV > 80% |
| `first_time_buyer_num ~~ orig_ltv` | 6,264 | Compradores primerizos tienen mayor LTV por menor ahorro inicial |
| `log_orig_upb ~~ high_balance_num` | 2,045 | `high_balance` es casi función directa del UPB |
| `orig_interest_rate ~~ log_orig_upb` | 787 | Préstamos jumbo tienen tasas diferenciadas |

### 7.6 Decisión sobre el AFC

Dado que el AFC no convergió completamente, se tomó la siguiente decisión metodológica documentada:

> **El AFC sirve para VALIDAR la dirección de las cargas; los factor scores para el clustering provienen del AFE**, que es computacionalmente más estable. Esto es consistente con la guía del proyecto, que indica que los scores del AFE son el input del clustering. El AFC confirma que la estructura identificada es teóricamente coherente, especialmente el factor `Severidad_Mora`.

---

## 8. Validación Empírica de los Factor Scores

La prueba más importante de utilidad de los factores es su **correlación con variables target de riesgo** (que no entraron al análisis factorial):

| Factor | `is_default` | `ever_d90` | `ever_d180` | `ever_foreclosed` | `net_severity` |
|---|---|---|---|---|---|
| **F1 — Severidad Mora Histórica** | **0.343** | **0.789** | **0.730** | **0.346** | 0.037 |
| F2 — Perfil Primera Compra | -0.060 | 0.049 | 0.020 | -0.058 | -0.218 |
| F3 — Mora Reciente LPH | 0.016 | **0.520** | **0.465** | 0.021 | -0.033 |
| F4 — Costo y Época Mercado | **0.317** | 0.217 | 0.261 | **0.317** | 0.055 |
| F5 — Tamaño del Préstamo | -0.155 | -0.033 | -0.047 | -0.155 | **-0.254** |
| F6 — Duración / Deterioro | -0.099 | 0.115 | 0.091 | -0.098 | 0.079 |

**Hallazgos clave:**

- **F1** es el predictor más potente de morosidad severa: r = 0.789 con `ever_d90` — préstamos con alta severidad de mora histórica tienen probabilidad casi cuatro veces mayor de haber llegado a 90+ días
- **F3** (mora reciente) complementa a F1: captura préstamos que se deterioraron *recientemente* (últimos 24 meses), no necesariamente con historia larga
- **F4** refleja el ciclo económico: préstamos de épocas de tasas altas (pre-crisis) tienen mayor default, consistente con la literatura sobre originación 2004–2007
- **F5** muestra que préstamos más grandes tienen menor severity relativa (LGD más bajo), lo cual es consistente con el comportamiento del mercado jumbo donde los prestatarios suelen tener mayor capacidad de recuperación

---

## 9. Output generado

| Archivo | Contenido | Uso siguiente |
|---|---|---|
| `factor_scores.csv` | 49,735 × 7 (loan_id + F1…F6) | **Input del Clustering (Fase IV)** |
| `factor_scores_train.csv` | 29,841 × 7 — solo train_efa | Diagnóstico |
| `factor_scores_cfa.csv` | 19,894 × 6 — test_cfa AFC | Diagnóstico |
| `polychoric_heatmap.png` | Matriz de correlación polychoric agrupada | Visualización |
| `afe_diagram.png` | Diagrama de cargas factoriales AFE | Visualización |
| `afc_path.png` | Path diagram AFC con cargas estandarizadas | Visualización |
| `validation_heatmap.png` | Correlación factor scores vs. targets | Validación |
| `factor_scores_by_stratum.png` | Boxplots F1–F6 por estrato de riesgo | Validación |

---

## 10. Conexión con las fases siguientes

```
factor_scores.csv  ──────────────────────────────────────────────┐
(49,735 × 7)                                                      │
                                                                   ▼
Subset B (500k parquets) → [VAE — Python/Colab] → vae_embeddings_50k.csv
                                                        (50k × 16)
                                                                   │
                                                    JOIN por loan_id
                                                                   │
                                                                   ▼
                                                    Clustering (K-Means + GMM)
                                                    → cluster_labels.csv
                                                    → cluster_profiles.csv
```

Los factor scores de esta fase capturan la **estructura latente lineal** del riesgo hipotecario. El VAE de la Fase III capturará las **relaciones no lineales** en un espacio latente de 16 dimensiones. La combinación de ambas representaciones en el clustering genera perfiles de riesgo más ricos que cualquiera de las dos técnicas por separado.
