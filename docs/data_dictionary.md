# Diccionario de Datos: Capa Gold (Data Marts y ML)

Este documento contiene la estructura exacta de las tablas que se exportarán desde la capa Gold hacia Parquet para ser consumidas en Power BI y Streamlit.

---

## 1. `mart_demand_volume`
**Propósito:** Dashboard Descriptivo de Volumen y Demanda.

| Columna | Tipo de Dato | Descripción |
| :--- | :--- | :--- |
| `vehicle_type` | Texto | Tipo de taxi (`yellow`, `green`, `fhv`, `hvfhv`) |
| `year` | Número Entero | Año de recogida |
| `month` | Número Entero | Mes de recogida (1 a 12) |
| `day` | Número Entero | Día del mes (1 a 31) |
| `hour` | Número Entero | Hora de recogida (0 a 23) |
| `is_weekend` | Verdadero/Falso | Si el viaje fue en fin de semana |
| `total_trips` | Número Entero | Cantidad total de viajes |
| `total_passengers` | Número Entero | Cantidad total de pasajeros transportados |
| `avg_duration_min` | Número Decimal | Duración promedio del viaje en minutos |
| `avg_distance_miles` | Número Decimal | Distancia promedio del viaje en millas |
| `zone_name` | Texto | Nombre de la zona de recogida |
| `borough` | Texto | Distrito de recogida |

---

## 2. `mart_financial_performance`
**Propósito:** Dashboard Descriptivo Financiero.

| Columna | Tipo de Dato | Descripción |
| :--- | :--- | :--- |
| `vehicle_type` | Texto | Tipo de taxi |
| `year` | Número Entero | Año de recogida |
| `month` | Número Entero | Mes de recogida |
| `payment_type` | Texto | Método de pago (`Credit Card`, `Cash`, etc.) |
| `total_trips` | Número Entero | Cantidad de viajes |
| `total_revenue` | Número Decimal | Ingreso bruto total |
| `avg_revenue_per_trip` | Número Decimal | Ingreso promedio por viaje |
| `total_fare` | Número Decimal | Tarifa base cobrada |
| `total_tips` | Número Decimal | Total de propinas recibidas |
| `avg_tip` | Número Decimal | Propina promedio por viaje |
| `total_tolls` | Número Decimal | Total pagado en peajes |
| `total_congestion_surcharge`| Número Decimal | Total impuesto de congestión |
| `total_cbd_fee` | Número Decimal | Total impuesto CBD |
| `tip_rate_pct` | Número Decimal | Porcentaje (0.0 a 1.0) que dejaron propina |
| `zone_name` | Texto | Nombre de la zona de recogida |
| `borough` | Texto | Distrito de recogida |

---

## 3. `mart_operational_profile`
**Propósito:** Dashboard Descriptivo Operativo.

| Columna | Tipo de Dato | Descripción |
| :--- | :--- | :--- |
| `vehicle_type` | Texto | Tipo de taxi |
| `year` | Número Entero | Año de recogida |
| `month` | Número Entero | Mes de recogida |
| `hour` | Número Entero | Hora de recogida (0 a 23) |
| `pickup_zone_name` | Texto | Zona de origen |
| `pickup_borough` | Texto | Distrito de origen |
| `dropoff_zone_name`| Texto | Zona de destino |
| `dropoff_borough` | Texto | Distrito de destino |
| `total_trips` | Número Entero | Cantidad de viajes en esa ruta |
| `avg_duration_min` | Número Decimal | Tiempo promedio (minutos) |
| `avg_distance_miles`| Número Decimal | Distancia promedio (millas) |
| `avg_speed_mph` | Número Decimal | Velocidad promedio (MPH) |
| `median_duration_min`| Número Decimal | Mediana del tiempo |
| `median_distance_miles`| Número Decimal| Mediana de la distancia |

---

## 4. `mart_congestion_impact`
**Propósito:** Dashboard Diagnóstico del Impuesto de Congestión.

| Columna | Tipo de Dato | Descripción |
| :--- | :--- | :--- |
| `vehicle_type` | Texto | Tipo de taxi |
| `year` | Número Entero | Año de recogida |
| `month` | Número Entero | Mes de recogida |
| `payment_type` | Texto | Método de pago |
| `has_cbd_fee` | Verdadero/Falso | ¿Se cobró impuesto CBD? |
| `has_congestion_surcharge`| Verdadero/Falso | ¿Se cobró surcharge? |
| `total_trips` | Número Entero | Cantidad de viajes |
| `avg_tip` | Número Decimal | Propina promedio |
| `avg_total` | Número Decimal | Costo total promedio |
| `total_cbd_collected`| Número Decimal | Dinero recaudado por CBD |
| `total_surcharge_collected`| Número Decimal | Dinero recaudado por Surcharge |
| `tip_to_fare_ratio`| Número Decimal | Ratio Propina / Tarifa base |
| `zone_name` | Texto | Nombre de la zona de recogida |
| `borough` | Texto | Distrito de recogida |

---

## 5. `mart_abc_xyz_zones`
**Propósito:** Dashboard Diagnóstico ABC-XYZ (Rentabilidad vs Volatilidad).

| Columna | Tipo de Dato | Descripción |
| :--- | :--- | :--- |
| `zone_name` | Texto | Nombre de la zona |
| `borough` | Texto | Distrito de la zona |
| `service_zone` | Texto | Tipo de servicio (Boro Zone, Yellow Zone) |
| `revenue_rank` | Número Entero | Ranking por ingresos |
| `total_revenue` | Número Decimal | Ingreso total histórico |
| `cumulative_revenue`| Número Decimal | Ingreso acumulado (Pareto) |
| `cumulative_pct` | Número Decimal | Porcentaje acumulado (0.0 a 1.0) |
| `total_trips` | Número Entero | Viajes totales históricos |
| `avg_monthly_trips`| Número Decimal | Promedio de viajes al mes |
| `stddev_monthly_trips`| Número Decimal | Desviación estándar mensual |
| `cv` | Número Decimal | Coeficiente de Variación |
| `abc_class` | Texto | Clase Pareto: `A`, `B`, `C` |
| `xyz_class` | Texto | Estabilidad: `X`, `Y`, `Z` |
| `segment` | Texto | Segmento combinado (Ej: `AX`) |

---

## 6. `mart_supply_demand_balance`
**Propósito:** Dashboard Diagnóstico de Equilibrio de Flota.

| Columna | Tipo de Dato | Descripción |
| :--- | :--- | :--- |
| `vehicle_type` | Texto | Tipo de taxi |
| `year` | Número Entero | Año |
| `month` | Número Entero | Mes |
| `hour` | Número Entero | Hora del día |
| `zone_name` | Texto | Nombre de la zona analizada |
| `borough` | Texto | Distrito |
| `pickups` | Número Entero | Demanda (Pasajeros salientes) |
| `dropoffs` | Número Entero | Oferta (Taxis entrantes) |
| `balance` | Número Entero | Diferencia: pickups - dropoffs |
| `supply_status`| Texto | `SURPLUS`, `DEFICIT`, `BALANCED` |
| `balance_ratio`| Número Decimal | Ratio: pickups / dropoffs |

---

## 7. `ml_sarima_forecast` (Dashboard 8)
**Propósito:** Predicción de demanda futura usando Series de Tiempo (SARIMA).

| Columna | Tipo de Dato | Descripción |
| :--- | :--- | :--- |
| `forecast_dt` | Fecha y Hora | Fecha y hora de la predicción futura (Ej: `2026-07-20 08:00:00`) |
| `predicted_demand` | Número Entero | Cantidad de viajes predichos |
| `ci_lower` | Número Entero | Límite inferior de confianza (Pésimo escenario) |
| `ci_upper` | Número Entero | Límite superior de confianza (Mejor escenario) |
| `model` | Texto | Nombre del modelo (Ej: `SARIMA(1,1,1)x(1,1,1,24)`) |
| `train_days` | Número Entero | Días de historia usados para entrenar (Ej: 30) |
| `generated_at`| Fecha y Hora | Cuándo se ejecutó el algoritmo |

---

## 8. `ml_zone_segments` (Dashboard 9)
**Propósito:** Clusters espaciales de Zonas (K-Means) según perfil de viaje.

| Columna | Tipo de Dato | Descripción |
| :--- | :--- | :--- |
| `pickup_zone_id` | Número Entero | ID de la zona de recogida |
| `zone_name` | Texto | Nombre de la zona |
| `borough` | Texto | Distrito |
| `cluster` | Número Entero | ID del cluster asignado (0, 1, 2, 3...) |
| `cluster_label`| Texto | Nombre descriptivo del cluster (Ej: `Premium/High Revenue`) |
| `total_trips` | Número Entero | Total de viajes en la zona |
| `total_revenue` | Número Decimal | Ingreso total de la zona |
| `avg_tip` | Número Decimal | Propina promedio en la zona |
| `tip_rate_pct` | Número Decimal | Porcentaje (0.0 a 1.0) que dejaron propina |
| `avg_fare` | Número Decimal | Tarifa base promedio |
| `avg_duration_min` | Número Decimal | Duración promedio del viaje en minutos |
| `avg_distance_miles` | Número Decimal | Distancia promedio del viaje en millas |
| `peak_hour_approx` | Número Entero | Hora pico aproximada de la zona |

---

## 9. `ml_anomaly_zones` (Dashboard 10)
**Propósito:** Detección de Anomalías (Isolation Forest) para identificar zonas con comportamiento atípico.

| Columna | Tipo de Dato | Descripción |
| :--- | :--- | :--- |
| `pickup_zone_id` | Número Entero | ID de la zona de recogida |
| `zone_name` | Texto | Nombre de la zona |
| `borough` | Texto | Distrito |
| `vehicle_type` | Texto | Tipo de taxi (`yellow`, `green`, `fhv`, `hvfhv`) |
| `year` | Número Entero | Año de evaluación |
| `month` | Número Entero | Mes de evaluación |
| `avg_fare` | Número Decimal | Tarifa base promedio |
| `avg_tip` | Número Decimal | Propina promedio |
| `tip_rate_pct` | Número Decimal | Porcentaje (0.0 a 1.0) que dejaron propina |
| `avg_speed_mph` | Número Decimal | Velocidad promedio (MPH) |
| `avg_duration_min` | Número Decimal | Duración promedio (minutos) |
| `avg_distance_miles`| Número Decimal | Distancia promedio (millas) |
| `fare_per_mile` | Número Decimal | Tarifa cobrada por milla |
| `fare_per_min` | Número Decimal | Tarifa cobrada por minuto |
| `anomaly_score` | Número Decimal | Puntuación de anomalía (menor = más anómalo) |
| `anomaly_label` | Texto | Etiqueta (`Normal` o `Anomaly`) |
| `is_anomaly` | Verdadero/Falso | Si la zona es considerada una anomalía |
