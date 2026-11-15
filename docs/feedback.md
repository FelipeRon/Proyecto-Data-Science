# Correcciones al PSet 1

| Comentario | Cambio concreto |
|---|---|
| Cuantificar errores | Para diagnóstico: FP cuesta inspección USD 500; FN, contingencia USD 8.000. Para la idea RUL, ejemplo separado: cambiar 100 h antes a USD 20/h de vida consumible desperdicia USD 2.000; sobrestimar 100 h que cause una falla implica USD 8.000 bajo el supuesto de emergencia. No es una relación lineal entre cada hora de error y riesgo. No se estima RUL con AI4I. |
| Horizonte y umbral | Detección de estado de falla en la lectura actual. Umbral económico provisional 0,076923 para probabilidad calibrada; no 60–70% arbitrario. |
| Regla de expertos | Propuesta auditable: desgaste ≥200 min O torque ≥60 Nm. No validada por expertos reales; aprobar con técnico antes del piloto. Su cálculo no utiliza etiquetas ni subtipos de falla. |
| Fuente de costos | Procedimiento CMMS + facturas + margen de contribución; muestras de órdenes planificadas/emergentes, medianas e intervalos, sin inventar costos observados. |
| Planta de referencia | Escenario 100 evaluaciones independientes/turno, hasta 12 inspecciones; no inferir equipos reales ni turnos del CSV. Costos explícitos en memo. |
| Sensibilidad | Seis escenarios de costos y cuatro de capacidad reproducibles; con emergencia USD 3.000 la regla empeora el costo 20,21%; sin beneficio de detección temprana empeora 18,81%. |
| Justificación de implementación | Futuro modelo: costo ≤90% del baseline, límite inferior IC bootstrap 95% de ahorro >0, recall ≥0,70 y precisión ≥0,25 con ≤12% alertas. Condiciones sujetas a validación con datos reales; PSet 2 no entrena modelo. |
