# Recomendador de becas

Repositorio para generar el dataset a partir de datos scrapeado de PRONABEC. Incluye modelado en ```.pml```

## Uso

Descargar 20 CSVs
```bash
go run . scrape
```
Cambiar ```workerCount``` y/o ```Timeout``` si hay errores en descargar algunos datasets

---

Construir los datasets limpios
```bash
go run . build --input ./output --output ./datasets
```
---

Expandir hasta 1M de registros con perfiles sintéticos
```bash
go run . expand --input ./datasets/ds_perfiles_credito.csv --target 1000000
```
---
Generar historial de interacciones
```bash
go run . history --limit 10000
```
---

Verificar en Promela

```bash
spin -search -safety model.pml
```

## Datasets generados

| Archivo | Descripción |
|---|---|
| `ds_programas.csv` | Becas y créditos educativos|
| `ds_perfiles_credito.csv` | Perfiles socioeconómicos de estudiantes|
| `ds_interacciones.csv` | Historial simulado de postulaciones |
