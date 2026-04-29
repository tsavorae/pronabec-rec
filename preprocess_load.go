package main

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
)

func cargarProgramas(path string) ([]Programa, error) {
	rows, err := leerCSV(path)
	if err != nil {
		return nil, fmt.Errorf("cargarProgramas: %w", err)
	}
	if len(rows) < 2 {
		return nil, fmt.Errorf("cargarProgramas: archivo vacío")
	}

	idx := headerIdx(rows[0])
	col := func(row []string, name string) string {
		i, ok := idx[name]
		if !ok || i >= len(row) {
			return ""
		}
		return strings.TrimSpace(row[i])
	}

	var progs []Programa
	for _, row := range rows[1:] {
		edadMin := parseFloatOr(col(row, "edad_min"), 0)
		edadMax := parseFloatOr(col(row, "edad_max"), 99)

		region := col(row, "region")
		if region == "" {
			region = "CUALQUIER_REGION"
		}
		area := col(row, "area_estudio")
		if area == "" {
			area = "GENERAL"
		}

		progs = append(progs, Programa{
			ProgramID:   col(row, "program_id"),
			TipoFin:     col(row, "tipo_financiamiento"),
			Nombre:      col(row, "nombre_programa"),
			Modalidad:   col(row, "modalidad"),
			NivelEduc:   col(row, "nivel_educativo"),
			TipoInst:    col(row, "tipo_institucion"),
			TipoGestion: col(row, "tipo_gestion"),
			Institucion: col(row, "institucion"),
			Carrera:     col(row, "carrera"),
			AreaEstudio: area,
			Region:      region,
			Pais:        col(row, "pais_estudio"),
			EdadMin:     edadMin,
			EdadMax:     edadMax,
		})
	}
	return progs, nil
}

func calcularStats(path string) (StatsDataset, error) {
	rows, err := leerCSV(path)
	if err != nil {
		return StatsDataset{}, err
	}
	if len(rows) < 2 {
		return StatsDataset{}, fmt.Errorf("calcularStats: archivo vacío")
	}

	idx := headerIdx(rows[0])
	col := func(row []string, name string) string {
		i, ok := idx[name]
		if !ok || i >= len(row) {
			return ""
		}
		return strings.TrimSpace(row[i])
	}

	var ingresos, gastos []float64
	freqUbigeo := make(map[string]int)
	freqGenero := make(map[string]int)
	freqTipoEst := make(map[string]int)
	freqTipoFam := make(map[string]int)

	for _, row := range rows[1:] {
		if v, err := strconv.ParseFloat(col(row, "ingresos_mensuales"), 64); err == nil {
			ingresos = append(ingresos, v)
		}
		if v, err := strconv.ParseFloat(col(row, "gastos_mensuales"), 64); err == nil {
			gastos = append(gastos, v)
		}
		if u := col(row, "ubigeo_nacimiento"); u != "" {
			freqUbigeo[u]++
		}
		if g := col(row, "genero"); g != "" {
			freqGenero[g]++
		}
		if t := col(row, "tipo_estudiante"); t != "" {
			freqTipoEst[t]++
		}
		if f := col(row, "tipo_familiar"); f != "" {
			freqTipoFam[f]++
		}
	}

	return StatsDataset{
		MedianaIngresos: mediana(ingresos),
		MedianaGastos:   mediana(gastos),
		ModaUbigeo:      moda(freqUbigeo),
		ModaGenero:      moda(freqGenero),
		ModaTipoEst:     moda(freqTipoEst),
		ModaTipoFam:     moda(freqTipoFam),
	}, nil
}

func cargarEstudiantesChunk(path string, start, size int, stats StatsDataset) ([]Estudiante, error) {
	rows, err := leerCSV(path)
	if err != nil {
		return nil, fmt.Errorf("cargarEstudiantesChunk: %w", err)
	}
	if len(rows) < 2 {
		return nil, fmt.Errorf("cargarEstudiantesChunk: archivo vacío")
	}

	idx := headerIdx(rows[0])
	data := rows[1:]

	end := start + size
	if end > len(data) {
		end = len(data)
	}
	chunk := data[start:end]

	col := func(row []string, name string) string {
		i, ok := idx[name]
		if !ok || i >= len(row) {
			return ""
		}
		return strings.TrimSpace(row[i])
	}

	var estudiantes []Estudiante
	for _, row := range chunk {
		ing := parseFloatOr(col(row, "ingresos_mensuales"), stats.MedianaIngresos)
		gas := parseFloatOr(col(row, "gastos_mensuales"), stats.MedianaGastos)

		ubigeo := col(row, "ubigeo_nacimiento")
		if ubigeo == "" {
			ubigeo = stats.ModaUbigeo
		}
		genero := col(row, "genero")
		if genero == "" {
			genero = stats.ModaGenero
		}
		tipoEst := col(row, "tipo_estudiante")
		if tipoEst == "" {
			tipoEst = stats.ModaTipoEst
		}
		tipoFam := col(row, "tipo_familiar")
		if tipoFam == "" {
			tipoFam = stats.ModaTipoFam
		}

		estudiantes = append(estudiantes, Estudiante{
			PerfilID:     col(row, "perfil_id"),
			Genero:       genero,
			Ubigeo:       ubigeo,
			Ingresos:     ing,
			Gastos:       gas,
			TieneFicha:   col(row, "tiene_ficha_socioeconomica") == "1",
			Convocatoria: col(row, "convocatoria"),
			TipoEst:      tipoEst,
			TipoFam:      tipoFam,
		})
	}
	return estudiantes, nil
}

func leerCSV(path string) ([][]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	skipBOM(f)

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	r.LazyQuotes = true
	return r.ReadAll()
}

func headerIdx(header []string) map[string]int {
	m := make(map[string]int, len(header))
	for i, h := range header {
		m[strings.TrimSpace(h)] = i
	}
	return m
}

func parseFloatOr(s string, fallback float64) float64 {
	v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return fallback
	}
	return v
}

func mediana(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sorted := make([]float64, len(vals))
	copy(sorted, vals)
	sort.Float64s(sorted)
	n := len(sorted)
	if n%2 == 0 {
		return (sorted[n/2-1] + sorted[n/2]) / 2
	}
	return sorted[n/2]
}

func moda(freq map[string]int) string {
	best, bestCount := "", 0
	for k, v := range freq {
		if v > bestCount {
			best, bestCount = k, v
		}
	}
	return best
}