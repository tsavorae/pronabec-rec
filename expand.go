package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func leerPerfilesReales(path string) ([]perfilReal, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("abrir %s: %w", path, err)
	}
	defer f.Close()

	skipBOM(f)

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	r.LazyQuotes = true

	rows, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("leer CSV: %w", err)
	}
	if len(rows) < 2 {
		return nil, fmt.Errorf("archivo vacío o sin datos")
	}

	header := rows[0]
	idx := make(map[string]int)
	for i, col := range header {
		idx[strings.TrimSpace(col)] = i
	}

	col := func(row []string, name string) string {
		i, ok := idx[name]
		if !ok || i >= len(row) {
			return ""
		}
		return strings.TrimSpace(row[i])
	}

	parseFloat := func(s string) float64 {
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return -1
		}
		return v
	}

	var perfiles []perfilReal
	for _, row := range rows[1:] {
		perfiles = append(perfiles, perfilReal{
			perfilID:     col(row, "perfil_id"),
			genero:       col(row, "genero"),
			ubigeo:       col(row, "ubigeo_nacimiento"),
			ingresos:     parseFloat(col(row, "ingresos_mensuales")),
			gastos:       parseFloat(col(row, "gastos_mensuales")),
			tieneFicha:   col(row, "tiene_ficha_socioeconomica"),
			convocatoria: col(row, "convocatoria"),
			tipoEst:      col(row, "tipo_estudiante"),
			tipoFam:      col(row, "tipo_familiar"),
		})
	}
	return perfiles, nil
}

var perfilCols = []string{
	"perfil_id", "genero", "ubigeo_nacimiento",
	"ingresos_mensuales", "gastos_mensuales",
	"tiene_ficha_socioeconomica", "convocatoria",
	"tipo_estudiante", "tipo_familiar",
}

func escribirPerfiles(path string, reales []perfilReal, sinteticos [][]perfilSintetico) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	f.Write([]byte{0xEF, 0xBB, 0xBF})

	w := csv.NewWriter(f)
	defer w.Flush()

	w.Write(perfilCols)

	for _, p := range reales {
		ing := ""
		if p.ingresos >= 0 {
			ing = strconv.FormatFloat(p.ingresos, 'f', 2, 64)
		}
		gas := ""
		if p.gastos >= 0 {
			gas = strconv.FormatFloat(p.gastos, 'f', 2, 64)
		}
		w.Write([]string{
			p.perfilID, p.genero, p.ubigeo,
			ing, gas,
			p.tieneFicha, p.convocatoria,
			p.tipoEst, p.tipoFam,
		})
	}

	total := 0
	for _, chunk := range sinteticos {
		for _, p := range chunk {
			row := make([]string, len(perfilCols))
			for i, col := range perfilCols {
				row[i] = p[col]
			}
			w.Write(row)
			total++
		}
	}

	return w.Error()
}

func generarConcurrente(gen *generadorSintetico, total, workers int) [][]perfilSintetico {
	chunkSize := total / workers
	results := make([][]perfilSintetico, workers)

	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		startID := w * chunkSize
		size := chunkSize
		if w == workers-1 {
			size = total - startID
		}
		go func(workerID, start, n int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID*1000)))
			results[workerID] = gen.generar(n, start, rng)
		}(w, startID, size)
	}

	wg.Wait()
	return results
}

func runExpand(args []string) {
	fs := flag.NewFlagSet("expand", flag.ExitOnError)
	inputFile := fs.String("input", "datasets/ds_perfiles_credito.csv",
		"Archivo ds_perfiles_credito.csv generado por build")
	target := fs.Int("target", 1_000_000,
		"Total de filas deseadas (reales + sintéticos)")
	workers := fs.Int("workers", 8,
		"Goroutines para generación paralela")
	fs.Parse(args)

	fmt.Println("Expand — generación de perfiles sintéticos")
	fmt.Printf("   Entrada : %s\n", *inputFile)
	fmt.Printf("   Objetivo: %d filas\n", *target)
	fmt.Printf("   Workers : %d\n", *workers)
	fmt.Println(strings.Repeat("─", 55))

	fmt.Printf("   Leyendo perfiles reales...")
	reales, err := leerPerfilesReales(*inputFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "\n Error:", err)
		os.Exit(1)
	}
	fmt.Printf(" %d perfiles\n", len(reales))

	if len(reales) == 0 {
		fmt.Fprintln(os.Stderr, " No hay perfiles reales. Corre primero: go run . build")
		os.Exit(1)
	}

	nSinteticos := *target - len(reales)
	if nSinteticos <= 0 {
		fmt.Printf("    Hay %d filas (>= %d). Nada que hacer.\n", len(reales), *target)
		return
	}
	fmt.Printf(" Sintéticos a generar: %d\n", nSinteticos)

	gen := nuevoGenerador(reales)
	fmt.Printf("   Distribuciones calculadas (ingresos: media=%.0f stddev=%.0f)\n",
		gen.stats.ingMedia, gen.stats.ingStd)

	fmt.Printf("   Generando con %d workers...", *workers)
	t0 := time.Now()
	chunks := generarConcurrente(gen, nSinteticos, *workers)
	elapsed := time.Since(t0)

	total := 0
	for _, c := range chunks {
		total += len(c)
	}
	fmt.Printf(" %d sintéticos en %s\n", total, elapsed.Round(time.Millisecond))

	fmt.Printf("   Escribiendo %s...", *inputFile)
	if err := escribirPerfiles(*inputFile, reales, chunks); err != nil {
		fmt.Fprintln(os.Stderr, "\n Error escribiendo:", err)
		os.Exit(1)
	}

	fmt.Println("\n" + strings.Repeat("─", 55))
	fmt.Printf(" Total filas: %d (%d reales + %d sintéticos)\n",
		len(reales)+total, len(reales), total)
	fmt.Printf("   Archivo: %s\n", *inputFile)
}