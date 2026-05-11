package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type workerResult struct {
	workerID int
	recs     []Recomendacion
}

func procesarChunk(
	workerID, start, size int,
	perfilesPath string,
	programas []Programa,
	stats StatsDataset,
	results chan<- workerResult,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	estudiantes, err := cargarEstudiantesChunk(perfilesPath, start, size, stats)
	if err != nil {
		fmt.Fprintf(os.Stderr, "worker %d error cargando chunk: %v\n", workerID, err)
		results <- workerResult{workerID: workerID, recs: nil}
		return
	}

	var todasRecs []Recomendacion

	for _, est := range estudiantes {
		type scored struct {
			prog  Programa
			total float64
			sR, sN, sI, sE float64
		}
		scores := make([]scored, len(programas))
		for i, prog := range programas {
			total, sR, sN, sI, sE := calcScore(est, prog)
			scores[i] = scored{prog, total, sR, sN, sI, sE}
		}

		sort.Slice(scores, func(i, j int) bool {
			return scores[i].total > scores[j].total
		})

		limit := TopN
		if limit > len(scores) {
			limit = len(scores)
		}
		for rank, s := range scores[:limit] {
			todasRecs = append(todasRecs, Recomendacion{
				PerfilID:      est.PerfilID,
				ProgramID:     s.prog.ProgramID,
				Score:         s.total,
				Rank:          rank + 1,
				ScoreRegion:   s.sR,
				ScoreNivel:    s.sN,
				ScoreIngresos: s.sI,
				ScoreEdad:     s.sE,
				TipoFin:       s.prog.TipoFin,
				Nombre:        s.prog.Nombre,
			})
		}
	}

	results <- workerResult{workerID: workerID, recs: todasRecs}
}

func writerGoroutine(outPath string, results <-chan workerResult, total int, done chan<- error) {
	f, err := os.Create(outPath)
	if err != nil {
		done <- fmt.Errorf("crear archivo de salida: %w", err)
		return
	}
	defer f.Close()
	f.Write([]byte{0xEF, 0xBB, 0xBF})

	w := csv.NewWriter(f)
	defer w.Flush()
	w.Write(recCols)

	written := 0
	for res := range results {
		for _, r := range res.recs {
			w.Write([]string{
				r.PerfilID,
				r.ProgramID,
				strconv.Itoa(r.Rank),
				strconv.FormatFloat(r.Score, 'f', 4, 64),
				strconv.FormatFloat(r.ScoreRegion, 'f', 4, 64),
				strconv.FormatFloat(r.ScoreNivel, 'f', 4, 64),
				strconv.FormatFloat(r.ScoreIngresos, 'f', 4, 64),
				strconv.FormatFloat(r.ScoreEdad, 'f', 4, 64),
				r.TipoFin,
				r.Nombre,
			})
			written++
		}
		fmt.Printf("\r   Procesados: %d/%d workers", res.workerID+1, total)
	}
	fmt.Println()

	if err := w.Error(); err != nil {
		done <- fmt.Errorf("escribir CSV: %w", err)
		return
	}
	done <- nil
}

func runPreprocess(args []string) {
	fs := flag.NewFlagSet("preprocess", flag.ExitOnError)
	programasPath := fs.String("programas", "datasets/ds_programas.csv",
		"Ruta a ds_programas.csv")
	perfilesPath := fs.String("perfiles", "datasets/ds_perfiles_credito.csv",
		"Ruta a ds_perfiles_credito.csv (puede tener 1M filas)")
	outPath := fs.String("output", "datasets/recomendaciones.csv",
		"Ruta del archivo de salida")
	workers := fs.Int("workers", 8,
		"Número de goroutines worker")
	fs.Parse(args)

	fmt.Println("  Preprocesamiento concurrente")
	fmt.Printf("   Programas : %s\n", *programasPath)
	fmt.Printf("   Perfiles  : %s\n", *perfilesPath)
	fmt.Printf("   Salida    : %s\n", *outPath)
	fmt.Printf("   Workers   : %d\n", *workers)
	fmt.Println(strings.Repeat("─", 55))

	t0 := time.Now()

	fmt.Printf("   Cargando programas...")
	programas, err := cargarProgramas(*programasPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "\n", err)
		os.Exit(1)
	}
	fmt.Printf(" %d programas\n", len(programas))

	fmt.Printf("   Calculando estadísticas de imputación...")
	stats, err := calcularStats(*perfilesPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "\n", err)
		os.Exit(1)
	}
	fmt.Printf(" mediana_ingresos=%.0f mediana_gastos=%.0f moda_ubigeo=%s\n",
		stats.MedianaIngresos, stats.MedianaGastos, stats.ModaUbigeo)

	fmt.Printf("  Contando perfiles...")
	totalPerfiles, err := contarFilas(*perfilesPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "\n", err)
		os.Exit(1)
	}
	fmt.Printf(" %d perfiles\n", totalPerfiles)

	chunkSize := totalPerfiles / *workers
	if chunkSize == 0 {
		chunkSize = 1
	}

	fmt.Printf("   Chunk por worker: ~%d perfiles\n", chunkSize)
	fmt.Println(strings.Repeat("─", 55))

	results := make(chan workerResult, *workers)
	writerDone := make(chan error, 1)
	go writerGoroutine(*outPath, results, *workers, writerDone)

	var wg sync.WaitGroup
	for w := range *workers {
		wg.Add(1)
		start := w * chunkSize
		size := chunkSize
		if w == *workers-1 {
			size = totalPerfiles - start
		}
		go procesarChunk(w, start, size, *perfilesPath, programas, stats, results, &wg)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	if err := <-writerDone; err != nil {
		fmt.Fprintln(os.Stderr, "Error en writer:", err)
		os.Exit(1)
	}

	elapsed := time.Since(t0)
	fmt.Println(strings.Repeat("─", 55))
	fmt.Printf(" Preprocesamiento completado en %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("   Salida: %s\n", *outPath)
	fmt.Printf("   Aprox. filas generadas: %d (= %d perfiles × top%d)\n",
		totalPerfiles*TopN, totalPerfiles, TopN)
}

func contarFilas(path string) (int, error) {
	rows, err := leerCSV(path)
	if err != nil {
		return 0, err
	}
	if len(rows) < 1 {
		return 0, nil
	}
	return len(rows) - 1, nil
}