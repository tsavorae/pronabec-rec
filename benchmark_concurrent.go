package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type workerBatchResult struct {
	workerID    int
	batch       int
	nEstGlobal  int
	tiempoMs    float64
	acumuladoMs float64
	recsGen     int
}

func recomendarConcurrenteConBatches(
	estudiantes []Estudiante,
	programas []Programa,
	numWorkers int,
	batchSize int,
) ([]Recomendacion, []workerBatchResult) {

	if numWorkers <= 0 {
		numWorkers = 1
	}
	chunkSize := len(estudiantes) / numWorkers
	if chunkSize == 0 {
		chunkSize = len(estudiantes)
		numWorkers = 1
	}

	resultsCh := make(chan []workerBatchResult, numWorkers)
	recsCh := make(chan []Recomendacion, numWorkers)
	var wg sync.WaitGroup
	var goroutinesActivas atomic.Int64

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		start := w * chunkSize
		end := start + chunkSize
		if w == numWorkers-1 {
			end = len(estudiantes)
		}

		go func(workerID, s, e int) {
			defer wg.Done()
			goroutinesActivas.Add(1)
			defer goroutinesActivas.Add(-1)

			chunk := estudiantes[s:e]
			var workerRecs []Recomendacion
			var workerSteps []workerBatchResult
			var acumulado float64
			batchNum := 0

			for offset := 0; offset < len(chunk); offset += batchSize {
				bEnd := offset + batchSize
				if bEnd > len(chunk) {
					bEnd = len(chunk)
				}
				subBatch := chunk[offset:bEnd]

				t0 := time.Now()
				recs := recomendarSecuencialBatch(subBatch, programas)
				elapsed := time.Since(t0)

				ms := float64(elapsed.Microseconds()) / 1000.0
				acumulado += ms
				workerRecs = append(workerRecs, recs...)
				batchNum++

				workerSteps = append(workerSteps, workerBatchResult{
					workerID:    workerID,
					batch:       batchNum,
					nEstGlobal:  s + bEnd, 
					tiempoMs:    ms,
					acumuladoMs: acumulado,
					recsGen:     len(workerRecs),
				})
			}

			resultsCh <- workerSteps
			recsCh <- workerRecs
		}(w, start, end)
	}

	go func() {
		wg.Wait()
		close(resultsCh)
		close(recsCh)
	}()

	var allSteps []workerBatchResult
	var allRecs []Recomendacion

	for steps := range resultsCh {
		allSteps = append(allSteps, steps...)
	}
	for recs := range recsCh {
		allRecs = append(allRecs, recs...)
	}

	return allRecs, allSteps
}

type concRunResult struct {
	steps   []workerBatchResult
	totalMs float64
	nRecs   int
}

func runBenchmarkConc(args []string) {
	fs := flag.NewFlagSet("benchmark-conc", flag.ExitOnError)
	perfilesPath := fs.String("perfiles", "datasets/ds_perfiles_credito.csv",
		"Ruta a ds_perfiles_credito.csv")
	programasPath := fs.String("programas", "datasets/ds_programas.csv",
		"Ruta a ds_programas.csv")
	nPerfiles := fs.Int("n", 2000,
		"Cantidad de perfiles a usar en el benchmark")
	maxWorkers := fs.Int("max-workers", 16,
		"Máximo de workers para la prueba de escalabilidad")
	runs := fs.Int("runs", defaultRuns,
		"Número de ejecuciones para media recortada")
	batchSize := fs.Int("batch", 100,
		"Tamaño del batch para medición por worker")
	logPath := fs.String("log", "logs/log_concurrente.csv",
		"Ruta del archivo de log de salida")
	fs.Parse(args)

	workersList := []int{}
	for w := 1; w <= *maxWorkers; w *= 2 {
		workersList = append(workersList, w)
	}
	if workersList[len(workersList)-1] != *maxWorkers {
		workersList = append(workersList, *maxWorkers)
	}

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════════════════╗")
	fmt.Println("║         BENCHMARK CONCURRENTE — Algoritmo de Recomendación              ║")
	fmt.Println("╠══════════════════════════════════════════════════════════════════════════╣")
	fmt.Printf("║  CPU cores: %-4d  |  Runs: %d  |  Batch: %d  |  Perfiles: %-6d        ║\n",
		runtime.NumCPU(), *runs, *batchSize, *nPerfiles)
	fmt.Printf("║  Workers a probar: %v%-*s║\n",
		workersList, 53-len(fmt.Sprint(workersList)), "")
	fmt.Println("╚══════════════════════════════════════════════════════════════════════════╝")
	fmt.Println()

	fmt.Printf("  Cargando programas desde %s...", *programasPath)
	programas, err := cargarProgramas(*programasPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n  Error: %v\n", err)
		return
	}
	fmt.Printf(" %d programas\n", len(programas))

	fmt.Printf("  Cargando hasta %d perfiles desde %s...", *nPerfiles, *perfilesPath)
	stats, err := calcularStats(*perfilesPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n  Error stats: %v\n", err)
		return
	}
	totalRows, err := contarFilas(*perfilesPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n  Error contando filas: %v\n", err)
		return
	}
	limit := *nPerfiles
	if limit > totalRows {
		limit = totalRows
	}
	estudiantes, err := cargarEstudiantesChunk(*perfilesPath, 0, limit, stats)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n  Error cargando: %v\n", err)
		return
	}
	fmt.Printf(" %d estudiantes\n", len(estudiantes))

	if len(estudiantes) == 0 || len(programas) == 0 {
		fmt.Println("  Sin datos suficientes.")
		return
	}

	totalOps := len(estudiantes) * len(programas)
	fmt.Printf("  Operaciones de scoring: %d estudiantes × %d programas = %d\n",
		len(estudiantes), len(programas), totalOps)
	fmt.Println()

	fmt.Printf("  [REFERENCIA SECUENCIAL] %d runs...\n", *runs)
	seqTotalTimes := make([]time.Duration, *runs)
	for r := 0; r < *runs; r++ {
		t0 := time.Now()
		_ = ejecutarSecuencialConBatches(estudiantes, programas, *batchSize)
		seqTotalTimes[r] = time.Since(t0)
		fmt.Printf("    run %d: %s\n", r+1, seqTotalTimes[r].Round(time.Millisecond))
	}
	seqMedia := mediaRecortadaDur(seqTotalTimes)
	fmt.Printf("  Media recortada secuencial: %s\n\n", seqMedia.Round(time.Millisecond))

	os.MkdirAll("logs", 0755)
	f, err := os.Create(*logPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  Error creando log: %v\n", err)
		return
	}
	defer f.Close()

	csvW := csv.NewWriter(f)
	defer csvW.Flush()
	csvW.Write([]string{
		"workers", "worker_id", "batch", "n_estudiantes_global",
		"run", "tiempo_batch_ms", "tiempo_acumulado_worker_ms",
		"recomendaciones_worker", "goroutines_activas",
	})

	type workerSummary struct {
		workers  int
		media    time.Duration
		speedup  float64
		nRecs    int
	}
	var summaries []workerSummary

	for _, nw := range workersList {
		fmt.Printf("  [WORKERS=%d] %d runs: ", nw, *runs)
		runResults := make([]concRunResult, *runs)

		for r := 0; r < *runs; r++ {
			t0 := time.Now()
			recs, steps := recomendarConcurrenteConBatches(estudiantes, programas, nw, *batchSize)
			elapsed := time.Since(t0)
			totalMs := float64(elapsed.Microseconds()) / 1000.0

			runResults[r] = concRunResult{
				steps:   steps,
				totalMs: totalMs,
				nRecs:   len(recs),
			}

			for _, step := range steps {
				csvW.Write([]string{
					strconv.Itoa(nw),
					strconv.Itoa(step.workerID),
					strconv.Itoa(step.batch),
					strconv.Itoa(step.nEstGlobal),
					strconv.Itoa(r + 1),
					strconv.FormatFloat(step.tiempoMs, 'f', 3, 64),
					strconv.FormatFloat(step.acumuladoMs, 'f', 3, 64),
					strconv.Itoa(step.recsGen),
					strconv.Itoa(nw), 
				})
			}

			fmt.Printf("%s ", elapsed.Round(time.Millisecond))
		}

		var totalTimes []time.Duration
		for _, rr := range runResults {
			totalTimes = append(totalTimes, time.Duration(rr.totalMs*1e6))
		}
		media := mediaRecortadaDur(totalTimes)
		sp := float64(seqMedia) / float64(media)
		summaries = append(summaries, workerSummary{nw, media, sp, runResults[0].nRecs})

		csvW.Write([]string{
			strconv.Itoa(nw),
			"all",
			"total",
			strconv.Itoa(len(estudiantes)),
			"media_recortada",
			strconv.FormatFloat(float64(media.Microseconds())/1000.0, 'f', 3, 64),
			strconv.FormatFloat(float64(media.Microseconds())/1000.0, 'f', 3, 64),
			strconv.Itoa(runResults[0].nRecs),
			strconv.Itoa(nw),
		})

		fmt.Printf("-> media=%s  speedup=%.2fx\n", media.Round(time.Millisecond), sp)
	}

	fmt.Println()
	fmt.Println("  ┌─────────────┬────────────────┬──────────┬─────────────┬────────────┐")
	fmt.Println("  │ Modo        │ Media Recort.  │ Speedup  │ Eficiencia  │    Recs    │")
	fmt.Println("  ├─────────────┼────────────────┼──────────┼─────────────┼────────────┤")
	fmt.Printf("  │ Secuencial  │ %12s   │    1.00x │    100.00%%  │            │\n",
		seqMedia.Round(time.Millisecond))
	for _, s := range summaries {
		eficiencia := (s.speedup / float64(s.workers)) * 100.0
		label := fmt.Sprintf("Conc w=%-3d", s.workers)
		fmt.Printf("  │ %-11s │ %12s   │  %6.2fx │   %6.2f%%   │ %8d   │\n",
			label, s.media.Round(time.Millisecond), s.speedup, eficiencia, s.nRecs)
	}
	fmt.Println("  └─────────────┴────────────────┴──────────┴─────────────┴────────────┘")

	fmt.Println()
	bestSpeedup := 0.0
	bestWorkers := 1
	for _, s := range summaries {
		if s.speedup > bestSpeedup {
			bestSpeedup = s.speedup
			bestWorkers = s.workers
		}
	}
	fmt.Printf("      Mejor speedup: %.2fx con %d workers\n", bestSpeedup, bestWorkers)
	fmt.Printf("      CPU cores disponibles: %d\n", runtime.NumCPU())

	for i := 1; i < len(summaries); i++ {
		if summaries[i].speedup < summaries[i-1].speedup {
			fmt.Printf("  ⚠  Degradación detectada a partir de %d workers (speedup %.2fx → %.2fx)\n",
				summaries[i].workers, summaries[i-1].speedup, summaries[i].speedup)
			break
		}
	}

	fmt.Printf("\n  Log generado: %s\n", *logPath)
	fmt.Println(strings.Repeat("═", 74))
}
