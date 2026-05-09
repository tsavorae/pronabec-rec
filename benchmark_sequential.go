package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

const defaultRuns = 5

func mediaRecortadaDur(durations []time.Duration) time.Duration {
	if len(durations) <= 2 {
		var sum time.Duration
		for _, d := range durations {
			sum += d
		}
		return sum / time.Duration(len(durations))
	}
	sorted := make([]time.Duration, len(durations))
	copy(sorted, durations)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	trimmed := sorted[1 : len(sorted)-1]
	var sum time.Duration
	for _, d := range trimmed {
		sum += d
	}
	return sum / time.Duration(len(trimmed))
}

func recomendarSecuencialBatch(estudiantes []Estudiante, programas []Programa) []Recomendacion {
	var recs []Recomendacion
	for _, est := range estudiantes {
		type scored struct {
			prog                  Programa
			total, sR, sN, sI, sE float64
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
			recs = append(recs, Recomendacion{
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
	return recs
}

type batchStep struct {
	batch       int
	nEst        int
	tiempoMs    float64
	acumuladoMs float64
	recsGen     int
}

func ejecutarSecuencialConBatches(estudiantes []Estudiante, programas []Programa, batchSize int) []batchStep {
	var steps []batchStep
	var acumulado float64
	totalRecs := 0
	batchNum := 0

	for offset := 0; offset < len(estudiantes); offset += batchSize {
		end := offset + batchSize
		if end > len(estudiantes) {
			end = len(estudiantes)
		}
		chunk := estudiantes[offset:end]

		t0 := time.Now()
		recs := recomendarSecuencialBatch(chunk, programas)
		elapsed := time.Since(t0)

		ms := float64(elapsed.Microseconds()) / 1000.0
		acumulado += ms
		totalRecs += len(recs)
		batchNum++

		steps = append(steps, batchStep{
			batch:       batchNum,
			nEst:        end,
			tiempoMs:    ms,
			acumuladoMs: acumulado,
			recsGen:     totalRecs,
		})
	}
	return steps
}

func runBenchmarkSeq(args []string) {
	fs := flag.NewFlagSet("benchmark-seq", flag.ExitOnError)
	perfilesPath := fs.String("perfiles", "datasets/ds_perfiles_credito.csv",
		"Ruta a ds_perfiles_credito.csv")
	programasPath := fs.String("programas", "datasets/ds_programas.csv",
		"Ruta a ds_programas.csv")
	nPerfiles := fs.Int("n", 2000,
		"Cantidad de perfiles a usar en el benchmark")
	runs := fs.Int("runs", defaultRuns,
		"Número de ejecuciones para media recortada")
	batchSize := fs.Int("batch", 100,
		"Tamaño del batch para medición")
	logPath := fs.String("log", "logs/log_secuencial.csv",
		"Ruta del archivo de log de salida")
	fs.Parse(args)

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════════════════╗")
	fmt.Println("║          BENCHMARK SECUENCIAL — Algoritmo de Recomendación              ║")
	fmt.Println("╠══════════════════════════════════════════════════════════════════════════╣")
	fmt.Printf("║  CPU cores: %-4d  |  Runs: %d  |  Batch: %d  |  Perfiles: %-6d        ║\n",
		runtime.NumCPU(), *runs, *batchSize, *nPerfiles)
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

	nBatches := (len(estudiantes) + *batchSize - 1) / *batchSize
	allRuns := make([][]batchStep, *runs)

	for r := 0; r < *runs; r++ {
		fmt.Printf("  [RUN %d/%d] Ejecutando secuencial...", r+1, *runs)
		t0 := time.Now()
		allRuns[r] = ejecutarSecuencialConBatches(estudiantes, programas, *batchSize)
		elapsed := time.Since(t0)
		fmt.Printf(" %s\n", elapsed.Round(time.Millisecond))
	}

	fmt.Println()
	fmt.Println("  Calculando media recortada por batch...")

	os.MkdirAll("logs", 0755)
	f, err := os.Create(*logPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  Error creando log: %v\n", err)
		return
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()
	w.Write([]string{
		"batch", "n_estudiantes", "run", "tiempo_ms",
		"tiempo_acumulado_ms", "recomendaciones_generadas",
	})

	for r := 0; r < *runs; r++ {
		for _, step := range allRuns[r] {
			w.Write([]string{
				strconv.Itoa(step.batch),
				strconv.Itoa(step.nEst),
				strconv.Itoa(r + 1),
				strconv.FormatFloat(step.tiempoMs, 'f', 3, 64),
				strconv.FormatFloat(step.acumuladoMs, 'f', 3, 64),
				strconv.Itoa(step.recsGen),
			})
		}
	}

	for b := 0; b < nBatches; b++ {
		var batchTimes []time.Duration
		var acumTimes []time.Duration
		var nEst, recsGen int

		for r := 0; r < *runs; r++ {
			if b < len(allRuns[r]) {
				step := allRuns[r][b]
				batchTimes = append(batchTimes, time.Duration(step.tiempoMs*1e6))
				acumTimes = append(acumTimes, time.Duration(step.acumuladoMs*1e6))
				nEst = step.nEst
				recsGen = step.recsGen
			}
		}

		mediaBatch := mediaRecortadaDur(batchTimes)
		mediaAcum := mediaRecortadaDur(acumTimes)

		w.Write([]string{
			strconv.Itoa(b + 1),
			strconv.Itoa(nEst),
			"media_recortada",
			strconv.FormatFloat(float64(mediaBatch.Microseconds())/1000.0, 'f', 3, 64),
			strconv.FormatFloat(float64(mediaAcum.Microseconds())/1000.0, 'f', 3, 64),
			strconv.Itoa(recsGen),
		})
	}

	var totalTimes []time.Duration
	for r := 0; r < *runs; r++ {
		lastStep := allRuns[r][len(allRuns[r])-1]
		totalTimes = append(totalTimes, time.Duration(lastStep.acumuladoMs*1e6))
	}
	mediaTotal := mediaRecortadaDur(totalTimes)

	fmt.Println()
	fmt.Println("  ┌──────────────────────────────────────────────────────────────────┐")
	fmt.Println("  │                    RESUMEN SECUENCIAL                            │")
	fmt.Println("  ├──────────────────────────────────────────────────────────────────┤")
	fmt.Printf("  │  Estudiantes:       %-6d                                       │\n", len(estudiantes))
	fmt.Printf("  │  Programas:         %-6d                                       │\n", len(programas))
	fmt.Printf("  │  Operaciones:       %-10d                                   │\n", totalOps)
	fmt.Printf("  │  Batches de %d:     %-6d                                       │\n", *batchSize, nBatches)
	fmt.Printf("  │  Runs:              %-6d                                       │\n", *runs)
	fmt.Printf("  │  Media recortada:   %-12s                                 │\n",
		time.Duration(mediaTotal).Round(time.Millisecond))
	fmt.Printf("  │  CPU cores:         %-6d                                       │\n", runtime.NumCPU())
	fmt.Println("  └──────────────────────────────────────────────────────────────────┘")
	fmt.Printf("\n  Log generado: %s\n", *logPath)
	fmt.Println(strings.Repeat("═", 74))
}
