package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

const benchRuns = 5

type memSnapshot struct {
	allocs uint64
	bytes  uint64
}

func takeMemSnapshot() memSnapshot {
	var m runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m)
	return memSnapshot{allocs: m.TotalAlloc, bytes: m.Sys}
}

func memDiff(before, after memSnapshot) (allocMB float64, sysMB float64) {
	allocMB = float64(after.allocs-before.allocs) / (1024 * 1024)
	sysMB = float64(after.bytes) / (1024 * 1024)
	return
}

func mediaRecortada(durations []time.Duration) time.Duration {
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

func formatMB(mb float64) string {
	if mb >= 1024 {
		return fmt.Sprintf("%.1f GB", mb/1024)
	}
	return fmt.Sprintf("%.1f MB", mb)
}

func recomendarSecuencial(estudiantes []Estudiante, programas []Programa) []Recomendacion {
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

func recomendarConcurrente(estudiantes []Estudiante, programas []Programa, workers int) []Recomendacion {
	chunkSize := len(estudiantes) / workers
	if chunkSize == 0 {
		chunkSize = len(estudiantes)
		workers = 1
	}
	type result struct {
		id   int
		recs []Recomendacion
	}
	ch := make(chan result, workers)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		start := w * chunkSize
		end := start + chunkSize
		if w == workers-1 {
			end = len(estudiantes)
		}
		go func(id, s, e int) {
			defer wg.Done()
			chunk := estudiantes[s:e]
			recs := recomendarSecuencial(chunk, programas)
			ch <- result{id: id, recs: recs}
		}(w, start, end)
	}
	go func() {
		wg.Wait()
		close(ch)
	}()
	var all []Recomendacion
	for res := range ch {
		all = append(all, res.recs...)
	}
	return all
}

func runBenchmark(args []string) {
	fs := flag.NewFlagSet("benchmark", flag.ExitOnError)
	perfilesPath := fs.String("perfiles", "datasets/ds_perfiles_credito.csv",
		"Ruta a ds_perfiles_credito.csv")
	programasPath := fs.String("programas", "datasets/ds_programas.csv",
		"Ruta a ds_programas.csv")
	nPerfiles := fs.Int("perfiles-rec", 5000,
		"Cantidad de perfiles a usar en el benchmark de recomendación")
	maxWorkers := fs.Int("max-workers", 16,
		"Máximo de workers para la prueba de escalabilidad")
	fs.Parse(args)

	workersList := []int{}
	for w := 1; w <= *maxWorkers; w *= 2 {
		workersList = append(workersList, w)
	}

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════════════════╗")
	fmt.Println("║     BENCHMARK: Algoritmo de Recomendación — Secuencial vs Concurrente   ║")
	fmt.Println("╠══════════════════════════════════════════════════════════════════════════╣")
	fmt.Printf("║  CPU cores: %-4d  |  Repeticiones: %d  |  Media: recortada (sin min/max)    ║\n", runtime.NumCPU(), benchRuns)
	fmt.Printf("║  Workers a probar: %v%-*s║\n", workersList, 53-len(fmt.Sprint(workersList)), "")
	fmt.Println("╚══════════════════════════════════════════════════════════════════════════╝")
	fmt.Println()

	benchmarkRecomendacion(*perfilesPath, *programasPath, *nPerfiles, workersList)

	fmt.Println()
	fmt.Println(strings.Repeat("═", 74))
	fmt.Printf("  Cada valor es media recortada de %d ejecuciones (descarta min y max).\n", benchRuns)
}

func benchmarkRecomendacion(perfilesPath, programasPath string, nPerfiles int, workersList []int) {
	fmt.Println("┌──────────────────────────────────────────────────────────────────────────┐")
	fmt.Println("│  Scoring de recomendaciones: estudiante x programa → top-5 matching     │")
	fmt.Println("└──────────────────────────────────────────────────────────────────────────┘")

	fmt.Printf("  Cargando programas desde %s...", programasPath)
	programas, err := cargarProgramas(programasPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n  Error: %v\n", err)
		return
	}
	fmt.Printf(" %d programas\n", len(programas))

	fmt.Printf("  Cargando hasta %d perfiles desde %s...", nPerfiles, perfilesPath)
	stats, err := calcularStats(perfilesPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n  Error stats: %v\n", err)
		return
	}
	limit := nPerfiles
	totalRows, err := contarFilas(perfilesPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n  Error contando filas: %v\n", err)
		return
	}
	if limit > totalRows {
		limit = totalRows
	}
	estudiantes, err := cargarEstudiantesChunk(perfilesPath, 0, limit, stats)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n  Error cargando: %v\n", err)
		return
	}
	fmt.Printf(" %d estudiantes\n", len(estudiantes))
	if len(estudiantes) == 0 || len(programas) == 0 {
		return
	}

	totalOps := len(estudiantes) * len(programas)
	fmt.Printf("  Operaciones: %d estudiantes x %d programas = %d\n", len(estudiantes), len(programas), totalOps)
	fmt.Printf("  Repeticiones: %d\n\n", benchRuns)

	fmt.Printf("  [SECUENCIAL] %d ejecuciones...\n", benchRuns)
	seqTimes := make([]time.Duration, benchRuns)
	var seqAllocMB, seqSysMB float64
	var seqRecs int
	for i := range benchRuns {
		before := takeMemSnapshot()
		t0 := time.Now()
		recs := recomendarSecuencial(estudiantes, programas)
		seqTimes[i] = time.Since(t0)
		after := takeMemSnapshot()
		a, s := memDiff(before, after)
		seqAllocMB += a
		seqSysMB = s
		seqRecs = len(recs)
		fmt.Printf("    run %d: %s\n", i+1, seqTimes[i].Round(time.Millisecond))
	}
	seqMedia := mediaRecortada(seqTimes)
	seqAllocMB /= float64(benchRuns)
	seqOpsPerSec := float64(totalOps) / seqMedia.Seconds()

	fmt.Println()
	fmt.Printf("  [CONCURRENTE] Escalabilidad con distintos workers:\n")

	type concRow struct {
		workers   int
		media     time.Duration
		speedup   float64
		opsPerSec float64
		allocMB   float64
		sysMB     float64
		recs      int
	}
	var concRows []concRow

	for _, w := range workersList {
		fmt.Printf("    workers=%d: ", w)
		times := make([]time.Duration, benchRuns)
		var totalAlloc float64
		var lastSys float64
		var nRecs int
		for i := range benchRuns {
			before := takeMemSnapshot()
			t0 := time.Now()
			recs := recomendarConcurrente(estudiantes, programas, w)
			times[i] = time.Since(t0)
			after := takeMemSnapshot()
			a, s := memDiff(before, after)
			totalAlloc += a
			lastSys = s
			nRecs = len(recs)
			fmt.Printf("%s ", times[i].Round(time.Millisecond))
		}
		m := mediaRecortada(times)
		sp := float64(seqMedia) / float64(m)
		ops := float64(totalOps) / m.Seconds()
		concRows = append(concRows, concRow{w, m, sp, ops, totalAlloc / float64(benchRuns), lastSys, nRecs})
		fmt.Printf(" -> media_recortada=%s  speedup=%.2fx\n", m.Round(time.Millisecond), sp)
	}

	fmt.Println()
	fmt.Println("  ┌─────────────┬────────────────┬──────────┬──────────────┬────────────┬────────────┬────────┐")
	fmt.Println("  │ Modo        │ Media Recort.  │ Speedup  │   Ops/seg    │ Alloc(MB)  │ Sys(MB)    │  Recs  │")
	fmt.Println("  ├─────────────┼────────────────┼──────────┼──────────────┼────────────┼────────────┼────────┤")
	fmt.Printf("  │ Secuencial  │ %12s   │    1.00x │ %12.0f │ %8.1f   │ %8s   │ %6d │\n",
		seqMedia.Round(time.Millisecond), seqOpsPerSec, seqAllocMB, formatMB(seqSysMB), seqRecs)
	for _, r := range concRows {
		label := fmt.Sprintf("Conc w=%-3d", r.workers)
		fmt.Printf("  │ %-11s │ %12s   │  %6.2fx │ %12.0f │ %8.1f   │ %8s   │ %6d │\n",
			label, r.media.Round(time.Millisecond), r.speedup, r.opsPerSec, r.allocMB, formatMB(r.sysMB), r.recs)
	}
	fmt.Println("  └─────────────┴────────────────┴──────────┴──────────────┴────────────┴────────────┴────────┘")
}
