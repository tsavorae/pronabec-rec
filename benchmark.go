package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

func generarSecuencial(gen *generadorSintetico, total int) []perfilSintetico {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return gen.generar(total, 0, rng)
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
	nSinteticos := fs.Int("sinteticos", 100_000,
		"Cantidad de perfiles sintéticos a generar en el benchmark de expand")
	nPerfiles := fs.Int("perfiles-rec", 5000,
		"Cantidad de perfiles a usar en el benchmark de recomendación")
	workers := fs.Int("workers", 8,
		"Número de goroutines para la versión concurrente")
	fs.Parse(args)

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║       BENCHMARK: Secuencial vs Concurrente                   ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Printf("  Workers concurrentes: %d\n", *workers)
	fmt.Println()

	// ─── Benchmark 1: Generación de perfiles sintéticos (expand) ───
	benchmarkExpand(*perfilesPath, *nSinteticos, *workers)

	fmt.Println()

	// ─── Benchmark 2: Scoring de recomendaciones (preprocess) ──────
	benchmarkPreprocess(*perfilesPath, *programasPath, *nPerfiles, *workers)

	fmt.Println()
	fmt.Println(strings.Repeat("═", 62))
	fmt.Println("  Benchmark completado.")
	fmt.Println("  El speedup indica cuántas veces más rápido es el concurrente.")
	fmt.Println("  Speedup ideal ≈ número de workers (limitado por cores de CPU).")
}
func benchmarkExpand(perfilesPath string, nSinteticos, workers int) {
	fmt.Println("┌──────────────────────────────────────────────────────────────┐")
	fmt.Println("│  BENCHMARK 1: Generación de perfiles sintéticos (expand)     │")
	fmt.Println("└──────────────────────────────────────────────────────────────┘")
	fmt.Printf("  Perfiles sintéticos a generar: %d\n", nSinteticos)

	fmt.Printf("  Cargando perfiles reales desde %s...", perfilesPath)
	reales, err := leerPerfilesReales(perfilesPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n  Error: %v\n", err)
		fmt.Println("  ⚠ Saltando benchmark de expand (ejecuta primero: go run . build)")
		return
	}
	fmt.Printf(" %d perfiles\n", len(reales))

	if len(reales) == 0 {
		fmt.Println("  ⚠ Sin perfiles reales, saltando.")
		return
	}

	gen := nuevoGenerador(reales)

	// --- Secuencial ---
	fmt.Printf("\n  [SECUENCIAL] Generando %d perfiles...", nSinteticos)
	t0 := time.Now()
	seqResult := generarSecuencial(gen, nSinteticos)
	tSeq := time.Since(t0)
	fmt.Printf(" %s\n", tSeq.Round(time.Millisecond))

	// --- Concurrente ---
	fmt.Printf("  [CONCURRENTE] Generando %d perfiles con %d workers...", nSinteticos, workers)
	t0 = time.Now()
	concResult := generarConcurrente(gen, nSinteticos, workers)
	tConc := time.Since(t0)
	totalConc := 0
	for _, c := range concResult {
		totalConc += len(c)
	}
	fmt.Printf(" %s\n", tConc.Round(time.Millisecond))

	// --- Resultados ---
	speedup := float64(tSeq) / float64(tConc)
	fmt.Println()
	fmt.Println("  ┌─────────────┬────────────────┬──────────────┐")
	fmt.Println("  │   Modo      │   Tiempo       │   Perfiles   │")
	fmt.Println("  ├─────────────┼────────────────┼──────────────┤")
	fmt.Printf("  │ Secuencial  │ %12s   │ %10d   │\n", tSeq.Round(time.Millisecond), len(seqResult))
	fmt.Printf("  │ Concurrente │ %12s   │ %10d   │\n", tConc.Round(time.Millisecond), totalConc)
	fmt.Println("  ├─────────────┼────────────────┴──────────────┤")
	fmt.Printf("  │ Speedup     │ %.2fx                         │\n", speedup)
	fmt.Println("  └─────────────┴───────────────────────────────┘")
}

func benchmarkPreprocess(perfilesPath, programasPath string, nPerfiles, workers int) {
	fmt.Println("┌──────────────────────────────────────────────────────────────┐")
	fmt.Println("│  BENCHMARK 2: Scoring de recomendaciones (preprocess)       │")
	fmt.Println("└──────────────────────────────────────────────────────────────┘")

	// Cargar programas
	fmt.Printf("  Cargando programas desde %s...", programasPath)
	programas, err := cargarProgramas(programasPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n  Error: %v\n", err)
		fmt.Println("  ⚠ Saltando benchmark de preprocess (ejecuta primero: go run . build)")
		return
	}
	fmt.Printf(" %d programas\n", len(programas))

	// Cargar perfiles y convertir a Estudiante
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
		fmt.Println("  ⚠ Sin datos suficientes, saltando.")
		return
	}

	totalOps := len(estudiantes) * len(programas)
	fmt.Printf("  Operaciones de scoring: %d estudiantes × %d programas = %d\n",
		len(estudiantes), len(programas), totalOps)

	// --- Secuencial ---
	fmt.Printf("\n  [SECUENCIAL] Calculando recomendaciones...")
	t0 := time.Now()
	recsSeq := recomendarSecuencial(estudiantes, programas)
	tSeq := time.Since(t0)
	fmt.Printf(" %s (%d recomendaciones)\n", tSeq.Round(time.Millisecond), len(recsSeq))

	// --- Concurrente ---
	fmt.Printf("  [CONCURRENTE] Calculando con %d workers...", workers)
	t0 = time.Now()
	recsConc := recomendarConcurrente(estudiantes, programas, workers)
	tConc := time.Since(t0)
	fmt.Printf(" %s (%d recomendaciones)\n", tConc.Round(time.Millisecond), len(recsConc))

	// --- Resultados ---
	speedup := float64(tSeq) / float64(tConc)
	seqOpsPerSec := float64(totalOps) / tSeq.Seconds()
	concOpsPerSec := float64(totalOps) / tConc.Seconds()

	fmt.Println()
	fmt.Println("  ┌─────────────┬────────────────┬──────────────┬────────────────┐")
	fmt.Println("  │   Modo      │   Tiempo       │   Recs       │   Ops/seg      │")
	fmt.Println("  ├─────────────┼────────────────┼──────────────┼────────────────┤")

	fmt.Printf("  │ Secuencial  │ %12s   │ %10d   │ %12.0f   │\n",
		tSeq.Round(time.Millisecond), len(recsSeq), seqOpsPerSec)
	fmt.Printf("  │ Concurrente │ %12s   │ %10d   │ %12.0f   │\n",
		tConc.Round(time.Millisecond), len(recsConc), concOpsPerSec)
	fmt.Println("  ├─────────────┼────────────────┴──────────────┴────────────────┤")
	fmt.Printf("  │ Speedup     │ %.2fx                                          │\n", speedup)
	fmt.Println("  └─────────────┴─────────────────────────────────────────────────┘")
}
