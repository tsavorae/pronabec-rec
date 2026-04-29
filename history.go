package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

type Interaccion struct {
	PerfilID   string
	ProgramID  string
	TipoAccion string 
	Rating     float64 
}

func runHistory(args []string) {
	fs := flag.NewFlagSet("history", flag.ExitOnError)
	perfilesPath := fs.String("perfiles", "datasets/ds_perfiles_credito.csv", "Ruta a perfiles")
	programasPath := fs.String("programas", "datasets/ds_programas.csv", "Ruta a programas")
	outPath := fs.String("output", "datasets/ds_interacciones.csv", "Ruta de salida")
	limitPerfiles := fs.Int("limit", 10000, "Número de perfiles a simular")
	fs.Parse(args)

	fmt.Println("=== Generando Historial de Interacciones Sintético ===")
	
	perfiles, err := leerValoresUnicos(*perfilesPath, "perfil_id", *limitPerfiles)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error leyendo perfiles:", err)
		os.Exit(1)
	}

	programas, err := leerValoresUnicos(*programasPath, "program_id", -1)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error leyendo programas:", err)
		os.Exit(1)
	}

	if len(perfiles) == 0 || len(programas) == 0 {
		fmt.Fprintln(os.Stderr, "No hay suficientes perfiles o programas para generar interacciones.")
		os.Exit(1)
	}

	fmt.Printf("Generando interacciones para %d perfiles usando %d programas disponibles...\n", len(perfiles), len(programas))

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	acciones := []string{"VISITA", "POSTULACION", "ADJUDICACION"}
	pesos := []float64{1.0, 3.0, 5.0}

	var interacciones []Interaccion

	for _, perfil := range perfiles {
			numInteracciones := rng.Intn(5) + 1
		
		for i := 0; i < numInteracciones; i++ {
			progIdx := rng.Intn(len(programas))
			accIdx := rng.Intn(len(acciones))
			
			interacciones = append(interacciones, Interaccion{
				PerfilID:   perfil,
				ProgramID:  programas[progIdx],
				TipoAccion: acciones[accIdx],
				Rating:     pesos[accIdx],
			})
		}
	}

	f, err := os.Create(*outPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error creando archivo:", err)
		os.Exit(1)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	w.Write([]string{"perfil_id", "program_id", "tipo_accion", "rating"})

	for _, req := range interacciones {
		w.Write([]string{
			req.PerfilID,
			req.ProgramID,
			req.TipoAccion,
			strconv.FormatFloat(req.Rating, 'f', 1, 64),
		})
	}

	fmt.Printf("Se generaron %d interacciones en %s\n", len(interacciones), *outPath)
}

func leerValoresUnicos(path string, columnName string, limit int) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.LazyQuotes = true
	
	rows, err := r.ReadAll()
	if err != nil {
		return nil, err
	}

	if len(rows) < 2 {
		return nil, nil
	}

	colIdx := -1
	for i, name := range rows[0] {
		cleanName := strings.TrimSpace(name)
		if len(cleanName) > 3 && cleanName[0:3] == "\xef\xbb\xbf" {
			cleanName = cleanName[3:]
		}
		if cleanName == columnName {
			colIdx = i
			break
		}
	}

	if colIdx == -1 {
		return nil, fmt.Errorf("columna %s no encontrada en %s", columnName, path)
	}

	var values []string
	count := 0
	for _, row := range rows[1:] {
		if limit > 0 && count >= limit {
			break
		}
		if colIdx < len(row) {
			val := strings.TrimSpace(row[colIdx])
			if val != "" {
				values = append(values, val)
				count++
			}
		}
	}

	return values, nil
}
