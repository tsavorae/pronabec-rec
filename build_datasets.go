package main

import (
	"crypto/md5"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"unicode"
)

var ds1Cols = []string{
	"program_id", "tipo_financiamiento", "nombre_programa", "modalidad",
	"nivel_educativo", "tipo_institucion", "tipo_gestion",
	"institucion", "carrera", "area_estudio",
	"region", "pais_estudio", "edad_min", "edad_max", "id_convocatoria",
}

var ds2Cols = []string{
	"perfil_id", "genero", "ubigeo_nacimiento",
	"ingresos_mensuales", "gastos_mensuales",
	"tiene_ficha_socioeconomica", "convocatoria",
	"tipo_estudiante", "tipo_familiar",
}

type Row = map[string]string

type convMeta struct {
	descripcion string
	modalidad   string
	edadMin     string
	edadMax     string
}

func readRows(path string) ([][]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	bom := make([]byte, 3)
	n, _ := f.Read(bom)
	if n < 3 || bom[0] != 0xEF || bom[1] != 0xBB || bom[2] != 0xBF {
		f.Seek(0, io.SeekStart)
	}

	r := csv.NewReader(f)
	r.LazyQuotes = true
	r.TrimLeadingSpace = true
	r.FieldsPerRecord = -1
	return r.ReadAll()
}

func findBase(row []string) int {
	if len(row) > 9 && isDigits(row[8]) {
		return 9
	}
	return -1
}

func isDigits(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	for _, r := range s {
		if !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

func getField(row []string, base, offset int) string {
	idx := base + offset
	if idx < 0 || idx >= len(row) {
		return ""
	}
	return cleanStr(row[idx])
}

func cleanStr(s string) string {
	s = strings.TrimSpace(s)
	s = strings.Trim(s, `"`)
	return strings.TrimSpace(s)
}

func mkID(prefix string, parts ...string) string {
	raw := strings.Join(parts, "_")
	h := md5.Sum([]byte(raw))
	return fmt.Sprintf("%s_%x", prefix, h[:4])
}

func normGestion(v string) string {
	u := strings.ToUpper(cleanStr(v))
	if strings.Contains(u, "PRIV") {
		return "Privada"
	}
	if strings.Contains(u, "PÚBLICA") || strings.Contains(u, "PUB") {
		return "Pública"
	}
	return titleCase(cleanStr(v))
}

func normNivel(v string) string {
	u := strings.ToUpper(cleanStr(v))
	for _, x := range []string{"POST", "MAES", "DOCT"} {
		if strings.Contains(u, x) {
			return "Postgrado"
		}
	}
	for _, x := range []string{"TÉC", "TEC", "INS"} {
		if strings.Contains(u, x) {
			return "Técnico"
		}
	}
	for _, x := range []string{"UNIV", "PRE", "CARR", "LICEN"} {
		if strings.Contains(u, x) {
			return "Pregrado"
		}
	}
	return titleCase(cleanStr(v))
}

func normInst(v string) string {
	u := strings.ToUpper(cleanStr(v))
	if strings.Contains(u, "UNIV") {
		return "Universidad"
	}
	for _, x := range []string{"INST", "IES", "TÉC", "TEC"} {
		if strings.Contains(u, x) {
			return "Instituto"
		}
	}
	return titleCase(cleanStr(v))
}

func normPais(v string) string {
	u := strings.ToUpper(cleanStr(v))
	if u == "PERU" || u == "PERÚ" || u == "PE" || u == "" {
		return "PERU"
	}
	return u
}

func extractRegionFromSede(sede, regionCol, pais string) string {
	if pais != "PERU" {
		return ""
	}
	if r := strings.TrimSpace(regionCol); r != "" && !strings.Contains(strings.ToUpper(r), "UNIV") {
		return strings.ToUpper(r)
	}

	sede = strings.ToUpper(strings.TrimSpace(sede))
	if strings.HasPrefix(sede, "SEDE ") {
		sede = strings.TrimPrefix(sede, "SEDE ")
	}
	
	parts := strings.Split(sede, "-")
	if len(parts) > 1 {
		return strings.TrimSpace(parts[len(parts)-1])
	}
	
	for _, sep := range []string{",", "/"} {
		if idx := strings.Index(sede, sep); idx > 0 {
			sede = sede[:idx]
		}
	}
	return strings.TrimSpace(sede)
}

func titleCase(s string) string {
	words := strings.Fields(s)
	for i, w := range words {
		r := []rune(w)
		if len(r) > 0 {
			words[i] = strings.ToUpper(string(r[:1])) + strings.ToLower(string(r[1:]))
		}
	}
	return strings.Join(words, " ")
}

func isNumericStr(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	dots := 0
	for _, r := range s {
		if r == '.' {
			dots++
			if dots > 1 {
				return false
			}
		} else if !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

func loadConvocatorias(path string) (map[string]convMeta, error) {
	rows, err := readRows(path)
	if err != nil {
		return nil, err
	}
	result := make(map[string]convMeta)
	for _, row := range rows[1:] {
		base := findBase(row)
		if base < 0 || len(row) < base+4 {
			continue
		}
		cid := getField(row, base, 0)
		if cid == "" {
			continue
		}
		result[cid] = convMeta{
			descripcion: getField(row, base, 1),
			modalidad:   getField(row, base, 2),
			edadMin:     getField(row, base, 13),
			edadMax:     getField(row, base, 14),
		}
	}
	return result, nil
}

func buildBecas(inputDir string) ([]Row, error) {
	convPath := filepath.Join(inputDir, "Convocatorias.csv")
	sedePath := filepath.Join(inputDir, "ConvocatoriaPorCarreraSede.csv")

	convMap, err := loadConvocatorias(convPath)
	if err != nil {
		return nil, fmt.Errorf("Convocatorias: %w", err)
	}
	fmt.Printf("  Convocatorias cargadas: %d\n", len(convMap))

	sedeRows, err := readRows(sedePath)
	if err != nil {
		return nil, fmt.Errorf("ConvocatoriaPorCarreraSede: %w", err)
	}

	var result []Row
	skipped := 0

	for _, row := range sedeRows[1:] {
		if len(row) < 15 {
			skipped++
			continue
		}

		base := 10
		if !isDigits(row[base]) {
			found := false
			for i := 8; i < 15 && i < len(row); i++ {
				if isDigits(row[i]) && len(row[i]) < 5 {
					base = i
					found = true
					break
				}
			}
			if !found {
				skipped++
				continue
			}
		}

		convID := getField(row, base, 0)
		pais   := getField(row, base, 2)
		nivel  := getField(row, base, 3)
		tipoI  := getField(row, base, 4)
		sede   := getField(row, base, 5)

		idxGestion := -1
		for i := base + 6; i < len(row); i++ {
			u := strings.ToUpper(row[i])
			if strings.Contains(u, "PÚBLICA") || strings.Contains(u, "PRIVADA") || strings.Contains(u, "PUB") {
				idxGestion = i
				break
			}
		}

		var nombre, carrera, gestion, regCol string
		if idxGestion != -1 {
			nombre = getField(row, base, 6)
			if idxGestion > base+8 {
				parts := []string{}
				for i := base + 6; i <= idxGestion-3; i++ {
					parts = append(parts, row[i])
				}
				nombre = strings.Join(parts, ", ")
				carrera = row[idxGestion-2]
			} else {
				carrera = getField(row, base, 7)
			}
			gestion = row[idxGestion]
			if idxGestion+2 < len(row) {
				regCol = row[idxGestion+2]
			}
		} else {
			nombre = getField(row, base, 6)
			carrera = getField(row, base, 7)
			gestion = getField(row, base, 9)
			regCol = getField(row, base, 11)
		}

		region := extractRegionFromSede(sede, regCol, normPais(pais))

		meta, hasMeta := convMap[convID]
		nombreProg, modalidad, edadMin, edadMax := "", "", "", ""
		if hasMeta {
			nombreProg = meta.descripcion
			modalidad  = meta.modalidad
			edadMin    = meta.edadMin
			edadMax    = meta.edadMax
		}
		if nombreProg == "" {
			nombreProg = getField(row, base, 1)
		}

		result = append(result, Row{
			"program_id":          mkID("B", convID, nombre, carrera, sede),
			"tipo_financiamiento": "BECA",
			"nombre_programa":     nombreProg,
			"modalidad":           modalidad,
			"nivel_educativo":     normNivel(nivel),
			"tipo_institucion":    normInst(tipoI),
			"tipo_gestion":        normGestion(gestion),
			"institucion":         nombre,
			"carrera":             carrera,
			"area_estudio":        "",
			"region":              region,
			"pais_estudio":        normPais(pais),
			"edad_min":            edadMin,
			"edad_max":            edadMax,
			"id_convocatoria":     convID,
		})
	}

	fmt.Printf("  Becas generadas: %d  (saltadas: %d)\n", len(result), skipped)
	return result, nil
}


func buildCreditos(inputDir string) ([]Row, error) {
	entPath  := filepath.Join(inputDir, "EntidadCredito18.csv")
	evalPath := filepath.Join(inputDir, "Credito18EvaluacionSolicitud.csv")

	regionMap := make(map[string]map[string]bool)
	if evalRows, err := readRows(evalPath); err == nil {
		for _, row := range evalRows[1:] {
			base := findBase(row)
			if base < 0 || len(row) < base+9 {
				continue
			}
			inst   := getField(row, base, 2)
			ubigeo := getField(row, base, 8)
			if inst != "" && ubigeo != "" {
				if regionMap[inst] == nil {
					regionMap[inst] = make(map[string]bool)
				}
				regionMap[inst][ubigeo] = true
			}
		}
	}

	entRows, err := readRows(entPath)
	if err != nil {
		return nil, fmt.Errorf("EntidadCredito18: %w", err)
	}

	var result []Row
	for _, row := range entRows[1:] {
		base := findBase(row)
		if base < 0 || len(row) < base+4 {
			continue
		}

		tipoEntidad := getField(row, base, 0)
		entidad     := getField(row, base, 1)
		area        := getField(row, base, 2)

		var carreraParts []string
		end := len(row) - 1 
		for i := base + 3; i < end; i++ {
			if p := cleanStr(row[i]); p != "" {
				carreraParts = append(carreraParts, p)
			}
		}
		carrera := strings.Join(carreraParts, ", ")

		regions := regionMap[entidad]
		if len(regions) == 0 {
			regions = map[string]bool{"": true}
		}

		for region := range regions {
			result = append(result, Row{
				"program_id":          mkID("C", entidad, carrera, region),
				"tipo_financiamiento": "CREDITO",
				"nombre_programa":     "Crédito 18",
				"modalidad":           "Crédito Educativo",
				"nivel_educativo":     normNivel(area),
				"tipo_institucion":    normInst(area),
				"tipo_gestion":        "",
				"institucion":         entidad,
				"carrera":             carrera,
				"area_estudio":        tipoEntidad,
				"region":              region,
				"pais_estudio":        "PERU",
				"edad_min":            "",
				"edad_max":            "",
				"id_convocatoria":     "",
			})
		}
	}

	fmt.Printf("  Créditos generados: %d\n", len(result))
	return result, nil
}

func buildPerfiles(inputDir string) ([]Row, error) {
	path := filepath.Join(inputDir, "Credito18DatosSolicitante.csv")
	rows, err := readRows(path)
	if err != nil {
		return nil, fmt.Errorf("Credito18DatosSolicitante: %w", err)
	}

	var result []Row
	for _, row := range rows[1:] {
		base := findBase(row)
		if base < 0 || len(row) < base+13 {
			continue
		}

		ing := getField(row, base, 7)
		gas := getField(row, base, 8)
		if !isNumericStr(ing) {
			ing = ""
		}
		if !isNumericStr(gas) {
			gas = ""
		}

		fichaSoc := "0"
		if strings.Contains(getField(row, base, 10), "Tiene") {
			fichaSoc = "1"
		}

		tipoEst := getField(row, base, 2)
		if tipoEst == "" {
			tipoEst = "Pregrado"
		}

		result = append(result, Row{
			"perfil_id":                  getField(row, base, 0),
			"genero":                     getField(row, base, 11),
			"ubigeo_nacimiento":          getField(row, base, 12),
			"ingresos_mensuales":         ing,
			"gastos_mensuales":           gas,
			"tiene_ficha_socioeconomica": fichaSoc,
			"convocatoria":               getField(row, base, 1),
			"tipo_estudiante":            tipoEst,
			"tipo_familiar":              getField(row, base, 3),
		})
	}

	fmt.Printf("  Perfiles cargados: %d\n", len(result))
	return result, nil
}

func writeCSV(path string, cols []string, rows []Row) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	f.Write([]byte{0xEF, 0xBB, 0xBF})

	w := csv.NewWriter(f)
	defer w.Flush()

	if err := w.Write(cols); err != nil {
		return err
	}
	for _, row := range rows {
		record := make([]string, len(cols))
		for i, col := range cols {
			record[i] = row[col]
		}
		if err := w.Write(record); err != nil {
			return err
		}
	}
	fmt.Printf("  %s  (%d filas)\n", path, len(rows))
	return nil
}

func runBuildDatasets(args []string) {
	fs := flag.NewFlagSet("build", flag.ExitOnError)
	inputDir  := fs.String("input",  ".", "Directorio con los CSVs de PRONABEC")
	outputDir := fs.String("output", ".", "Directorio donde guardar los datasets")
	fs.Parse(args)

	if err := os.MkdirAll(*outputDir, 0755); err != nil {
		fmt.Fprintln(os.Stderr, "Error creando directorio de salida:", err)
		os.Exit(1)
	}

	fmt.Println("\n══ Dataset 1: ds_programas.csv ══")

	becas, err := buildBecas(*inputDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error buildBecas:", err)
	}
	creditos, err := buildCreditos(*inputDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error buildCreditos:", err)
	}

	seen := make(map[string]bool)
	var programas []Row
	for _, p := range append(becas, creditos...) {
		id := p["program_id"]
		if !seen[id] {
			seen[id] = true
			programas = append(programas, p)
		}
	}

	nBecas, nCreditos := 0, 0
	for _, p := range programas {
		if p["tipo_financiamiento"] == "BECA" {
			nBecas++
		} else {
			nCreditos++
		}
	}
	fmt.Printf("  Programas únicos: %d  (%d becas, %d créditos)\n",
		len(programas), nBecas, nCreditos)

	if err := writeCSV(filepath.Join(*outputDir, "ds_programas.csv"), ds1Cols, programas); err != nil {
		fmt.Fprintln(os.Stderr, "Error escribiendo ds_programas:", err)
	}

	fmt.Println("\n══ Dataset 2: ds_perfiles_credito.csv ══")

	perfiles, err := buildPerfiles(*inputDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error buildPerfiles:", err)
	}
	if err := writeCSV(filepath.Join(*outputDir, "ds_perfiles_credito.csv"), ds2Cols, perfiles); err != nil {
		fmt.Fprintln(os.Stderr, "Error escribiendo ds_perfiles_credito:", err)
	}

	fmt.Printf("\n  Archivos en: %s\n", *outputDir)
}