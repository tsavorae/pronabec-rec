package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	scrapeHost    = "https://datosabiertos.pronabec.gob.pe"
	scrapeOutDir  = "output"
	rowsPerPage   = 500
	delayMs       = 600
	workerCount   = 4
)

var datasets = []struct{ name, guid string }{
	{"Becario - Cambio de Especialidad", "CambioDeEspecialidadBecario"},
	{"Credito18 - Postulacion", "Credito18Postulacion"},
	{"Credito18 - Entidades Educativas", "EntidadCredito18"},
	{"Credito18 - Datos del Solicitante", "Credito18DatosSolicitante"},
	{"Credito18 - Evaluacion Solicitud", "Credito18EvaluacionSolicitud"},
	{"Beca18 - Becarios por Provincia", "Beca18BecariosPorProvincia"},
	{"Becarios por Pais de Estudio", "BecariosPorPaisDeEstudio"},
	{"Colegios Habiles", "ColegiosHabiles"},
	{"Concepto de Pago", "ConceptoDePago"},
	{"Convocatoria por Carrera y Sede", "ConvocatoriaPorCarreraSede"},
	{"Convocatorias Realizadas", "Convocatorias"},
	{"Documentos Solicitados a Becarios", "DocumentosSolicitadosABecarios"},
	{"Estados del Becario", "EstadosDelBecario"},
	{"Moneda de la Subvencion", "MonedaDeLaSubvencion"},
	{"Nota Promedio Postulantes Region", "NotaPromedioDelPostulantePorRegion"},
	{"Perdida de Becas", "PerdidaDeBecas"},
	{"Periodos Academicos de Becarios", "PeriodosAcademicosDeBecarios"},
	{"Ubigeo de Postulacion a Becas", "UbigeoDePostulacionABecas"},
	{"Asignacion Presupuestal", "AsignacionPresupuestal"},
	{"Convenio", "Convenio"},
}

func newHTTPClient() *http.Client {
	return &http.Client{Timeout: 30 * time.Second}
}

func exportURL(guid string, page, rows int) string {
	return fmt.Sprintf(
		"%s/Dataset/Export%sCSV?sidx=NRO_FILA&sord=asc&page=%d&rows=%d",
		scrapeHost, guid, page, rows,
	)
}

func fetchPage(client *http.Client, guid string, page int) (headers []string, rows [][]string, err error) {
	url := exportURL(guid, page, rowsPerPage)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0")
	req.Header.Set("Referer", scrapeHost+"/developer/data")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,*/*")

	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, nil, fmt.Errorf("HTTP %d — %s", resp.StatusCode, url)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}

	r := csv.NewReader(strings.NewReader(string(body)))
	r.LazyQuotes = true
	r.TrimLeadingSpace = true
	r.FieldsPerRecord = -1

	all, err := r.ReadAll()
	if err != nil {
		_ = os.WriteFile(
			filepath.Join(scrapeOutDir, guid+"_p"+fmt.Sprint(page)+"_raw.txt"),
			body, 0644,
		)
		return nil, nil, fmt.Errorf("parsear CSV: %w", err)
	}
	if len(all) == 0 {
		return nil, nil, nil
	}
	return all[0], all[1:], nil
}

type scrapeResult struct {
	name string
	guid string
	err  error
	rows int
}

func scrapeDataset(name, guid string, log *logger) scrapeResult {
	client := newHTTPClient()

	var allRows [][]string
	var finalHeaders []string

	for page := 1; ; page++ {
		log.printf("[%s] página %d...", name, page)

		headers, rows, err := fetchPage(client, guid, page)
		if err != nil {
			log.printf("[%s] %v\n", name, err)
			if page == 1 {
				return scrapeResult{name: name, guid: guid, err: err}
			}
			break
		}

		if page == 1 {
			finalHeaders = headers
		}

		log.printf("[%s] página %d → %d filas\n", name, page, len(rows))

		if len(rows) == 0 {
			break
		}
		allRows = append(allRows, rows...)

		if len(rows) < rowsPerPage {
			break
		}

		time.Sleep(time.Duration(delayMs) * time.Millisecond)
	}

	if len(allRows) == 0 {
		log.printf("[%s]  sin datos\n", name)
		return scrapeResult{name: name, guid: guid}
	}

	outPath := filepath.Join(scrapeOutDir, sanitizeFilename(guid)+".csv")
	if err := writeCSVFile(outPath, finalHeaders, allRows); err != nil {
		return scrapeResult{name: name, guid: guid, err: err}
	}

	log.printf("[%s] %d filas → %s\n", name, len(allRows), outPath)
	return scrapeResult{name: name, guid: guid, rows: len(allRows)}
}

type logger struct{ mu sync.Mutex }

func (l *logger) printf(format string, args ...any) {
	l.mu.Lock()
	fmt.Printf(format, args...)
	l.mu.Unlock()
}

type job struct{ name, guid string }

func runScrape(_ []string) {
	os.MkdirAll(scrapeOutDir, 0755)

	fmt.Printf(" Datasets: %d | Workers: %d | Filas/página: %d\n",
		len(datasets), workerCount, rowsPerPage)
	fmt.Println(strings.Repeat("─", 60))

	jobs := make(chan job, len(datasets))
	results := make(chan scrapeResult, len(datasets))
	log := &logger{}

	var wg sync.WaitGroup
	for range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				results <- scrapeDataset(j.name, j.guid, log)
			}
		}()
	}

	for _, ds := range datasets {
		jobs <- job{ds.name, ds.guid}
	}
	close(jobs)

	go func() {
		wg.Wait()
		close(results)
	}()

	var ok, failed int
	var errList []string
	for r := range results {
		if r.err != nil {
			failed++
			errList = append(errList, fmt.Sprintf("%s: %v", r.guid, r.err))
		} else {
			ok++
		}
	}

	fmt.Println("\n" + strings.Repeat("─", 60))
	fmt.Printf(" OK: %d  Fallidos: %d\n", ok, failed)
	if len(errList) > 0 {
		fmt.Println("\nErrores:")
		for _, e := range errList {
			fmt.Printf("  • %s\n", e)
		}
	}
	fmt.Printf("\noutput: ./%s/\n", scrapeOutDir)
}

func writeCSVFile(path string, headers []string, rows [][]string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	f.Write([]byte{0xEF, 0xBB, 0xBF}) // BOM UTF-8

	w := csv.NewWriter(f)
	defer w.Flush()
	if len(headers) > 0 {
		w.Write(headers)
	}
	for _, row := range rows {
		w.Write(row)
	}
	return nil
}

func sanitizeFilename(s string) string {
	return strings.NewReplacer(" ", "_", "/", "-", ":", "-").Replace(s)
}