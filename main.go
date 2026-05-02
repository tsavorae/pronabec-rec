package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		printHelp()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "scrape":
		runScrape(os.Args[2:])
	case "build":
		runBuildDatasets(os.Args[2:])
	case "expand":
		runExpand(os.Args[2:])
	case "history":
		runHistory(os.Args[2:])
	case "preprocess":
		runPreprocess(os.Args[2:])
	case "benchmark":
		runBenchmark(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "Subcomando desconocido: %q\n\n", os.Args[1])
		printHelp()
		os.Exit(1)
	}
}

func printHelp() {
	fmt.Println(`
Uso:
  go run . scrape                                              Descarga CSVs (4 workers)
  go run . build      [--input DIR]  [--output DIR]           Construye datasets
  go run . expand     [--input FILE] [--target N] [--workers W]  Expande con sintéticos
  go run . history    [--perfiles FILE] [--programas FILE] [--limit N]
  go run . preprocess [--programas FILE] [--perfiles FILE] [--output FILE] [--workers W]
  go run . benchmark  [--perfiles FILE] [--programas FILE] [--sinteticos N] [--max-workers W]

Flujo completo:
  go run . scrape
  go run . build      --input ./output --output ./datasets
  go run . expand     --input ./datasets/ds_perfiles_credito.csv --target 1000000
  go run . history    --limit 10000
  go run . preprocess --workers 8
  go run . benchmark  --sinteticos 100000 --perfiles-rec 5000 --max-workers 16`)
}
