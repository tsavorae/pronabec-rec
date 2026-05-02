package main

import (
	"math"
	"strings"
)

const (
	wRegion   = 0.35
	wNivel    = 0.30
	wIngresos = 0.25
	wEdad     = 0.10
)

const (
	umbralIngBajo  = 1500.0
	umbralIngMedio = 3500.0
)

func calcScore(e Estudiante, p Programa) (total, sRegion, sNivel, sIngresos, sEdad float64) {
	sRegion = scoreRegion(e, p)
	sNivel = scoreNivel(e, p)
	sIngresos = scoreIngresos(e, p)
	sEdad = scoreEdad(e, p)

	total = wRegion*sRegion + wNivel*sNivel + wIngresos*sIngresos + wEdad*sEdad
	total = math.Round(total*10000) / 10000
	return
}

func scoreRegion(e Estudiante, p Programa) float64 {
	if p.Region == "CUALQUIER_REGION" {
		return 0.7
	}
	if p.Pais != "PERU" {
		return 0.7
	}
	if normStr(p.Region) == normStr(e.Ubigeo) {
		return 1.0
	}
	return 0.0
}

func scoreNivel(e Estudiante, p Programa) float64 {
	nivel := normStr(p.NivelEduc)

	switch normStr(e.TipoEst) {
	case "ingresante", "egresadosecundaria", "egresado", "bachiller", "pregrado", "estudiante":
		if nivel == "pregrado" {
			return 1.0
		}
		if nivel == "tecnico" {
			return 0.6
		}
		return 0.2

	case "universitario", "estudianteuniversitario":
		if nivel == "pregrado" {
			return 1.0
		}
		if nivel == "postgrado" {
			return 0.4
		}
		return 0.5

	case "tecnico", "estudiantetecnico":
		if nivel == "tecnico" {
			return 1.0
		}
		if nivel == "pregrado" {
			return 0.5
		}
		return 0.3
	}

	return 0.5
}

func scoreIngresos(e Estudiante, p Programa) float64 {
	switch p.TipoFin {
	case "BECA":
		if e.Ingresos < umbralIngBajo {
			return 1.0
		}
		if e.Ingresos < umbralIngMedio {
			return 0.6
		}
		return 0.2

	case "CREDITO":
		if e.Ingresos >= umbralIngMedio {
			return 1.0
		}
		if e.Ingresos >= umbralIngBajo {
			return 0.6
		}
		return 0.3
	}
	return 0.5
}

func scoreEdad(e Estudiante, p Programa) float64 {
	if p.EdadMin == 0 && p.EdadMax == 99 {
		return 1.0
	}
	if e.Convocatoria != "" {
		return 0.8 
	}
	return 0.5
}

func normStr(s string) string {
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, " ", "")
	s = strings.ReplaceAll(s, "_", "")
	return s
}