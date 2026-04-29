package main

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
)

type perfilReal struct {
	perfilID    string
	genero      string
	ubigeo      string
	ingresos    float64
	gastos      float64
	tieneFicha  string
	convocatoria string
	tipoEst     string
	tipoFam     string
}

type statsNumericas struct {
	ingMedia  float64
	ingStd    float64
	gasMedia  float64
	gasStd    float64
}

type distCategorica struct {
	valores []string
	pesos   []float64
}

func (d *distCategorica) sample(rng *rand.Rand) string {
	if len(d.valores) == 0 {
		return ""
	}
	r := rng.Float64()
	for i, p := range d.pesos {
		if r <= p {
			return d.valores[i]
		}
	}
	return d.valores[len(d.valores)-1]
}

func buildDist(vals []string) distCategorica {
	freq := make(map[string]int)
	for _, v := range vals {
		freq[v]++
	}
	total := float64(len(vals))
	var valores []string
	var pesos []float64
	acum := 0.0
	for v, c := range freq {
		valores = append(valores, v)
		acum += float64(c) / total
		pesos = append(pesos, acum)
	}
	if len(pesos) > 0 {
		pesos[len(pesos)-1] = 1.0
	}
	return distCategorica{valores: valores, pesos: pesos}
}

func calcStats(vals []float64) (mean, std float64) {
	var valid []float64
	for _, v := range vals {
		if v >= 0 {
			valid = append(valid, v)
		}
	}
	if len(valid) == 0 {
		return 0, 100
	}
	sum := 0.0
	for _, v := range valid {
		sum += v
	}
	mean = sum / float64(len(valid))
	variance := 0.0
	for _, v := range valid {
		diff := v - mean
		variance += diff * diff
	}
	variance /= float64(len(valid))
	std = math.Sqrt(variance)
	if std < 1 {
		std = mean * 0.15
	}
	return
}

type perfilSintetico map[string]string

var tiposEstudianteFixed = distCategorica{
	valores: []string{"Ingresante", "Técnico", "Postgrado", "Universitario"},
	pesos:   []float64{0.55, 0.75, 0.93, 1.0},
}

type generadorSintetico struct {
	reales []perfilReal
	stats  statsNumericas
	dists  struct {
		genero       distCategorica
		ubigeo       distCategorica
		tieneFicha   distCategorica
		convocatoria distCategorica
		tipoFam      distCategorica
	}
}

func nuevoGenerador(reales []perfilReal) *generadorSintetico {
	g := &generadorSintetico{reales: reales}

	var generos, ubigeos, fichas, convs, tiposFam []string
	var ingresos, gastos []float64

	for _, p := range reales {
		generos = append(generos, p.genero)
		ubigeos = append(ubigeos, p.ubigeo)
		fichas = append(fichas, p.tieneFicha)
		convs = append(convs, p.convocatoria)
		tiposFam = append(tiposFam, p.tipoFam)
		ingresos = append(ingresos, p.ingresos)
		gastos = append(gastos, p.gastos)
	}

	g.stats.ingMedia, g.stats.ingStd = calcStats(ingresos)
	g.stats.gasMedia, g.stats.gasStd = calcStats(gastos)

	g.dists.genero = buildDist(generos)
	g.dists.ubigeo = buildDist(ubigeos)
	g.dists.tieneFicha = buildDist(fichas)
	g.dists.convocatoria = buildDist(convs)
	g.dists.tipoFam = buildDist(tiposFam)

	return g
}

func (g *generadorSintetico) generar(n, startID int, rng *rand.Rand) []perfilSintetico {
	out := make([]perfilSintetico, 0, n)

	for i := range n {
		ing := g.stats.ingMedia + rng.NormFloat64()*g.stats.ingStd
		if ing < 0 {
			ing = 0
		}
		gas := g.stats.gasMedia + rng.NormFloat64()*g.stats.gasStd
		if gas < 0 {
			gas = 0
		}
		if gas > ing {
			gas = ing * (0.4 + rng.Float64()*0.4)
		}

		out = append(out, perfilSintetico{
			"perfil_id":                  fmt.Sprintf("SINT_%07d", startID+i),
			"genero":                     g.dists.genero.sample(rng),
			"ubigeo_nacimiento":          g.dists.ubigeo.sample(rng),
			"ingresos_mensuales":         strconv.FormatFloat(math.Round(ing*100)/100, 'f', 2, 64),
			"gastos_mensuales":           strconv.FormatFloat(math.Round(gas*100)/100, 'f', 2, 64),
			"tiene_ficha_socioeconomica": g.dists.tieneFicha.sample(rng),
			"convocatoria":               g.dists.convocatoria.sample(rng),
			"tipo_estudiante":            tiposEstudianteFixed.sample(rng),
			"tipo_familiar":              g.dists.tipoFam.sample(rng),
		})
	}
	return out
}