package main

type Estudiante struct {
	PerfilID     string
	Genero       string
	Ubigeo       string
	Ingresos     float64
	Gastos       float64
	TieneFicha   bool
	Convocatoria string
	TipoEst      string
	TipoFam      string
}

type Programa struct {
	ProgramID   string
	TipoFin     string
	Nombre      string
	Modalidad   string
	NivelEduc   string
	TipoInst    string
	TipoGestion string
	Institucion string
	Carrera     string
	AreaEstudio string
	Region      string
	Pais        string
	EdadMin     float64
	EdadMax     float64
}

type Recomendacion struct {
	PerfilID      string
	ProgramID     string
	Score         float64
	Rank          int
	ScoreRegion   float64
	ScoreNivel    float64
	ScoreIngresos float64
	ScoreEdad     float64
	TipoFin       string
	Nombre        string
}

var recCols = []string{
	"perfil_id", "program_id", "rank", "score",
	"score_region", "score_nivel", "score_ingresos", "score_edad",
	"tipo_financiamiento", "nombre_programa",
}

type StatsDataset struct {
	MedianaIngresos float64
	MedianaGastos   float64
	ModaUbigeo      string
	ModaGenero      string
	ModaTipoEst     string
	ModaTipoFam     string
}

const TopN = 5