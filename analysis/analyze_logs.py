import argparse
import os
import sys

import matplotlib
matplotlib.use("Agg")  
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


plt.rcParams.update({
    "figure.figsize": (10, 6),
    "axes.grid": True,
    "grid.alpha": 0.3,
    "font.size": 11,
    "axes.titlesize": 14,
    "axes.labelsize": 12,
})
COLORS = ["#2196F3", "#4CAF50", "#FF9800", "#E91E63", "#9C27B0", "#00BCD4", "#FF5722", "#795548"]


def media_recortada(values):
    """Media recortada: descarta min y max, promedia el resto."""
    if len(values) <= 2:
        return np.mean(values)
    arr = np.sort(values)
    return np.mean(arr[1:-1])


def cargar_log_secuencial(path):
    """Carga log_secuencial.csv y retorna DataFrames separados por runs y resumen."""
    df = pd.read_csv(path)
    df_runs = df[df["run"] != "media_recortada"].copy()
    df_runs["run"] = df_runs["run"].astype(int)
    df_runs["tiempo_ms"] = df_runs["tiempo_ms"].astype(float)
    df_runs["tiempo_acumulado_ms"] = df_runs["tiempo_acumulado_ms"].astype(float)
    df_runs["n_estudiantes"] = df_runs["n_estudiantes"].astype(int)

    df_resumen = df[df["run"] == "media_recortada"].copy()
    df_resumen["tiempo_ms"] = df_resumen["tiempo_ms"].astype(float)
    df_resumen["tiempo_acumulado_ms"] = df_resumen["tiempo_acumulado_ms"].astype(float)
    df_resumen["n_estudiantes"] = df_resumen["n_estudiantes"].astype(int)

    return df_runs, df_resumen


def cargar_log_concurrente(path):
    """Carga log_concurrente.csv y retorna DataFrame."""
    df = pd.read_csv(path)
    return df


def extraer_resumen_concurrente(df_conc):
    """Extrae las filas de resumen (media_recortada) del log concurrente."""
    resumen = df_conc[df_conc["run"] == "media_recortada"].copy()
    resumen["workers"] = resumen["workers"].astype(int)
    resumen["tiempo_batch_ms"] = resumen["tiempo_batch_ms"].astype(float)
    return resumen


def extraer_tiempos_por_workers(df_conc):
    """Extrae el tiempo total (media_recortada) por cada configuración de workers."""
    resumen = extraer_resumen_concurrente(df_conc)
    result = []
    for _, row in resumen.iterrows():
        result.append({
            "workers": int(row["workers"]),
            "tiempo_total_ms": float(row["tiempo_batch_ms"]),
        })
    return pd.DataFrame(result)


def calcular_tiempo_total_por_run_conc(df_conc):
    """Para cada (workers, run), calcula el tiempo total como el máx acumulado de todos los workers."""
    df_runs = df_conc[df_conc["run"] != "media_recortada"].copy()
    df_runs["run"] = df_runs["run"].astype(int)
    df_runs["workers"] = df_runs["workers"].astype(int)
    df_runs["tiempo_acumulado_worker_ms"] = df_runs["tiempo_acumulado_worker_ms"].astype(float)

    grouped = df_runs.groupby(["workers", "run"])["tiempo_acumulado_worker_ms"].max().reset_index()
    grouped.columns = ["workers", "run", "tiempo_total_ms"]

    result = []
    for w, group in grouped.groupby("workers"):
        times = group["tiempo_total_ms"].values
        result.append({
            "workers": w,
            "tiempo_total_ms": media_recortada(times),
        })
    return pd.DataFrame(result)


def grafico_speedup(tiempo_seq_ms, df_workers, output_dir):
    """Gráfico 1: Speedup vs Número de Workers."""
    fig, ax = plt.subplots()

    workers = df_workers["workers"].values
    speedups = tiempo_seq_ms / df_workers["tiempo_total_ms"].values

    ax.plot(workers, speedups, "o-", color=COLORS[0], linewidth=2, markersize=8, label="Speedup real")
    ax.plot(workers, workers, "--", color="#999", linewidth=1, label="Speedup ideal (lineal)")

    ax.set_xlabel("Número de Workers (goroutines)")
    ax.set_ylabel("Speedup (T_seq / T_conc)")
    ax.set_title("Speedup vs Número de Workers")
    ax.set_xticks(workers)
    ax.legend()

    for w, sp in zip(workers, speedups):
        ax.annotate(f"{sp:.2f}x", (w, sp), textcoords="offset points",
                    xytext=(0, 12), ha="center", fontsize=9, fontweight="bold")

    fig.tight_layout()
    path = os.path.join(output_dir, "01_speedup_vs_workers.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  [OK] {path}")
    return speedups


def grafico_tiempo_batch(df_seq_resumen, df_conc, output_dir):
    """Gráfico 2: Tiempo acumulado vs Número de estudiantes (de 100 en 100)."""
    fig, ax = plt.subplots()

    ax.plot(df_seq_resumen["n_estudiantes"], df_seq_resumen["tiempo_acumulado_ms"],
            "o-", color=COLORS[3], linewidth=2, markersize=4, label="Secuencial")

    df_runs = df_conc[df_conc["run"] != "media_recortada"].copy()
    if len(df_runs) > 0:
        df_runs["run"] = df_runs["run"].astype(int)
        df_runs["workers"] = df_runs["workers"].astype(int)

        for idx, w in enumerate(sorted(df_runs["workers"].unique())):
            df_w = df_runs[df_runs["workers"] == w]
            run_times = df_w.groupby("run")["tiempo_acumulado_worker_ms"].max()
            media_total = media_recortada(run_times.values)

            n_est = df_w["n_estudiantes_global"].max()
            color = COLORS[idx % len(COLORS)]
            ax.scatter([n_est], [media_total], marker="s", s=60, color=color, zorder=5)
            ax.annotate(f"w={w}: {media_total:.0f}ms", (n_est, media_total),
                       textcoords="offset points", xytext=(10, 5 + idx*12), fontsize=8)

    ax.set_xlabel("Número de Estudiantes Procesados")
    ax.set_ylabel("Tiempo Acumulado (ms)")
    ax.set_title("Tiempo de Ejecución vs Tamaño del Dataset (batches de 100)")
    ax.legend()

    fig.tight_layout()
    path = os.path.join(output_dir, "02_tiempo_vs_batch.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  [OK] {path}")

def grafico_escalabilidad(tiempo_seq_ms, df_workers, output_dir):
    """Gráfico 3: Strong Scaling — Tiempo total vs Workers."""
    fig, ax = plt.subplots()

    workers = df_workers["workers"].values
    tiempos = df_workers["tiempo_total_ms"].values

    ax.plot(workers, tiempos, "o-", color=COLORS[0], linewidth=2, markersize=8, label="Tiempo real")

    ideal = tiempo_seq_ms / workers
    ax.plot(workers, ideal, "--", color="#999", linewidth=1, label="Escalamiento ideal")

    ax.set_xlabel("Número de Workers (goroutines)")
    ax.set_ylabel("Tiempo Total (ms)")
    ax.set_title("Escalabilidad (Strong Scaling)")
    ax.set_xticks(workers)
    ax.legend()

    for w, t in zip(workers, tiempos):
        ax.annotate(f"{t:.0f}ms", (w, t), textcoords="offset points",
                    xytext=(0, 12), ha="center", fontsize=9)

    fig.tight_layout()
    path = os.path.join(output_dir, "03_escalabilidad_strong.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)
    print(f"  [OK] {path}")


def tabla_resumen(tiempo_seq_ms, df_workers):
    """Imprime tabla resumen en consola."""
    workers = df_workers["workers"].values
    tiempos = df_workers["tiempo_total_ms"].values
    speedups = tiempo_seq_ms / tiempos
    eficiencias = (speedups / workers) * 100

    print()
    print("  +-------------+---------------+----------+-------------+")
    print("  | Modo        | Tiempo (ms)   | Speedup  | Eficiencia  |")
    print("  +-------------+---------------+----------+-------------+")
    print(f"  | Secuencial  | {tiempo_seq_ms:>11.1f}   |    1.00x |   100.00%   |")
    for w, t, sp, ef in zip(workers, tiempos, speedups, eficiencias):
        label = f"Conc w={w:<3d}"
        print(f"  | {label:<11s} | {t:>11.1f}   |  {sp:>6.2f}x |   {ef:>6.2f}%   |")
    print("  +-------------+---------------+----------+-------------+")

    best_idx = np.argmax(speedups)
    print(f"\n  >> Mejor speedup: {speedups[best_idx]:.2f}x con {workers[best_idx]} workers")

    for i in range(1, len(speedups)):
        if speedups[i] < speedups[i-1]:
            print(f"  !! Degradacion a partir de {workers[i]} workers "
                  f"({speedups[i-1]:.2f}x -> {speedups[i]:.2f}x)")
            break


def main():
    parser = argparse.ArgumentParser(description="Análisis de logs de benchmark secuencial vs concurrente")
    parser.add_argument("--seq", default="logs/log_secuencial.csv", help="Log secuencial CSV")
    parser.add_argument("--conc", default="logs/log_concurrente.csv", help="Log concurrente CSV")
    parser.add_argument("--output", default="analysis/graficos", help="Directorio de salida para gráficos")
    args = parser.parse_args()

    if not os.path.exists(args.seq):
        print(f"ERROR: No se encontró {args.seq}")
        print("  Ejecuta primero: go run . benchmark-seq")
        sys.exit(1)
    if not os.path.exists(args.conc):
        print(f"ERROR: No se encontró {args.conc}")
        print("  Ejecuta primero: go run . benchmark-conc")
        sys.exit(1)

    os.makedirs(args.output, exist_ok=True)

    print("=" * 70)
    print("  ANALISIS DE RENDIMIENTO: Secuencial vs Concurrente")
    print("=" * 70)
    print()

    print("  Cargando logs...")
    df_seq_runs, df_seq_resumen = cargar_log_secuencial(args.seq)
    df_conc = cargar_log_concurrente(args.conc)

    tiempo_seq_ms = df_seq_resumen["tiempo_acumulado_ms"].iloc[-1]
    print(f"  Tiempo secuencial (media recortada): {tiempo_seq_ms:.1f} ms")

    df_workers = calcular_tiempo_total_por_run_conc(df_conc)
    print(f"  Configuraciones de workers: {df_workers['workers'].tolist()}")
    print()

    print("  Generando gráficos...")
    grafico_speedup(tiempo_seq_ms, df_workers, args.output)
    grafico_tiempo_batch(df_seq_resumen, df_conc, args.output)
    grafico_escalabilidad(tiempo_seq_ms, df_workers, args.output)

    tabla_resumen(tiempo_seq_ms, df_workers)

    print()
    print(f"  Gráficos guardados en: {args.output}/")
    print("=" * 70)


if __name__ == "__main__":
    main()
