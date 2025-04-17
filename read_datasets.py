#!/usr/bin/env python3
"""
read_datasets.py: Extract PNGs from Parquet-based dataset and
decompress WebDataset shards using layout:

    material/shape/direction/grit/weight/distance[/replicate]/map.png
"""
import io
import json
import tarfile
import logging
import base64
import concurrent.futures
from pathlib import Path
from typing import Optional

import pandas as pd
from PIL import Image
import typer
from rich.progress import Progress, TextColumn, BarColumn, TimeElapsedColumn, TimeRemainingColumn

# -----------------------------------
# Configuration & Logging
# -----------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)
app = typer.Typer(help="Tools for reading Friction Surfaces dataset.")

# -----------------------------------
# Progress Context
# -----------------------------------
def progress_context(total: int, description: str) -> Progress:
    return Progress(
        TextColumn(f"[bold cyan]{description}"),
        BarColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn()
    )

# -----------------------------------
# PARQUET EXTRACTION
# -----------------------------------
def read_metadata(parquet_dir: Path) -> pd.DataFrame:
    """Load the top‐level metadata.parquet that indexes all Parquet shards."""
    meta_file = parquet_dir / "metadata.parquet"
    if not meta_file.exists():
        logger.error(f"Metadata file not found: {meta_file}")
        raise typer.Exit(code=1)
    return pd.read_parquet(meta_file, engine="pyarrow")

def extract_from_parquet(
    parquet_dir: Path,
    output_dir: Path,
    material: Optional[str],
    shape: Optional[str],
    direction: Optional[str],
    grit: Optional[int],
    weight: Optional[int],
    distance: Optional[int],
    replicate: Optional[int],
    workers: int
):
    """Filter metadata, load matching Parquet shards, and write out PNGs."""
    df_meta = read_metadata(parquet_dir)

    # apply folder‐level filters
    for col, val in [
        ("material", material),
        ("shape", shape),
        ("direction", direction),
        ("grit", grit),
        ("weight", weight),
        ("distance", distance),
    ]:
        if val is not None:
            df_meta = df_meta[df_meta[col] == val]

    if df_meta.empty:
        typer.echo("No matching shards in metadata.")
        return

    # load all matching Parquet files
    shards = []
    with progress_context(len(df_meta), "Loading Parquet shards") as prog:
        task = prog.add_task("", total=len(df_meta))
        for row in df_meta.itertuples(index=False):
            path = parquet_dir / row.parquet_path
            try:
                shards.append(pd.read_parquet(path, engine="pyarrow"))
            except Exception as e:
                logger.warning(f"Failed to read {path}: {e}")
            prog.update(task, advance=1)

    if not shards:
        typer.echo("No data loaded; aborting extraction.")
        return

    df_all = pd.concat(shards, ignore_index=True)

    # apply record‐level replicate filter if requested
    if replicate is not None and "replicate" in df_all.columns:
        df_all = df_all[df_all.replicate == replicate]

    total = len(df_all)
    if total == 0:
        typer.echo("No images to extract after filtering.")
        return

    # write out PNGs
    with progress_context(total, "Writing PNGs") as prog:
        task = prog.add_task("", total=total)

        def _save(record):
            dest = (
                output_dir
                / record.material
                / record.shape
                / record.direction
                / f"S{record.grit}"
                / f"{record.weight}g"
                / f"{record.distance}mm"
            )
            if getattr(record, "replicate", None) is not None:
                dest = dest / f"rep{int(record.replicate)}"
            dest.mkdir(parents=True, exist_ok=True)

            out_file = dest / f"{record.map_type}.png"
            data = record.image_data
            if isinstance(data, str):
                data = base64.b64decode(data)
            try:
                img = Image.open(io.BytesIO(data))
                img.save(out_file)
            except Exception as e:
                logger.warning(f"Failed saving {out_file}: {e}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as exe:
            futures = [exe.submit(_save, row) for row in df_all.itertuples(index=False)]
            for _ in concurrent.futures.as_completed(futures):
                prog.update(task, advance=1)

    typer.echo(f"Extracted {total} images to {output_dir}")

@app.command()
def extract(
    parquet_dir: Path = typer.Option(..., exists=True, file_okay=False, help="Parquet root (contains metadata.parquet)"),
    output_dir: Path  = typer.Option(Path("extracted_images"), file_okay=False, help="Where to write PNGs"),
    material: Optional[str]  = typer.Option(None, help="Filter by material"),
    shape: Optional[str]     = typer.Option(None, help="Filter by shape"),
    direction: Optional[str] = typer.Option(None, help="Filter by direction"),
    grit: Optional[int]      = typer.Option(None, help="Filter by grit (e.g. 60)"),
    weight: Optional[int]    = typer.Option(None, help="Filter by weight (e.g. 100)"),
    distance: Optional[int]  = typer.Option(None, help="Filter by distance (e.g. 1200)"),
    replicate: Optional[int] = typer.Option(None, help="Filter by replicate number"),
    workers: int             = typer.Option(8, help="Parallel workers")
):
    """Extract PNGs from Parquet storage based on optional filters."""
    extract_from_parquet(
        parquet_dir, output_dir,
        material, shape, direction,
        grit, weight, distance, replicate, workers
    )

# -----------------------------------
# WEBDATASET DECOMPRESSION
# -----------------------------------
def decompress_shards(
    shards_dir: Path,
    output_dir: Path,
    workers: int
):
    """Decompress WebDataset .tar shards back into PNG folders."""
    tar_files = list(shards_dir.glob("*.tar"))
    if not tar_files:
        typer.echo(f"No .tar shards found in {shards_dir}")
        return

    with progress_context(len(tar_files), "Decompressing shards") as prog:
        task = prog.add_task("", total=len(tar_files))

        def _process(tar_path: Path):
            try:
                with tarfile.open(tar_path, "r") as tar:
                    samples = {}
                    for member in tar.getmembers():
                        key, ext = Path(member.name).stem, Path(member.name).suffix
                        samples.setdefault(key, {})[ext] = member

                    for files in samples.values():
                        j, i = files.get(".json"), files.get(".jpg")
                        if not (j and i):
                            continue
                        meta = json.load(tar.extractfile(j))
                        dest = (
                            output_dir
                            / meta.get("material","unknown")
                            / meta.get("shape","unknown")
                            / meta.get("direction","unknown")
                            / f"S{meta.get('grit','unknown')}"
                            / f"{meta.get('weight','')}g"
                            / f"{meta.get('distance','')}mm"
                        )
                        if meta.get("replicate") is not None:
                            dest = dest / f"rep{meta['replicate']}"
                        dest.mkdir(parents=True, exist_ok=True)

                        out_file = dest / f"{meta.get('map_type','map')}.png"
                        with tar.extractfile(i) as img_f:
                            try:
                                img = Image.open(img_f)
                                img.save(out_file)
                            except Exception as e:
                                logger.warning(f"Failed saving {out_file}: {e}")
            except Exception as e:
                logger.error(f"Error in {tar_path}: {e}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as exe:
            for _ in exe.map(_process, tar_files):
                prog.update(task, advance=1)

    typer.echo(f"Decompressed shards into {output_dir}")

@app.command()
def decompress(
    shards_dir: Path = typer.Option(..., exists=True, file_okay=False, help="Directory of .tar shards"),
    output_dir: Path = typer.Option(Path("decompressed_images"), file_okay=False, help="Where to write PNGs"),
    workers: int    = typer.Option(4, help="Parallel workers")
):
    """Decompress WebDataset tar shards into the original PNG hierarchy."""
    decompress_shards(shards_dir, output_dir, workers)

if __name__ == "__main__":
    app()
