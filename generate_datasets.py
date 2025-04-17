#!/usr/bin/env python3
"""
generate_datasets.py: Friction Surfaces Dataset Pipeline

Commands:
  - parquet      Convert raw PNG maps into Parquet files + metadata
  - webdataset   Bundle maps + JSON into WebDataset .tar shards
  - extract      Reconstruct PNGs from Parquet store
  - decompress   Recreate PNGs from WebDataset shards
  - smart-update Incrementally update Parquet + WebDataset (only changed files)
  - run-all      Full rebuild: Parquet → metadata → WebDataset
"""
import os
import io
import json
import tarfile
import logging
import hashlib
import re
from datetime import datetime
from pathlib import Path
import concurrent.futures

import pandas as pd
from PIL import Image
from rich.progress import Progress, TextColumn, BarColumn, TimeElapsedColumn, TimeRemainingColumn
import typer

# -----------------------------------
# Configuration & Logging
# -----------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)
app = typer.Typer()

# -----------------------------------
# Helpers
# -----------------------------------
GRIT_RE   = re.compile(r"^S(\d+)$")
WEIGHT_RE = re.compile(r"^(\d+)g$")
DIST_RE   = re.compile(r"^(\d+)(mm|m)$")
CHANGELOG = {}

def compute_hash(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()

def has_changed(path: Path) -> bool:
    try:
        h = compute_hash(path)
    except FileNotFoundError:
        return False
    prev = CHANGELOG.get(str(path))
    if prev != h:
        CHANGELOG[str(path)] = h
        return True
    return False

def progress_bar(total: int, desc: str):
    return Progress(
        TextColumn(f"[bold cyan]{desc}"),
        BarColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn()
    )

def parse_png(png: Path, root: Path):
    parts = png.relative_to(root).parts
    if len(parts) < 6:
        return None
    mat, shape, direction, grit_p, weight_p, dist_p, *rest = parts
    m1, m2, m3 = GRIT_RE.match(grit_p), WEIGHT_RE.match(weight_p), DIST_RE.match(dist_p)
    if not (m1 and m2 and m3):
        return None
    grit    = int(m1.group(1))
    weight  = int(m2.group(1))
    dv, u   = int(m3.group(1)), m3.group(2)
    dist    = dv * (1000 if u=='m' else 1)
    rep     = int(rest[0]) if rest and rest[0].isdigit() else 1
    return dict(
        material=mat, shape=shape, direction=direction,
        grit=grit, weight=weight, distance=dist, replicate=rep,
        map_type=png.stem, png=png
    )

def _encode_record(r: dict) -> dict:
    png = r["png"]
    if not has_changed(png):
        return None
    try:
        with Image.open(png) as img:
            w,h,mode = *img.size, img.mode
            if mode in ("RGBA","LA","P"):
                img = img.convert("RGB")
            buf = io.BytesIO()
            img.save(buf, format="JPEG", optimize=True, quality=85, progressive=True)
        return {
            **r,
            "image_width": w,
            "image_height": h,
            "image_mode": mode,
            "capture_date": datetime.utcnow().isoformat(),
            "image_data": buf.getvalue()
        }
    except Exception as e:
        logger.error(f"Failed encoding {png}: {e}")
        return None

# -----------------------------------
# 1) PARQUET
# -----------------------------------
@app.command()
def parquet(
    input_dir: Path = typer.Option(..., exists=True, file_okay=False),
    output_dir: Path = typer.Option(Path("parquet_data"), file_okay=False),
    workers: int   = typer.Option(8)
):
    """Convert raw PNG maps into Parquet files + metadata."""
    # Scan and parse all PNGs
    recs = []
    for png in input_dir.rglob("*.png"):
        info = parse_png(png, input_dir)
        if info:
            recs.append(info)

    # Encode to JPEG bytes in parallel
    processed = []
    with progress_bar(len(recs), "Encoding images") as pb:
        task = pb.add_task("", total=len(recs))
        with concurrent.futures.ThreadPoolExecutor(workers) as exe:
            futures = [exe.submit(_encode_record, r) for r in recs]
            for f in concurrent.futures.as_completed(futures):
                out = f.result()
                if out:
                    processed.append(out)
                pb.update(task, advance=1)

    # Group by sample and write Parquet files
    groups = {}
    for r in processed:
        key = tuple(r[k] for k in ("material","shape","direction","grit","weight","distance","replicate"))
        groups.setdefault(key, []).append(r)

    for key, items in groups.items():
        mat, shape, direction, grit, weight, dist, rep = key
        folder = output_dir / mat / shape / direction / f"S{grit}" / f"{weight}g" / f"{dist}mm"
        folder.mkdir(parents=True, exist_ok=True)
        fname = f"rep{rep}.parquet" if rep != 1 else "data.parquet"
        df = pd.DataFrame(items).drop(columns=["png"])
        df.to_parquet(folder / fname, compression="snappy")
        logger.info(f"Wrote Parquet: {folder / fname}")

    # Create global metadata.parquet index
    entries = []
    for p in output_dir.rglob("*.parquet"):
        rel = p.relative_to(output_dir)
        mat, shape, direction, grit_p, weight_p, dist_p, fname = rel.parts
        entries.append(dict(
            material=mat,
            shape=shape,
            direction=direction,
            grit=int(grit_p[1:]),
            weight=int(weight_p[:-1]),
            distance=int(dist_p[:-2]),
            parquet_path=str(rel)
        ))
    pd.DataFrame(entries).to_parquet(output_dir / "metadata.parquet", compression="snappy")
    typer.echo("Parquet conversion complete.")

# -----------------------------------
# 2) WEBDATASET
# -----------------------------------
@app.command()
def webdataset(
    input_dir: Path = typer.Option(..., exists=True, file_okay=False),
    output_dir: Path = typer.Option(Path("webdataset_shards"), file_okay=False),
    shard_size: int = typer.Option(100),
    workers: int   = typer.Option(8)
):
    """Bundle raw PNG maps + metadata into sharded WebDataset .tar archives."""
    # Scan & parse all PNGs
    recs = []
    for png in input_dir.rglob("*.png"):
        info = parse_png(png, input_dir)
        if info:
            recs.append(info)

    # Encode to JPEG bytes + metadata in parallel
    processed = []
    with progress_bar(len(recs), "Encoding for shards") as pb:
        task = pb.add_task("", total=len(recs))
        with concurrent.futures.ThreadPoolExecutor(workers) as exe:
            futures = [exe.submit(_encode_record, r) for r in recs]
            for f in concurrent.futures.as_completed(futures):
                out = f.result()
                if out:
                    processed.append(out)
                pb.update(task, advance=1)

    # Shard into .tar files
    output_dir.mkdir(parents=True, exist_ok=True)
    num_shards = (len(processed) + shard_size - 1) // shard_size
    for i in range(num_shards):
        batch = processed[i*shard_size:(i+1)*shard_size]
        shard_path = output_dir / f"shard-{i:04d}.tar"
        with tarfile.open(shard_path, 'w') as tar:
            for idx, r in enumerate(batch):
                key = f"{idx:06d}"
                data = r.pop("image_data")
                ti = tarfile.TarInfo(key + ".jpg"); ti.size = len(data)
                tar.addfile(ti, io.BytesIO(data))
                meta = json.dumps(r).encode()
                ti = tarfile.TarInfo(key + ".json"); ti.size = len(meta)
                tar.addfile(ti, io.BytesIO(meta))
        logger.info(f"Wrote shard: {shard_path}")

    typer.echo("WebDataset shards created.")

# -----------------------------------
# 3) EXTRACT
# -----------------------------------
@app.command()
def extract(
    parquet_dir: Path = typer.Option(..., exists=True, file_okay=False),
    output_dir: Path  = Path("extracted_images"),
    material: str     = None,
    shape: str        = None,
    direction: str    = None,
    grit: int         = None,
    weight: int       = None,
    distance: int     = None,
    workers: int      = typer.Option(8)
):
    """Reconstruct PNGs from Parquet store."""
    df = pd.read_parquet(parquet_dir / "metadata.parquet")
    mask = True
    for col,val in [("material",material),("shape",shape),("direction",direction),
                    ("grit",grit),("weight",weight),("distance",distance)]:
        if val is not None:
            mask &= df[col] == val
    df = df[mask]
    if df.empty:
        typer.echo("No matching records.")
        return

    parts = []
    for row in df.itertuples():
        parts.append(pd.read_parquet(parquet_dir / row.parquet_path))
    all_df = pd.concat(parts, ignore_index=True)

    with progress_bar(len(all_df), "Writing PNGs") as pb:
        task = pb.add_task("", total=len(all_df))
        def _save(r):
            out = (output_dir / r.material / r.shape / r.direction /
                   f"S{r.grit}" / f"{r.weight}g" / f"{r.distance}mm" /
                   f"{r.map_type}.png")
            out.parent.mkdir(parents=True, exist_ok=True)
            img = Image.open(io.BytesIO(r.image_data))
            img.save(out)

        with concurrent.futures.ThreadPoolExecutor(workers) as exe:
            for _ in exe.map(_save, all_df.itertuples()):
                pb.update(task, advance=1)

    typer.echo(f"Extracted to {output_dir}")

# -----------------------------------
# 4) DECOMPRESS
# -----------------------------------
@app.command()
def decompress(
    shards_dir: Path = typer.Option(..., exists=True, file_okay=False),
    output_dir: Path = Path("decompressed_images"),
    workers: int    = typer.Option(4)
):
    """Recreate PNGs from WebDataset .tar shards."""
    tars = list(shards_dir.glob("*.tar"))
    if not tars:
        typer.echo("No shards found."); return

    with progress_bar(len(tars), "Decompressing shards") as pb:
        task = pb.add_task("", total=len(tars))
        def _unpack(tar_path):
            with tarfile.open(tar_path) as tar:
                samples = {}
                for m in tar.getmembers():
                    k, ext = Path(m.name).stem, Path(m.name).suffix
                    samples.setdefault(k, {})[ext] = m
                for files in samples.values():
                    j = files.get(".json"); i = files.get(".jpg")
                    if not (j and i):
                        continue
                    meta = json.load(tar.extractfile(j))
                    out = (output_dir / meta["material"] / meta["shape"] /
                           meta["direction"] / f"S{meta['grit']}" /
                           f"{meta['weight']}g" / f"{meta['distance']}mm" /
                           f"{meta['map_type']}.png")
                    out.parent.mkdir(parents=True, exist_ok=True)
                    img = Image.open(tar.extractfile(i))
                    img.save(out)
        with concurrent.futures.ThreadPoolExecutor(workers) as exe:
            for _ in exe.map(_unpack, tars):
                pb.update(task, advance=1)

    typer.echo(f"Decompressed to {output_dir}")

# -----------------------------------
# 5) SMART UPDATE & RUN ALL
# -----------------------------------
@app.command()
def smart_update(
    wear_dir: Path    = typer.Option(..., exists=True, file_okay=False),
    parquet_dir: Path = typer.Option(Path("parquet_data"), file_okay=False),
    shards_dir: Path  = typer.Option(Path("webdataset_shards"), file_okay=False),
    workers: int      = typer.Option(8),
    shard_size: int   = typer.Option(100)
):
    """Incrementally reprocess only changed files."""
    typer.echo("Smart‑update: Parquet → sharding")
    parquet(wear_dir, parquet_dir, workers)
    webdataset(wear_dir, shards_dir, shard_size, workers)

@app.command()
def run_all(
    wear_dir: Path    = typer.Option(..., exists=True, file_okay=False),
    parquet_dir: Path = typer.Option(Path("parquet_data"), file_okay=False),
    shards_dir: Path  = typer.Option(Path("webdataset_shards"), file_okay=False),
    workers: int      = typer.Option(8),
    shard_size: int   = typer.Option(100)
):
    """Full rebuild: Parquet → sharding."""
    parquet(wear_dir, parquet_dir, workers)
    webdataset(wear_dir, shards_dir, shard_size, workers)

if __name__ == "__main__":
    app()
