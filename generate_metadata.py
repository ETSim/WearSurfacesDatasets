#!/usr/bin/env python3
"""
generate_metadata.py: Produce per-leaf and global metadata for the Friction Surfaces dataset.

Features:
  - Leaf metadata: per-leaf `metadata.json` capturing structural tokens + detailed file info (size, dims).
  - Global metadata: optionally load a JSON template to write a top-level `metadata.json` with updated timestamp.
  - Index command: aggregate all leaf metadata into a master `index.json`.
  - Built with Typer for a unified CLI, Rich progress bars, and robust validation.
"""
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict

import typer
from rich.progress import Progress, TextColumn, BarColumn, TimeElapsedColumn, TimeRemainingColumn
from PIL import Image

app = typer.Typer(help="Generate and update metadata for the Friction Surfaces dataset.")

# expected map file stems
IMG_STEMS = ["ao", "bump", "displacement", "height", "hillshade", "normal", "roughness"]
SHAPES = {"Circle", "Line"}
DIRECTIONS = {"Linear", "NoLinear"}
TOKEN_RE = {
    'grit': re.compile(r"S(\d+)$"),
    'load': re.compile(r"(\d+)g$"),
    'distance': re.compile(r"(\d+)(mm|m)$"),
}


def is_leaf_dir(p: Path) -> bool:
    """Return True if directory `p` contains map files and no nested map subfolders."""
    has_png = any(f.suffix.lower() == '.png' and f.stem in IMG_STEMS for f in p.iterdir())
    if not has_png:
        return False
    for sub in p.iterdir():
        if sub.is_dir() and any(f.suffix.lower() == '.png' for f in sub.rglob('*.png')):
            return False
    return True


def parse_leaf_tokens(rel: Path) -> dict:
    """Extract metadata tokens from a relative leaf path."""
    info = {
        "material": None,
        "shape": None,
        "direction": None,
        "grit": None,
        "load_g": None,
        "distance_mm": None,
        "replicate": 1,
    }
    parts = list(rel.parts)
    if parts:
        info['material'] = parts[0]
    for token in parts:
        if token in SHAPES:
            info['shape'] = token
        if token in DIRECTIONS:
            info['direction'] = token
        m = TOKEN_RE['grit'].match(token)
        if m:
            info['grit'] = int(m.group(1))
        m = TOKEN_RE['load'].match(token)
        if m:
            info['load_g'] = int(m.group(1))
        m = TOKEN_RE['distance'].match(token)
        if m:
            val = int(m.group(1))
            unit = m.group(2)
            info['distance_mm'] = val if unit == 'mm' else val * 1000
        if token.isdigit():
            info['replicate'] = int(token)
    return info


def write_leaf_metadata(leaf: Path, root: Path, dry_run: bool) -> None:
    rel = leaf.relative_to(root)
    tokens = parse_leaf_tokens(rel)
    files: Dict[str, dict] = {}
    for stem in IMG_STEMS:
        f = leaf / f"{stem}.png"
        if f.exists():
            try:
                with Image.open(f) as img:
                    width, height = img.size
                size = f.stat().st_size
                files[stem] = {
                    "file": f.name,
                    "size_bytes": size,
                    "width_px": width,
                    "height_px": height
                }
            except Exception:
                files[stem] = {"file": f.name}
    leaf_meta = {**tokens, "files": files, "num_files": len(files)}
    out = leaf / "metadata.json"
    content = json.dumps(leaf_meta, indent=2, sort_keys=True)
    if dry_run:
        typer.secho(f"[dry-run] {out}", fg=typer.colors.YELLOW)
        typer.echo(content)
    else:
        out.write_text(content, encoding='utf-8')


def collect_leaf_dirs(root: Path):
    """Yield all leaf directories under `root`."""
    for p in sorted(root.rglob("*")):
        if p.is_dir() and is_leaf_dir(p):
            yield p


@app.command()
def leaf(
    root: Path = typer.Argument(..., exists=True, file_okay=False, help="Dataset root directory"),
    dry_run: bool = typer.Option(False, help="Print JSON instead of writing."),
    verbose: bool = typer.Option(False, help="Log each processed path."),
):
    """Generate per-leaf metadata JSON files."""
    leaves = list(collect_leaf_dirs(root))
    if not leaves:
        typer.secho("No leaf directories found.", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    with Progress(
        TextColumn("[cyan]Leaf dirs processed:"),
        BarColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn()
    ) as prog:
        task = prog.add_task("", total=len(leaves))
        for leaf_dir in leaves:
            if verbose:
                typer.echo(f"Processing: {leaf_dir}")
            write_leaf_metadata(leaf_dir, root, dry_run)
            prog.update(task, advance=1)
    typer.secho(f"✔ Leaf metadata {'(dry-run) ' if dry_run else ''}written for {len(leaves)} directories.", fg=typer.colors.GREEN)


@app.command()
def global_
(
    root: Path = typer.Argument(..., exists=True, file_okay=False, help="Dataset root directory"),
    template: Path = typer.Option(..., exists=True, help="JSON template for global metadata."),
    dry_run: bool = typer.Option(False, help="Print JSON instead of writing."),
):
    """Generate top-level metadata.json from a template, updating lastUpdated."""
    tpl = json.loads(template.read_text(encoding='utf-8'))
    info = tpl.setdefault('datasetInfo', {})
    info['lastUpdated'] = datetime.utcnow().isoformat()
    out = root / 'metadata.json'
    content = json.dumps(tpl, indent=2, sort_keys=True)
    if dry_run:
        typer.secho(f"[dry-run] {out}", fg=typer.colors.YELLOW)
        typer.echo(content)
    else:
        out.write_text(content, encoding='utf-8')
        typer.secho(f"✔ Global metadata written to {out}.", fg=typer.colors.GREEN)


@app.command()
def index(
    root: Path = typer.Argument(..., exists=True, file_okay=False, help="Dataset root directory"),
    output: Path = typer.Option('index.json', help="Path for aggregated index file."),
    dry_run: bool = typer.Option(False, help="Print JSON instead of writing."),
):
    """Aggregate all leaf metadata.json into a single index JSON."""
    all_meta: List[dict] = []
    for leaf_dir in collect_leaf_dirs(root):
        f = leaf_dir / 'metadata.json'
        if f.exists():
            try:
                m = json.loads(f.read_text(encoding='utf-8'))
                m['path'] = str(leaf_dir.relative_to(root))
                all_meta.append(m)
            except Exception:
                typer.secho(f"Warning: invalid JSON in {f}", fg=typer.colors.YELLOW)
    index_data = {'generated': datetime.utcnow().isoformat(), 'entries': all_meta}
    content = json.dumps(index_data, indent=2)
    if dry_run:
        typer.echo(content)
    else:
        output.write_text(content, encoding='utf-8')
        typer.secho(f"✔ Index written to {output} with {len(all_meta)} entries.", fg=typer.colors.GREEN)


if __name__ == "__main__":
    app()