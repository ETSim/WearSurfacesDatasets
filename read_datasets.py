#!/usr/bin/env python3
import os
import io
import json
import tarfile
import concurrent.futures
import base64
import pandas as pd
from PIL import Image
from rich.progress import Progress, TextColumn, BarColumn, TimeElapsedColumn, TimeRemainingColumn
import typer

app = typer.Typer(help="Extract images from Friction Surfaces Parquet files or decompress WebDataset shards.")

#####################################
# PARQUET EXTRACTION FUNCTIONS      #
#####################################

def read_parquet_metadata(parquet_dir: str) -> pd.DataFrame:
    """
    Read the metadata Parquet file containing folder information.
    
    Args:
        parquet_dir: Directory where metadata.parquet is located.
    Returns:
        A DataFrame with metadata or None if not found.
    """
    metadata_file = os.path.join(parquet_dir, "metadata.parquet")
    if not os.path.exists(metadata_file):
        typer.echo(f"Metadata file not found: {metadata_file}")
        return None
    return pd.read_parquet(metadata_file, engine='pyarrow')

def copy_image(row: pd.Series, dest_path: str) -> bool:
    """
    Recreate and save an image using only the embedded binary image data.
    
    Args:
        row: A pandas Series containing image metadata and raw binary image data.
        dest_path: The destination file path where the image will be saved.
    Returns:
        True if the image is saved successfully; otherwise, False.
    """
    try:
        if 'image_data' in row and pd.notnull(row['image_data']):
            image_data = row['image_data']
            image_bytes = base64.b64decode(image_data) if isinstance(image_data, str) else image_data
            img = Image.open(io.BytesIO(image_bytes))
            img.save(dest_path)
        else:
            typer.echo(f"Warning: No embedded image data for {row.get('image_type', 'unknown')}. Skipping.")
        return True
    except Exception as e:
        typer.echo(f"Error extracting image {row.get('image_type', 'unknown')} to {dest_path}: {str(e)}")
        return False

def extract_images(df: pd.DataFrame, output_dir: str, workers: int = 8) -> None:
    """
    Extract images from the combined DataFrame to a directory structure using only the embedded binary image data.
    
    Args:
        df: DataFrame containing image metadata and embedded image data.
        output_dir: Base output directory where images will be saved.
        workers: Number of concurrent worker threads.
    """
    if df.empty:
        typer.echo("No data to extract images from.")
        return

    os.makedirs(output_dir, exist_ok=True)
    grouped = df.groupby(['material', 'grit', 'weight', 'distance'])
    total_images = len(df)
    
    with Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn()
    ) as progress:
        extract_task = progress.add_task(f"Extracting {total_images} images...", total=total_images)
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = []
            for (material, grit, weight, distance), group in grouped:
                dest_dir = os.path.join(output_dir, material, grit, f"{weight}g", f"{distance}mm")
                os.makedirs(dest_dir, exist_ok=True)
                for _, row in group.iterrows():
                    image_type = row['image_type']
                    dest_file = os.path.join(dest_dir, f"{image_type}.png")
                    futures.append(executor.submit(copy_image, row, dest_file))
            # Update progress as each future completes:
            for future in concurrent.futures.as_completed(futures):
                future.result()  # Capture any exceptions.
                progress.update(extract_task, advance=1)
    typer.echo(f"Extracted {total_images} images to {output_dir}")

def extract_images_by_query(material: str = None, grit: str = None, weight: str = None, distance: str = None,
                            input_dir: str = "parquet_data", output_dir: str = "extracted_images", workers: int = 8) -> None:
    """
    Extract images based on query parameters from the Parquet metadata using only the embedded image data.
    
    Args:
        material: Filter by material.
        grit: Filter by grit.
        weight: Filter by weight.
        distance: Filter by distance.
        input_dir: Directory containing Parquet data and metadata.
        output_dir: Directory where extracted images will be saved.
        workers: Number of concurrent worker threads.
    """
    metadata = read_parquet_metadata(input_dir)
    if metadata is None:
        return
    filters = []
    if material:
        filters.append(f"material == '{material}'")
    if grit:
        filters.append(f"grit == '{grit}'")
    if weight:
        filters.append(f"weight == '{weight}'")
    if distance:
        filters.append(f"distance == '{distance}'")
    query = " and ".join(filters) if filters else None
    filtered = metadata.query(query) if query else metadata
    if filtered.empty:
        typer.echo(f"No files match the query: {query}")
        return
    typer.echo(f"Found {len(filtered)} matching folders.")
    all_data = []
    with Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn()
    ) as progress:
        read_task = progress.add_task("Reading Parquet files...", total=len(filtered))
        for _, row in filtered.iterrows():
            parquet_path = os.path.join(input_dir, row['parquet_path'])
            try:
                df = pd.read_parquet(parquet_path, engine='pyarrow')
                all_data.append(df)
            except Exception as e:
                typer.echo(f"Error reading {parquet_path}: {str(e)}")
            progress.update(read_task, advance=1)
    if not all_data:
        typer.echo("No data could be read from Parquet files.")
        return
    combined_df = pd.concat(all_data)
    typer.echo(f"Total images to extract: {len(combined_df)}")
    extract_images(combined_df, output_dir, workers)

#########################################
# WEBDATASET SHARD DECOMPRESSION BLOCK  #
#########################################

def decompress_webdataset(input_dir: str, output_dir: str, workers: int = 8) -> None:
    """
    Decompress WebDataset shards (tar files) into a folder structure based on JSON metadata.
    
    For each tar shard in input_dir, this function extracts pairs of files:
    <key>.jpg and <key>.json. The JSON file should contain keys such as 'material',
    'grit', 'weight', 'distance', and 'image_type' to define the output structure.
    
    Args:
        input_dir: Directory containing WebDataset tar files.
        output_dir: Base output directory for decompressed images.
        workers: Number of concurrent worker threads.
    """
    tar_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith(".tar")]
    if not tar_files:
        typer.echo("No tar files (WebDataset shards) found in input directory.")
        return

    os.makedirs(output_dir, exist_ok=True)
    
    # Create a progress instance for decompression.
    with Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn()
    ) as progress:
        task = progress.add_task("Decompressing shards...", total=len(tar_files))
        
        def process_shard(tar_path: str):
            with tarfile.open(tar_path, 'r') as tar:
                members = tar.getmembers()
                # Group members by key (assume filenames like 000001.jpg and 000001.json)
                samples = {}
                for member in members:
                    key, ext = os.path.splitext(member.name)
                    if key not in samples:
                        samples[key] = {}
                    samples[key][ext] = member
                for key, files in samples.items():
                    if ".json" in files and ".jpg" in files:
                        json_file = tar.extractfile(files[".json"])
                        if not json_file:
                            continue
                        try:
                            meta = json.load(json_file)
                        except Exception as e:
                            typer.echo(f"Error parsing JSON in {files['.json'].name}: {e}")
                            continue
                        # Determine destination from metadata.
                        mat = meta.get("material", "unknown")
                        gr = meta.get("grit", "unknown")
                        wt = meta.get("weight", "unknown") + "g"
                        dist = meta.get("distance", "unknown") + "mm"
                        img_type = meta.get("image_type", key)
                        dest_dir = os.path.join(output_dir, mat, gr, wt, dist)
                        os.makedirs(dest_dir, exist_ok=True)
                        dest_file = os.path.join(dest_dir, f"{img_type}.png")
                        jpg_file = tar.extractfile(files[".jpg"])
                        if not jpg_file:
                            continue
                        try:
                            img = Image.open(jpg_file)
                            img.save(dest_file)
                        except Exception as e:
                            typer.echo(f"Error saving image from {files['.jpg'].name}: {e}")
            return tar_path

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            for _ in executor.map(process_shard, tar_files):
                progress.update(task, advance=1)
    typer.echo(f"Decompressed WebDataset shards to {output_dir}")

####################################
# TYPER COMMANDS                   #
####################################

@app.command(name="extract")
def extract_command(
    input_dir: str = typer.Option("parquet_data", help="Input directory containing Parquet data and metadata."),
    output_dir: str = typer.Option("extracted_images", help="Output directory for extracted images."),
    material: str = typer.Option(None, help="Filter by material."),
    grit: str = typer.Option(None, help="Filter by grit."),
    weight: str = typer.Option(None, help="Filter by weight."),
    distance: str = typer.Option(None, help="Filter by distance."),
    workers: int = typer.Option(8, help="Number of concurrent worker threads.")
):
    """
    Extract images from Parquet files using only the embedded binary image data.
    """
    extract_images_by_query(material, grit, weight, distance, input_dir, output_dir, workers)

@app.command(name="decompress-webdataset")
def decompress_webdataset_command(
    input_dir: str = typer.Option("webdataset_shards", help="Input directory containing WebDataset tar shards."),
    output_dir: str = typer.Option("decompressed_webdataset", help="Output directory for decompressed images."),
    workers: int = typer.Option(8, help="Number of concurrent worker threads.")
):
    """
    Decompress WebDataset shards (tar files) into a folder structure using JSON metadata.
    """
    decompress_webdataset(input_dir, output_dir, workers)

if __name__ == "__main__":
    app()
