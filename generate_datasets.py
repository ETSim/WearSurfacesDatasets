import os
import io
import json
import tarfile
import concurrent.futures
import pandas as pd
import numpy as np
from PIL import Image
from rich.progress import Progress, TextColumn, BarColumn, TimeElapsedColumn, TimeRemainingColumn
from tqdm import tqdm
import base64
import typer

app = typer.Typer(help="Friction Surfaces Dataset Pipeline: Convert to Parquet, create WebDataset shards, or extract images.")

#############################
# HELPER FUNCTIONS          #
#############################

def get_wear_dir(base_dir: str) -> str:
    """Return the correct Wear directory given a base directory.
    If the base directory’s name is already 'Wear' (case insensitive), return it; otherwise, append 'Wear'."""
    if os.path.basename(os.path.normpath(base_dir)).lower() == "wear":
        return base_dir
    else:
        return os.path.join(base_dir, "Wear")

#############################
# PARQUET CONVERSION BLOCK  #
#############################

def process_distance_folder(material, grit, weight, distance, base_path, output_dir):
    """
    Process a single distance folder and create a Parquet file.
    
    Reads each PNG file from the folder, converts it to a compressed JPEG,
    and stores the raw binary data (as bytes) along with metadata.
    The original image path is not saved.
    """
    wear_dir = get_wear_dir(base_path)
    distance_path = os.path.join(wear_dir, material, grit, weight, distance)
    weight_value = weight.replace('g', '')
    distance_value = distance.replace('mm', '')

    if not os.path.isdir(distance_path):
        return None

    image_files = [f for f in os.listdir(distance_path) if f.endswith('.png')]
    if not image_files:
        return None

    records = []
    for image_file in image_files:
        image_path = os.path.join(distance_path, image_file)
        image_type = os.path.splitext(image_file)[0]
        try:
            with Image.open(image_path) as img:
                width, height = img.size
                mode = img.mode
                if img.mode in ("RGBA", "LA", "P"):
                    img = img.convert("RGB")
                buffer = io.BytesIO()
                img.save(buffer, format="JPEG", quality=85, optimize=True, progressive=True)
                image_bytes = buffer.getvalue()
            record = {
                'material': material,
                'grit': grit,
                'weight': weight_value,
                'distance': distance_value,
                'image_type': image_type,
                'image_width': width,
                'image_height': height,
                'image_mode': mode,
                'image_data': image_bytes
            }
            records.append(record)
        except Exception as e:
            print(f"Error processing {image_path}: {str(e)}")
    
    if not records:
        return None

    parquet_dir = os.path.join(output_dir, material, grit, weight)
    os.makedirs(parquet_dir, exist_ok=True)
    df = pd.DataFrame(records)
    parquet_file = os.path.join(parquet_dir, f"{distance}.parquet")
    df.to_parquet(parquet_file, compression='snappy', engine='pyarrow')
    return parquet_file

def find_all_folders(base_path):
    """
    Find all material/grit/weight/distance folder combinations from the dataset.
    """
    wear_dir = get_wear_dir(base_path)
    folders = []
    for material in os.listdir(wear_dir):
        material_path = os.path.join(wear_dir, material)
        if not os.path.isdir(material_path):
            continue
        for grit in os.listdir(material_path):
            grit_path = os.path.join(material_path, grit)
            if not os.path.isdir(grit_path):
                continue
            for weight in os.listdir(grit_path):
                weight_path = os.path.join(material_path, grit, weight)
                if not os.path.isdir(weight_path):
                    continue
                for distance in os.listdir(weight_path):
                    distance_path = os.path.join(material_path, grit, weight, distance)
                    if os.path.isdir(distance_path):
                        folders.append((material, grit, weight, distance))
    return folders

def convert_to_parquet_per_folder(base_dir=".", output_dir="parquet_data", workers=8):
    """
    Convert the Friction Surfaces dataset into Parquet files (one per folder).
    """
    os.makedirs(output_dir, exist_ok=True)
    print("Scanning directory structure for Parquet conversion...")
    all_folders = find_all_folders(base_dir)
    total_folders = len(all_folders)
    print(f"Found {total_folders} folders to process.")

    successful = 0

    with Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    ) as progress:
        task = progress.add_task("Converting folders to Parquet...", total=total_folders)
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_folder = {
                executor.submit(process_distance_folder, material, grit, weight, distance, base_dir, output_dir):
                    (material, grit, weight, distance)
                for material, grit, weight, distance in all_folders
            }
            for future in concurrent.futures.as_completed(future_to_folder):
                folder = future_to_folder[future]
                try:
                    result = future.result()
                    if result:
                        successful += 1
                        folder_desc = f"{folder[0]}/{folder[1]}/{folder[2]}/{folder[3]}"
                        progress.update(task, description=f"Processed {folder_desc}")
                except Exception as exc:
                    print(f"Folder {folder} generated an exception: {exc}")
                progress.update(task, advance=1)
    print(f"Conversion complete. {successful}/{total_folders} folders successfully converted.")
    print(f"Parquet files are stored in: {output_dir}")
    return output_dir

def create_metadata_parquet(base_dir=".", output_dir="parquet_data"):
    """
    Create a metadata Parquet file with information about each processed folder.
    """
    parquet_files = []
    for root, dirs, files in os.walk(output_dir):
        for file in files:
            if file.endswith(".parquet"):
                parquet_path = os.path.join(root, file)
                rel_path = os.path.relpath(parquet_path, output_dir)
                parts = rel_path.split(os.sep)
                if len(parts) >= 4:
                    material, grit, weight, distance_file = parts[:4]
                    distance = distance_file.replace(".parquet", "")
                    parquet_files.append({
                        'material': material,
                        'grit': grit,
                        'weight': weight.replace('g', ''),
                        'distance': distance.replace('mm', ''),
                        'parquet_path': rel_path
                    })
    df = pd.DataFrame(parquet_files)
    metadata_file = os.path.join(output_dir, "metadata.parquet")
    df.to_parquet(metadata_file, compression='snappy', engine='pyarrow')
    print(f"Metadata file created: {metadata_file}")
    print(f"Contains information about {len(df)} Parquet files")
    return metadata_file

def read_parquet_metadata(parquet_dir="parquet_data"):
    """
    Read the metadata Parquet file.
    """
    metadata_file = os.path.join(parquet_dir, "metadata.parquet")
    if not os.path.exists(metadata_file):
        print(f"Metadata file not found: {metadata_file}")
        return None
    return pd.read_parquet(metadata_file, engine='pyarrow')

#############################
# IMAGE EXTRACTION (Parquet)#
#############################

def copy_image(row, dest_path):
    """
    Recreate and save an image using ONLY the embedded binary image data.
    """
    try:
        if 'image_data' in row and pd.notnull(row['image_data']):
            image_data = row['image_data']
            if isinstance(image_data, str):
                image_bytes = base64.b64decode(image_data)
            else:
                image_bytes = image_data
            img = Image.open(io.BytesIO(image_bytes))
            img.save(dest_path)
        else:
            print(f"Warning: No embedded image data for {row.get('image_type', 'unknown')}. Skipping extraction.")
        return True
    except Exception as e:
        print(f"Error extracting image {row.get('image_type', 'unknown')} to {dest_path}: {str(e)}")
        return False

def extract_images(df, output_dir, workers=8):
    """
    Extract images from the DataFrame to a directory structure using ONLY the embedded binary image data.
    """
    if df is None or df.empty:
        print("No data to extract images from.")
        return
    os.makedirs(output_dir, exist_ok=True)
    grouped = df.groupby(['material', 'grit', 'weight', 'distance'])
    total_images = len(df)
    with Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
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
            for future in concurrent.futures.as_completed(futures):
                future.result()
                progress.update(extract_task, advance=1)
    print(f"Extracted {total_images} images to {output_dir}")

def extract_images_by_query(material=None, grit=None, weight=None, distance=None,
                            input_dir="parquet_data", output_dir="extracted_images", workers=8):
    """
    Extract images based on query parameters from the Parquet metadata using only embedded image data.
    """
    metadata = read_parquet_metadata(input_dir)
    if metadata is None:
        print(f"No metadata found in {input_dir}")
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
    filtered_files = metadata.query(query) if query else metadata
    if filtered_files.empty:
        print(f"No files match the query: {query}")
        return
    print(f"Found {len(filtered_files)} matching folders")
    all_data = []
    with Progress(
        TextColumn("[bold blue]Reading parquet files..."),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
    ) as progress:
        read_task = progress.add_task("Reading parquet files...", total=len(filtered_files))
        for _, row in filtered_files.iterrows():
            parquet_path = os.path.join(input_dir, row['parquet_path'])
            try:
                df = pd.read_parquet(parquet_path, engine='pyarrow')
                all_data.append(df)
            except Exception as e:
                print(f"Error reading {parquet_path}: {str(e)}")
            progress.update(read_task, advance=1)
    if not all_data:
        print("No data could be read from parquet files")
        return
    combined_df = pd.concat(all_data)
    print(f"Total images to extract: {len(combined_df)}")
    extract_images(combined_df, output_dir, workers)

#############################
# WEBDATASET SHARD CREATION #
#############################

def process_image_record(record):
    """
    Given a record with keys:
      'material', 'grit', 'weight', 'distance', 'image_type', 'image_path',
    Open the image, convert to compressed JPEG, and add 'image_data'.
    Removes 'image_path' from the returned record.
    """
    image_path = record['image_path']
    try:
        with Image.open(image_path) as img:
            width, height = img.size
            mode = img.mode
            if img.mode in ("RGBA", "LA", "P"):
                img = img.convert("RGB")
            buffer = io.BytesIO()
            img.save(buffer, format="JPEG", quality=85, optimize=True, progressive=True)
            image_bytes = buffer.getvalue()
        new_record = record.copy()
        new_record.pop('image_path', None)
        new_record['image_width'] = width
        new_record['image_height'] = height
        new_record['image_mode'] = mode
        new_record['image_data'] = image_bytes
        return new_record
    except Exception as e:
        print(f"Error processing {image_path}: {str(e)}")
        return None

def create_shard(shard_path, records):
    """
    Create a WebDataset shard (tar archive) from a list of records.
    Each record is stored as a JPEG file and a corresponding JSON file.
    """
    with tarfile.open(shard_path, 'w') as tar:
        for idx, rec in enumerate(records):
            key = f"{idx:06d}"
            img_bytes = rec['image_data']
            img_info = tarfile.TarInfo(name=key + ".jpg")
            img_info.size = len(img_bytes)
            tar.addfile(img_info, io.BytesIO(img_bytes))
            meta = rec.copy()
            meta.pop('image_data', None)
            meta_bytes = json.dumps(meta).encode("utf-8")
            meta_info = tarfile.TarInfo(name=key + ".json")
            meta_info.size = len(meta_bytes)
            tar.addfile(meta_info, io.BytesIO(meta_bytes))

def create_shards_from_records(records, shard_size, output_dir, workers=8):
    """
    Split the list of records into shards of shard_size and create tar archives.
    """
    os.makedirs(output_dir, exist_ok=True)
    num_shards = (len(records) + shard_size - 1) // shard_size
    for i in range(num_shards):
        shard_records = records[i * shard_size:(i + 1) * shard_size]
        shard_name = f"shard-{i:04d}.tar"
        shard_path = os.path.join(output_dir, shard_name)
        create_shard(shard_path, shard_records)
        print(f"Created shard: {shard_path}")

def collect_records_for_shards(input_dir):
    """
    Traverse the dataset directory structure (Wear/Material/Grit/Weight/Distance)
    and collect records from PNG files with keys 'material', 'grit', 'weight',
    'distance', 'image_type', and 'image_path'.
    Uses tqdm for progress feedback.
    """
    records = []
    wear_dir = get_wear_dir(input_dir)
    for material in tqdm(os.listdir(wear_dir), desc="Materials", leave=False):
        material_path = os.path.join(wear_dir, material)
        if not os.path.isdir(material_path):
            continue
        for grit in os.listdir(material_path):
            grit_path = os.path.join(material_path, grit)
            if not os.path.isdir(grit_path):
                continue
            for weight in os.listdir(grit_path):
                weight_path = os.path.join(material_path, grit, weight)
                if not os.path.isdir(weight_path):
                    continue
                for distance in os.listdir(weight_path):
                    distance_path = os.path.join(material_path, grit, weight, distance)
                    if not os.path.isdir(distance_path):
                        continue
                    for image_file in os.listdir(distance_path):
                        if not image_file.endswith('.png'):
                            continue
                        record = {
                            'material': material,
                            'grit': grit,
                            'weight': weight.replace('g', ''),
                            'distance': distance.replace('mm', ''),
                            'image_type': os.path.splitext(image_file)[0],
                            'image_path': os.path.join(distance_path, image_file)
                        }
                        records.append(record)
    return records

def create_webdataset_shards(input_dir, output_dir, shard_size, workers=8):
    """
    Create WebDataset shards from the dataset directory.
    Uses threading to process image records concurrently.
    """
    raw_records = collect_records_for_shards(input_dir)
    print(f"Found {len(raw_records)} raw records for WebDataset creation.")
    processed_records = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(process_image_record, rec) for rec in raw_records]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result is not None:
                processed_records.append(result)
    print(f"Processed {len(processed_records)} records successfully.")
    create_shards_from_records(processed_records, shard_size, output_dir, workers)

#############################
# TYPER COMMANDS            #
#############################

@app.command()
def parquet(input_dir: str = "Wear", output_dir: str = "parquet_data", workers: int = 8):
    """
    Convert the Friction Surfaces dataset into Parquet files and create metadata.
    """
    out_dir = convert_to_parquet_per_folder(base_dir=input_dir, output_dir=output_dir, workers=workers)
    create_metadata_parquet(input_dir, out_dir)

@app.command()
def webdataset(input_dir: str = "Wear", output_dir: str = "webdataset_shards", shard_size: int = 100, workers: int = 8):
    """
    Create WebDataset shards from the dataset.
    """
    create_webdataset_shards(input_dir, output_dir, shard_size, workers=workers)

@app.command()
def extract(input_dir: str = "parquet_data", output_dir: str = "extracted_images", 
            material: str = None, grit: str = None, weight: str = None, distance: str = None, workers: int = 8):
    """
    Extract images from Parquet files using only embedded binary image data.
    
    Optional filters: --material, --grit, --weight, --distance.
    """
    # The extract_images_by_query function uses the existing input directory as the base for parquet files.
    # It will search the metadata and extract all matching images.
    from sys import exit
    try:
        # We assume that metadata.parquet is in the input_dir.
        # Reuse the extract_images_by_query function from earlier.
        # (If needed, you can modify this function to use Typer types too.)
        extract_images_by_query(material=material, grit=grit, weight=weight, distance=distance,
                                input_dir=input_dir, output_dir=output_dir, workers=workers)
    except Exception as e:
        typer.echo(f"Error during extraction: {str(e)}")
        exit(1)

if __name__ == "__main__":
    app()
