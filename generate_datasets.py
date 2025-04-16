#!/usr/bin/env python3
import os
import io
import json
import tarfile
import concurrent.futures
import base64
import logging
import pandas as pd
from PIL import Image
from rich.progress import Progress, TextColumn, BarColumn, TimeElapsedColumn, TimeRemainingColumn
from tqdm import tqdm
import typer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

app = typer.Typer(help="Friction Surfaces Dataset Pipeline: Convert to Parquet, create WebDataset shards, or extract images.")

#####################################
# HELPER FUNCTIONS                  #
#####################################

def get_wear_dir(base_dir: str) -> str:
    """Return the correct Wear directory given a base directory.
    If the base directory’s name is already 'Wear' (case insensitive), return it;
    otherwise, append 'Wear'."""
    if os.path.basename(os.path.normpath(base_dir)).lower() == "wear":
        logger.info("Input directory is already 'Wear'.")
        return base_dir
    else:
        wear_dir = os.path.join(base_dir, "Wear")
        logger.info(f"Appending 'Wear' to base directory, using: {wear_dir}")
        return wear_dir

#####################################
# PARQUET CONVERSION BLOCK          #
#####################################

def process_distance_folder(material, grit, weight, distance, base_dir, output_dir):
    """
    Process a single distance folder and create a Parquet file.
    
    Reads each PNG file from the folder, converts it to a compressed JPEG,
    and stores the raw binary data (as bytes) along with metadata.
    The original image path is not saved.
    """
    wear_dir = get_wear_dir(base_dir)
    distance_path = os.path.join(wear_dir, material, grit, weight, distance)
    weight_value = weight.replace('g', '')
    distance_value = distance.replace('mm', '')

    if not os.path.isdir(distance_path):
        logger.warning(f"Folder not found: {distance_path}")
        return None

    image_files = [f for f in os.listdir(distance_path) if f.endswith('.png')]
    if not image_files:
        logger.warning(f"No PNG files in {distance_path}")
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
            logger.debug(f"Processed image {image_path} successfully.")
        except Exception as e:
            logger.error(f"Error processing {image_path}: {str(e)}")
    
    if not records:
        return None

    parquet_dir = os.path.join(output_dir, material, grit, weight)
    os.makedirs(parquet_dir, exist_ok=True)
    df = pd.DataFrame(records)
    parquet_file = os.path.join(parquet_dir, f"{distance}.parquet")
    df.to_parquet(parquet_file, compression='snappy', engine='pyarrow')
    logger.info(f"Created Parquet file: {parquet_file}")
    return parquet_file

def find_all_folders(base_dir):
    """
    Find all material/grit/weight/distance folder combinations from the dataset.
    """
    wear_dir = get_wear_dir(base_dir)
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
                weight_path = os.path.join(grit_path, weight)
                if not os.path.isdir(weight_path):
                    continue
                for distance in os.listdir(weight_path):
                    distance_path = os.path.join(weight_path, distance)
                    if os.path.isdir(distance_path):
                        folders.append((material, grit, weight, distance))
    logger.info(f"Found {len(folders)} folders for processing.")
    return folders

def convert_to_parquet_per_folder(base_dir=".", output_dir="parquet_data", workers=8):
    """
    Convert the Friction Surfaces dataset into Parquet files (one per folder).
    """
    os.makedirs(output_dir, exist_ok=True)
    logger.info("Scanning directory structure for Parquet conversion...")
    all_folders = find_all_folders(base_dir)
    total_folders = len(all_folders)
    logger.info(f"Found {total_folders} folders to process.")

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
                        logger.info(f"Successfully processed folder: {folder_desc}")
                except Exception as exc:
                    logger.error(f"Folder {folder} generated an exception: {exc}")
                progress.update(task, advance=1)
    logger.info(f"Conversion complete. {successful}/{total_folders} folders successfully converted.")
    logger.info(f"Parquet files are stored in: {output_dir}")
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
    logger.info(f"Metadata file created: {metadata_file} (contains {len(df)} files)")
    return metadata_file

#####################################
# WEBDATASET SHARD CREATION BLOCK  #
#####################################

def process_image_record(record):
    """
    Given a record with keys:
      'material', 'grit', 'weight', 'distance', 'image_type', 'image_path',
    open the image, convert to compressed JPEG, and add 'image_data'.
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
        logger.debug(f"Processed record for image type {new_record.get('image_type')}")
        return new_record
    except Exception as e:
        logger.error(f"Error processing {image_path}: {str(e)}")
        return None

def create_shard(shard_path, records):
    """
    Create a WebDataset shard (tar archive) from a list of records.
    Each record is stored as a JPEG file and a corresponding JSON file.
    """
    try:
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
        logger.info(f"Created shard: {shard_path}")
    except Exception as e:
        logger.error(f"Error creating shard {shard_path}: {str(e)}")

def create_shards_from_records(records, shard_size, output_dir, workers=8):
    """
    Split the list of records into shards of shard_size and create tar archives.
    """
    os.makedirs(output_dir, exist_ok=True)
    num_shards = (len(records) + shard_size - 1) // shard_size
    logger.info(f"Creating {num_shards} shards (each with up to {shard_size} samples)...")
    for i in range(num_shards):
        shard_records = records[i * shard_size:(i + 1) * shard_size]
        shard_name = f"shard-{i:04d}.tar"
        shard_path = os.path.join(output_dir, shard_name)
        create_shard(shard_path, shard_records)

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
                weight_path = os.path.join(grit_path, weight)
                if not os.path.isdir(weight_path):
                    continue
                for distance in os.listdir(weight_path):
                    distance_path = os.path.join(weight_path, distance)
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
    logger.info(f"Collected {len(records)} records for shard creation.")
    return records

def create_webdataset_shards(input_dir, output_dir, shard_size, workers=8):
    """
    Create WebDataset shards from the dataset directory.
    Uses threading to process image records concurrently.
    """
    raw_records = collect_records_for_shards(input_dir)
    logger.info(f"Found {len(raw_records)} raw records for WebDataset creation.")
    processed_records = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(process_image_record, rec) for rec in raw_records]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result is not None:
                processed_records.append(result)
    logger.info(f"Processed {len(processed_records)} records successfully for WebDataset shards.")
    create_shards_from_records(processed_records, shard_size, output_dir, workers)

#####################################
# TYPER COMMANDS                    #
#####################################

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

if __name__ == "__main__":
    app()
