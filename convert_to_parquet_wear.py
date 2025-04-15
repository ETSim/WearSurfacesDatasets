import os
import pandas as pd
import numpy as np
from PIL import Image
import io
from rich.progress import Progress, TextColumn, BarColumn, TimeElapsedColumn, TimeRemainingColumn
import concurrent.futures
import argparse

def process_distance_folder(material, grit, weight, distance, base_path, output_dir):
    """Process a single distance folder and create a parquet file for it."""
    distance_path = os.path.join(base_path, "Wear", material, grit, weight, distance)
    weight_value = weight.replace('g', '')
    distance_value = distance.replace('mm', '')
    
    # Skip if not a directory
    if not os.path.isdir(distance_path):
        return None
    
    # Get all PNG files
    image_files = [f for f in os.listdir(distance_path) if f.endswith('.png')]
    if not image_files:
        return None
    
    # Process all images in this folder
    records = []
    for image_file in image_files:
        image_path = os.path.join(distance_path, image_file)
        image_type = os.path.splitext(image_file)[0]
        
        try:
            with Image.open(image_path) as img:
                # Just get metadata, don't store actual image data
                width, height = img.size
                mode = img.mode
            
            # Create record with path instead of binary data
            record = {
                'material': material,
                'grit': grit,
                'weight': weight_value,
                'distance': distance_value,
                'image_type': image_type,
                'image_width': width,
                'image_height': height,
                'image_mode': mode,
                'image_path': os.path.relpath(image_path, base_path)
            }
            records.append(record)
        except Exception as e:
            print(f"Error processing {image_path}: {str(e)}")
    
    if not records:
        return None
    
    # Create output directory if it doesn't exist
    parquet_dir = os.path.join(output_dir, material, grit, weight)
    os.makedirs(parquet_dir, exist_ok=True)
    
    # Create and save DataFrame
    df = pd.DataFrame(records)
    parquet_file = os.path.join(parquet_dir, f"{distance}.parquet")
    df.to_parquet(parquet_file, compression='snappy')
    
    return parquet_file

def find_all_folders(base_path):
    """Find all material/grit/weight/distance folder combinations."""
    wear_dir = os.path.join(base_path, "Wear")
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
    Convert the friction surfaces dataset to multiple parquet files (one per folder).
    
    Args:
        base_dir: Base directory containing the dataset
        output_dir: Output directory for parquet files
        workers: Number of worker threads to use
    """
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Find all folders to process
    print("Scanning directory structure...")
    all_folders = find_all_folders(base_dir)
    
    total_folders = len(all_folders)
    print(f"Found {total_folders} folders to process")
    
    # Process folders in parallel
    completed = 0
    successful = 0
    
    with Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    ) as progress:
        task = progress.add_task("Converting folders to parquet...", total=total_folders)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            # Submit all processing tasks
            future_to_folder = {
                executor.submit(
                    process_distance_folder, material, grit, weight, distance, base_dir, output_dir
                ): (material, grit, weight, distance)
                for material, grit, weight, distance in all_folders
            }
            
            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_folder):
                folder = future_to_folder[future]
                material, grit, weight, distance = folder
                
                try:
                    result = future.result()
                    if result:
                        successful += 1
                        folder_desc = f"{material}/{grit}/{weight}/{distance}"
                        progress.update(task, description=f"Processed {folder_desc}")
                except Exception as exc:
                    print(f"Folder {folder} generated an exception: {exc}")
                
                completed += 1
                progress.update(task, completed=completed)
    
    print(f"Conversion complete. {successful}/{total_folders} folders successfully converted to parquet.")
    print(f"Parquet files are stored in: {output_dir}")
    
    return output_dir

def create_metadata_parquet(base_dir=".", output_dir="parquet_data"):
    """Create a metadata parquet file with folder information."""
    # Find all parquet files
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
    
    # Create metadata DataFrame
    df = pd.DataFrame(parquet_files)
    metadata_file = os.path.join(output_dir, "metadata.parquet")
    df.to_parquet(metadata_file, compression='snappy')
    
    print(f"Metadata file created: {metadata_file}")
    print(f"Contains information about {len(df)} parquet files")
    
    return metadata_file

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert friction surfaces dataset to parquet files (one per folder)')
    parser.add_argument('--workers', type=int, default=8, help='Number of worker threads')
    parser.add_argument('--output', type=str, default='parquet_data', help='Output directory')
    parser.add_argument('--input', type=str, default='.', help='Input directory path')
    
    args = parser.parse_args()
    
    # Convert to parquet
    output_dir = convert_to_parquet_per_folder(
        base_dir=args.input,
        output_dir=args.output,
        workers=args.workers
    )
    
    # Create metadata
    create_metadata_parquet(args.input, output_dir)