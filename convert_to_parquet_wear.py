import os
import pandas as pd
import numpy as np
from PIL import Image
import io
import shutil
from rich.progress import Progress, TextColumn, BarColumn, TimeElapsedColumn, TimeRemainingColumn
import argparse
import concurrent.futures

def extract_images(df, output_dir, base_dir=".", max_workers=8):
    """
    Extract images from dataframe to directory structure.
    
    Args:
        df: DataFrame containing image information
        output_dir: Base output directory
        base_dir: Base directory containing original images
        max_workers: Maximum number of concurrent workers
    """
    if df is None or df.empty:
        print("No data to extract images from.")
        return
    
    os.makedirs(output_dir, exist_ok=True)
    
    # Group by material/grit/weight/distance
    grouped = df.groupby(['material', 'grit', 'weight', 'distance'])
    
    total_groups = len(grouped)
    total_images = len(df)
    
    with Progress(
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
    ) as progress:
        extract_task = progress.add_task(f"Extracting {total_images} images...", total=total_images)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            
            for (material, grit, weight, distance), group in grouped:
                # Create destination directory
                dest_dir = os.path.join(output_dir, material, grit, f"{weight}g", f"{distance}mm")
                os.makedirs(dest_dir, exist_ok=True)
                
                # Process each image in the group
                for _, row in group.iterrows():
                    image_type = row['image_type']
                    image_path = row['image_path']
                    dest_file = os.path.join(dest_dir, f"{image_type}.png")
                    
                    # Submit copy job to executor
                    futures.append(
                        executor.submit(
                            copy_image, 
                            row, 
                            image_path, 
                            dest_file, 
                            base_dir
                        )
                    )
            
            # Process results as they complete
            for future in concurrent.futures.as_completed(futures):
                future.result()  # Get result to catch any exceptions
                progress.update(extract_task, advance=1)
    
    print(f"Extracted {total_images} images to {output_dir}")

def copy_image(row, src_path, dest_path, base_dir):
    """Copy a single image from source to destination."""
    try:
        # If we have image_data in the dataframe, use it
        if 'image_data' in row and row['image_data'] is not None:
            img = Image.open(io.BytesIO(row['image_data']))
            img.save(dest_path)
        else:
            # Otherwise copy from the original path
            full_src_path = os.path.join(base_dir, src_path)
            if os.path.exists(full_src_path):
                shutil.copy2(full_src_path, dest_path)
            else:
                print(f"Warning: Source image not found: {full_src_path}")
        return True
    except Exception as e:
        print(f"Error copying image {src_path} to {dest_path}: {str(e)}")
        return False

def extract_images_by_query(material=None, grit=None, weight=None, distance=None, 
                           input_dir="parquet_data", output_dir="extracted_images", 
                           base_dir=".", max_workers=8):
    """Extract images based on query parameters."""
    # Read metadata
    metadata = read_parquet_metadata(input_dir)
    if metadata is None:
        print(f"No metadata found in {input_dir}")
        return
    
    # Build query
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
    
    # Get filtered files
    if query:
        filtered_files = metadata.query(query)
    else:
        filtered_files = metadata
    
    if filtered_files.empty:
        print(f"No files match the query: {query}")
        return
    
    print(f"Found {len(filtered_files)} matching folders")
    
    # Read and process all matching parquet files
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
                df = pd.read_parquet(parquet_path)
                all_data.append(df)
            except Exception as e:
                print(f"Error reading {parquet_path}: {str(e)}")
            
            progress.update(read_task, advance=1)
    
    if not all_data:
        print("No data could be read from parquet files")
        return
    
    # Combine all dataframes
    combined_df = pd.concat(all_data)
    print(f"Total images to extract: {len(combined_df)}")
    
    # Extract images
    extract_images(combined_df, output_dir, base_dir, max_workers)

# Re-use read_parquet_metadata from the provided code
def read_parquet_metadata(parquet_dir="parquet_data"):
    """Read the metadata parquet file."""
    metadata_file = os.path.join(parquet_dir, "metadata.parquet")
    if not os.path.exists(metadata_file):
        print(f"Metadata file not found: {metadata_file}")
        return None
    
    return pd.read_parquet(metadata_file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Extract images from friction surfaces parquet files')
    parser.add_argument('--input-dir', type=str, default='parquet_data', help='Input parquet directory')
    parser.add_argument('--output-dir', type=str, default='extracted_images', help='Output directory for images')
    parser.add_argument('--base-dir', type=str, default='.', help='Base directory for resolving image paths')
    parser.add_argument('--material', type=str, help='Filter by material')
    parser.add_argument('--grit', type=str, help='Filter by grit')
    parser.add_argument('--weight', type=str, help='Filter by weight')
    parser.add_argument('--distance', type=str, help='Filter by distance')
    parser.add_argument('--workers', type=int, default=8, help='Maximum concurrent workers')
    
    args = parser.parse_args()
    
    extract_images_by_query(
        material=args.material,
        grit=args.grit,
        weight=args.weight,
        distance=args.distance,
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        base_dir=args.base_dir,
        max_workers=args.workers
    )
