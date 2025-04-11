import os
import re
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from PIL import Image
import time
import datetime
import logging
import threading
import concurrent.futures
from threading import Lock
import io
import shutil

# Set up logging with thread safety
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s',
    handlers=[
        logging.FileHandler('parquet_conversion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Create a lock for thread-safe logging
log_lock = Lock()

def thread_safe_log(level, message):
    """Thread-safe logging function"""
    with log_lock:
        if level == 'info':
            logger.info(message)
        elif level == 'error':
            logger.error(message)
        elif level == 'warning':
            logger.warning(message)

def create_parquet_dataset(root_dir, output_base_dir, max_workers=None):
    """
    Create Parquet files for each test condition in the dataset.
    
    Parameters:
    -----------
    root_dir : str
        Root directory containing the image dataset
    output_base_dir : str
        Base directory to store the output Parquet structure
    max_workers : int, optional
        Maximum number of worker threads. If None, uses default based on system.
    """
    start_time = time.time()
    thread_safe_log('info', f"Starting Parquet conversion at {datetime.datetime.now()}")
    
    # If max_workers not specified, use a reasonable default (number of CPUs + 4)
    if max_workers is None:
        import multiprocessing
        max_workers = multiprocessing.cpu_count() + 4
    
    thread_safe_log('info', f"Using up to {max_workers} worker threads")
    
    # Create output base directory if it doesn't exist
    os.makedirs(output_base_dir, exist_ok=True)
    
    # Create Wear folder structure in output directory
    wear_output_dir = os.path.join(output_base_dir, "Wear")
    os.makedirs(wear_output_dir, exist_ok=True)
    
    # Master metadata to track all directories
    master_metadata = []
    master_metadata_lock = Lock()
    
    # Statistics tracking with thread safety
    materials_lock = Lock()
    surfaces_lock = Lock()
    loads_lock = Lock()
    distances_lock = Lock()
    materials_found = set()
    surfaces_found = set()
    loads_found = set()
    distances_found = set()
    
    # Gather all directories to process
    dirs_to_process = []
    
    # Extract S-value (grit) pattern
    grit_pattern = re.compile(r'S(\d+)')
    
    # Extract load pattern
    load_pattern = re.compile(r'(\d+)g')
    
    # Traverse the directory structure
    wear_path = os.path.join(root_dir, 'Wear')
    for material_dir in os.listdir(wear_path):
        # Skip non-directories and hidden files
        material_path = os.path.join(wear_path, material_dir)
        if not os.path.isdir(material_path) or material_dir.startswith('.'):
            continue
            
        with materials_lock:
            materials_found.add(material_dir)
        
        # Create material directory in output
        material_output_dir = os.path.join(wear_output_dir, material_dir)
        os.makedirs(material_output_dir, exist_ok=True)
        
        thread_safe_log('info', f"Finding directories for material: {material_dir}")
        
        # Process surface type directories (e.g., S40, Linear)
        for surface_dir in os.listdir(material_path):
            surface_path = os.path.join(material_path, surface_dir)
            if not os.path.isdir(surface_path):
                continue
                
            # Create surface directory in output
            surface_output_dir = os.path.join(material_output_dir, surface_dir)
            os.makedirs(surface_output_dir, exist_ok=True)
            
            # Extract surface properties (grit value)
            grit_value = -1
            grit_match = grit_pattern.search(surface_dir)
            if grit_match:
                grit_value = int(grit_match.group(1))
            
            with surfaces_lock:
                surfaces_found.add(surface_dir)
            
            # Process load directories
            for load_dir in os.listdir(surface_path):
                load_path = os.path.join(surface_path, load_dir)
                if not os.path.isdir(load_path):
                    continue
                
                # Create load directory in output
                load_output_dir = os.path.join(surface_output_dir, load_dir)
                os.makedirs(load_output_dir, exist_ok=True)
                
                # Extract load value
                load_value = -1
                load_match = load_pattern.search(load_dir)
                if load_match:
                    load_value = int(load_match.group(1))
                
                with loads_lock:
                    loads_found.add(load_dir)
                
                # Process distance directories
                for distance_dir in os.listdir(load_path):
                    distance_path = os.path.join(load_path, distance_dir)
                    if not os.path.isdir(distance_path):
                        continue
                    
                    # Extract distance value
                    distance_value = -1
                    if distance_dir.endswith('mm'):
                        try:
                            distance_value = int(distance_dir[:-2])  # Remove 'mm' and convert to int
                        except ValueError:
                            pass
                    
                    with distances_lock:
                        distances_found.add(distance_dir)
                    
                    # Create the output Parquet filename
                    parquet_filename = f"{material_dir}_{surface_dir}_{load_dir}_{distance_dir}.parquet"
                    parquet_path = os.path.join(load_output_dir, parquet_filename)
                    
                    # Add this directory to the processing list
                    dirs_to_process.append({
                        'path': distance_path,
                        'material': material_dir,
                        'surface': surface_dir,
                        'grit': grit_value,
                        'load_dir': load_dir,
                        'load_g': load_value,
                        'distance_dir': distance_dir,
                        'distance_mm': distance_value,
                        'parquet_path': parquet_path,
                        'relative_path': os.path.join(material_dir, surface_dir, load_dir, distance_dir)
                    })
    
    # Process directories using a thread pool
    thread_safe_log('info', f"Starting processing of {len(dirs_to_process)} directories using ThreadPoolExecutor")
    
    total_images = 0
    total_images_lock = Lock()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all processing tasks
        futures = {
            executor.submit(
                process_directory_to_parquet,
                dir_info['path'],
                dir_info['parquet_path'],
                dir_info,
                master_metadata,
                master_metadata_lock,
                total_images_lock
            ): dir_info for dir_info in dirs_to_process
        }
        
        # Process completed futures as they finish
        for future in concurrent.futures.as_completed(futures):
            dir_info = futures[future]
            try:
                # Each completed future returns the number of images processed
                num_processed, parquet_size = future.result()
                with total_images_lock:
                    total_images += num_processed
                thread_safe_log('info', f"Completed directory {dir_info['relative_path']} with {num_processed} images, Parquet size: {parquet_size:.2f} MB")
            except Exception as e:
                thread_safe_log('error', f"Error processing {dir_info['relative_path']}: {e}")
    
    # Create master metadata file in root output directory
    thread_safe_log('info', f"Creating master metadata file with {len(master_metadata)} directory records")
    master_df = pd.DataFrame(master_metadata)
    
    # Add dataset-level metadata
    master_df['dataset_info'] = 'Friction Surfaces Dataset'
    master_df['creation_date'] = datetime.datetime.now().isoformat()
    
    # Optimize categorical columns
    for col in ['material', 'surface', 'load_dir', 'distance_dir', 'map_type']:
        if col in master_df.columns:
            master_df[col] = master_df[col].astype('category')
    
    # Write to parquet with compression
    master_file = os.path.join(output_base_dir, "master_metadata.parquet")
    thread_safe_log('info', f"Writing master metadata to {master_file}")
    table = pa.Table.from_pandas(master_df)
    pq.write_table(table, master_file, compression='snappy')
    
    # Copy README and LICENSE if they exist
    for file_name in ['README.md', 'LICENSE']:
        source_file = os.path.join(root_dir, file_name)
        if os.path.exists(source_file):
            dest_file = os.path.join(output_base_dir, file_name)
            shutil.copy2(source_file, dest_file)
            thread_safe_log('info', f"Copied {file_name} to output directory")
    
    elapsed_time = time.time() - start_time
    thread_safe_log('info', f"Parquet dataset creation completed successfully")
    thread_safe_log('info', f"Total processing time: {elapsed_time:.2f} seconds")
    thread_safe_log('info', f"Total images processed: {total_images}")
    thread_safe_log('info', f"Materials: {', '.join(materials_found)}")
    thread_safe_log('info', f"Surfaces: {', '.join(surfaces_found)}")
    thread_safe_log('info', f"Loads: {', '.join(sorted(loads_found, key=lambda x: int(x[:-1]) if x[:-1].isdigit() else 0))}")
    thread_safe_log('info', f"Distances: {', '.join(sorted(distances_found, key=lambda x: int(x[:-2]) if x[:-2].isdigit() else 0))}")
    thread_safe_log('info', f"Master metadata file size: {os.path.getsize(master_file) / (1024*1024):.2f} MB")


def process_directory_to_parquet(directory_path, output_file, metadata, 
                              master_metadata, master_metadata_lock, total_images_lock):
    """
    Process a directory of images and save all images to a single Parquet file.
    
    Parameters:
    -----------
    directory_path : str
        Path to the directory containing images
    output_file : str
        Path to the output Parquet file
    metadata : dict
        Dictionary containing metadata about this directory
    master_metadata : list
        List to store metadata about this directory
    master_metadata_lock : threading.Lock
        Lock for thread-safe metadata updates
    total_images_lock : threading.Lock
        Lock for thread-safe image counting
    
    Returns:
    --------
    tuple
        (Number of images processed, Parquet file size in MB)
    """
    thread_name = threading.current_thread().name
    relative_path = metadata['relative_path']
    thread_safe_log('info', f"Thread {thread_name} processing directory: {relative_path}")
    dir_start_time = time.time()
    
    try:
        # Create output directory path if it doesn't exist
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Collect all image data and metadata
        all_images_data = []
        all_image_metadata = []
        map_types_in_dir = []
        
        # Find all PNG files in the directory
        png_files = [f for f in os.listdir(directory_path) if f.lower().endswith('.png')]
        
        for img_file in png_files:
            # Try to extract map type from filename
            map_type = None
            if "_" in img_file:
                # If file has format like 'something_maptype.png'
                map_type = img_file.split('_')[-1].split('.')[0]
            else:
                # If file is just named like 'maptype.png'
                map_type = img_file.split('.')[0]
            
            map_types_in_dir.append(map_type)
            
            # Generate a unique ID for this image
            image_id = f"{metadata['material']}_{metadata['surface']}_{metadata['load_dir']}_{metadata['distance_dir']}_{map_type}"
            
            # Load image to extract data and dimensions
            try:
                img_path = os.path.join(directory_path, img_file)
                img = Image.open(img_path)
                width, height = img.size
                
                # Determine number of channels
                if img.mode == 'RGB':
                    channels = 3
                elif img.mode == 'RGBA':
                    channels = 4
                elif img.mode in ['L', '1']:
                    channels = 1
                else:
                    channels = -1  # Unknown
                
                # Convert image to bytes
                img_buffer = io.BytesIO()
                img.save(img_buffer, format='PNG')
                img_bytes = img_buffer.getvalue()
                
                # Store image data
                image_data = {
                    'image_id': image_id,
                    'map_type': map_type,
                    'width': width,
                    'height': height,
                    'channels': channels,
                    'image_bytes': img_bytes
                }
                all_images_data.append(image_data)
                
                # Store metadata
                image_meta = {
                    'image_id': image_id,
                    'material': metadata['material'],
                    'surface': metadata['surface'],
                    'grit': metadata['grit'],
                    'load_dir': metadata['load_dir'],
                    'load_g': metadata['load_g'],
                    'distance_dir': metadata['distance_dir'],
                    'distance_mm': metadata['distance_mm'],
                    'map_type': map_type,
                    'filename': img_file,
                    'width': width,
                    'height': height,
                    'channels': channels,
                    'parquet_path': os.path.relpath(output_file, output_file.split('Wear')[0])
                }
                all_image_metadata.append(image_meta)
                
            except Exception as e:
                thread_safe_log('error', f"Thread {thread_name} error processing {img_file}: {e}")
        
        # Create DataFrame and save to Parquet
        if all_images_data:
            # Create DataFrame with all image data
            df = pd.DataFrame(all_images_data)
            
            # Save as Parquet
            table = pa.Table.from_pandas(df)
            pq.write_table(table, output_file, compression='snappy')
            
            # Log completion
            file_size_mb = os.path.getsize(output_file) / (1024 * 1024)
            dir_elapsed = time.time() - dir_start_time
            
            # Add directory info to master metadata
            with master_metadata_lock:
                master_metadata.extend(all_image_metadata)
            
            thread_safe_log('info', f"Thread {thread_name} finished {relative_path} in {dir_elapsed:.2f}s with {len(all_images_data)} images")
            return len(all_images_data), file_size_mb
        else:
            thread_safe_log('warning', f"Thread {thread_name} found no valid images in {relative_path}")
            return 0, 0
        
    except Exception as e:
        thread_safe_log('error', f"Thread {thread_name} encountered error in {relative_path}: {e}")
        return 0, 0


if __name__ == "__main__":
    # Update these paths for your specific setup
    ROOT_DIR = "."  # Current directory, change as needed
    OUTPUT_DIR = "FrictionSurfacesDatasets_Parquet"
    
    # Use 16 worker threads - adjust based on your system
    MAX_WORKERS = 16
    
    create_parquet_dataset(ROOT_DIR, OUTPUT_DIR, MAX_WORKERS)
