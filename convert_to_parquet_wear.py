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
import queue
import concurrent.futures
from threading import Lock
import io

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

def create_directory_parquet_datasets(root_dir, output_dir, max_workers=None):
    """
    Create a separate Parquet file for each directory, with each file containing all images from that directory.
    Uses threading for improved performance.
    
    Parameters:
    -----------
    root_dir : str
        Root directory containing the image dataset
    output_dir : str
        Directory to store the output Parquet files
    max_workers : int, optional
        Maximum number of worker threads. If None, uses default based on system.
    """
    start_time = time.time()
    thread_safe_log('info', f"Starting directory-based Parquet conversion at {datetime.datetime.now()}")
    
    # If max_workers not specified, use a reasonable default (number of CPUs + 4)
    if max_workers is None:
        import multiprocessing
        max_workers = multiprocessing.cpu_count() + 4
    
    thread_safe_log('info', f"Using up to {max_workers} worker threads")
    
    # Regular expressions to extract metadata from directory names
    distance_pattern = re.compile(r'(\d+)mm')
    load_pattern = re.compile(r'(\d+)g')
    force_pattern = re.compile(r'_(\d+)_')  # For load values like 60, 120, 240
    trial_pattern = re.compile(r'_(\d+)_heightmap')  # For trial numbers in Cartboard
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Master metadata to track all directories
    master_metadata = []
    master_metadata_lock = Lock()
    
    # Statistics tracking with thread safety
    materials_lock = Lock()
    distances_lock = Lock()
    forces_lock = Lock()
    materials_found = set()
    distances_found = set()
    forces_found = set()
    
    # Gather all directories to process
    dirs_to_process = []
    
    # Traverse the directory structure
    wear_path = os.path.join(root_dir, 'Wear')
    for material_dir in os.listdir(wear_path):
        # Skip non-directories and hidden files
        material_path = os.path.join(wear_path, material_dir)
        if not os.path.isdir(material_path) or material_dir.startswith('.'):
            continue
            
        with materials_lock:
            materials_found.add(material_dir)
        
        thread_safe_log('info', f"Finding directories for material: {material_dir}")
        
        # Process material directory based on its structure
        if material_dir == 'Cartboard':
            # Handle Cartboard with numbered trials
            for trial_dir in os.listdir(material_path):
                if trial_dir.isdigit():
                    trial_num = int(trial_dir)
                    trial_path = os.path.join(material_path, trial_dir)
                    
                    # Process each distance directory within the trial
                    for distance_dir in os.listdir(trial_path):
                        distance_path = os.path.join(trial_path, distance_dir)
                        if not os.path.isdir(distance_path):
                            continue
                        
                        # Extract metadata from directory name
                        distance_match = distance_pattern.search(distance_dir)
                        force_match = force_pattern.search(distance_dir)
                        
                        distance_mm = int(distance_match.group(1)) if distance_match else -1
                        force_value = int(force_match.group(1)) if force_match else -1
                        
                        with distances_lock:
                            distances_found.add(distance_mm)
                        with forces_lock:
                            forces_found.add(force_value)
                        
                        # Add this directory to the processing list
                        dirs_to_process.append({
                            'path': distance_path,
                            'material': material_dir,
                            'distance_mm': distance_mm,
                            'force_value': force_value,
                            'load_g': 0,
                            'trial_number': trial_num,
                            'directory_name': distance_dir,
                            'parquet_name': f"{material_dir}_{distance_mm}mm_force{force_value}_trial{trial_num}.parquet"
                        })
        else:
            # Handle MDF and PLA directories
            for exp_dir in os.listdir(material_path):
                exp_path = os.path.join(material_path, exp_dir)
                if not os.path.isdir(exp_path):
                    continue
                    
                # Extract metadata from directory name
                distance_match = distance_pattern.search(exp_dir)
                load_match = load_pattern.search(exp_dir)
                force_match = force_pattern.search(exp_dir)
                
                distance_mm = int(distance_match.group(1)) if distance_match else -1
                load_value = int(load_match.group(1)) if load_match else -1
                force_value = int(force_match.group(1)) if force_match else -1
                
                # Special case for PLA: extract the force value
                if material_dir == 'PLA' and force_value == -1:
                    # Try to find force values like "60", "120", "240" in the directory name
                    force_parts = [part for part in exp_dir.split('_') if part.isdigit() and part not in ['0', '200', '400', '600', '800', '1000', '1200', '1400']]
                    if force_parts:
                        force_value = int(force_parts[0])
                
                with distances_lock:
                    distances_found.add(distance_mm)
                if force_value != -1:
                    with forces_lock:
                        forces_found.add(force_value)
                
                # Create a parquet file name based on the directory
                if load_value > 0:
                    parquet_name = f"{material_dir}_{distance_mm}mm_load{load_value}g_force{force_value}.parquet"
                else:
                    parquet_name = f"{material_dir}_{distance_mm}mm_force{force_value}.parquet"
                
                # Add this directory to the processing list
                dirs_to_process.append({
                    'path': exp_path,
                    'material': material_dir,
                    'distance_mm': distance_mm,
                    'force_value': force_value,
                    'load_g': load_value,
                    'trial_number': -1,
                    'directory_name': exp_dir,
                    'parquet_name': parquet_name
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
                os.path.join(output_dir, dir_info['parquet_name']),
                dir_info['material'],
                dir_info['distance_mm'],
                dir_info['force_value'],
                dir_info['load_g'],
                dir_info['trial_number'],
                dir_info['directory_name'],
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
                thread_safe_log('info', f"Completed directory {dir_info['directory_name']} with {num_processed} images, Parquet size: {parquet_size:.2f} MB")
            except Exception as e:
                thread_safe_log('error', f"Error processing {dir_info['directory_name']}: {e}")
    
    # Create master metadata file
    thread_safe_log('info', f"Creating master metadata file with {len(master_metadata)} directory records")
    master_df = pd.DataFrame(master_metadata)
    
    # Add dataset-level metadata
    master_df['dataset_info'] = 'Friction Surfaces Dataset'
    master_df['creation_date'] = datetime.datetime.now().isoformat()
    master_df['total_images'] = total_images
    
    # Optimize categorical columns
    for col in ['material', 'directory_name', 'parquet_file']:
        if col in master_df.columns:
            master_df[col] = master_df[col].astype('category')
    
    # Write to parquet with compression
    master_file = os.path.join(output_dir, "master_metadata.parquet")
    thread_safe_log('info', f"Writing master metadata to {master_file}")
    table = pa.Table.from_pandas(master_df)
    pq.write_table(table, master_file, compression='snappy')
    
    elapsed_time = time.time() - start_time
    thread_safe_log('info', f"Parquet dataset creation completed successfully")
    thread_safe_log('info', f"Total processing time: {elapsed_time:.2f} seconds")
    thread_safe_log('info', f"Total images processed: {total_images}")
    thread_safe_log('info', f"Materials: {', '.join(materials_found)}")
    thread_safe_log('info', f"Distance values (mm): {sorted(distances_found)}")
    thread_safe_log('info', f"Force values: {sorted(forces_found)}")
    thread_safe_log('info', f"Master metadata file size: {os.path.getsize(master_file) / (1024*1024):.2f} MB")


def process_directory_to_parquet(directory_path, output_file, material, distance_mm, force_value, 
                               load_g, trial_number, directory_name, master_metadata, master_metadata_lock,
                               total_images_lock):
    """
    Process a directory of images and save all images to a single Parquet file.
    
    Parameters:
    -----------
    directory_path : str
        Path to the directory containing images
    output_file : str
        Path to the output Parquet file
    material : str
        Material name (MDF, PLA, Cartboard)
    distance_mm : int
        Distance value in mm
    force_value : int
        Force value
    load_g : int
        Load value in grams
    trial_number : int
        Trial number (-1 if not applicable)
    directory_name : str
        Original directory name
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
    thread_safe_log('info', f"Thread {thread_name} processing directory: {directory_name}")
    dir_start_time = time.time()
    
    try:
        # Collect all image data and metadata
        all_images_data = []
        all_image_metadata = []
        map_types_in_dir = []
        
        # Find all PNG files in the directory
        png_files = [f for f in os.listdir(directory_path) if f.lower().endswith('.png')]
        
        for img_file in png_files:
            # Extract map type (ao, bump, height, etc.)
            map_type = img_file.split('_')[-1].split('.')[0]
            map_types_in_dir.append(map_type)
            
            # Generate a unique ID for this image
            image_id = f"{material}_{distance_mm}mm_{force_value}_{trial_number}_{map_type}"
            
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
                    'material': material,
                    'distance_mm': distance_mm,
                    'force_value': force_value,
                    'load_g': load_g,
                    'trial_number': trial_number,
                    'map_type': map_type,
                    'directory_name': directory_name,
                    'filename': img_file,
                    'width': width,
                    'height': height,
                    'channels': channels
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
                master_metadata.append({
                    'material': material,
                    'distance_mm': distance_mm,
                    'force_value': force_value,
                    'load_g': load_g,
                    'trial_number': trial_number,
                    'directory_name': directory_name,
                    'parquet_file': os.path.basename(output_file),
                    'num_images': len(all_images_data),
                    'map_types': ', '.join(sorted(set(map_types_in_dir))),
                    'width': width,
                    'height': height,
                    'file_size_mb': file_size_mb
                })
            
            thread_safe_log('info', f"Thread {thread_name} finished {directory_name} in {dir_elapsed:.2f}s with {len(all_images_data)} images")
            return len(all_images_data), file_size_mb
        else:
            thread_safe_log('warning', f"Thread {thread_name} found no valid images in {directory_name}")
            return 0, 0
        
    except Exception as e:
        thread_safe_log('error', f"Thread {thread_name} encountered error in {directory_name}: {e}")
        return 0, 0


def read_images_from_parquet(parquet_file):
    """
    Read images from a Parquet file.
    
    Parameters:
    -----------
    parquet_file : str
        Path to the Parquet file
    
    Returns:
    --------
    dict
        Dictionary mapping image_id to PIL Image
    """
    # Read the Parquet file
    df = pd.read_parquet(parquet_file)
    
    # Dictionary to hold all images
    images = {}
    
    # Process each row
    for _, row in df.iterrows():
        try:
            # Convert bytes back to image
            img_bytes = row['image_bytes']
            img = Image.open(io.BytesIO(img_bytes))
            
            # Store in dictionary with image_id as key
            images[row['image_id']] = img
        except Exception as e:
            logger.error(f"Error reading image {row['image_id']}: {e}")
    
    return images


if __name__ == "__main__":
    # Update these paths for your specific setup
    ROOT_DIR = "."  # Current directory, change as needed
    OUTPUT_DIR = "parquet_by_directory"
    
    # Use 16 worker threads - adjust based on your system
    MAX_WORKERS = 16
    
    create_directory_parquet_datasets(ROOT_DIR, OUTPUT_DIR, MAX_WORKERS)