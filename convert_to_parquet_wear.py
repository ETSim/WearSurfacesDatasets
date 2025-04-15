import os
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
import json
from tqdm import tqdm

# Set up logging with thread safety
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s',
    handlers=[
        logging.FileHandler('image_extraction.log'),
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

def load_master_metadata(parquet_dir):
    """
    Load and return master metadata from parquet file
    """
    master_metadata_path = os.path.join(parquet_dir, "master_metadata.parquet")
    if not os.path.exists(master_metadata_path):
        thread_safe_log('error', f"Master metadata file not found at {master_metadata_path}")
        return None
    
    # Load master metadata
    thread_safe_log('info', f"Loading master metadata from {master_metadata_path}")
    try:
        master_df = pd.read_parquet(master_metadata_path)
        thread_safe_log('info', f"Master metadata contains {len(master_df)} image records")
        return master_df
    except Exception as e:
        thread_safe_log('error', f"Failed to load master metadata: {e}")
        return None

def load_dataset_json_metadata(parquet_dir):
    """
    Load the JSON metadata file containing dataset information
    """
    metadata_path = os.path.join(parquet_dir, "metadata.json")
    if not os.path.exists(metadata_path):
        thread_safe_log('warning', f"Dataset metadata JSON not found at {metadata_path}")
        return None
    
    try:
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        thread_safe_log('info', f"Loaded dataset metadata JSON successfully")
        return metadata
    except Exception as e:
        thread_safe_log('error', f"Failed to load dataset metadata JSON: {e}")
        return None

def extract_images_from_parquet(parquet_dir, output_base_dir, max_workers=None, progress_bar=True):
    """
    Extract images from Parquet files back to their original directory structure.
    
    Parameters:
    -----------
    parquet_dir : str
        Root directory containing the Parquet dataset
    output_base_dir : str
        Base directory to store the extracted images
    max_workers : int, optional
        Maximum number of worker threads. If None, uses default based on system.
    progress_bar : bool, optional
        Whether to display progress bars for extraction
    """
    start_time = time.time()
    thread_safe_log('info', f"Starting image extraction at {datetime.datetime.now()}")
    
    # If max_workers not specified, use a reasonable default (number of CPUs + 4)
    if max_workers is None:
        import multiprocessing
        max_workers = multiprocessing.cpu_count() + 4
    
    thread_safe_log('info', f"Using up to {max_workers} worker threads")
    
    # Create output base directory if it doesn't exist
    os.makedirs(output_base_dir, exist_ok=True)
    
    # Load master metadata
    master_df = load_master_metadata(parquet_dir)
    if master_df is None:
        return
    
    # Load JSON metadata
    json_metadata = load_dataset_json_metadata(parquet_dir)
    
    # Create Wear folder structure in output directory
    wear_output_dir = os.path.join(output_base_dir, "Wear")
    os.makedirs(wear_output_dir, exist_ok=True)
    
    # Get unique parquet files to process
    parquet_files = master_df['parquet_path'].unique()
    thread_safe_log('info', f"Found {len(parquet_files)} unique Parquet files to process")
    
    # Statistics tracking with thread safety
    total_images = 0
    total_images_lock = Lock()
    
    # Process Parquet files using a thread pool
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all processing tasks
        futures = {
            executor.submit(
                process_parquet_to_images,
                os.path.join(parquet_dir, parquet_path.lstrip('/')),
                output_base_dir,
                master_df[master_df['parquet_path'] == parquet_path],
                total_images_lock,
                progress_bar
            ): parquet_path for parquet_path in parquet_files
        }
        
        # Process completed futures as they finish
        for future in concurrent.futures.as_completed(futures):
            parquet_path = futures[future]
            try:
                # Each completed future returns the number of images processed
                num_processed = future.result()
                with total_images_lock:
                    total_images += num_processed
                thread_safe_log('info', f"Completed processing Parquet file {parquet_path} with {num_processed} images")
            except Exception as e:
                thread_safe_log('error', f"Error processing {parquet_path}: {e}")
    
    # Generate and save material-specific metadata
    if json_metadata:
        thread_safe_log('info', "Generating material-specific metadata files")
        generate_material_metadata_files(json_metadata, output_base_dir)
    
    # Copy README and LICENSE if they exist
    for file_name in ['README.md', 'LICENSE']:
        source_file = os.path.join(parquet_dir, file_name)
        if os.path.exists(source_file):
            dest_file = os.path.join(output_base_dir, file_name)
            shutil.copy2(source_file, dest_file)
            thread_safe_log('info', f"Copied {file_name} to output directory")
    
    # Generate comprehensive dataset report
    generate_dataset_report(master_df, json_metadata, output_base_dir, total_images)
    
    elapsed_time = time.time() - start_time
    thread_safe_log('info', f"Image extraction completed successfully")
    thread_safe_log('info', f"Total processing time: {elapsed_time:.2f} seconds")
    thread_safe_log('info', f"Total images extracted: {total_images}")


def process_parquet_to_images(parquet_file, output_base_dir, metadata_df, total_images_lock, progress_bar=True):
    """
    Process a Parquet file and extract images to their original directory structure.
    
    Parameters:
    -----------
    parquet_file : str
        Path to the Parquet file containing image data
    output_base_dir : str
        Base directory to extract images to
    metadata_df : pandas.DataFrame
        DataFrame containing metadata for images in this Parquet file
    total_images_lock : threading.Lock
        Lock for thread-safe image counting
    progress_bar : bool
        Whether to show progress bar for this file processing
    
    Returns:
    --------
    int
        Number of images processed
    """
    thread_name = threading.current_thread().name
    thread_safe_log('info', f"Thread {thread_name} processing Parquet file: {parquet_file}")
    dir_start_time = time.time()
    
    try:
        # Check if Parquet file exists
        if not os.path.exists(parquet_file):
            thread_safe_log('error', f"Parquet file not found: {parquet_file}")
            return 0
        
        # Load Parquet file
        df = pd.read_parquet(parquet_file)
        thread_safe_log('info', f"Loaded Parquet file with {len(df)} images")
        
        # Extract images
        images_processed = 0
        
        # Create iterator with optional progress bar
        if progress_bar:
            iterator = tqdm(df.iterrows(), total=len(df), desc=f"Processing {os.path.basename(parquet_file)}")
        else:
            iterator = df.iterrows()
        
        for _, row in iterator:
            try:
                # Get image metadata
                image_id = row['image_id']
                image_bytes = row['image_bytes']
                
                # Get corresponding metadata row
                meta_row = metadata_df[metadata_df['image_id'] == image_id]
                if len(meta_row) == 0:
                    thread_safe_log('warning', f"No metadata found for image ID: {image_id}")
                    continue
                
                # Extract metadata
                material = meta_row['material'].iloc[0]
                surface = meta_row['surface'].iloc[0]
                load_dir = meta_row['load_dir'].iloc[0]
                distance_dir = meta_row['distance_dir'].iloc[0]
                map_type = meta_row['map_type'].iloc[0]
                filename = meta_row['filename'].iloc[0]
                
                # Create output directory structure
                output_dir = os.path.join(output_base_dir, "Wear", material, surface, load_dir, distance_dir)
                os.makedirs(output_dir, exist_ok=True)
                
                # Create output file path
                output_file = os.path.join(output_dir, filename)
                
                # Convert bytes to image and save
                img = Image.open(io.BytesIO(image_bytes))
                img.save(output_file, optimize=True)  # Added optimize for better file size
                
                # Create local JSON metadata if this is a height map
                if map_type == 'height' and 'metadata.json' not in os.listdir(output_dir):
                    create_local_metadata(meta_row, output_dir)
                
                images_processed += 1
                
            except Exception as e:
                thread_safe_log('error', f"Error extracting image {image_id}: {e}")
        
        dir_elapsed = time.time() - dir_start_time
        thread_safe_log('info', f"Thread {thread_name} extracted {images_processed} images in {dir_elapsed:.2f}s")
        
        with total_images_lock:
            return images_processed
            
    except Exception as e:
        thread_safe_log('error', f"Thread {thread_name} encountered error processing {parquet_file}: {e}")
        return 0


def create_local_metadata(meta_row, output_dir):
    """
    Create local metadata.json file in the specific sample directory
    """
    try:
        # Extract data from meta_row
        material = meta_row['material'].iloc[0]
        surface = meta_row['surface'].iloc[0]
        load = meta_row['load_dir'].iloc[0]
        distance = meta_row['distance_dir'].iloc[0]
        
        # Create local metadata object
        local_meta = {
            "sampleInfo": {
                "material": material,
                "surface": surface,
                "load": load,
                "distanceTraveled": distance,
                "captureDate": meta_row['capture_date'].iloc[0] if 'capture_date' in meta_row else None,
                "sampleId": meta_row['sample_id'].iloc[0] if 'sample_id' in meta_row else None
            },
            "processingInfo": {
                "extractedDate": datetime.datetime.now().isoformat(),
                "software": "Friction Surface Dataset Extractor",
                "version": "2.0.0"
            }
        }
        
        # Save to file
        metadata_file = os.path.join(output_dir, "metadata.json")
        with open(metadata_file, 'w') as f:
            json.dump(local_meta, f, indent=2)
            
    except Exception as e:
        thread_safe_log('error', f"Error creating local metadata: {e}")


def generate_material_metadata_files(json_metadata, output_base_dir):
    """
    Generate material-specific metadata files for each material in the dataset
    """
    if not json_metadata or 'materialProfiles' not in json_metadata:
        return
    
    wear_dir = os.path.join(output_base_dir, "Wear")
    
    for material, material_data in json_metadata['materialProfiles'].items():
        material_dir = os.path.join(wear_dir, material)
        if os.path.exists(material_dir):
            metadata_file = os.path.join(material_dir, "metadata.json")
            try:
                with open(metadata_file, 'w') as f:
                    json.dump(material_data, f, indent=2)
                thread_safe_log('info', f"Created material metadata for {material}")
            except Exception as e:
                thread_safe_log('error', f"Error creating metadata for {material}: {e}")


def generate_dataset_report(master_df, json_metadata, output_base_dir, total_images):
    """
    Generate a comprehensive dataset report with statistics and visualization examples
    """
    try:
        # Generate summary statistics
        materials = master_df['material'].unique()
        surfaces = master_df['surface'].unique()
        loads = master_df['load_dir'].unique()
        distances = master_df['distance_dir'].unique()
        map_types = master_df['map_type'].unique()
        
        # Create detailed report with counts per category
        report = {
            'datasetSummary': {
                'totalImages': total_images,
                'totalMaterials': len(materials),
                'totalSurfaces': len(surfaces),
                'totalLoads': len(loads),
                'totalDistances': len(distances),
                'totalMapTypes': len(map_types),
                'generationDate': datetime.datetime.now().isoformat()
            },
            'materialStats': {},
            'surfaceStats': {},
            'loadStats': {},
            'distanceStats': {},
            'mapTypeStats': {}
        }
        
        # Count images per material
        for material in materials:
            count = len(master_df[master_df['material'] == material])
            report['materialStats'][material] = count
            
        # Count images per surface
        for surface in surfaces:
            count = len(master_df[master_df['surface'] == surface])
            report['surfaceStats'][surface] = count
            
        # Count images per load
        for load in loads:
            count = len(master_df[master_df['load_dir'] == load])
            report['loadStats'][load] = count
            
        # Count images per distance
        for distance in distances:
            count = len(master_df[master_df['distance_dir'] == distance])
            report['distanceStats'][distance] = count
            
        # Count images per map type
        for map_type in map_types:
            count = len(master_df[master_df['map_type'] == map_type])
            report['mapTypeStats'][map_type] = count
            
        # Add metadata if available
        if json_metadata:
            report['datasetInfo'] = json_metadata.get('datasetInfo', {})
            report['mapDescriptions'] = json_metadata.get('mapTypes', {})
            
        # Save detailed report as JSON
        report_path = os.path.join(output_base_dir, "dataset_report.json")
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=4)
            
        # Create a human-readable markdown report
        create_markdown_report(report, json_metadata, output_base_dir)
        
        thread_safe_log('info', f"Generated comprehensive dataset report")
        
    except Exception as e:
        thread_safe_log('error', f"Error generating dataset report: {e}")


def create_markdown_report(report, json_metadata, output_base_dir):
    """Create a human-readable markdown report from the dataset statistics"""
    
    report_path = os.path.join(output_base_dir, "README.md")
    
    with open(report_path, 'w') as f:
        # Title and introduction
        f.write("# Friction Surfaces Dataset\n\n")
        
        if json_metadata and 'datasetInfo' in json_metadata:
            f.write(f"## Dataset Description\n\n")
            f.write(f"{json_metadata['datasetInfo'].get('description', 'Surface texture dataset')}\n\n")
        
        # Summary statistics
        f.write("## Dataset Statistics\n\n")
        f.write(f"- **Total Images**: {report['datasetSummary']['totalImages']}\n")
        f.write(f"- **Materials**: {report['datasetSummary']['totalMaterials']}\n")
        f.write(f"- **Surface Types**: {report['datasetSummary']['totalSurfaces']}\n")
        f.write(f"- **Load Conditions**: {report['datasetSummary']['totalLoads']}\n")
        f.write(f"- **Distance Measurements**: {report['datasetSummary']['totalDistances']}\n")
        f.write(f"- **Map Types**: {report['datasetSummary']['totalMapTypes']}\n\n")
        
        # Materials
        f.write("## Materials\n\n")
        for material, count in report['materialStats'].items():
            f.write(f"### {material} ({count} images)\n\n")
            if json_metadata and 'materialProfiles' in json_metadata and material in json_metadata['materialProfiles']:
                material_info = json_metadata['materialProfiles'][material]
                if 'materialProperties' in material_info:
                    props = material_info['materialProperties']
                    f.write(f"**Name**: {props.get('name', material)}\n\n")
                    f.write(f"**Type**: {props.get('type', 'Not specified')}\n\n")
                    f.write(f"**Description**: {props.get('description', 'Not provided')}\n\n")
                    
                    if 'physicalProperties' in props:
                        f.write("**Physical Properties**:\n\n")
                        for prop, value in props['physicalProperties'].items():
                            f.write(f"- {prop.capitalize()}: {value}\n")
                        f.write("\n")
        
        # Map Types
        f.write("## Map Types\n\n")
        for map_type, count in report['mapTypeStats'].items():
            f.write(f"### {map_type} ({count} images)\n\n")
            if json_metadata and 'mapTypes' in json_metadata and map_type in json_metadata['mapTypes']:
                map_info = json_metadata['mapTypes'][map_type]
                f.write(f"**Name**: {map_info.get('name', map_type)}\n\n")
                f.write(f"**Description**: {map_info.get('description', 'Not provided')}\n\n")
                f.write(f"**Format**: {map_info.get('dataFormat', 'Not specified')}\n\n")
                f.write(f"**Usage**: {map_info.get('renderingUsage', 'Not specified')}\n\n")
        
        # Directory Structure
        f.write("## Directory Structure\n\n")
        f.write("```\n")
        f.write("Wear/\n")
        f.write("└── [Material]/\n")
        f.write("    └── [Surface]/\n")
        f.write("        └── [Load]/\n")
        f.write("            └── [Distance]/\n")
        f.write("                ├── ao.png\n")
        f.write("                ├── bump.png\n")
        f.write("                ├── displacement.png\n")
        f.write("                ├── height.png\n")
        f.write("                ├── hillshade.png\n")
        f.write("                ├── metadata.json\n")
        f.write("                ├── metallic.png\n")
        f.write("                ├── normal.png\n")
        f.write("                └── roughness.png\n")
        f.write("```\n\n")
        
        # Generation information
        f.write(f"## Report Generation\n\n")
        f.write(f"This report was automatically generated on {report['datasetSummary']['generationDate']}.\n")


if __name__ == "__main__":
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Extract friction surfaces dataset from Parquet format')
    parser.add_argument('--parquet-dir', type=str, default="FrictionSurfacesDatasets_Parquet", 
                        help='Directory with Parquet dataset')
    parser.add_argument('--output-dir', type=str, default="ExtractedFrictionSurfacesDataset", 
                        help='Output directory for extracted images')
    parser.add_argument('--workers', type=int, default=None, 
                        help='Number of worker threads (default: CPU count + 4)')
    parser.add_argument('--no-progress', action='store_true', 
                        help='Disable progress bars')
    
    args = parser.parse_args()
    
    # Extract images
    extract_images_from_parquet(
        args.parquet_dir, 
        args.output_dir, 
        max_workers=args.workers,
        progress_bar=not args.no_progress
    )
