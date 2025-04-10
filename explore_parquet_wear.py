import os
import pandas as pd
import pyarrow.parquet as pq
import matplotlib.pyplot as plt
import numpy as np
from PIL import Image
import logging
import time
import threading
import concurrent.futures
from threading import Lock

# Set up logging with thread safety
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s',
    handlers=[
        logging.FileHandler('parquet_explorer.log'),
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

def explore_parquet_dataset(parquet_file):
    """
    Explore the contents of a Parquet dataset and print its structure.
    
    Parameters:
    -----------
    parquet_file : str
        Path to the Parquet file
    
    Returns:
    --------
    pandas.DataFrame
        Metadata DataFrame
    """
    start_time = time.time()
    thread_safe_log('info', f"Loading Parquet file: {parquet_file}")
    
    # Read the parquet file
    metadata_df = pd.read_parquet(parquet_file)
    
    # Print summary information
    thread_safe_log('info', "=== Dataset Summary ===")
    thread_safe_log('info', f"Parquet file size: {os.path.getsize(parquet_file) / (1024*1024):.2f} MB")
    thread_safe_log('info', f"Total records: {len(metadata_df)}")
    
    # Materials
    materials = metadata_df['material'].unique()
    thread_safe_log('info', f"Materials ({len(materials)}): {', '.join(materials)}")
    
    # Distance values
    distances = sorted(metadata_df['distance_mm'].unique())
    thread_safe_log('info', f"Distance values (mm): {distances}")
    
    # Force values
    forces = sorted(metadata_df['force_value'].unique())
    thread_safe_log('info', f"Force values: {forces}")
    
    # Map types
    map_types = sorted(metadata_df['map_type'].unique())
    thread_safe_log('info', f"Map types: {', '.join(map_types)}")
    
    # Print metadata structure
    thread_safe_log('info', "\n=== Metadata Structure ===")
    thread_safe_log('info', f"Columns: {metadata_df.columns.tolist()}")
    thread_safe_log('info', "\nColumn types:")
    for col, dtype in zip(metadata_df.dtypes.index, metadata_df.dtypes.values):
        thread_safe_log('info', f"  {col}: {dtype}")
    
    # Analyze distribution by material and map type
    thread_safe_log('info', "\n=== Distribution by Material ===")
    material_counts = metadata_df['material'].value_counts()
    for material, count in material_counts.items():
        thread_safe_log('info', f"  {material}: {count} images")
    
    thread_safe_log('info', "\n=== Distribution by Map Type ===")
    map_type_counts = metadata_df['map_type'].value_counts()
    for map_type, count in map_type_counts.items():
        thread_safe_log('info', f"  {map_type}: {count} images")
    
    # Check for missing values
    missing_values = metadata_df.isnull().sum()
    if missing_values.sum() > 0:
        thread_safe_log('info', "\n=== Missing Values ===")
        for col, count in missing_values[missing_values > 0].items():
            thread_safe_log('info', f"  {col}: {count} missing values")
    
    elapsed_time = time.time() - start_time
    thread_safe_log('info', f"Exploration completed in {elapsed_time:.2f} seconds")
    
    return metadata_df


def load_image_thread(sample, output_path=None):
    """
    Thread worker function to load and process an image.
    
    Parameters:
    -----------
    sample : pandas.Series
        Row from metadata DataFrame containing image information
    output_path : str, optional
        Path to save the image if desired
    
    Returns:
    --------
    dict
        Dictionary containing the loaded image and its metadata
    """
    thread_name = threading.current_thread().name
    image_id = sample['image_id']
    
    try:
        # Load the image
        img_path = sample['file_path']
        thread_safe_log('info', f"Thread {thread_name} loading image: {image_id}")
        
        img = Image.open(img_path)
        img_array = np.array(img)
        
        # Save the image if output path is provided
        if output_path:
            img.save(output_path)
            thread_safe_log('info', f"Thread {thread_name} saved image to {output_path}")
        
        # Prepare title parts
        material_name = sample['material']
        distance_mm = sample['distance_mm']
        force_value = sample['force_value']
        trial_number = sample['trial_number']
        map_type_name = sample['map_type']
        
        title = f"{material_name} at {distance_mm}mm"
        if force_value > 0:
            title += f"\nforce: {force_value}"
        if trial_number > 0:
            title += f", trial: {trialimport os
import pandas as pd
import pyarrow.parquet as pq
import matplotlib.pyplot as plt
import numpy as np
from PIL import Image
import logging
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('parquet_explorer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def explore_parquet_dataset(parquet_file):
    """
    Explore the contents of a Parquet dataset and print its structure.
    
    Parameters:
    -----------
    parquet_file : str
        Path to the Parquet file
    
    Returns:
    --------
    pandas.DataFrame
        Metadata DataFrame
    """
    start_time = time.time()
    logger.info(f"Loading Parquet file: {parquet_file}")
    
    # Read the parquet file
    metadata_df = pd.read_parquet(parquet_file)
    
    # Print summary information
    logger.info("=== Dataset Summary ===")
    logger.info(f"Parquet file size: {os.path.getsize(parquet_file) / (1024*1024):.2f} MB")
    logger.info(f"Total records: {len(metadata_df)}")
    
    # Materials
    materials = metadata_df['material'].unique()
    logger.info(f"Materials ({len(materials)}): {', '.join(materials)}")
    
    # Distance values
    distances = sorted(metadata_df['distance_mm'].unique())
    logger.info(f"Distance values (mm): {distances}")
    
    # Force values
    forces = sorted(metadata_df['force_value'].unique())
    logger.info(f"Force values: {forces}")
    
    # Map types
    map_types = sorted(metadata_df['map_type'].unique())
    logger.info(f"Map types: {', '.join(map_types)}")
    
    # Print metadata structure
    logger.info("\n=== Metadata Structure ===")
    logger.info(f"Columns: {metadata_df.columns.tolist()}")
    logger.info("\nColumn types:")
    for col, dtype in zip(metadata_df.dtypes.index, metadata_df.dtypes.values):
        logger.info(f"  {col}: {dtype}")
    
    # Analyze distribution by material and map type
    logger.info("\n=== Distribution by Material ===")
    material_counts = metadata_df['material'].value_counts()
    for material, count in material_counts.items():
        logger.info(f"  {material}: {count} images")
    
    logger.info("\n=== Distribution by Map Type ===")
    map_type_counts = metadata_df['map_type'].value_counts()
    for map_type, count in map_type_counts.items():
        logger.info(f"  {map_type}: {count} images")
    
    # Check for missing values
    missing_values = metadata_df.isnull().sum()
    if missing_values.sum() > 0:
        logger.info("\n=== Missing Values ===")
        for col, count in missing_values[missing_values > 0].items():
            logger.info(f"  {col}: {count} missing values")
    
    elapsed_time = time.time() - start_time
    logger.info(f"Exploration completed in {elapsed_time:.2f} seconds")
    
    return metadata_df


def visualize_samples(metadata_df, output_dir="sample_images", num_samples=5):
    """
    Visualize random sample images from the dataset.
    
    Parameters:
    -----------
    metadata_df : pandas.DataFrame
        Metadata DataFrame
    output_dir : str
        Directory to save sample images
    num_samples : int
        Number of random samples to visualize
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Get different materials
    materials = metadata_df['material'].unique()
    map_types = metadata_df['map_type'].unique()
    
    logger.info(f"Visualizing {num_samples} random samples")
    
    # Select samples from different materials and map types if possible
    samples = []
    
    # Try to get one sample from each material
    for material in materials[:min(len(materials), num_samples)]:
        material_df = metadata_df[metadata_df['material'] == material]
        
        # Try to include different map types
        for map_type in map_types:
            filtered_df = material_df[material_df['map_type'] == map_type]
            if len(filtered_df) > 0:
                samples.append(filtered_df.sample(1).iloc[0])
                break
        
        # If no sample added yet for this material, add any available
        if len(samples) < len(materials[:min(len(materials), num_samples)]):
            if len(material_df) > 0:
                samples.append(material_df.sample(1).iloc[0])
    
    # Add more random samples if needed
    while len(samples) < num_samples and len(metadata_df) > len(samples):
        # Select a random row that's not already in samples
        sample_ids = [s['image_id'] for s in samples]
        remaining_df = metadata_df[~metadata_df['image_id'].isin(sample_ids)]
        if len(remaining_df) > 0:
            samples.append(remaining_df.sample(1).iloc[0])
    
    # Create a grid of plots
    n_samples = len(samples)
    if n_samples == 0:
        logger.error("No samples found to visualize")
        return
    
    cols = min(3, n_samples)
    rows = (n_samples + cols - 1) // cols
    
    fig, axes = plt.subplots(rows, cols, figsize=(5*cols, 4*rows))
    if rows == 1 and cols == 1:
        axes = [axes]  # Make it iterable
    elif rows == 1 or cols == 1:
        axes = axes.flatten()
    else:
        # Convert 2D array to 1D for easier iteration
        axes = axes.flatten()
    
    # Plot each sample
    for i, sample in enumerate(samples):
        try:
            # Load the image
            img_path = sample['file_path']
            img = Image.open(img_path)
            
            # Plot the image
            ax = axes[i]
            ax.imshow(np.array(img))
            
            # Prepare title
            material_name = sample['material']
            distance_mm = sample['distance_mm']
            force_value = sample['force_value']
            trial_number = sample['trial_number']
            map_type_name = sample['map_type']
            
            title = f"{material_name} at {distance_mm}mm"
            if force_value > 0:
                title += f"\nforce: {force_value}"
            if trial_number > 0:
                title += f", trial: {trial_number}"
            title += f"\nmap: {map_type_name}"
            
            ax.set_title(title, fontsize=10)
            ax.axis('off')
            
            # Save the individual image
            sample_filename = f"sample_{i+1}_{material_name}_{distance_mm}mm_{map_type_name}.png"
            sample_path = os.path.join(output_dir, sample_filename)
            img.save(sample_path)
            logger.info(f"Saved sample image to {sample_path}")
            
        except Exception as e:
            logger.error(f"Error visualizing sample {i}: {e}")
            ax = axes[i]
            ax.text(0.5, 0.5, f"Error loading image", 
                    horizontalalignment='center', verticalalignment='center')
            ax.axis('off')
    
    # Hide unused subplots
    for i in range(len(samples), len(axes)):
        axes[i].axis('off')
    
    plt.tight_layout()
    
    # Save the grid of samples
    grid_path = os.path.join(output_dir, "sample_grid.png")
    plt.savefig(grid_path, dpi=150)
    logger.info(f"Saved sample grid to {grid_path}")
    plt.close()


def compare_wear_progression(metadata_df, material, map_type="height", output_dir="wear_comparison"):
    """
    Compare wear progression across different distances for a specific material.
    
    Parameters:
    -----------
    metadata_df : pandas.DataFrame
        Metadata DataFrame
    material : str
        Material to visualize
    map_type : str
        Map type to visualize (default: "height")
    output_dir : str
        Directory to save comparison images
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Filter for the specified material and map type
    filtered_df = metadata_df[
        (metadata_df['material'] == material) & 
        (metadata_df['map_type'] == map_type)
    ]
    
    if len(filtered_df) == 0:
        logger.error(f"No images found for material={material}, map_type={map_type}")
        return
    
    # Get unique distances for this material
    distances = sorted(filtered_df['distance_mm'].unique())
    
    if len(distances) <= 1:
        logger.error(f"Not enough distance values for material={material} to show progression")
        return
    
    logger.info(f"Comparing wear progression for {material} across {len(distances)} distances")
    
    # For consistent comparison, try to use images with the same force value
    force_values = filtered_df['force_value'].value_counts().index
    selected_force = force_values[0]  # Use the most common force value
    
    # Create a grid of plots
    cols = min(4, len(distances))
    rows = (len(distances) + cols - 1) // cols
    
    fig, axes = plt.subplots(rows, cols, figsize=(4*cols, 3*rows))
    if rows == 1 and cols == 1:
        axes = [axes]  # Make it iterable
    elif rows == 1 or cols == 1:
        axes = axes.flatten()
    else:
        # Convert 2D array to 1D for easier iteration
        axes = axes.flatten()
    
    # Plot each distance
    for i, distance in enumerate(distances):
        # Get images for this distance
        distance_df = filtered_df[
            (filtered_df['distance_mm'] == distance) & 
            (filtered_df['force_value'] == selected_force)
        ]
        
        # If no images with the selected force, use any image at this distance
        if len(distance_df) == 0:
            distance_df = filtered_df[filtered_df['distance_mm'] == distance]
        
        if len(distance_df) == 0:
            logger.warning(f"No images found for distance={distance}mm")
            continue
        
        # Use the first available image
        sample = distance_df.iloc[0]
        
        try:
            # Load the image
            img_path = sample['file_path']
            img = Image.open(img_path)
            
            # Plot the image
            ax = axes[i]
            ax.imshow(np.array(img))
            
            # Create title
            title = f"{distance}mm"
            if i == 0:
                title = f"{material} - {title}"
                
            ax.set_title(title)
            ax.axis('off')
            
            # Save individual image
            img_filename = f"{material}_{distance}mm_{map_type}.png"
            img_path = os.path.join(output_dir, img_filename)
            img.save(img_path)
            
        except Exception as e:
            logger.error(f"Error visualizing distance {distance}mm: {e}")
            ax = axes[i]
            ax.text(0.5, 0.5, f"Error: {distance}mm", 
                    horizontalalignment='center', verticalalignment='center')
            ax.axis('off')
    
    # Hide unused subplots
    for i in range(len(distances), len(axes)):
        axes[i].axis('off')
    
    plt.suptitle(f"{material} - {map_type} maps - Wear Progression", fontsize=16)
    plt.tight_layout()
    
    # Save the comparison grid
    comparison_path = os.path.join(output_dir, f"{material}_{map_type}_wear_progression.png")
    plt.savefig(comparison_path, dpi=150)
    logger.info(f"Saved wear progression comparison to {comparison_path}")
    plt.close()


if __name__ == "__main__":
    # Update this path for your specific setup
    PARQUET_FILE = "friction_surfaces_metadata.parquet"
    
    # Explore the Parquet dataset
    metadata_df = explore_parquet_dataset(PARQUET_FILE)
    
    # Visualize random samples
    visualize_samples(metadata_df, num_samples=6)
    
    # Compare wear progression for each material
    for material in metadata_df['material'].unique():
        compare_wear_progression(metadata_df, material, "height")