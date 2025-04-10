import os
import h5py
import numpy as np
from PIL import Image
import re

def create_hdf5_from_image_dataset(root_dir, output_file):
    """
    Create an HDF5 file from a directory of images organized by material, distance, and map type.
    
    Parameters:
    -----------
    root_dir : str
        Root directory containing the image dataset
    output_file : str
        Path to the output HDF5 file
    """
    # Regular expressions to extract metadata from directory names
    distance_pattern = re.compile(r'(\d+)mm')
    load_pattern = re.compile(r'(\d+)g')
    force_pattern = re.compile(r'_(\d+)_')  # For load values like 60, 120, 240
    trial_pattern = re.compile(r'_(\d+)_heightmap')  # For trial numbers in Cartboard
    
    # Create HDF5 file
    with h5py.File(output_file, 'w') as f:
        # Create metadata group for dataset info
        metadata = f.create_group('metadata')
        metadata.attrs['description'] = 'Material wear image dataset with various map types'
        metadata.attrs['created_from'] = root_dir
        
        # Traverse the directory structure
        for material_dir in os.listdir(os.path.join(root_dir, 'Wear')):
            # Skip non-directories and hidden files
            material_path = os.path.join(root_dir, 'Wear', material_dir)
            if not os.path.isdir(material_path) or material_dir.startswith('.'):
                continue
                
            # Create a group for the material
            material_group = f.create_group(f'materials/{material_dir}')
            
            # Process each experiment directory
            for exp_dir in os.listdir(material_path):
                exp_path = os.path.join(material_path, exp_dir)
                
                # Handle Cartboard's numbered trials (1,2,3,4,5)
                if material_dir == 'Cartboard' and exp_dir.isdigit():
                    trial_num = int(exp_dir)
                    trial_group = material_group.create_group(f'trial_{trial_num}')
                    
                    # Process each distance directory within the trial
                    for distance_dir in os.listdir(exp_path):
                        distance_path = os.path.join(exp_path, distance_dir)
                        if not os.path.isdir(distance_path):
                            continue
                        
                        # Extract metadata from directory name
                        distance_match = distance_pattern.search(distance_dir)
                        force_match = force_pattern.search(distance_dir)
                        
                        distance_mm = int(distance_match.group(1)) if distance_match else -1
                        force_value = int(force_match.group(1)) if force_match else -1
                        
                        # Create a group for this distance/experiment
                        experiment_group = trial_group.create_group(f'distance_{distance_mm}mm_force_{force_value}')
                        experiment_group.attrs['directory_name'] = distance_dir
                        experiment_group.attrs['distance_mm'] = distance_mm
                        experiment_group.attrs['force_value'] = force_value
                        experiment_group.attrs['trial_number'] = trial_num
                        
                        # Process each image in this experiment
                        process_images(distance_path, experiment_group)
                
                # Handle MDF and PLA directories
                elif not os.path.isdir(exp_path) or exp_dir.startswith('.'):
                    continue
                else:
                    # For MDF and PLA, exp_dir is actually the distance/experiment directory
                    distance_path = exp_path
                    
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
                    
                    # Create a structured group name based on available metadata
                    group_name = f'distance_{distance_mm}mm'
                    if load_value > 0:
                        group_name += f'_load_{load_value}g'
                    if force_value > 0:
                        group_name += f'_force_{force_value}'
                    
                    # Create a group for this experiment
                    experiment_group = material_group.create_group(group_name)
                    experiment_group.attrs['directory_name'] = exp_dir
                    experiment_group.attrs['distance_mm'] = distance_mm
                    if load_value > 0:
                        experiment_group.attrs['load_g'] = load_value
                    if force_value > 0:
                        experiment_group.attrs['force_value'] = force_value
                    
                    # Process each image in this experiment
                    process_images(distance_path, experiment_group)
    
    print(f"HDF5 file created successfully at {output_file}")


def process_images(directory_path, h5_group):
    """
    Process all image files in a directory and add them to the HDF5 group.
    
    Parameters:
    -----------
    directory_path : str
        Path to the directory containing images
    h5_group : h5py.Group
        HDF5 group to add the images to
    """
    for img_file in os.listdir(directory_path):
        if not img_file.lower().endswith('.png'):
            continue
            
        # Extract map type (ao, bump, height, etc.)
        map_type = img_file.split('_')[-1].split('.')[0]
        
        # Load image
        try:
            img_path = os.path.join(directory_path, img_file)
            img = Image.open(img_path)
            img_array = np.array(img)
            
            # Store the image in the HDF5 file
            dataset = h5_group.create_dataset(
                map_type, 
                data=img_array,
                compression="gzip",  # Use compression to save space
                compression_opts=9    # Maximum compression level
            )
            
            # Add metadata for the image
            dataset.attrs['filename'] = img_file
            dataset.attrs['shape'] = img_array.shape
            dataset.attrs['dtype'] = str(img_array.dtype)
            
            print(f"Added {img_file} to HDF5")
        except Exception as e:
            print(f"Error processing {img_file}: {e}")


if __name__ == "__main__":
    # Update these paths for your specific setup
    ROOT_DIR = "."  # Current directory, change as needed
    OUTPUT_FILE = "wear_dataset.h5"
    
    create_hdf5_from_image_dataset(ROOT_DIR, OUTPUT_FILE)