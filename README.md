# Friction Surfaces Dataset

Welcome to the Friction Surfaces Dataset—a comprehensive collection of images that capture how various surfaces react and change due to wear under different conditions. This dataset is ideally suited for research and projects in computer vision, material science, and machine learning.

---

## Dataset Overview

The Friction Surfaces Dataset provides high-quality images captured from various materials as they undergo wear. The images include different types such as height maps, normal maps, and more, which offer insight into how surfaces deteriorate with different grit levels, weights, and distances.

**Key Features:**

- **Materials:**  
  - Cartboard  
  - MDF  
  - PLA

- **Grit Levels:**  
  - Examples: S40, S60, S120, etc.

- **Weights:**  
  - Examples: 10g, 20g, 50g, etc.

- **Distances:**  
  - Examples: 0mm, 200mm, etc.

- **Image Variants:**  
  - Height maps  
  - Normal maps  
  - Ambient Occlusion (AO)  
  - Bump maps  
  - Displacement maps  
  - Hillshade images  
  - Metallic maps  
  - Roughness maps  
  - Additional metadata files (when available)

---

## Directory Structure

The dataset is organized into a hierarchical structure that reflects the different parameters (materials, grit, weight, distance) which affect surface wear. Below is an illustrative example of the directory tree:

```
Wear/
├── Cartboard/
│   └── S60/
│       ├── 10g/
│       │   ├── 0mm/
│       │   │   ├── height.png
│       │   │   ├── normal.png
│       │   │   └── ... 
│       │   └── 200mm/
│       │       └── ...
│       ├── 100g/
│       │   └── 0mm/
│       │       └── ...
│       └── 50g/
│           ├── 0mm/
│           │   └── ...
│           └── 200mm/
│               └── ...
├── MDF/
│   └── S40/
│       └── 200g/
│           └── ...
├── PLA/
│   └── Linear/
│       ├── S120/
│       │   ├── 0mm/
│       │   │   └── ...
│       │   └── 1000mm/
│       │       └── ...
│       └── metadata.json
```

---

## Tools and Processing Pipelines

This repository includes an integrated set of Python tools designed to help you process and analyze the dataset. The tools are optimized for flexibility and scalability, supporting multiple modes of operation:

### 1. Parquet Conversion

- **Description:**  
  Convert the dataset images into Parquet files with embedded raw binary JPEG data. This enables efficient storage and metadata querying.
  
- **Script:** `generate_datasets.py`  
- **Usage Example:**

  ```bash
  python generate_datasets.py --mode parquet --input-dir /path/to/Wear --output-dir parquet_data
  ```

### 2. WebDataset Shard Creation

- **Description:**  
  Create WebDataset shards (tar archives containing JPEG images and JSON metadata) for efficient streaming into PyTorch DataLoaders.
  
- **Script:** `generate_datasets.py` (use `--mode webdataset`)  
- **Usage Example:**

  ```bash
  python generate_datasets.py --mode webdataset --input-dir /path/to/Wear --output-dir webdataset_shards --shard-size 100
  ```

### 3. Image Extraction

- **Description:**  
  Extract and save images from Parquet files based solely on the embedded binary data—making the dataset completely self-contained.
  
- **Script:** `read_datasets.py` (use `--mode extract`)  
- **Usage Example:**

  ```bash
  python read_datasets.py --mode extract --input-dir parquet_data --output-dir extracted_images --material MDF
  ```

### 4. Dataset Downloading

- **Description:**  
  Automatically download the dataset from a provided URL (assumed to point to a ZIP archive) if it is not already present.
  
- **Usage Example:**

  ```bash
  python generate_datasets.py --dataset-url http://example.com/dataset.zip --input-dir /path/to/download
  ```

---

## Installation & Dependencies

Ensure you have **Python 3.7+** installed. The following Python packages are required:

- **pandas**
- **numpy**
- **Pillow**
- **rich**
- **pyarrow**
- **requests** (for dataset downloading)

You can install these dependencies via pip:

```bash
pip install pandas numpy pillow rich pyarrow requests
```

---

## Getting Started

1. **Download the Dataset:**  
   If you have a dataset URL, run the script in download mode to get started. Otherwise, ensure that your `Wear/` directory is in place.

2. **Convert to Parquet:**  
   Use the parquet conversion mode to process and compress the dataset images into Parquet files.

3. **(Optional) Create WebDataset Shards:**  
   For integration with PyTorch and high-performance data streaming, create WebDataset shards.

4. **Extract Images:**  
   When needed, extract images from the Parquet files using the image extraction tools.