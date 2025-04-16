# Friction Surfaces Dataset Pipeline

The **Friction Surfaces Dataset Pipeline** is a high-performance Python-based I/O system designed for processing large-scale deep learning datasets. This repository provides tools to convert the raw Friction Surfaces dataset into efficient storage formats (Parquet files and WebDataset shards) and to easily extract images from those formats—all with a focus on seamless integration into PyTorch workflows.

---

## Table of Contents

- [Dataset Overview](#dataset-overview)
- [The WebDataset Format](#the-webdataset-format)
- [Pipeline Components](#pipeline-components)
  - [Parquet Conversion](#parquet-conversion)
  - [WebDataset Shard Creation](#webdataset-shard-creation)
  - [Image Extraction](#image-extraction)
- [Installation](#installation)
- [Usage](#usage)
- [Examples](#examples)
- [License](#license)
- [Acknowledgements](#acknowledgements)
- [Contact](#contact)

---

## Dataset Overview

The **Friction Surfaces Dataset** is a collection of surface images that depict how various materials change under wear. The dataset includes:

- **Materials:** Cartboard, MDF, PLA
- **Grit Levels:** S40, S60, S120, etc.
- **Weights:** 10g, 20g, 50g, etc.
- **Distances:** 0mm, 200mm, etc.
- **Image Types:** Height maps, normal maps, ambient occlusion (AO), bump maps, displacement maps, hillshade, metallic, roughness, and more.

The images are organized in the following folder hierarchy:

```
Wear/
├── Material (e.g., Cartboard, MDF, PLA)
    ├── Grit (e.g., S40, S60, S120, etc.)
        ├── Weight (e.g., 10g, 20g, 50g, etc.)
            ├── Distance (e.g., 0mm, 200mm, etc.)
                └── Image files (e.g., height.png, normal.png, etc.)
```

---

## The WebDataset Format

WebDataset is a file format and accompanying Python library designed for high-throughput streaming of large-scale datasets.  
Key points include:

- **Sharded Archives:**  
  Data is stored in tar archives (shards). Each shard is named sequentially (e.g., `shard-000000.tar`) and contains samples grouped by a common key.

- **File Grouping Convention:**  
  Within each tar file, files that belong together share the same basename when their extension is removed (e.g., `000001.jpg` and `000001.json`).  
  The image file (e.g., `.jpg`) contains the sample data (often in its native format), and the JSON file contains metadata.

- **Efficiency:**  
  WebDataset allows purely sequential I/O, taking full advantage of local disk performance and cloud object stores with minimal random I/O overhead.

---

## Pipeline Components

The Friction Surfaces Dataset Pipeline includes the following components:

### Parquet Conversion

- **Purpose:**  
  Convert each folder of the raw dataset into a Parquet file that embeds compressed JPEG images (stored as raw binary data) with accompanying metadata.  
- **Output:**  
  Parquet files and a metadata file (`metadata.parquet`).

### WebDataset Shard Creation

- **Purpose:**  
  Process the raw dataset to create WebDataset shards. Each shard is a tar archive containing JPEG files and their corresponding JSON metadata files.
- **Output:**  
  Shards are created under a specified output directory.

### Image Extraction

- **Purpose:**  
  Extract images from Parquet files by reading the embedded binary image data without any dependency on the original file paths.
- **Output:**  
  Reconstructed image files organized in a folder structure similar to the original dataset hierarchy.

---

## Installation

Ensure you have Python 3.7 or later installed. The pipeline requires the following packages:

- pandas
- numpy
- Pillow
- rich
- pyarrow
- tqdm
- typer

You can install the required packages via pip:

```bash
pip install pandas numpy pillow rich pyarrow tqdm typer
```

---

## Usage

Two main scripts are provided in this repository:

1. **generate_datasets.py** – for generating Parquet files and WebDataset shards.  
2. **read_datasets.py** – for extracting images from Parquet files or decompressing WebDataset shards.

### Generate Datasets

- **Parquet Conversion:**
  
  ```bash
  python3 generate_datasets.py parquet --input-dir Wear --output-dir data --workers 8
  ```
  
- **WebDataset Shard Creation:**

  ```bash
  python3 generate_datasets.py webdataset --input-dir Wear --output-dir webdataset_shards --shard-size 100 --workers 8
  ```

### Read / Extract Datasets

- **Extract Images from Parquet Files:**

  ```bash
  python3 read_datasets.py extract --input-dir data --output-dir extracted_images
  ```

- **Decompress WebDataset Shards:**

  ```bash
  python3 read_datasets.py decompress-webdataset --input-dir webdataset_shards --output-dir decompressed_webdataset --workers 8
  ```
