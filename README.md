# Friction Surfaces Dataset

This repository contains tools and code for working with a dataset of friction surface images across different materials, conditions, and wear progressions.

## Dataset Structure

The dataset consists of surface scan images organized by:
- **Material type** (Cartboard, MDF, PLA, etc.)
- **Surface type** (S40, S60, S120, S220, S240, Linear)
- **Load/weight** (10g, 20g, 50g, 100g, 200g)
- **Distance** (0mm to 1400mm) - represents wear progression
- **Map types** (height, displacement, normal, roughness, bump, ao, hillshade)

```
Wear/
├── Cartboard/
│   └── S40/
│       ├── 10g/
│       │   ├── 0mm/
│       │   │   ├── height.png
│       │   │   ├── displacement.png
│       │   │   └── ...
│       │   └── 200mm/
│       │       └── ...
│       └── ...
├── MDF/
│   └── ...
└── PLA/
    └── ...
```

## Tools Included

### 1. Parquet Converter (`convert_to_parquet_wear.py`)

Converts the raw image dataset to a more efficient Parquet format for faster analysis and retrieval.

```python
python convert_to_parquet_wear.py
```

### 2. Dataset Explorer (`parquet_explorer.py`)

Provides tools for exploring and visualizing the Parquet-formatted dataset.

```python
python explore_parquet_wear.py
```

## Data Organization

The Parquet dataset organizes images with the following metadata:
- `image_id`: Unique identifier for each image
- `material`: Material type (Cartboard, MDF, PLA, etc.)
- `surface`: Surface type (S40, S60, etc.)
- `grit`: Numerical grit value extracted from surface type
- `load_dir`: Load directory name
- `load_g`: Load value in grams
- `distance_dir`: Distance directory name
- `distance_mm`: Distance value in millimeters
- `map_type`: Type of surface map (height, displacement, etc.)
- `width`, `height`, `channels`: Image dimensions
- `image_bytes`: Raw image data

Install with:
```
pip install -r requirements.txt
```


## Research Applications

This dataset is designed for research on:
- Surface wear analysis
- Material friction properties
- Wear progression patterns
- Surface roughness characterization

## Performance Notes

- The Parquet format significantly improves data loading speed compared to raw images
- Multi-threading is used to parallelize both conversion and exploration tasks
- Memory usage is optimized for efficient processing of large datasets
