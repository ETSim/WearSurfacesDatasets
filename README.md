```markdown
# Friction Surfaces Dataset Pipeline

A suite of Python tools for processing, converting and managing high‑resolution surface‑texture datasets captured under controlled wear tests. 

## 🔍 Key Features

- **Parquet Conversion**  
  Aggregate raw PNG “maps” (AO, bump, displacement, height, hillshade, normal, roughness) plus per‑folder metadata into snappy‑compressed Parquet files with embedded JPEG bytes.

- **WebDataset Shards**  
  Package images and JSON metadata into sharded `.tar` archives for efficient, scalable data loading.

- **Image Extraction & Decompression**  
  • Rebuild original PNG hierarchy from Parquet storage.  
  • Decompress WebDataset shards back into leaf directories.

- **Metadata Generation**  
  Traverse every leaf folder, infer `material`, `shape`, `direction`, `grit`, `load`, `distance`, `replicate`, list available map files, and emit a `metadata.json`.

- **Smart Updates & Full Pipeline**  
  • **Smart‑update**: incrementally reprocess only changed files.  
  • **Run‑all**: full end‑to‑end rebuild.

---

## 📂 Repository Layout

```
wear/                     # Raw dataset root (leaf dirs contain maps + metadata.json)
├── Cartboard/
├── FOAM/
├── MDF/
├── PLA/
├── Sandpaper/
├── Soft_Wood/
└── metadata.json          # Global metadata (version, material profiles, structure)

data/                     # Outputs
├── parquet/              # Parquet files + metadata.parquet
├── webdataset_shards/    # TAR shards
└── extracted_images/     # Reconstructed PNGs

generate_datasets.py      # CLI for parquet, webdataset, extract, decompress, smart‑update, run‑all  
generate_metadata.py      # Infer & write leaf‑level metadata.json  
read_datasets.py          # Helpers for Parquet extraction & shard decompression  
wear_tree.txt             # Sample `wear/` directory tree  
test.ipynb                # Exploratory notebook  
LICENSE  
README.md                 # This document  
```

---

## ⚙️ Installation

Requires Python 3.7+:

```bash
pip install pandas numpy pillow rich pyarrow tqdm typer
```

---

## 🚀 Usage

### 1. Generate per‑folder metadata

```bash
python generate_metadata.py wear/
```

Creates a `metadata.json` in every leaf directory under `wear/`.

### 2. Parquet Conversion

```bash
python3 generate_datasets.py parquet \
  --input-dir wear \
  --output-dir data/parquet \
  --workers 8
```

Bundles all maps + metadata into Parquet shards and produces `data/parquet/metadata.parquet`.

### 3. Create WebDataset Shards

```bash
python3 generate_datasets.py webdataset \
  --input-dir wear \
  --output-dir data/shards \
  --shard-size 100 \
  --workers 8
```

Generates `shard-0000.tar`, `shard-0001.tar`, … under `data/shards/`.

### 4. Extract PNGs from Parquet

```bash
python3 generate_datasets.py extract \
  --input-dir data/parquet \
  --output-dir data/images \
  [--material Cartboard] [--grit S60] [--weight 100] [--distance 0]
```

Reconstructs PNG files to `data/images/[…]`.

### 5. Decompress WebDataset Shards

```bash
python3 generate_datasets.py decompress \
  --shards-dir data/shards \
  --output-dir data/images \
  --workers 8
```

Unpacks every `.tar` back to PNG maps.

### 6. Smart‑Update vs. Full Rebuild

- **Smart update** (only changed files):  
  ```bash
  python generate_datasets.py smart-update \
    --wear-dir wear \
    --parquet-dir data/parquet \
    --shards-dir data/shards \
    --shard-size 1 \
    --workers 8
  ```

- **Full pipeline**:  
  ```bash
  python generate_datasets.py run-all \
    --wear-dir wear \
    --parquet-dir data/parquet \
    --shards-dir data/shards \
    --shard-size 1 \
    --workers 8
  ```

---

## 🌲 Example `wear/` Structure

```
wear/
├── Cartboard/
│   └── Normal/
│       └── S60/
│           ├── 100g/0mm/       ← maps + metadata.json  
│           ├── 100g/1200mm/  
│           ├── 100g/2400mm/1/  
│           └── 100g/2400mm/2/
├── FOAM/…  
├── MDF/…  
├── PLA/…  
├── Sandpaper/…  
├── Soft_Wood/…  
└── metadata.json
```

---

## 📄 License

This project is released under the **MIT License**. See [LICENSE](LICENSE) for details.