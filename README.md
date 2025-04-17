# Wear Surfaces Dataset Pipeline

A suite of Python CLIs for processing, converting and managing high‑resolution surface‑texture datasets captured under controlled wear tests.

---

## 🔍 Key Features

- **Per‑Leaf Metadata** (`generate_metadata.py`)  
  • Infer `material`, `shape`, `direction`, `grit`, `load_g`, `distance_mm`, `replicate` from folder structure  
  • Record file size & dimensions for each map (`ao.png`, `bump.png`, …)  
  • Emit one `metadata.json` per leaf and an aggregated `index.json`

- **Parquet Conversion** (`generate_datasets.py parquet`)  
  • Scan your `wear/` tree of PNG “maps”  
  • Compress each PNG to **JPEG (quality=50)** in‑memory  
  • Bundle per‑sample groups into Snappy‑compressed Parquet shards  
  • Produce a top‑level `metadata.parquet` index

- **WebDataset Shards** (`generate_datasets.py webdataset`)  
  • Re‑encode JPEG + JSON metadata  
  • Shard into `.tar` archives for efficient streaming

- **Image Extraction & Decompression**  
  • `extract` (Parquet → PNG)  
  • `decompress` (WebDataset .tar → PNG)

- **Smart‑Update & Full Pipeline**  
  • **smart‑update**: only reprocess changed files (SHA256‑based)  
  • **run‑all**: end‑to‑end rebuild (wear → Parquet → shards)

---

## 📂 Repository Layout

```
wear/                       # Raw dataset root
├── Cartboard/              # … leaf dirs with maps + metadata.json
└── Soft_Wood/

data/                       # Outputs
├── parquet/           # Parquet shards + metadata.parquet
├── webdataset/      # TAR shards of JPEG+JSON
├── images/       # PNGs reconstructed 

├── generate_metadata.py    # leaf / global / index metadata
├── generate_datasets.py    # parquet / webdataset / extract / decompress / smart-update / run-all
├── read_datasets.py        # parquet‐only extract & shard decompress helpers

LICENSE                     # MIT license
README.md                   # This document
```

---

## ⚙️ Installation

Requires Python 3.7+ and:

```bash
pip install pandas numpy pillow rich pyarrow typer
```

---

## 🚀 Usage

### A. Metadata

#### 1. Per‑leaf metadata

```bash
python3 scripts/generate_metadata.py leaf wear/ \
  [--dry-run] [--verbose]
```

Writes a `metadata.json` in each leaf directory under `wear/`.

#### 2. Global metadata

```bash
python3 scripts/generate_metadata.py global_ wear/ \
  --template global_template.json \
  [--dry-run]
```

Loads a JSON template, updates `datasetInfo.lastUpdated`, writes top‑level `wear/metadata.json`.

#### 3. Aggregate index

```bash
python3 scripts/generate_metadata.py index wear/ \
  [--output all_leaf_index.json] [--dry-run]
```

Scans every leaf’s `metadata.json` and writes a combined `index.json`.

---

### B. Data Conversion & Sharding

All commands assume your raw PNGs live in `wear/`.

#### 1. Parquet Conversion

```bash
python3 scripts/generate_datasets.py parquet \
  --input-dir wear \
  --output-dir data/parquet_data \
  --workers 8
```

- Compresses each PNG → JPEG (quality=50)  
- Writes per‑sample Parquet:  
  `data/parquet_data/<Material>/<Shape>/<Direction>/S<grit>/<load>g/<distance>mm/[rep<…>.parquet|data.parquet]`  
- Creates `data/parquet_data/metadata.parquet`

#### 2. WebDataset Shards

```bash
python3 generate_datasets.py webdataset \
  --input-dir wear \
  --output-dir data/webdataset_shards \
  --shard-size 100 \
  --workers 8
```

Encodes JPEG + JSON and shards into `.tar` files under `data/shards/`.


#### Smart‑Update & Full Pipeline

- **Smart‑update** (only changed files):

  ```bash
  python generate_datasets.py smart-update \
    --wear-dir wear \
    --parquet-dir data/parquet_data \
    --shards-dir data/webdataset_shards \
    --shard-size 100 \
    --workers 8
  ```

- **Full rebuild**:

  ```bash
  python scripts/generate_datasets.py run-all \
    --wear-dir wear \
    --parquet-dir data/parquet_data \
    --shards-dir data/webdataset_shards \
    --shard-size 100 \
    --workers 8
  ```

---

### C. Reconstruction

#### 3. Extract from Parquet

```bash
python3 read_datasets.py extract \
  --parquet-dir data/parquet \
  --output-dir data/images \
  [--material Cartboard] [--shape Circle] [--direction Linear] \
  [--grit 60] [--weight 100] [--distance 1200] [--workers 8]
```

Rebuilds the original PNG maps into the same folder hierarchy under `data/images/`.

#### 4. Decompress WebDataset

```bash
python3 read_datasets.py decompress \
  --shards-dir data/shards \
  --output-dir data/images \
  --workers 4
```

Unpacks each `.tar` back into `data/images/...`.


---

## 🌲 Example `wear/` Structure

```
wear/
└── Cartboard/
    └── Circle/
        └── Linear/
            └── S60/
                ├── 100g/0mm/       ← ao.png, bump.png, …, metadata.json
                ├── 100g/1200mm/
                └── 100g/2400mm/2/  ← replicate #2
```

---

## 📄 License

Released under the **MIT License**. See [LICENSE](LICENSE) for details.
