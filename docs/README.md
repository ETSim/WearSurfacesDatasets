# Surface Texture Dataset Documentation

## 1. Overview

**Name:** Surface Texture  
**Description:** High‑resolution surface textures captured from physical material samples under controlled wear conditions, with corresponding friction measurements.  
**Version:** 1.2.1  
**Last Updated:** 2025‑04‑17  

---

## 2. Material Profiles

Each material has its own entry under `materialProfiles`. Example for **Cartboard**:

- **materialProperties**
  - name: Cartboard  
  - type: Pressed paper‑based material  
  - description: Layered cellulose material with directional grain pattern  
  - **physicalProperties**
    - density: 0.5–0.8 g/cm³  
    - Young’s modulus: 2–5 GPa  
    - hardness: 20–40 (Shore D)  
    - tensileStrength: 15–30 MPa  
    - thermalConductivity: 0.05–0.1 W/m·K  
    - surfaceRoughness: Medium to high  
    - porosity: Medium to high  
  - **visualProperties**
    - baseColor: Light brown  
    - surfaceAppearance: Matte, visible fiber structure  
    - anisotropy:
      - Linear: High — distinct grain direction  
      - NoLinear: Low  
    - specularResponse: Very low  
    - microstructureScale: ~0.1–0.5 mm  

- **testParameters**
  - samplePreparation: Die‑cut samples from single‑source material  
  - surfacePreparation: None  
  - note: Standard commercial cardboard used  

- **wearTestResults**
  - wearRate: 0.3–0.8 mg/Nm  
  - coefficientOfFriction: 0.35–0.45  
  - abrasionResistance: Low  
  - failureMode: Fiber tearing, delamination  
  - moistureSensitivity: High  

> *(Similar entries exist for FOAM, MDF, PLA, Sandpaper, Soft_Wood.)*

---

## 3. Surface (Grit) Properties

| Grit  | Description  | Abrasive Type | Avg. Particle Size | Standard       |
|------:|--------------|---------------|--------------------|----------------|
| S40   | Coarse       | Al₂O₃         | 425 µm             | FEPA P‑grade   |
| S60   | Med‑coarse   | Al₂O₃         | 268 µm             | FEPA P‑grade   |
| S80   | Medium       | Al₂O₃         | 201 µm             | FEPA P‑grade   |
| S120  | Med‑fine     | Al₂O₃         | 115 µm             | FEPA P‑grade   |
| S220  | Fine         | Al₂O₃         | 68 µm              | FEPA P‑grade   |
| S240  | Ultra‑fine   | Al₂O₃         | 58 µm              | FEPA P‑grade   |

---

## 4. Test Parameters

### Wear Test

- apparatus: Custom linear reciprocating tester  
- loadRanges: 10g, 20g, 50g, 100g, 200g  
- distanceRanges: 0mm … 8000mm  
- speed: 10 mm/s  
- ambient: 23 ± 2 °C, 50 ± 5 % RH  
- friction: strain gauge (± 0.01 CoF, 100 Hz)  
- wear: gravimetric (± 0.1 mg) + 3D profilometry  

### Texture Capture

- method: Photometric stereo  
- scanner: Gelsight 3D  
- camera: Canon EOS 5D Mark III  
- resolution: 6022 × 4024 px  
- lighting: 8 × polarized LED  
- verticalResolution: 5 µm  
- horizontalResolution: 10 µm  

---

## 5. Map Types

| Key          | Name               | Format       | Usage                       | Extension |
|--------------|--------------------|--------------|-----------------------------|-----------|
| ao           | Ambient Occlusion  | 8‑bit gray   | Ambient lighting modulator | png       |
| bump         | Bump Map           | 8‑bit gray   | Surface perturbation        | png       |
| displacement | Displacement       | 16‑bit gray  | Geometry displacement       | png       |
| height       | Height             | 16‑bit gray  | Precise elevation           | png       |
| hillshade    | Hill Shade         | 8‑bit gray   | Visualization               | png       |
| normal       | Normal Map         | 24‑bit RGB   | Lighting detail             | png       |
| roughness    | Roughness Map      | 8‑bit gray   | Specular control            | png       |

---

## 6. Directory Structure

```
wear/
└── <Material>/             
    └── <Shape>/           # Circle or Line
        └── <Direction>/   # Linear or NoLinear
            └── <Grit>/    # e.g. S60
                └── <Load>/      # e.g. 100g
                    └── <Distance>/   # e.g. 1200mm
                        └── [<Replicate>/]  # optional “1”, “2”, …
                            ├── ao.png
                            ├── bump.png
                            ├── … 
                            └── metadata.json
```

---

## 7. Leaf Metadata Example

```json
{
  "material": "Cartboard",
  "shape": "Circle",
  "direction": "Linear",
  "grit": 60,
  "load_g": 100,
  "distance_mm": 1200,
  "replicate": 1,
  "files": {
    "ao": "ao.png",
    "bump": "bump.png",
    "displacement": "displacement.png",
    "height": "height.png",
    "hillshade": "hillshade.png",
    "normal": "normal.png",
    "roughness": "roughness.png"
  }
}
```

---

## 8. Data Formats

- **Images:** PNG  
- **Metadata:** JSON  
- **Database:** Parquet (snappy‑compressed)  
- **Shards:** TAR archives (WebDataset format)

---

## 9. WebDataset Shards

- **Purpose:** Packs each image + its JSON metadata into sharded `.tar` files for scalable loading.  
- **Structure:**  
  ```
  webdataset_shards/
  ├── shard-0000.tar
  ├── shard-0001.tar
  └── ...
  ```
- **Contents of each shard:**  
  - `<idx>.jpg`  — JPEG-encoded image  
  - `<idx>.json` — metadata JSON with keys: `material`, `shape`, `direction`, `grit`, `weight`, `distance`, `replicate`, `image_type`, etc.

---
