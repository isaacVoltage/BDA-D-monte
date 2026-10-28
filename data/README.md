# RiverWatch Dataset Documentation

## 1. Dataset Origin & Background
This dataset is modeled after the **National Water Quality Monitoring Programme (NWMP)** administered by the **Central Pollution Control Board (CPCB)**, Ministry of Environment, Forest and Climate Change, Government of India.

CPCB monitors water quality across thousands of stations on major Indian rivers (Ganga, Yamuna, Godavari, Krishna, Kaveri, Sabarmati, Narmada, Brahmaputra, Mahanadi, Sutlej) across multiple states.

> **Status Notice**: The included dataset contains **simulated environmental observations based on published CPCB scientific profiles and geographic coordinates of real monitoring stations**. It is clearly labeled as simulated/demonstration data for college mini-project research and analytics pipeline benchmarking. It does not represent live telemetry from government sensors.

---

## 2. Water Quality Standards (CPCB Designated Best Use)
The CPCB classifies inland surface waters into primary water-use classes:

| Parameter | Unit | CPCB Class A (Drinking Water without treatment) | CPCB Class B (Outdoor Bathing) | CPCB Class C (Drinking Water with treatment) | Critical Threshold (Hotspot Indicator) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **pH** | Standard Units | 6.5 – 8.5 | 6.5 – 8.5 | 6.0 – 9.0 | < 6.5 or > 8.5 |
| **Dissolved Oxygen (DO)** | mg/L | ≥ 6.0 | ≥ 5.0 | ≥ 4.0 | < 5.0 mg/L (Critical < 4.0) |
| **Biochemical Oxygen Demand (BOD)** | mg/L | ≤ 2.0 | ≤ 3.0 | ≤ 3.0 | > 3.0 mg/L (Critical > 8.0) |
| **Fecal Coliform (FC)** | MPN / 100 mL | ≤ 50 | ≤ 500 (desirable), 2500 (max) | ≤ 5000 | > 2500 MPN / 100 mL |
| **Total Coliform (TC)** | MPN / 100 mL | ≤ 50 | ≤ 500 | ≤ 5000 | > 5000 MPN / 100 mL |
| **Nitrate ($NO_3$)** | mg/L | ≤ 20.0 | ≤ 20.0 | ≤ 50.0 | > 10.0 mg/L |
| **Conductivity** | µmhos/cm | ≤ 275 | ≤ 1000 | ≤ 1500 | > 1000 µmhos/cm |
| **Turbidity** | NTU | ≤ 5.0 | ≤ 10.0 | ≤ 50.0 | > 25.0 NTU |

---

## 3. Data Schema and Column Definitions

| Column Name | PySpark Data Type | Description | Unit / Format | Example Value |
| :--- | :--- | :--- | :--- | :--- |
| `station_id` | `StringType` | Unique identifier for the river monitoring station | Text code | `CPCB_YAM_03` |
| `station_name` | `StringType` | Common geographic landmark or ghat name | Text | `Delhi - ITO Bridge` |
| `river_name` | `StringType` | Name of the river basin | Text | `Yamuna` |
| `state` | `StringType` | Indian State or Union Territory | Text | `Delhi` |
| `district` | `StringType` | Administrative district | Text | `Central Delhi` |
| `latitude` | `DoubleType` | Geographic latitude coordinate (WGS84) | Decimal Degrees | `28.6289` |
| `longitude` | `DoubleType` | Geographic longitude coordinate (WGS84) | Decimal Degrees | `77.2514` |
| `sampling_date` | `DateType` | Date of field water sample collection | `YYYY-MM-DD` | `2023-04-12` |
| `temperature_c` | `DoubleType` | In-situ water temperature | °Celsius | `26.5` |
| `dissolved_oxygen_mg_l` | `DoubleType` | Dissolved oxygen concentration | mg/L | `1.8` |
| `ph` | `DoubleType` | Hydrogen ion concentration | pH units (0-14) | `8.2` |
| `conductivity_umhos_cm`| `DoubleType` | Electrical conductivity | µmhos/cm at 25°C| `1240.0` |
| `bod_mg_l` | `DoubleType` | 5-day Biochemical Oxygen Demand at 20°C | mg/L | `16.4` |
| `nitrate_mg_l` | `DoubleType` | Dissolved nitrate concentration | mg/L | `18.2` |
| `fecal_coliform_mpn_100ml`| `LongType` | Microbial fecal coliform density | MPN / 100 mL | `34000` |
| `total_coliform_mpn_100ml`| `LongType` | Total coliform bacteria density | MPN / 100 mL | `78000` |
| `turbidity_ntu` | `DoubleType` | Water clarity/cloudiness | NTU | `45.0` |

---

## 4. Intentional Data Quality Anomalies Included
For evaluating Big Data Analytics cleaning and validation pipelines, this dataset incorporates realistic data anomalies:
1. **Missing values (NULL/NaN)**: Simulating temporary sensor dropouts or lab omissions.
2. **Duplicate rows**: Simulating repeated ingestion batch uploads.
3. **Out-of-range physical readings**: Negative values or values exceeding physical bounds (e.g. pH > 14 or DO < 0).

Our PySpark pipeline explicitly identifies, logs, audits, and cleans these records before analytical aggregations.
