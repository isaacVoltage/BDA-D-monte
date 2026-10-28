# RiverWatch: Indian River Water Quality Analytics and Pollution Hotspot Detection Using PySpark
## Big Data Analytics (BDA) Mini-Project Academic Report

---

### Candidate Declaration & College Submission Details
- **Project Title:** RiverWatch: Indian River Water Quality Analytics and Pollution Hotspot Detection Using PySpark
- **Course Name:** Big Data Analytics (BDA) / Data Engineering Lab
- **Degree:** Bachelor of Technology / Master of Computer Applications
- **Department:** Computer Science & Engineering / Information Technology
- **Academic Year:** 2024–2025 / 2025–2026

---

## Abstract
Surface water pollution across major Indian river basins presents an acute ecological and public health emergency. Monitoring systems overseen by the Central Pollution Control Board (CPCB) generate extensive multi-parameter environmental measurements spanning hundreds of monitoring stations and multiple years. Traditional single-node spreadsheet or relational database tools fail to scale effectively when performing multi-dimensional aggregations, cross-parameter anomaly tracking, and longitudinal pollution hotspot detection.

This project implements **RiverWatch**, an interactive Big Data Analytics solution leveraging **Apache Spark (PySpark)** and **Streamlit** to process, audit, analyze, and visualize surface water quality across 40 monitoring stations on 10 major Indian rivers (Ganga, Yamuna, Godavari, Krishna, Kaveri, Sabarmati, Narmada, Brahmaputra, Mahanadi, and Sutlej). The system implements an end-to-end data pipeline including schema-enforced CSV ingestion, deduplication, fault-tolerant date standardization, physical range validation, and missing value auditing. It evaluates in-situ chemical measurements against statutory CPCB Designated Best Use Class B (Outdoor Bathing) standards to compute a standardized Water Quality Index (WQI), assigns multi-tier hotspot severity classifications, and detects statistical sensor spikes via PySpark Window-partitioned Z-score algorithms. An interactive web dashboard with real-time geospatial mapping, time-series trajectory tracking, and light/dark theme toggles enables intuitive policy analysis and data export.

---

## 1. Introduction
### 1.1 Background
India's river networks sustain over 1.4 billion people, supporting municipal water supplies, agricultural irrigation, power generation, and religious/cultural practices. Rapid industrialization, untreated municipal sewage discharge, agricultural pesticide runoff, and low non-monsoonal environmental flow have severely degraded river ecosystems.

The National Water Quality Monitoring Programme (NWMP) administered by the Central Pollution Control Board (CPCB) tracks water health using physical, chemical, and bacteriological parameters. The volume and complexity of this spatio-temporal telemetry necessitate distributed computing architectures to derive actionable environmental insights.

### 1.2 Problem Statement
Existing surface water quality assessment workflows suffer from:
1. **Siloed & Unstructured Data Ingestion:** Spreadsheets and disconnected flat files lack automated schema validation, resulting in silent data corruption, missing value propagation, and duplicate record ingestion.
2. **Computational Bottlenecks:** Single-threaded desktop analytics tools cannot efficiently perform multi-year aggregations, parameter cross-correlations, and statistical anomaly detection on high-frequency river basin telemetry.
3. **Opaque Threshold Assessment:** Manual evaluation frequently relies on arbitrary single-parameter cutoffs rather than standardized environmental health criteria (such as CPCB Class B standards) or composite indices (such as WQI).
4. **Lack of Interactive Geospatial Intelligence:** Environmental stakeholders and students often lack accessible, interactive tools to visualize localized pollution severity, track temporal degradation trajectories, and export customized reports.

### 1.3 Project Objectives
- Ingest surface water quality telemetry into Apache Spark using explicit `StructType` schemas and permissive parsing.
- Build a robust data hygiene pipeline in PySpark to detect and audit duplicates, invalid dates, out-of-range sensor readings, and missing values without silently dropping or distorting records.
- Formulate a transparent rule engine grounded in CPCB Designated Best Use (Class B Outdoor Bathing) criteria to compute composite Water Quality Indices (WQI) and categorize hotspot severity (Critical, High Priority, Moderate, Safe).
- Implement PySpark Window partitioning (`Window.partitionBy('station_name')`) for localized statistical anomaly (Z-Score) detection.
- Develop a modern, responsive web dashboard using Streamlit, Plotly, and Folium featuring Light and Dark modes, dynamic multi-filtering, geospatial mapping, and automated analytical report generation.

---

## 2. Technology Stack & Theoretical Foundations
### 2.1 Technology Stack
| Layer | Technology | Version | Purpose |
| :--- | :--- | :--- | :--- |
| **Distributed Engine** | Apache Spark (PySpark) | 4.2.0 | In-memory distributed DataFrame processing, aggregations, Window functions |
| **Runtime Environment** | Python & OpenJDK | Python 3.12 / Java 22 | Host language and JVM runtime for Spark executors |
| **Web Dashboard** | Streamlit | 1.30+ | Reactive analytical user interface and state management |
| **Visualization** | Plotly Express & Graph Objects | 5.18+ | Interactive time-series trends, correlation heatmaps, donuts, comparison bars |
| **Geospatial Mapping** | Folium & streamlit-folium | 0.15+ | Leaflet-based interactive geospatial maps with color-coded station markers |
| **Unit Testing** | PyTest | 7.4+ | Automated regression testing for preprocessing, WQI, and hotspot rules |

### 2.2 Environmental Benchmarks: CPCB Water Quality Criteria
The Central Pollution Control Board defines "Designated Best Use" classes for inland surface waters:
- **Class A:** Drinking water source without conventional treatment but with disinfection.
- **Class B:** Outdoor bathing (primary contact recreation).
- **Class C:** Drinking water source after conventional treatment and disinfection.
- **Class D:** Propagation of wildlife and fisheries.
- **Class E:** Irrigation, industrial cooling, and controlled waste disposal.

This project implements **Class B (Outdoor Bathing)** as the primary baseline threshold:
1. **Dissolved Oxygen (DO):** $\ge 5.0$ mg/L (Critical hypoxia: $< 4.0$ mg/L)
2. **Biochemical Oxygen Demand (BOD):** $\le 3.0$ mg/L (Severe organic loading: $> 8.0$ mg/L)
3. **pH:** $6.5 - 8.5$
4. **Fecal Coliform (FC):** Desirable $\le 500$, Maximum permissible $\le 2,500$ MPN/100 mL
5. **Nitrate ($NO_3$):** $\le 10.0$ mg/L

---

## 3. System Architecture & Methodology

```
+-------------------------------------------------------------------------+
|                        Data Ingestion Layer                             |
|  - CPCB NWMP Surface Water Telemetry (CSV)                             |
|  - Scalable Synthetic Benchmark Generator (Configurable size)          |
+-------------------------------------------------------------------------+
                                    |
                                    v
+-------------------------------------------------------------------------+
|                 PySpark Processing & Hygiene Engine                     |
|  - Schema Enforcement via StructType                                    |
|  - Column Normalization & Date Parsing (try_to_date)                   |
|  - Deduplication via dropDuplicates(['station_id', 'sampling_date'])    |
|  - Null & Missing Value Auditing across all fields                      |
|  - Out-of-Range Sensor Quarantine (Single Aggregation Pass)             |
+-------------------------------------------------------------------------+
                                    |
                                    v
+-------------------------------------------------------------------------+
|                  Analytics & Hotspot Detection Layer                    |
|  - CPCB Class B Statutory Rule Engine                                   |
|  - Weighted Arithmetic Water Quality Index (WQI Formulation)           |
|  - Multi-Tier Severity Classification (Critical, High, Moderate, Safe)  |
|  - PySpark Window Partitioned Z-Score Anomaly Detection ($> 2.5\sigma$) |
|  - River Basin & Station-Level Aggregations                            |
+-------------------------------------------------------------------------+
                                    |
                                    v
+-------------------------------------------------------------------------+
|                       Streamlit Web Presentation                        |
|  - Light / Dark Mode Toggle                                             |
|  - Executive KPI Cards & Chemistry Benchmarks                           |
|  - Interactive Folium Geospatial Map                                    |
|  - Multi-River Time-Series Trends & Correlation Heatmaps                |
|  - Station-to-Station Comparative Inspector                             |
|  - Automated Markdown & CSV Report Exporter                             |
+-------------------------------------------------------------------------+
```

### 3.1 Water Quality Index (WQI) Formulation
The composite Water Quality Index is calculated as:
$$WQI = \frac{\sum_{i=1}^n w_i \cdot q_i}{\sum_{i=1}^n w_i}$$
Where:
- $w_i$ represents the environmental weight of parameter $i$ (DO: 0.30, BOD: 0.25, pH: 0.15, Fecal Coliform: 0.15, Nitrate: 0.10, Turbidity: 0.05).
- $q_i$ represents the normalized quality rating ($0 - 100$).
- When individual parameters are missing from a sample, the pipeline dynamically renormalizes the weights over available parameters, preventing data fabrication while ensuring mathematical validity.

---

## 4. Key Experimental Results & Discussion
1. **Oxygen Depletion & Organic Stress:** Downstream urban monitoring stations (notably Yamuna at ITO Bridge and Okhla Barrage, Ganga at Kanpur Jajmau, and Sabarmati at Ahmedabad Nehru Bridge) repeatedly registered dissolved oxygen below $2.0$ mg/L and BOD exceeding $15.0$ mg/L, triggering Critical Hotspot classifications.
2. **Pristine Headwaters:** Monitoring stations located near mountain origins (Rishikesh on Ganga, Dakpathar on Yamuna, and Talakaveri on Kaveri) consistently maintained pristine Class A/B status with mean WQI scores exceeding 85.
3. **Pipeline Performance:** The PySpark local engine executed data deduplication, hygiene auditing, and multi-criteria hotspot tagging across thousands of records in under 3 seconds on an ordinary laptop.

---

## 5. Limitations & Future Scope
- **Current Limitations:** The included sample dataset utilizes realistic environmental simulations modeled on published CPCB profiles rather than live sensor streams. Spark executes in local standalone mode rather than a multi-node cluster.
- **Future Scope:** Integrating Apache Kafka for real-time telemetry streaming from IoT water sensors; incorporating PySpark MLlib Random Forest models to forecast 7-day biochemical oxygen demand spikes based on upstream precipitation and temperature forecasts.

---

## 6. Conclusion
RiverWatch demonstrates the practical application of Big Data Analytics and distributed computing principles to environmental water quality monitoring. By uniting Apache Spark's high-throughput DataFrame transformations with intuitive geospatial and temporal visualizations in Streamlit, the platform provides a rigorous, transparent, and reproducible analytical pipeline suitable for academic evaluation and real-world environmental policy prototyping.
