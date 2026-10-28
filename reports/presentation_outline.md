# RiverWatch: Viva Presentation Outline
## Big Data Analytics (BDA) College Mini-Project Presentation Deck (12 Slides)

---

### Slide 1: Title Slide
- **Title:** RiverWatch: Indian River Water Quality Analytics and Pollution Hotspot Detection Using PySpark
- **Domain:** Big Data Analytics (BDA) / Distributed Environmental Data Engineering
- **Team / Student Name:** [Student Name & Roll Number]
- **Academic Mentor / Guide:** [Professor / Department Guide]
- **Department:** Department of Computer Science & Engineering / Information Technology

---

### Slide 2: Problem Statement & Motivation
- **Context:** Indian river basins sustain 1.4B+ citizens, yet rampant urban sewage and industrial discharges threaten surface water safety.
- **The Data Challenge:** Thousands of CPCB NWMP monitoring stations report spatio-temporal telemetry with frequent missing values, duplicate uploads, and non-linear chemical interactions.
- **The Core Problem:** Legacy spreadsheet/single-node tools fail to scale, lack automated data quality auditing, and cannot efficiently perform multi-year distributed aggregations or statistical anomaly detection.

---

### Slide 3: Project Objectives
- Ingest surface water quality data using **Apache Spark (PySpark)** with strict schema enforcement.
- Implement an automated, fault-tolerant **data hygiene and auditing pipeline** (deduplication, date parsing, physical range quarantining, missingness tracking).
- Build a transparent **CPCB-compliant hotspot detection rule engine** and compute standardized Water Quality Index (WQI).
- Implement PySpark **Window Z-score anomaly detection** ($> 2.5\sigma$) to spot extreme sensor spikes.
- Deliver an interactive, modern **Streamlit dashboard** with Light/Dark mode, geospatial mapping, and automated report exports.

---

### Slide 4: System Architecture & Workflow
- **Data Layer:** CPCB-modeled multi-river dataset across 40 stations on 10 major Indian rivers + configurable benchmark generator.
- **Engine Layer:** PySpark DataFrame transformations, caching, and distributed aggregations.
- **Analytics Layer:** Statutory CPCB Class B rules + weighted arithmetic WQI formulation.
- **Presentation Layer:** Streamlit reactive UI, Plotly interactive graphics, Folium geospatial intelligence.

---

### Slide 5: Environmental Standards (CPCB Class B Criteria)
- **Primary Use Baseline:** CPCB Designated Best Use Class B (Outdoor Bathing):
  - **Dissolved Oxygen (DO):** $\ge 5.0$ mg/L (Critical hypoxia: $< 4.0$ mg/L)
  - **Biochemical Oxygen Demand (BOD):** $\le 3.0$ mg/L (Severe organic loading: $> 8.0$ mg/L)
  - **pH:** $6.5 - 8.5$
  - **Fecal Coliform (FC):** $\le 2,500$ MPN/100 mL
  - **Nitrate:** $\le 10.0$ mg/L

---

### Slide 6: PySpark Data Preprocessing & Hygiene Pipeline
- **Explicit Schema:** Ingests CSV using PySpark `StructType` rather than unsafe type inference.
- **Fault-Tolerant Parsing:** Uses `try_to_date` to prevent ANSI parse exceptions on corrupt dates.
- **Deduplication:** Groups by `[station_id, sampling_date]` and eliminates duplicate upload batches.
- **Single-Pass Outlier Quarantine:** Evaluates all physical ranges in one aggregated PySpark pass rather than repeated slow DataFrame scans.
- **Data Health Score:** Calculates an overall data integrity percentage reported in the dashboard.

---

### Slide 7: Hotspot Detection Logic & WQI Formulation
- **Weighted Arithmetic WQI Formulation:**
  $$WQI = \frac{\sum w_i \cdot q_i}{\sum w_i}$$
  Parameters: DO (30%), BOD (25%), pH (15%), Fecal Coliform (15%), Nitrate (10%), Turbidity (5%).
- **Dynamic Weight Renormalization:** Handles missing sensor fields mathematically without inventing artificial data.
- **Multi-Tier Severity Classification:**
  - 🔴 **Critical:** $DO < 4.0$ mg/L AND $BOD > 8.0$ mg/L, or $\ge 3$ parameter violations ($WQI < 35$).
  - 🟠 **High Priority:** $BOD > 6.0$ mg/L or $FC > 10,000$ MPN/100mL ($WQI < 55$).
  - 🟡 **Moderate:** 1 parameter breach.
  - 🟢 **Safe / Compliant:** Fully compliant with CPCB standards.

---

### Slide 8: Interactive Web Dashboard Demonstration
- **Key Tabs:**
  1. **Overview:** Top KPI cards, chemistry health vs CPCB standards, river summary table.
  2. **Water Quality Explorer:** Side-by-side station comparator and searchable raw data table.
  3. **Trend Analysis:** Multi-river time-series trajectories with CPCB threshold lines, correlation heatmap, and PySpark Z-score anomalies.
  4. **Hotspot Detection & Map:** Interactive Folium map with color-coded station markers and ranked leaderboard.
  5. **Data Quality Audit:** Missing value audit, duplicate counts, out-of-range sensor audit.
  6. **Insights & Export:** Automated analytical narrative and Markdown / CSV download buttons.

---

### Slide 9: Light and Dark Theme Engineering
- Custom CSS injected dynamically based on user toggle.
- Synchronized color mapping across Plotly charts (`plotly_dark` vs `plotly_white`), Folium tiles (`CartoDB dark_matter` vs `CartoDB positron`), and UI metric cards.
- High accessibility contrast compliant with modern web application standards.

---

### Slide 10: Testing, Reliability & Verification
- Comprehensive **PyTest automated test suite**:
  - `test_standardize_column_names`: Verifies lowercase and snake_case column normalization.
  - `test_clean_and_audit_pipeline`: Validates duplicate removal, date parsing, and out-of-bounds sanitization.
  - `test_filter_dataframe`: Validates multi-dimensional spatial/temporal filtering.
  - `test_evaluate_pristine_record`: Validates Class A/B classification.
  - `test_evaluate_critical_hotspot_record`: Validates severe multi-parameter hotspot detection.
  - `test_wqi_missing_parameter_handling`: Validates weight renormalization on partial data.
- **Test Result:** 100% Pass rate (6/6 tests passing in PyTest).

---

### Slide 11: PySpark Scalability & Performance Benchmarks
- Included **Scalable Benchmark Generator** within the dashboard UI.
- Generates 1,000 to 20,000+ synthetic observation records across 40 stations on-the-fly.
- Demonstrates PySpark partition parallelism (`spark.sql.shuffle.partitions = 4`) and in-memory execution efficiency on student laptops.

---

### Slide 12: Conclusion & Viva Q&A
- **Summary:** RiverWatch demonstrates a complete, transparent, distributed Big Data Analytics pipeline for Indian river water quality management.
- **Future Scope:** Apache Kafka streaming ingestion from live IoT water probes, and PySpark MLlib predictive regression models for 7-day biochemical oxygen demand forecasting.
- **Ready for Questions:** Open for examiner questions on PySpark execution DAG, WQI formulation, or data cleaning logic.
