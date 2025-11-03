# 📶 Cellular Score Documentation 
This document outlines the methodology used to compute a **daily cellular performance score** for each **Fixed Wireless Access (FWA)** service account (MDN).  
The score integrates **Availability**, **Link Capacity / Quality**, and **Throughput** into a single overall indicator of network performance.

<img width="1550" height="305" alt="Screenshot 2025-11-02 at 5 10 11 PM" src="https://github.com/user-attachments/assets/157fe70e-beac-4c79-bf2d-81b9fc14e982" />

---

## 🧮 Overview

- A **daily score** is calculated per **MDN** (FWA service account).  
- The score summarizes:
  1. **Availability** – measures uptime and connection failures.  
  2. **Link Capacity / Quality** – evaluates bandwidth potential and signal strength.  
  3. **Throughput** – measures actual data rates experienced by users.  
- Weighted combination of these components yields the **Overall Cellular Score**.  
- Applied only to **VCG devices** *without mmWave support*:  
  `Titan 1/1.5  |  Titan 2  |  Titan 3  |  Titan 4`.

---

## ⚙️ Availability Score

**Objective:** Estimate the percentage of time Internet service was **not available** during the day.

    Not Available % = (Service Downtime + 0.01 × RACH Failure Count + Connection Failure Count) / Service Time

### Calculation

  - **Service Downtime** – actual offline duration.  
  - **RACH Failures** – random-access connection failures (LTE & NR). Each adds 0.01 s downtime.  
  - **Connection Failures** – failed sessions; each adds 1 s downtime.  
  - **Availability Score** applies a ×20 amplification to penalize even short outages.  
  - Minimum score = 0 (even if computed value is negative).  
  - **Error Checks:**  
    - Use daily deltas of cumulative counters.  
    - Handle counter resets or spikes.  
    - Split downtime that spans midnight across days.

---

## 📡 Link Capacity / Quality Score

**Goal:** Quantify potential bandwidth and signal quality of the FWA link.

### A. Bandwidth Estimation
Derived per record from network band info:

| Fields | Examples |
|---------|-----------|
| `_4gsnr`, `_5gsnr` | Signal-to-Noise Ratios |
| `4GPccBand`, `5GPccBand` | Primary component carriers |
| `5GEARFCN_DL` | Downlink frequency |
| `_nwbandwidth`, `_cbandbandwidths` | Estimated bandwidths (MHz) |

### B. Quality %
Compare measured SINR to best-possible values for 4G and 5G.

### C. Capacity per Record

- **Service Downtime** – actual offline duration.  
- **RACH Failures** – random-access connection failures (LTE & NR). Each adds 0.01 s downtime.  
- **Connection Failures** – failed sessions; each adds 1 s downtime.  
- **Availability Score** applies a ×20 amplification to penalize even short outages.  
- Minimum score = 0 (even if computed value is negative).  
- **Error Checks:**  
  - Use daily deltas of cumulative counters.  
  - Handle counter resets or spikes.  
  - Split downtime that spans midnight across days.

---

## 📡 Link Capacity / Quality Score

**Goal:** Quantify potential bandwidth and signal quality of the FWA link.

### A. Bandwidth Estimation
Derived per record from network band info:

| Fields | Examples |
|---------|-----------|
| `_4gsnr`, `_5gsnr` | Signal-to-Noise Ratios |
| `4GPccBand`, `5GPccBand` | Primary component carriers |
| `5GEARFCN_DL` | Downlink frequency |
| `_nwbandwidth`, `_cbandbandwidths` | Estimated bandwidths (MHz) |

### B. Quality %
Compare measured SINR to best-possible values for 4G and 5G.

### C. Capacity per Record
  
    Capacity = (4G Quality % × 4G Bandwidth) + (5G Nationwide Quality % × 5G Bandwidth) + (5G C-Band Quality % × C-Band Bandwidth)

Average across records → MDN-level capacity.

### D. Error Checks
- Exclude records with invalid SINR (< -10, > 40, = 0) or zero bandwidth.

---

## ⚡ Throughput Score

### 1. Score Components
| Source | Purpose | Notes |
|---------|----------|-------|
| **Ookla Speed Test** | Measures DL/UL speeds | Capped at 100% of plan limit |
| **Ultragauge Data** | Measures max DL speed per day | Compared to plan cap; max 100% |

### 2. Weighted Aggregation
Weights reflect network resource ratios and sample frequency:

| Component | Weight |
|------------|---------|
| Download (DL) | 4 |
| Upload (UL) | 1 |
| Ultragauge (UG) | 28 |

Reasoning: C-Band allocates ≈ 4× more DL than UL, and Ultragauge data arrives ~7× more frequently than speedtests.

### 3. Missing-Value Handling
- If UG DL Score > 100 → set to 100.  
- If all zero → omit component.  
- If any component missing → exclude its weight from aggregation.

---

## 🧩 Final Combined Score

Weighted average of the three components:

| Component | Weight |
|------------|---------|
| Availability | 5 |
| Capacity / Quality | 2 |
| Throughput | 1 |


    Final Score = (5 × Availability + 2 × Capacity + 1 × Throughput) / Sum of available weights

If a component is missing, its weight is dropped.  
If all components missing → score = NULL.

### Examples
| Scenario | Availability | Capacity | Throughput | Result |
|-----------|--------------|-----------|-------------|---------|
| **All available** | 80 | 70 | 90 | Weighted mean = ~80 % |
| **Missing Throughput** | 80 | 70 | — | Weighted mean of 5 & 2 only |

---

## 🏷️ Categorical Mapping (PySpark Logic)

```python
# Throughput
when(col("throughput_score") >= 80, "Excellent") \
.when(col("throughput_score") >= 60, "Good") \
.when(col("throughput_score") >= 30, "Fair") \
.otherwise("Poor")

# Availability
when(col("availability_score") == 100, "Excellent") \
.when(col("availability_score") >= 99.77, "Good") \
.when(col("availability_score") >= 97.22, "Fair") \
.otherwise("Poor")

# Capacity / Quality
when(col("capacity_score") >= 80, "Excellent") \
.when(col("capacity_score") >= 50, "Good") \
.when(col("capacity_score") >= 30, "Fair") \
.otherwise("Poor")
