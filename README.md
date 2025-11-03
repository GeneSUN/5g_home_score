# 📶 Cellular Score Documentation (Z. Sun)

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

