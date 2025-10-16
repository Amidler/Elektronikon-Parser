# 🧰 AtlasCopco Elektronikon MkV Data Monitor

[![Atlas Copco Logo](https://www.atlascopco.com/content/dam/atlas-copco/corporate/logo/AC_LogoBlue_RGB.svg)](https://www.atlascopco.com)  
> Python tool for reading, decoding, and logging compressor data from **[Atlas Copco](https://www.atlascopco.com)** **Elektronikon MkV** controllers via their embedded web interface (`mkv.cgi`).

---

## 📦 Overview

This repository contains two generations of the Elektronikon monitoring script:

| Version | Web Interface | Description |
|----------|----------------|--------------|
| **v1.0.2.2** | Old MkV web (legacy firmware) | Basic implementation for early controllers using the classic CGI data model. |
| **v4.4.0.0** | New MkV web (modern firmware) | Complete re-implementation with extended register support, InfluxDB integration, and improved decoding. |

---

## 🧠 Functional Comparison

### ⚙️ Legacy Script — `v1.0.2.2`

**Features:**
- Simple communication with `mkv.cgi`
- Partial decoding of `Analog Inputs` and `Counters`
- Fixed register addresses (no dynamic discovery)
- Console output and InfluxDB logging

**Limitations:**
- Incorrect scaling for some parameters (temperature, pressure)

---

### 🚀 Modern Script — `v4.4.0.0`

**New Capabilities:**
- Full decoding of the **new MkV protocol structure**:
  - 🧮 **Analog Inputs** — all temperature, pressure, and humidity sensors  
  - ⏱ **Counters** — runtime, load hours, VSD histogram, fan starts, etc.  
  - ⚡ **Converters (NEOS)** — RPM, current, flow  
  - 🧩 **Machine State** — mapped numeric codes to readable text  
  - 🛡 **Special Protections** — detection of active safety conditions  
- 🗄 **InfluxDB Integration**  
  - Periodic write (every 5 seconds)  
  - Logs *Analog Inputs*, *Converters*, and *Machine State*  
- ✅ Improved register parsing (signed/unsigned 16-bit & 32-bit)  
- 📈 Auto-scaling for physical units (°C, Bar, %, m³)  
- 🔁 Continuous monitoring and fault-tolerant HTTP session  

---

## 📊 Data Visualization

All data recorded by this script to **InfluxDB** can be visualized and analyzed using **[Grafana](https://grafana.com/)** dashboards.

A sample dashboard allows you to display:
- Real-time temperature and pressure trends  
- Compressor runtime, load hours, and flow  
- NEOS converter current and speed  
- Machine state transitions  

📸 Example visualization (Grafana dashboard):

![Grafana Dashboard Screenshot](./Screenshot%202025-10-16%20152356.png)

---

## ☁️ Example InfluxDB Record

```text
compressor,MachineState=Load Controller_Temperature=21.6,Compressor_Outlet=7.3,Relative_Humidity=5.0,Vessel_Pressure=7.9,Element_Outlet=70.3,Dryer_PDP=1.9,Ambient_Air=14.4,RPM=1866,Amps=149,Flow=52,MachineStateCode=28
