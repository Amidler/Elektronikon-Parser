# 🧰 AtlasCopco Elektronikon MkV Data Monitor

> Python tool for reading, decoding, and logging compressor data from **Atlas Copco Elektronikon MkV** controllers via their embedded web interface (`mkv.cgi`).

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
- Console output and influxDB logging

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

