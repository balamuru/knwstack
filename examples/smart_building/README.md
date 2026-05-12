# Smart Building Reference Implementation

This example demonstrates how to build a KnwStack application that processes HVAC telemetry from a smart building using the **Split-Brain Architecture**.

## 1. Setup & Activate

Ensure NATS is running in the root directory (`docker compose up -d`). Then, set up and activate your environment:

```bash
uv sync
source .venv/bin/activate
```

## 2. Run the Application

Once activated, you can run the application directly with plain Python:

```bash
python app.py
```

### 🚀 Hybrid Performance
This example is configured in **Hybrid Mode** by default:
- **Alarms** use the **SuperHot (Push)** path for absolute minimum latency.
- **Telemetry** uses the **Reliable (Pull)** path to ensure zero data loss for AI analysis.

The framework automatically provisions the necessary NATS JetStream infrastructure on startup.

## 3. Test the System (Interactive Menu)

The easiest way to test the system is to use the **Interactive Generator**. In a new terminal window, **activate the venv** and run:

```bash
python generator.py
```

This will open a color-coded menu where you can trigger each path (Nominal, Hot, Warm, Cold) as many times as you like without restarting.

### Scenario Definitions
If you prefer single-shot commands, you can still use the `--mode` flag:

### A. Nominal Telemetry (Reliable Path)
**What it does**: Simulates standard room temperature and power usage data flowing into the system. This populates the "Reliable" stream, ensuring that history is available for subsequent Tactical or Strategic analysis.
```bash
python generator.py --mode telemetry
```
**Expected Output**: The engine terminal will show silent ingestion.
**Why**: Nominal data doesn't require immediate action; it is stored to build context for the rolling windows used by the Warm and Cold paths.

---

### B. Hot Path: Fire Alarm (Immediate Reflex)
**What it does**: Injects a high-priority "fire" alarm event.
```bash
python generator.py --mode fire_alarm
```
**Expected Output**: `🚨 FIRE ALARM DETECTED! Executing reflex action: SHUTDOWN HVAC`
**Why**: In a fire, every millisecond counts to prevent the ventilation system from spreading smoke. The **Reflex Path** bypasses all windowing and complex logic to execute a deterministic shutdown in <10ms.

---

### C. Warm Path: High Temp (Tactical Response)
**What it does**: Injects a series of temperature readings exceeding 28°C.
```bash
python generator.py --mode high_temp
```
**Expected Output**: `⚠️ High average temperature detected (...°C). Increasing cooling.`
**Why**: HVAC systems shouldn't react to a single "jittery" sensor reading. The **Tactical Path** uses a 5-second sliding window to calculate a rolling average. This ensures the cooling system only ramps up if the heat is sustained, preventing inefficient equipment "cycling."

---

### D. Cold Path: Anomaly Detection (Strategic Diagnosis)
**What it does**: Injects an anomalous state: High Power Draw (>10kW) but Low Temperature (<20°C).
```bash
# Ensure you have OPENAI_API_KEY in your .env or environment!
python generator.py --mode anomaly
```
**Expected Output**: `🧠 Anomalous power/temp correlation detected. Dispatching to LLM...` followed by an AI diagnosis.
**Why**: This scenario indicates a mechanical failure (e.g., a frozen compressor or a stuck valve) that simple rules cannot diagnose. The **Strategic Path** captures the last 10 seconds of context and sends it to an LLM to provide a reasoned diagnosis without blocking the critical Reflex or Tactical paths.
