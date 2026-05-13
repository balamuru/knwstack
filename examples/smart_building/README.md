# Smart Building Reference Implementation

This example demonstrates how to build a KnwStack application that processes HVAC telemetry from a smart building using the **Split-Brain Architecture**.

## 1. Setup & Activate

Ensure NATS is running in the root directory (`docker compose up -d`). Then, set up and activate your environment:

```bash
uv sync
source .venv/bin/activate
```

## 2. Start the Engine
In your primary terminal, run:
```bash
python app.py --log INFO
```
*Note: Use `--log DEBUG` if you want to see every single event ingestion, or `--log WARNING` for a production-clean output.*

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

## 4. Run Automated Test Suite

To verify the internal logic of the rules without running the full engine/NATS, you can use the automated test suite:

```bash
cd examples/smart_building
pytest tests/test_rules.py
```

This will validate:
- **Reflex Logic**: Immediate shutdown on fire.
- **Tactical Logic**: Windowed averaging and cooling triggers.
- **Strategic Logic**: Complex anomaly detection for LLM prompts.

## 5. Performance Benchmarking
The sample generator provides 4 scenarios that demonstrate how events flow through the Split-Brain:

1.  **🟢 Nominal Telemetry**: **Baseline Ingestion**. Fills the state windows but does not trigger an action.
2.  **🔴 Fire Alarm**: **Hot Path (Reflex)**. Triggers a sub-10ms hard shutdown.
3.  **🟠 High Temp**: **Warm Path (Tactical)**. Triggers a sub-100ms warning based on a 5-second window.
4.  **🔵 Anomaly**: **Cold Path (Strategic)**. Dispatches an async request to an LLM for diagnosis.

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
