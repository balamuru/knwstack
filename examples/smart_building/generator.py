import asyncio
import json
import random
import argparse
import curses
import time
from nats.aio.client import Client as NATS

class GeneratorTUI:
    def __init__(self, nc):
        self.nc = nc
        self.logs = []
        self.running = True
        self.selected_index = 0
        self.log_offset = 0
        self.options = [
            ("🟢 Nominal Telemetry", self.dispatch_telemetry, "Expected: SILENT (18-28°C)"),
            ("🔴 Fire Alarm       ", self.dispatch_fire_alarm, "Expected: REFLEX ACTION"),
            ("🟠 High Temp        ", self.dispatch_high_temp, "Expected: TACTICAL ALERT"),
            ("🔵 CEP: Anomaly Correlation", self.dispatch_anomaly, "Expected: STRATEGIC AI (Fuses Power + Temp)"),
            ("🏢 Campus Simulation", self.dispatch_campus_simulation, "Expected: NORMAL/HOT/COLD"),
            ("🚀 MEGA-STRESS TEST", self.dispatch_mega_stress, "High-Concurrency GIL-Bypass"),
        ]

    def add_log(self, msg):
        self.logs.append(f"[{time.strftime('%H:%M:%S')}] {msg}")
        # Auto-scroll to bottom
        if len(self.logs) > 10:
            self.log_offset = len(self.logs) - 10

    async def dispatch_telemetry(self):
        self.add_log("🟢 DISPATCH: TELEMETRY (Nominal)")
        self.add_log("   EXPECTED: SILENT (Temp 21-23°C in 18-28°C range)")
        for _ in range(5):
            payload = {
                "temperature": random.uniform(21.0, 23.0),
                "power_draw_kw": random.uniform(2.0, 3.5),
                "key": "bldg1",
                "timestamp": int(time.time() * 1000)
            }
            await self.nc.publish("bldg1.hvac.telemetry", json.dumps(payload).encode())
            await asyncio.sleep(0.05)
        await self.nc.publish("heartbeat", b"{}")
        self.add_log("✅ Nominal dispatch complete.")

    async def dispatch_fire_alarm(self):
        self.add_log("🔴 DISPATCH: FIRE_ALARM (Hot Path)")
        self.add_log("   EXPECTED: HOT REFLEX (Immediate HVAC Shutdown)")
        payload = {"type": "fire", "zone": "lobby"}
        await self.nc.publish("bldg1.hvac.alarm", json.dumps(payload).encode())
        await self.nc.publish("heartbeat", b"{}")
        self.add_log("✅ Fire alarm dispatch complete.")

    async def dispatch_high_temp(self):
        self.add_log("🟠 DISPATCH: HIGH_TEMP (Tactical Path)")
        self.add_log("   EXPECTED: TACTICAL ALERT (High Cooling action)")
        for _ in range(5):
            payload = {
                "temperature": random.uniform(29.0, 31.0),
                "power_draw_kw": random.uniform(4.0, 5.5),
                "key": "bldg1",
                "timestamp": int(time.time() * 1000)
            }
            await self.nc.publish("bldg1.hvac.telemetry", json.dumps(payload).encode())
            await asyncio.sleep(0.05)
        await self.nc.publish("heartbeat", b"{}")
        self.add_log("✅ High temp dispatch complete.")

    async def dispatch_anomaly(self):
        self.add_log("🔵 DISPATCH: CEP ANOMALY CORRELATION (Strategic Path)")
        self.add_log("   EXPECTED: STRATEGIC AI (Fusing High Power + Low Temp)")
        for _ in range(5):
            payload = {
                "temperature": random.uniform(18.0, 19.5),
                "power_draw_kw": random.uniform(11.0, 14.0),
                "key": "bldg1",
                "timestamp": int(time.time() * 1000)
            }
            await self.nc.publish("bldg1.hvac.telemetry", json.dumps(payload).encode())
            await asyncio.sleep(0.05)
        await self.nc.publish("heartbeat", b"{}")
        self.add_log("✅ Anomaly dispatch complete.")

    async def dispatch_campus_simulation(self):
        self.add_log("🏢 DISPATCH: CAMPUS SIMULATION (3-Way)")
        self.add_log("   EXPECTED: Alpha (SILENT), Beta (COOLING), Gamma (HEATING)")
        buildings = [
            ("bldg_alpha", 22.0, 2.5),
            ("bldg_beta", 32.0, 5.5),
            ("bldg_gamma", 15.0, 3.5)
        ]
        for b_id, t, p in buildings:
            for _ in range(3):
                payload = {
                    "key": b_id, "temperature": t, "power_draw_kw": p, 
                    "timestamp": int(time.time() * 1000)
                }
                await self.nc.publish("campus.telemetry", json.dumps(payload).encode())
                await asyncio.sleep(0.05)
        await self.nc.publish("heartbeat", b"{}")
        self.add_log("✅ Campus simulation complete.")

    def draw_screen(self, stdscr):
        stdscr.clear()
        h, w = stdscr.getmaxyx()

        # Header
        title = " KnwStack Smart Building Event Generator "
        stdscr.addstr(1, (w - len(title)) // 2, title, curses.A_REVERSE)

        # Menu
        menu_y = 4
        stdscr.addstr(menu_y, 4, "Navigate with Arrows, Press Enter to Dispatch:", curses.A_BOLD)
        
        for i, (label, _, expected) in enumerate(self.options):
            if i == self.selected_index:
                attr = curses.A_REVERSE | curses.color_pair(i + 1)
                stdscr.addstr(menu_y + 2 + i, 6, f" > {label} ", attr)
                stdscr.addstr(f"   ({expected})", curses.color_pair(6))
            else:
                stdscr.addstr(menu_y + 2 + i, 6, f"   {label} ", curses.color_pair(i + 1))

        # Logs
        log_start_y = h - 13
        stdscr.addstr(log_start_y, 4, "Recent Activity (PgUp/PgDn to scroll):", curses.A_UNDERLINE)
        
        visible_logs = self.logs[self.log_offset : self.log_offset + 10]
        for i, log in enumerate(visible_logs):
            stdscr.addstr(log_start_y + 2 + i, 6, log[:w-10])

        # Footer
        footer = f" NATS: Connected | Log: {len(self.logs)} events | 'Q' to Quit "
        stdscr.addstr(h - 2, (w - len(footer)) // 2, footer, curses.color_pair(1))
        
        stdscr.refresh()

    async def run(self, stdscr):
        curses.start_color()
        curses.init_pair(1, curses.COLOR_GREEN, curses.COLOR_BLACK)
        curses.init_pair(2, curses.COLOR_RED, curses.COLOR_BLACK)
        curses.init_pair(3, curses.COLOR_YELLOW, curses.COLOR_BLACK) 
        curses.init_pair(4, curses.COLOR_CYAN, curses.COLOR_BLACK) 
        curses.init_pair(5, curses.COLOR_MAGENTA, curses.COLOR_BLACK)
        curses.init_pair(6, curses.COLOR_WHITE, curses.COLOR_BLACK)
        
        stdscr.nodelay(True)
        stdscr.keypad(True)
        curses.curs_set(0)

        while self.running:
            self.draw_screen(stdscr)
            
            key = stdscr.getch()
            if key == curses.KEY_UP:
                self.selected_index = (self.selected_index - 1) % len(self.options)
            elif key == curses.KEY_DOWN:
                self.selected_index = (self.selected_index + 1) % len(self.options)
            elif key in [10, 13, curses.KEY_ENTER]:
                func = self.options[self.selected_index][1]
                await func()
            elif key == curses.KEY_PPAGE: # Page Up
                self.log_offset = max(0, self.log_offset - 5)
            elif key == curses.KEY_NPAGE: # Page Down
                self.log_offset = min(max(0, len(self.logs) - 10), self.log_offset + 5)
            elif key in [ord('q'), ord('Q')]:
                self.running = False
            
            await asyncio.sleep(0.02)

    async def dispatch_mega_stress(self):
        self.add_log("🚀 DISPATCH: MEGA-STRESS TEST (Horizontal Scale)")
        self.add_log("   Starting 8 processes / 100 workers / 5s burst...")
        # Launch as background task to avoid blocking TUI
        asyncio.create_task(run_stress_test(
            self.nc, duration=5, workers=100, processes=8, 
            log_func=self.add_log
        ))

async def run_stress_test(nc, duration=30, workers=50, processes=4, log_func=None):
    """Slams the app with concurrent traffic using multiple processes to bypass GIL."""
    import multiprocessing
    import httpx
    
    msg = f"🚀 MEGA-STRESS: Spawning {processes} processes with {workers} workers each for {duration}s..."
    if log_func: log_func(msg)
    else: print(msg)
    
    start_time = time.time()
    
    def process_entrypoint(p_id, num_workers, dur):
        async def run():
            from nats.aio.client import Client as NATS
            nc = NATS()
            await nc.connect("nats://localhost:4222")
            
            tasks = []
            for i in range(num_workers):
                async def worker(w_id):
                    b_id = f"p{p_id}_w{w_id}"
                    while time.time() - start_time < dur:
                        payload = {"t": 22.0, "p": 2.5, "k": b_id, "ts": int(time.time() * 1000)}
                        await nc.publish(f"stress.{b_id}", json.dumps(payload).encode())
                tasks.append(asyncio.create_task(worker(i)))
            
            await asyncio.sleep(dur)
            await nc.drain()
        
        asyncio.run(run())

    pool = []
    for i in range(processes):
        p = multiprocessing.Process(target=process_entrypoint, args=(i, workers, duration))
        p.start()
        pool.append(p)

    # Monitor metrics
    while any(p.is_alive() for p in pool):
        await asyncio.sleep(5)
        elapsed = time.time() - start_time
        try:
            async with httpx.AsyncClient() as client:
                res = await client.get("http://localhost:9090/metrics", timeout=1.0)
                if res.status_code == 200:
                    lines = res.text.split("\n")
                    latency = "N/A"
                    for line in lines:
                        if line.startswith("input_latency_ms"):
                            latency = line.split(" ")[1]
                    status = f"   [+{int(elapsed)}s] Engine Latency: {latency}ms"
                    if log_func: log_func(status)
                    else: print(status)
        except:
            pass

    for p in pool:
        p.join()
    
    final_msg = "✅ MEGA-STRESS COMPLETE."
    if log_func: log_func(final_msg)
    else: print(f"\n{final_msg}")

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stress", action="store_true", help="Run high-throughput stress test")
    parser.add_argument("--workers", type=int, default=50, help="Workers per process")
    parser.add_argument("--processes", type=int, default=4, help="Number of parallel processes")
    parser.add_argument("--duration", type=int, default=30, help="Duration in seconds")
    args = parser.parse_args()

    nc = NATS()
    try:
        await nc.connect("nats://localhost:4222")
    except Exception as e:
        print(f"❌ Failed to connect to NATS: {e}")
        return

    if args.stress:
        try:
            await run_stress_test(nc, duration=args.duration, workers=args.workers, processes=args.processes)
        finally:
            await nc.drain()
        return

    tui = GeneratorTUI(nc)
    stdscr = curses.initscr()
    curses.noecho()
    curses.cbreak()
    stdscr.keypad(True)
    
    try:
        await tui.run(stdscr)
    finally:
        curses.nocbreak()
        stdscr.keypad(False)
        curses.echo()
        curses.endwin()
        await nc.drain()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        import traceback
        print(f"❌ Error: {e}")
        traceback.print_exc()
