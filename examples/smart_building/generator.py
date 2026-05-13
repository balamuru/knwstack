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
            ("🔵 Anomaly          ", self.dispatch_anomaly, "Expected: STRATEGIC AI"),
            ("🏢 Campus Simulation", self.dispatch_campus_simulation, "Expected: NORMAL/HOT/COLD"),
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
        self.add_log("🔵 DISPATCH: ANOMALY (Strategic Path)")
        self.add_log("   EXPECTED: STRATEGIC AI (Concise LLM Diagnosis)")
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

async def main():
    nc = NATS()
    try:
        await nc.connect("nats://localhost:4222")
    except Exception as e:
        print(f"❌ Failed to connect to NATS: {e}")
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
        print(f"❌ Terminal Error: {e}")
