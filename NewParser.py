# -*- coding: utf-8 -*-
"""
Elektronikon (MkV) monitor — with InfluxDB logging every 5s.
"""

import requests, struct, binascii, time
from datetime import datetime
from typing import Dict, Tuple, List, Any

# ================== Utilities ==================

class Parsed:
    __slots__ = ("raw","ok","u32","i32","u16_hi","u16_lo","i16_hi","i16_lo")
    def __init__(self, raw: str):
        self.raw = raw
        self.ok = False
        self.u32 = self.i32 = 0
        self.u16_hi = self.u16_lo = 0
        self.i16_hi = self.i16_lo = 0
        self._parse()
    def _parse(self):
        h = (self.raw or "").strip().upper()
        if not h or h in ("X","XXXXXXXX") or len(h) < 8:
            return
        try:
            b = binascii.unhexlify(h[:8])
            self.u32 = struct.unpack(">I", b)[0]
            self.i32 = struct.unpack(">i", b)[0]
            self.u16_hi = struct.unpack(">H", b[0:2])[0]
            self.u16_lo = struct.unpack(">H", b[2:4])[0]
            self.i16_hi = struct.unpack(">h", b[0:2])[0]
            self.i16_lo = struct.unpack(">h", b[2:4])[0]
            self.ok = True
        except Exception:
            self.ok = False

def fmt_num(val: float, prec: int = 1) -> str:
    s = f"{val:.{prec}f}"
    return s.rstrip("0").rstrip(".") if "." in s else s

# ================== Protocol ==================

class ElektronikonProtocol:
    def __init__(self, host: str):
        self.url = f"http://{host}/cgi-bin/mkv.cgi"
        self.session = requests.Session()

    @staticmethod
    def hx(v: int, n: int) -> str:
        return f"{v:0{n}x}"

    def ask(self, qs: List[Tuple[int,int]]) -> str:
        if not qs:
            return ""
        payload = "".join(self.hx(i,4)+self.hx(si,2) for i,si in qs)
        try:
            r = self.session.post(self.url, data={"QUESTION": payload}, timeout=20)
            r.raise_for_status()
            return r.text.replace("\n","").replace("\r","").strip()
        except Exception as e:
            print("Request error:", e)
            return ""

    @staticmethod
    def split(resp: str, count: int) -> List[str]:
        out = []
        for i in range(count):
            off = i*8
            out.append(resp[off:off+8] if len(resp) >= off+8 else "XXXXXXXX")
        return out

# ================== Language ==================

class LanguageManager:
    def __init__(self):
        self.data = {
            "MSTATE": {
                3:"Off",5:"Standby",6:"Stopped",16:"Starting",18:"Unload",28:"Load",
                90:"Starting Load",100:"Blow Off",105:"Running",111:"Not Ready To Start",
                117:"Stopping",118:"Unloading"
            }
        }
    def mstate(self, code: int) -> str:
        return self.data["MSTATE"].get(code, f"State {code}")

# ================== Monitor ==================

class CompressorMonitor:
    def __init__(self, host: str):
        self.p = ElektronikonProtocol(host)
        self.lang = LanguageManager()
        self.analogs = {
            "Controller Temperature": (0x3002, 0x08, "temp"),
            "Compressor Outlet":      (0x3002, 0x09, "press"),
            "Relative Humidity":      (0x3002, 0x0A, "humid"),
            "Vessel Pressure":        (0x3002, 0x0C, "press"),
            "Element Outlet":         (0x3002, 0x0E, "temp"),
            "Dryer PDP":              (0x3002, 0x0F, "temp"),
            "Ambient Air":            (0x3002, 0x10, "temp"),
        }

    # --- Analog Inputs ---
    @staticmethod
    def _scale_ai(kind: str, raw_i16: int) -> Tuple[str,str,float]:
        if kind == "temp":
            v = raw_i16 / 10.0
            return fmt_num(v,1), "°C", v
        if kind == "press":
            x = abs(raw_i16)
            val = raw_i16/1000.0 if x>=2000 else raw_i16/10.0
            return fmt_num(val,1), "Bar", val
        if kind == "humid":
            x = abs(raw_i16)
            if x>1000: v = raw_i16/100.0
            elif x>100: v = raw_i16/10.0
            else: v = float(raw_i16)
            return fmt_num(v,1), "%", v
        return str(raw_i16), "", float(raw_i16)

    def read_analog_inputs(self) -> Dict[str,float]:
        qs = [(idx,si) for (_, (idx,si,_)) in self.analogs.items()]
        resp = self.p.ask(qs)
        blocks = self.p.split(resp, len(qs))
        out = {}
        for (name,(idx,si,kind)), raw in zip(self.analogs.items(), blocks):
            pr = Parsed(raw)
            if not pr.ok:
                out[name] = None
                continue
            _,_,v = self._scale_ai(kind, pr.i16_hi)
            out[name] = v
        return out

    # --- Machine State ---
    def read_machine_state(self) -> Tuple[int,str]:
        pr = Parsed(self.p.split(self.p.ask([(0x3001,8)]),1)[0])
        if not pr.ok:
            return (0,"Unknown")
        code = pr.u16_lo
        return code, self.lang.mstate(code)

    # --- Converters ---
    def read_converters(self) -> Dict[str,float]:
        qs = [(0x3021,1),(0x3021,10),(0x3021,5)]
        resp = self.p.ask(qs)
        blocks = self.p.split(resp, len(qs))
        rpm = Parsed(blocks[0]); amps = Parsed(blocks[1]); flow = Parsed(blocks[2])
        rpm_val = rpm.u16_hi if rpm.ok else 0
        amps_val = amps.u16_hi if amps.ok else 0
        flow_val = 0
        if flow.ok:
            flow_val = flow.u16_hi if flow.u16_hi != 0 else flow.u16_lo
            if flow_val > 200: flow_val = round(flow_val / 100.0, 1)
        return {"RPM": rpm_val, "Amps": amps_val, "Flow": flow_val}

# ================== InfluxDB ==================

class InfluxLogger:
    def __init__(self, url: str, org: str="", bucket: str="", token: str="", version: int=2):
        self.url = url.rstrip("/")
        self.org = org
        self.bucket = bucket
        self.token = token
        self.version = version
        self.session = requests.Session()

    def write(self, measurement: str, fields: Dict[str,Any], tags: Dict[str,str]={}):
        def esc(s: str) -> str:
            return str(s).replace(" ", "_").replace(",", "_").replace("=", "_").replace("%", "")
        
        tag_str = ",".join([f"{esc(k)}={esc(v)}" for k,v in tags.items()])
        field_parts = []
        for k,v in fields.items():
            if v is None:
                continue
            key = esc(k)
            if isinstance(v, (int, float)):
                field_parts.append(f"{key}={v}")
            else:
                field_parts.append(f'{key}="{v}"')
        field_str = ",".join(field_parts)

        line = f"{measurement}{','+tag_str if tag_str else ''} {field_str}"

        try:
            if self.version == 2:
                url = f"{self.url}/api/v2/write?org={self.org}&bucket={self.bucket}&precision=s"
                headers = {"Authorization": f"Token {self.token}"}
            else:  # v1
                url = f"{self.url}/write?db={self.bucket}"
                headers = {}
            r = self.session.post(url, data=line.encode("utf-8"), headers=headers, timeout=5)
            if r.status_code not in (204,200):
                print("Influx write error:", r.status_code, r.text)
        except Exception as e:
            print("Influx exception:", e)

# ================== Run Loop ==================

def main():
    ip = "10.4.5.168"
    mon = CompressorMonitor(ip)

    # InfluxDB Config
    influx = InfluxLogger(
        url="http://localhost:8086",  # InfluxDB
        org="Freshpack",                   # for v2
        bucket="compressor2",
        token="l9pHvLaOWe957c9JqO4U30T_Ol1KfHayTZa9AX_-V6-vmxhjxTyiOuHAsFWZWQ58oiU6Bv_iMVrZXLUnncqshQ==",    # for v2
        version=2                    
    )

    print("Starting data logging to InfluxDB every 5s... Press Ctrl+C to stop.\n")
    while True:
        try:
            analogs = mon.read_analog_inputs()
            code, state = mon.read_machine_state()
            conv = mon.read_converters()

            fields = {**analogs, **conv, "MachineStateCode": code}
            tags = {"MachineState": state}

            influx.write("compressor", fields, tags)

            print(f"[{datetime.now():%H:%M:%S}] Wrote {len(fields)} fields to InfluxDB ({state})")

        except KeyboardInterrupt:
            print("Stopped by user.")
            break
        except Exception as e:
            print("Error:", e)

        time.sleep(5)

if __name__ == "__main__":
    main()
