import requests
import struct
import binascii
from typing import List, Dict, Any, Optional
from datetime import datetime
import time
import threading
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

class LanguageManager:
    def __init__(self):
        self.languages = {
            "English": self._create_english_language()
        }
        self.current_language = "English"
    
    def _create_english_language(self):
        return {
            "MPL": {
                # Counters
                2700: "Running Hours",
                2701: "Loaded Hours", 
                2702: "Motor Starts",
                2703: "Module Hours",
                2704: "Accumulated Volume",
                2705: "Load Relay",
                2706: "VSD 1-20% RPM",
                2707: "VSD 20-40% RPM",
                2708: "VSD 40-60% RPM", 
                2709: "VSD 60-80% RPM",
                2710: "VSD 80-100% RPM",
                2777: "Fan Starts",
                # Analog Inputs
                509: "Compressor Outlet",
                602: "Dp Oil Separator", 
                1008: "Element Outlet",
                1000: "Ambient Air",
                # Digital Inputs
                2100: "Emergency Stop",
                2105: "Overload Fan Motor",
                2110: "Electronic Condensate Drain", 
                2124: "Pressure Setting Selection",
                # Digital Outputs
                2309: "Fan Motor",
                2313: "Blowoff",
                2306: "General Shutdown",
                2307: "Automatic Operation",
                2305: "General Warning",
                2310: "Run Enable Main Motor",
                # Special Protections
                2975: "No Valid Pressure Control",
                7385: "Motor Converter 1 Alarm",
                2991: "Expansion Module Communication",
            },
            "MSTATE": {
                1: "Power Up",
                2: "Power Up ES",
                3: "Off",
                4: "Local Operation ES",
                5: "Standby",
                6: "Stopped",
                7: "Central Stopped ES",
                8: "Off & Minimum Stop Time",
                9: "Standby & Minimum Stop Time",
                10: "Stopped & Minimum Stop Time",
                11: "Start Check",
                12: "Start Failure",
                13: "Shutdown",
                14: "Shutdown ES",
                15: "Shutting down",
                16: "Starting",
                17: "Starting Unload",
                18: "Unload",
                19: "Manual Unload & Starting",
                20: "Manual Unload",
                21: "Manual Unload & Unload Time",
                22: "Minimum Speed & Manual Stop",
                23: "Minimum Speed & Automatic Stop",
                24: "Automatic Stop & Converter Check",
                25: "Manual Stop & Converter Check",
                26: "Automatic Stop in progress",
                27: "Overpressure",
                28: "Load",
                29: "Minimum Speed & Manual Stop",
                30: "Automatic Operation ES",
                31: "Minimum Speed & Automatic Stop",
                32: "Forced Unload",
                33: "Dryer Off",
                34: "Shifting Vessels, Activate B",
                35: "Shifting Vessels, Isolate A",
                36: "A: Pressure Relief",
                37: "A: Open Regeneration Valve",
                38: "A: Starting Blower",
                39: "A: Heating With Blower",
                40: "A: Extra Heating With Blower",
                41: "A: Pre-Cooling With Blower",
                42: "A: Stopping Blower",
                43: "A: Heating With Dry Air",
                44: "A: Cooling With Dry Air",
                45: "A: Prepare Blower Cooling",
                46: "A: Cooling With Blower",
                47: "A: Stopping Cooling",
                48: "A: Pressure Equalization",
                49: "A: Split Flow Cooling",
                50: "A: Standby",
                51: "Shifting Vessels, Activate A",
                52: "Shifting Vessels, Isolate B",
                53: "B: Pressure Relief",
                54: "B: Open Regeneration Valve",
                55: "B: Starting Blower",
                56: "B: Heating With Blower",
                57: "B: Extra Heating With Blower",
                58: "B: Pre-Cooling With Blower",
                59: "B: Stopping Blower",
                60: "B: Heating With Dry Air",
                61: "B: Cooling With Dry Air",
                62: "B: Prepare Blower Cooling",
                63: "B: Cooling With Blower",
                64: "B: Stopping Cooling",
                65: "B: Pressure Equalization",
                66: "B: Split Flow Cooling",
                67: "B: Standby",
                68: "Dryer PowerUp",
                69: "Dryer Shutdown",
                70: "Pressure Relief Vessel A",
                71: "Open Regeneration Valve A",
                72: "Pressure Relief Vessel B",
                73: "Open Regeneration Valve B",
                74: "Pressure Equalization",
                75: "A: Regeneration",
                76: "A: Cooling",
                77: "B: Regeneration",
                78: "B: Cooling",
                79: "Initial Start",
                80: "Dryer Standby",
                81: "Dryer Start Check",
                82: "Dryer Start Failure",
                83: "Dryer Shutting Down",
                84: "Dryer Starting",
                85: "Dryer Running",
                86: "Output Test",
                87: "Safety Valve Test",
                88: "Regreasing",
                89: "Tuning Dryer",
                90: "Starting Load",
                91: "Start Delayed",
                92: "Low Speed & Manual Stop",
                93: "Low Speed & Automatic Stop",
                94: "Pre-Cooling",
                95: "ZR Start Request",
                96: "ZR Pre-Running",
                97: "ZD Load Delay Time",
                98: "ZD Stop Delay Time",
                99: "ZD Unload Delay Time",
                100: "Blow Off",
                101: "A: Stopping Dryer - Cooling Heater",
                102: "B: Stopping Dryer - Cooling Heater",
                103: "Go To Minimum Speed",
                104: "Manual Start Delay",
                105: "Running",
                106: "Loaded Capacity Regulation",
                107: "Loaded Flow Regulation",
                108: "Loaded Pressure Regulation",
                109: "Loading",
                110: "Manual Unloading",
                111: "Not Ready To Start",
                112: "Resetting",
                113: "Service Control Running",
                114: "Service Control Stopped",
                115: "Start Pending",
                116: "Stop Pending",
                117: "Stopping",
                118: "Unloading",
                119: "A: Shifting Vessels: Open V4",
                120: "A: Shifting Vessels: Close V3 + V8 + V14",
                121: "A: Prepare Regeneration: Open V1 + V30",
                122: "A: Prepare Regeneration: Open V9 + V13",
                123: "A: Full Flow Regeneration",
                124: "A: Controlled Flow Reg With Heaters",
                125: "A: Controlled Flow Cooling Down Heaters",
                126: "A: Prepare Cooling: Open V5",
                127: "A: Prepare Cooling: Open V16 + V23",
                128: "A: Prepare Cooling: Close V17 + V30",
                129: "A: Full Flow Cooling",
                130: "A: Prepare SFC: Open V3 + V17",
                131: "A: Prepare Standby: Open V13",
                132: "A: Prepare Cooling: Close V1",
                133: "A: Pressure Relief: Open V6",
                134: "A: Dry Air Cooling",
                135: "A: Prepare Regeneration: Open V13 + V22",
                136: "B: Shifting Vessels: Open V3",
                137: "B: Shifting Vessels: Close V4 + V9 + V13",
                138: "B: Prepare Regeneration: Open V2 + V30",
                139: "B: Prepare Regeneration: Open V8 + V14",
                140: "B: Full Flow Regeneration",
                141: "B: Controlled Flow Reg With Heaters",
                142: "B: Controlled Flow Cooling Down Heaters",
                143: "B: Prepare Cooling: Open V5",
                144: "B: Prepare Cooling: Open V17 + V23",
                145: "B: Prepare Cooling: Close V16 + V30",
                146: "B: Full Flow Cooling",
                147: "B: Prepare SFC: Open V4 + V16",
                148: "B: Prepare Standby: Open V14",
                149: "B: Prepare Cooling: Close V2",
                150: "B: Pressure Relief: Open V7",
                151: "B: Dry Air Cooling",
                152: "B: Prepare Regeneration: Open V14 + V22",
                153: "A: Pres.Equalization  B:Pres.Equalization",
                154: "A: Production  B: Regenerating",
                155: "A: Regenerating  B: Production",
                156: "A: No Production  B: Regenerating",
                157: "A: Regenerating  B: No Production",
                158: "Stopping: Equalize Vessels",
                159: "Stopping: Depressurize Vessels",
                160: "Off Wait",
                161: "Standby Wait",
                162: "Stopped Wait",
                163: "Stopped Standby Wait",
                164: "Manual Stop Command Stopping",
                165: "Auto Stopping",
                166: "Auto Stop Command Stopping",
                167: "Unload After Command Stopping",
                168: "Manual Stop Cmd Check Stop Converter",
                169: "Auto Stopping",
                170: "Auto Stop Cmd Check Stop Converter",
                171: "Stopped Standby Check Stop Converter",
                172: "Loading",
                173: "Man. Stop Cmd Go To Minimum RPM",
                174: "Do Unload Go To Minimum RPM",
                175: "Remote Stop Cmd Go To Minimum RPM",
                176: "Man. Unload Cmd Go To Minimum RPM",
                177: "Manual Stop Command Unloading",
                178: "Do Unload Unloading",
                179: "Auto Stop Command Unloading",
                180: "Manual Unload Command Unloading",
                181: "Low Suction",
                182: "High Suction",
                183: "A: Regeneration With Heaters",
                184: "B: Regeneration With Heaters",
                185: "Purge Saving",
                186: "A: Prepare SFC: Open V3 + V17 + V30",
                187: "B: Prepare SFC: Open V4 + V16 + V30",
                188: "A: Prepare Standby: Open V13 + V30",
                189: "B: Prepare Standby: Open V14 + V30",
                190: "Load 50%",
                191: "A: Shifting Vessels Phase 1",
                192: "A: Shifting Vessels Phase 2",
                193: "A: Prepare Regeneration Phase 1",
                194: "A: Prepare Regeneration Phase 2",
                195: "A: Prepare Cooling Phase 1",
                196: "A: Prepare Cooling Phase 2",
                197: "A: Prepare Cooling Phase 3",
                198: "A: repare Split Flow Cooling",
                199: "A: Prepare Standby",
                200: "B: Shifting Vessels Phase 1",
                201: "B: Shifting Vessels Phase 2",
                202: "B: Prepare Regeneration Phase 1",
                203: "B: Prepare Regeneration Phase 2",
                204: "B: Prepare Cooling Phase 1",
                205: "B: Prepare Cooling Phase 2",
                206: "B: repare Cooling Phase 3",
                207: "B: Prepare Split Flow Cooling",
                208: "B: Prepare Standby",
                209: "Recirculation",
                210: "Purge",
                211: "Preparing to go online",
                212: "Man. stop cmd go to min. RPM",
                213: "Remote stop cmd go to min. RPM",
                214: "Manual stop cmd run min. RPM",
                215: "Remote stop cmd run min. RPM",
                216: "Automatic stop run minimum RPM",
                217: "Manual stop cmd check stop converter",
                218: "Auto stop cmd check stop converter",
                219: "Automatic Stop check stop converter",
                220: "Vacuum control",
                221: "Pre-run",
                222: "Automatic stop go to min. RPM",
                223: "Remote start cmd check stop converter"
            },
			"Converters": {
				1: "ABB ACS600",
                2: "SIEMENS MASTERDRIVE",
                3: "VACON CX",
                4: "VACON NX",
                5: "ABB ACS140/350",
                6: "ABB ACS400",
                7: "SIEMENS Ој-MASTER",
                8: "WEG CFW09",
                9: "ABB ACS310/550",
                10: "VACON NXL",
                11: "SCHNEIDER ALTIVAR",
                12: "KEB",
                13: "SIEMENS SINAMICS 130",
                14: "WEG CFW11",
                15: "SIEMENS SINAMICS 120",
                16: "CT COMMANDER SK",
                17: "DANFOSS CDS",
                18: "YASKAWA SYNC",
                19: "ELSTO",
                20: "ABB ACS850",
                21: "INOVANCE",
                22: "WEG CFW700",
                23: "ABB ACS880",
                24: "YASKAWA ASYNC",
                50: "BEARING CONTROLLER",
                100: "NEOS",
                200: "MBC S2M"
			},
            "DIGITAL_STATES": {
                0: "Open",
                1: "Closed"
            },
            "PRESSURE_SETTING": {
                0: "Setpoint 1",
                1: "Setpoint 2" 
            },
            "SENSORERROR": {
                1: "Sensor Error"
            },
            "HOURS": {
                1: "hrs",
                2: "hr"
            },
            "TABLETITLE": {
                1: "Analog Inputs",
                3: "Counters", 
				4: "Converters",
                5: "Info",
                6: "Digital Inputs",
                7: "Digital Outputs",
                8: "Special Protections"
            },
            "TABLEVALUE": {
                1: "Value"
            }
        }
    
    def set_language(self, language: str):
        if language in self.languages:
            self.current_language = language
    
    def get_text(self, category: str, key: Any) -> str:
        lang_data = self.languages.get(self.current_language, {})
        category_data = lang_data.get(category, {})
        
        # For MPL, try to find the text, otherwise return the number
        if category == "MPL":
            return category_data.get(key, f"MPL {key}")
        
        return category_data.get(key, str(key))
    
    def get_machine_state_text(self, state_value: int) -> str:
        return self.get_text("MSTATE", state_value)
        

class ElektronikonProtocol:
    def __init__(self, host: str):
        self.host = host
        self.base_url = f"http://{host}/cgi-bin/mkv.cgi"
        self.session = requests.Session()
        self.session.timeout = 10
        
    def hex_string(self, value: int, length: int) -> str:
        return format(value, f'0{length}x')
    
    def build_question_packet(self, questions: List[tuple]) -> str:
        packet = ""
        for index, subindex in questions:
            packet += self.hex_string(index, 4) + self.hex_string(subindex, 2)
        return packet
    
    def send_questions(self, questions: List[tuple]) -> str:
        # Split into chunks to avoid timeout
        all_responses = ""
        for i in range(0, len(questions), 1000):
            chunk = questions[i:i + 1000]
            question_packet = self.build_question_packet(chunk)
            
            try:
                response = self.session.post(
                    self.base_url,
                    data={'QUESTION': question_packet},
                    headers={'Content-Type': 'application/x-www-form-urlencoded'},
                    timeout=30
                )
                response.raise_for_status()
                all_responses += response.text
            except requests.exceptions.RequestException as e:
                print(f"Request error: {e}")
                return ""
        
        return all_responses

class DataParser:
    @staticmethod
    def parse_hex_data(data_hex: str) -> Dict[str, Any]:
        if not data_hex or data_hex == "X" or data_hex == "XXXXXXXX":
            return {"error": "No data or error response"}
        
        try:
            data_bytes = binascii.unhexlify(data_hex)
            parsed_data = {}
            
            if len(data_bytes) >= 4:
                parsed_data['uint32'] = struct.unpack('>I', data_bytes[:4])[0]
                parsed_data['int32'] = struct.unpack('>i', data_bytes[:4])[0]
                parsed_data['uint16_word0'] = struct.unpack('>H', data_bytes[2:4])[0]
                parsed_data['uint16_word1'] = struct.unpack('>H', data_bytes[0:2])[0]
                parsed_data['int16_word0'] = struct.unpack('>h', data_bytes[2:4])[0]
                parsed_data['int16_word1'] = struct.unpack('>h', data_bytes[0:2])[0]
            
            if len(data_bytes) >= 4:
                for i in range(4):
                    parsed_data[f'byte_{i}'] = data_bytes[3-i]
            
            return parsed_data
            
        except Exception as e:
            return {"error": f"Parse error: {e}"}

class ValueFormatter:
    def __init__(self, language_manager: LanguageManager):
        self.language_manager = language_manager
    
    def format_ai_value(self, value: int, input_type: int, display_precision: int = 1) -> str:
        if value == 32767:
            return self.language_manager.get_text("SENSORERROR", 1)
        
        # Handle signed 16-bit values
        if value & 0x8000:
            value = -32768 + (value & 0x7FFF)
        
        formatted_value = value
        
        # Apply scaling based on input type
        if input_type == 0:  # Pressure (mbar to bar)
            formatted_value = value / 1000
            unit = "bar"
        elif input_type == 1:  # Temperature (0.1°C to °C)
            formatted_value = value / 10
            unit = "°C"
        else:
            unit = ""
        
        return f"{formatted_value:.{display_precision}f} {unit}"
    
    def format_counter_value(self, value: int, counter_unit: int) -> str:
        if counter_unit == 0:  # hours
            hours = value // 3600
            unit_key = 2 if hours == 1 else 1
            unit = self.language_manager.get_text("HOURS", unit_key)
            return f"{hours} {unit}"
        elif counter_unit == 1:  # no unit (count)
            return str(value)
        elif counter_unit == 2:  # m3
            return f"{value} m3"
        elif counter_unit == 3:  # %
            return f"{value} %"
        else:
            return str(value)
    
    def format_digital_value(self, value: int, mpl: int = None) -> str:
        """Format digital value with special handling for pressure setting"""
        if mpl == 2124:  # Pressure Setting Selection
            return self.language_manager.get_text("PRESSURE_SETTING", value)
        else:
            return self.language_manager.get_text("DIGITAL_STATES", value)
    
    def format_converter_value(self, value: int, converter_type: int) -> str:
        return f"{value} rpm"


class InfluxDBManager:
    def __init__(self, url: str, token: str, org: str, bucket: str):
        self.client = InfluxDBClient(url=url, token=token, org=org)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.bucket = bucket
        self.org = org
        
    def write_analog_inputs(self, ai_values: List[Dict[str, Any]], device_info: Dict[str, Any]):
        points = []
        timestamp = datetime.utcnow()
        
        for ai in ai_values:
            point = Point("analog_inputs") \
                .tag("device", device_info['serial']) \
                .tag("model", device_info['model']) \
                .tag("sensor_name", ai['name']) \
                .tag("mpl", str(ai['mpl'])) \
                .field("value", ai['value']) \
                .field("raw_value", ai['value']) \
                .field("status", ai['status']) \
                .time(timestamp)
            points.append(point)
        
        try:
            self.write_api.write(bucket=self.bucket, org=self.org, record=points)
            print(f"✓ Written {len(points)} analog inputs to InfluxDB")
        except Exception as e:
            print(f"✗ Error writing analog inputs to InfluxDB: {e}")
    
    def write_converters(self, converter_values: List[Dict[str, Any]], device_info: Dict[str, Any]):
        """Запись данных конвертеров в InfluxDB"""
        points = []
        timestamp = datetime.utcnow()
        
        for converter in converter_values:
            point = Point("converters") \
                .tag("device", device_info['serial']) \
                .tag("model", device_info['model']) \
                .tag("converter_type", str(converter['converter_type'])) \
                .field("rpm", converter['value']) \
                .field("flow_percent", converter['flow']) \
                .field("raw_rpm", converter['value']) \
                .field("raw_flow", converter['flow']) \
                .time(timestamp)
            points.append(point)
        
        try:
            self.write_api.write(bucket=self.bucket, org=self.org, record=points)
            print(f"✓ Written {len(points)} converter data points to InfluxDB")
        except Exception as e:
            print(f"✗ Error writing converters to InfluxDB: {e}")
    
    def write_machine_state(self, machine_state: Dict[str, Any], device_info: Dict[str, Any]):
        timestamp = datetime.utcnow()
        
        point = Point("machine_state") \
            .tag("device", device_info['serial']) \
            .tag("model", device_info['model']) \
            .field("state_code", machine_state['machine_state']) \
            .field("state_numeric", machine_state['machine_state']) \
            .field("state_text", machine_state['machine_state_text']) \
            .time(timestamp)
        
        try:
            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            print(f"✓ Written machine state to InfluxDB: {machine_state['machine_state_text']}")
        except Exception as e:
            print(f"✗ Error writing machine state to InfluxDB: {e}")


class CompressorMonitor:
    def __init__(self, host: str, influx_manager: InfluxDBManager = None):
        self.protocol = ElektronikonProtocol(host)
        self.parser = DataParser()
        self.language_manager = LanguageManager()
        self.formatter = ValueFormatter(self.language_manager)
        self.influx_manager = influx_manager
        
        self.machine_info = {
            'Model': 'GA90VSD_08',
            'Serial': 'API658748', 
            'Generation': 5
        }
        
        self._running = False
        self._data_thread = None
    
    def set_language(self, language: str):
        self.language_manager.set_language(language)
    
    def set_influx_manager(self, influx_manager: InfluxDBManager):
        self.influx_manager = influx_manager
    
    def start_data_collection(self, interval: int = 5):
        """Запуск периодического сбора данных"""
        if self._running:
            print("Data collection is already running")
            return
        
        self._running = True
        self._data_thread = threading.Thread(target=self._data_collection_loop, args=(interval,))
        self._data_thread.daemon = True
        self._data_thread.start()
        print(f"Started data collection with {interval} second interval")
    
    def stop_data_collection(self):
        """Остановка сбора данных"""
        self._running = False
        if self._data_thread:
            self._data_thread.join()
        print("Data collection stopped")
    
    def _data_collection_loop(self, interval: int):
        """Цикл сбора данных для InfluxDB"""
        while self._running:
            try:
                print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Collecting data...")
                if self.influx_manager:

                    ai_values = self.get_analog_input_values()
                    if ai_values and self.influx_manager:
                        self.influx_manager.write_analog_inputs(ai_values, self.get_device_info())
                    

                    converter_values = self.get_converter_values()
                    if converter_values and self.influx_manager:
                        self.influx_manager.write_converters(converter_values, self.get_device_info())
                    

                    machine_state = self.get_machine_state()
                    if 'error' not in machine_state and self.influx_manager:
                        self.influx_manager.write_machine_state(machine_state, self.get_device_info())
                

                time.sleep(interval)
                
            except Exception as e:
                print(f"Error in data collection loop: {e}")
                time.sleep(interval)
    
    def discover_analog_inputs(self) -> List[Dict[str, Any]]:
        """Discover active analog inputs"""
        questions = []
        for i in range(0x2010, 0x2090):
            questions.append((i, 1))  # Status
            questions.append((i, 4))  # Precision
        
        response = self.protocol.send_questions(questions)
        if not response:
            return []
        
        analog_inputs = []
        response_pos = 0
        
        for i in range(0x2010, 0x2090):
            status_hex = response[response_pos:response_pos+8]
            response_pos += 8
            precision_hex = response[response_pos:response_pos+8]
            response_pos += 8
            
            status_data = self.parser.parse_hex_data(status_hex)
            precision_data = self.parser.parse_hex_data(precision_hex)
            
            if "error" not in status_data and status_data.get('byte_0', 0) != 0:
                ai_data = {
                    "address": i,
                    "mpl": status_data.get('uint16_word1', 0),
                    "input_type": status_data.get('byte_1', 0),
                    "display_precision": precision_data.get('byte_3', 1),
                    "rtd_si": i - 0x2010 + 1,
                    "name": self.language_manager.get_text("MPL", status_data.get('uint16_word1', 0))
                }
                analog_inputs.append(ai_data)
        
        return analog_inputs
    
    def get_analog_input_values(self) -> List[Dict[str, Any]]:
        """Get current values for analog inputs"""
        analog_inputs = self.discover_analog_inputs()
        if not analog_inputs:
            return []
        
        questions = []
        for ai in analog_inputs:
            questions.append((0x3002, ai["rtd_si"]))
        
        response = self.protocol.send_questions(questions)
        if not response:
            return []
        
        ai_values = []
        response_pos = 0
        
        for ai in analog_inputs:
            data_hex = response[response_pos:response_pos+8]
            response_pos += 8
            
            parsed = self.parser.parse_hex_data(data_hex)
            
            if "error" not in parsed:
                ai_with_value = ai.copy()
                ai_with_value["value"] = parsed.get('int16_word1', 0)
                ai_with_value["status"] = parsed.get('uint16_word0', 0)
                ai_with_value["formatted_value"] = self.formatter.format_ai_value(
                    parsed.get('int16_word1', 0),
                    ai["input_type"],
                    ai["display_precision"]
                )
                ai_values.append(ai_with_value)
        
        return ai_values
    
    def discover_counters(self) -> List[Dict[str, Any]]:
        """Discover active counters"""
        questions = []
        for i in range(1, 256):
            questions.append((0x2607, i))
        
        response = self.protocol.send_questions(questions)
        if not response:
            return []
        
        counters = []
        response_pos = 0
        
        for i in range(1, 256):
            data_hex = response[response_pos:response_pos+8]
            response_pos += 8
            
            parsed = self.parser.parse_hex_data(data_hex)
            
            if "error" not in parsed and parsed.get('byte_0', 0) != 0:
                counter_data = {
                    "subindex": i,
                    "mpl": parsed.get('uint16_word1', 0),
                    "counter_unit": parsed.get('byte_1', 0),
                    "rtd_si": i,
                    "name": self.language_manager.get_text("MPL", parsed.get('uint16_word1', 0))
                }
                counters.append(counter_data)
        
        return counters
    
    def get_counter_values(self) -> List[Dict[str, Any]]:
        """Get current values for counters"""
        counters = self.discover_counters()
        if not counters:
            return []
        
        questions = []
        for counter in counters:
            questions.append((0x3007, counter["rtd_si"]))
        
        response = self.protocol.send_questions(questions)
        if not response:
            return []
        
        counter_values = []
        response_pos = 0
        
        for counter in counters:
            data_hex = response[response_pos:response_pos+8]
            response_pos += 8
            
            parsed = self.parser.parse_hex_data(data_hex)
            
            if "error" not in parsed:
                counter_with_value = counter.copy()
                counter_with_value["value"] = parsed.get('uint32', 0)
                counter_with_value["formatted_value"] = self.formatter.format_counter_value(
                    parsed.get('uint32', 0), 
                    counter["counter_unit"]
                )
                counter_values.append(counter_with_value)
        
        return counter_values
    
    def discover_digital_inputs(self) -> List[Dict[str, Any]]:
        """Discover active digital inputs"""
        questions = []
        for i in range(0x20b0, 0x2100):  
            questions.append((i, 1))
        
        response = self.protocol.send_questions(questions)
        if not response:
            return []
        
        digital_inputs = []
        response_pos = 0
        
        for i in range(0x20b0, 0x2100):
            data_hex = response[response_pos:response_pos+8]
            response_pos += 8
            
            parsed = self.parser.parse_hex_data(data_hex)
            
            if "error" not in parsed and parsed.get('byte_0', 0) != 0:
                di_data = {
                    "address": i,
                    "mpl": parsed.get('uint16_word1', 0),
                    "rtd_si": i - 0x20b0 + 1, 
                    "name": self.language_manager.get_text("MPL", parsed.get('uint16_word1', 0))
                }
                digital_inputs.append(di_data)
        
        return digital_inputs
    
    def get_digital_input_values(self) -> List[Dict[str, Any]]:
        """Get current values for digital inputs"""
        digital_inputs = self.discover_digital_inputs()
        if not digital_inputs:
            return []
        
        questions = []
        for di in digital_inputs:
            questions.append((0x3003, di["rtd_si"]))
        
        response = self.protocol.send_questions(questions)
        if not response:
            return []
        
        di_values = []
        response_pos = 0
        
        for di in digital_inputs:
            data_hex = response[response_pos:response_pos+8]
            response_pos += 8
            
            parsed = self.parser.parse_hex_data(data_hex)
            
            if "error" not in parsed:
                di_with_value = di.copy()
                di_with_value["value"] = parsed.get('uint16_word1', 0)
                di_with_value["status"] = parsed.get('uint16_word0', 0)
                di_with_value["formatted_value"] = self.formatter.format_digital_value(
                    parsed.get('uint16_word1', 0),
                    di["mpl"]
                )
                di_values.append(di_with_value)
        
        return di_values
    
    def discover_digital_outputs(self) -> List[Dict[str, Any]]:
        """Discover active digital outputs"""
        questions = []
        for i in range(0x2100, 0x2150):
            questions.append((i, 1))
        
        response = self.protocol.send_questions(questions)
        if not response:
            return []
        
        digital_outputs = []
        response_pos = 0
        
        for i in range(0x2100, 0x2150):
            data_hex = response[response_pos:response_pos+8]
            response_pos += 8
            
            parsed = self.parser.parse_hex_data(data_hex)
            
            if "error" not in parsed and parsed.get('byte_0', 0) != 0:
                do_data = {
                    "address": i,
                    "mpl": parsed.get('uint16_word1', 0),
                    "rtd_si": i - 0x2100 + 1,
                    "name": self.language_manager.get_text("MPL", parsed.get('uint16_word1', 0))
                }
                digital_outputs.append(do_data)
        
        return digital_outputs
    
    def get_digital_output_values(self) -> List[Dict[str, Any]]:
        """Get current values for digital outputs"""
        digital_outputs = self.discover_digital_outputs()
        if not digital_outputs:
            return []
        
        questions = []
        for do in digital_outputs:
            questions.append((0x3005, do["rtd_si"]))
        
        response = self.protocol.send_questions(questions)
        if not response:
            return []
        
        do_values = []
        response_pos = 0
        
        for do in digital_outputs:
            data_hex = response[response_pos:response_pos+8]
            response_pos += 8
            
            parsed = self.parser.parse_hex_data(data_hex)
            
            if "error" not in parsed:
                do_with_value = do.copy()
                do_with_value["value"] = parsed.get('uint16_word1', 0)
                do_with_value["status"] = parsed.get('uint16_word0', 0)
                do_with_value["formatted_value"] = self.formatter.format_digital_value(
                    parsed.get('uint16_word1', 0),
                    do["mpl"]
                )
                do_values.append(do_with_value)
        
        return do_values
    
    def discover_converters(self) -> List[Dict[str, Any]]:
        """Discover active converters"""
        questions = []
        for i in range(0x2681, 0x2689):  # Диапазон из Converters.js
            questions.append((i, 1))
            questions.append((i, 7))
        
        response = self.protocol.send_questions(questions)
        if not response:
            return []
        
        converters = []
        response_pos = 0
        
        for i in range(0x2681, 0x2689):
            status_hex = response[response_pos:response_pos+8]
            response_pos += 8
            device_type_hex = response[response_pos:response_pos+8]
            response_pos += 8
            
            status_data = self.parser.parse_hex_data(status_hex)
            device_type_data = self.parser.parse_hex_data(device_type_hex)
            
            if "error" not in status_data and status_data.get('byte_0', 0) != 0:
                converter_data = {
                    "address": i,
                    "converter_type": status_data.get('byte_1', 0),
                    "converter_device_type": device_type_data.get('byte_0', 0),
                    "rtd_si": i - 0x2681 + 1,
                    "name": self.language_manager.get_text("CVNAME", status_data.get('byte_1', 0))
                }
                converters.append(converter_data)
        
        return converters
    
    def get_converter_values(self) -> List[Dict[str, Any]]:
        """Get current values for converters"""
        converters = self.discover_converters()
        if not converters:
            return []
        
        questions = []
        questions.append((0x3021, 5))
        for converter in converters:
            questions.append((0x3020 + converter["rtd_si"], 1))
        
        response = self.protocol.send_questions(questions)
        if not response:
            return []
        
        converter_values = []
        response_pos = 0
        
        flow_hex = response[response_pos:response_pos+8]
        response_pos += 8
        flow_data = self.parser.parse_hex_data(flow_hex)
        
        for converter in converters:
            data_hex = response[response_pos:response_pos+8]
            response_pos += 8
            
            parsed = self.parser.parse_hex_data(data_hex)
            
            if "error" not in parsed:
                converter_with_value = converter.copy()
                converter_with_value["value"] = parsed.get('uint16_word1', 0)
                converter_with_value["flow"] = flow_data.get('uint16_word0', 0) if "error" not in flow_data else 0
                converter_with_value["formatted_value"] = self.formatter.format_converter_value(
                    parsed.get('uint16_word1', 0),
                    converter["converter_type"]
                )
                converter_with_value["formatted_flow"] = f"{converter_with_value['flow']} %"
                converter_values.append(converter_with_value)
        
        return converter_values
    
    def discover_special_protections(self) -> List[Dict[str, Any]]:
        """Discover active special protections"""
        questions = []
        for i in range(0x2300, 0x247F):
            questions.append((i, 1))
        
        response = self.protocol.send_questions(questions)
        if not response:
            return []
        
        special_protections = []
        response_pos = 0
        
        for i in range(0x2300, 0x247F):
            data_hex = response[response_pos:response_pos+8]
            response_pos += 8
            
            parsed = self.parser.parse_hex_data(data_hex)
            
            if "error" not in parsed and parsed.get('byte_0', 0) != 0:
                sp_data = {
                    "address": i,
                    "mpl": parsed.get('uint16_word1', 0),
                    "rtd_si": i - 0x2300 + 1,
                    "name": self.language_manager.get_text("MPL", parsed.get('uint16_word1', 0))
                }
                special_protections.append(sp_data)
        
        return special_protections	
    
    def discover_special_protections(self) -> List[Dict[str, Any]]:
        """Discover active special protections"""
        questions = []
        for i in range(0x2300, 0x247F):
            questions.append((i, 1))
        
        response = self.protocol.send_questions(questions)
        if not response:
            return []
        
        special_protections = []
        response_pos = 0
        
        for i in range(0x2300, 0x247F):
            data_hex = response[response_pos:response_pos+8]
            response_pos += 8
            
            parsed = self.parser.parse_hex_data(data_hex)
            
            if "error" not in parsed and parsed.get('byte_0', 0) != 0:
                sp_data = {
                    "address": i,
                    "mpl": parsed.get('uint16_word1', 0),
                    "rtd_si": i - 0x2300 + 1,
                    "name": self.language_manager.get_text("MPL", parsed.get('uint16_word1', 0))
                }
                special_protections.append(sp_data)
        
        return special_protections
    
    def get_special_protection_values(self) -> List[Dict[str, Any]]:
        """Get current values for special protections"""
        special_protections = self.discover_special_protections()
        if not special_protections:
            return []
        
        questions = []
        for sp in special_protections:
            questions.append((0x300E, sp["rtd_si"]))
        
        response = self.protocol.send_questions(questions)
        if not response:
            return []
        
        sp_values = []
        response_pos = 0
        
        for sp in special_protections:
            data_hex = response[response_pos:response_pos+8]
            response_pos += 8
            parsed = self.parser.parse_hex_data(data_hex) 
            if "error" not in parsed:
                sp_with_value = sp.copy()
                sp_with_value["status"] = parsed.get('uint16_word0', 0)
                sp_with_value["active"] = sp_with_value["status"] != 0
                status_icon = "🔴" if sp_with_value["active"] else "🟢"
                status_text = "Active" if sp_with_value["active"] else "Normal"
                sp_with_value["formatted_status"] = f"{status_icon} {status_text}"
                sp_values.append(sp_with_value)
        
        return sp_values
    
    def get_machine_state(self) -> Dict[str, Any]:
        questions = [(0x3001, 8)]
        response = self.protocol.send_questions(questions)
        
        if not response or len(response) < 8:
            return {"error": "No response or invalid length"}
        
        data_hex = response[:8]
        parsed = self.parser.parse_hex_data(data_hex)
        
        machine_state = parsed.get('uint16_word0', 0)
        state_text = self.language_manager.get_machine_state_text(machine_state)
        
        return {
            "machine_state": machine_state,
            "machine_state_hex": hex(machine_state),
            "machine_state_text": state_text,
            "raw_data": data_hex,
            "parsed": parsed
        }
    
    def get_device_info(self) -> Dict[str, Any]:
        return {
            "model": self.machine_info.get('Model', 'Unknown'),
            "serial": self.machine_info.get('Serial', 'Unknown'),
            "generation": self.machine_info.get('Generation', 'Unknown')
        }

def print_detailed_report(monitor: CompressorMonitor):
    """Print detailed device status report with all values"""
    print("=" * 70)
    print("ELEKTRONIKON COMPRESSOR - DETAILED STATUS REPORT")
    print("=" * 70)
    
    # Device information
    device_info = monitor.get_device_info()
    print(f"\nDEVICE:")
    print(f"  Model: {device_info['model']}")
    print(f"  Serial: {device_info['serial']}")
    print(f"  Generation: {device_info['generation']}")
    print(f"  Language: {monitor.language_manager.current_language}")
    
    # Machine state
    machine_state = monitor.get_machine_state()
    if 'error' not in machine_state:
        print(f"\n{monitor.language_manager.get_text('TABLETITLE', 5)}:")
        print(f"  {machine_state['machine_state_text']}")
        print(f"  (code: {machine_state['machine_state']} [0x{machine_state['machine_state']:04X}])")
    
    # Analog Inputs with values
    print(f"\n{monitor.language_manager.get_text('TABLETITLE', 1)}:")
    ai_values = monitor.get_analog_input_values()
    if ai_values:
        for ai in ai_values:
            print(f"  {ai['name']:30} {ai['formatted_value']}")
    else:
        print("  No analog inputs data")
    
    # Counters with values
    print(f"\n{monitor.language_manager.get_text('TABLETITLE', 3)}:")
    counter_values = monitor.get_counter_values()
    if counter_values:
        for counter in counter_values:
            print(f"  {counter['name']:30} {counter['formatted_value']}")
    else:
        print("  No counters data")

  # Converters with values
    print(f"\n{monitor.language_manager.get_text('TABLETITLE', 4)}:")
    converter_values = monitor.get_converter_values()
    if converter_values:
        for converter in converter_values:
            print(f"  {monitor.language_manager.get_text('Converters', 7)} {converter['formatted_value']}" )
            print(f"    flow: {converter['formatted_flow']}" )
            
    else:
        print("  No converters data")

    # Digital Inputs with values
    print(f"\n{monitor.language_manager.get_text('TABLETITLE', 6)}:")
    di_values = monitor.get_digital_input_values()
    if di_values:
        for di in di_values:
            print(f"  {di['name']:30} {di['formatted_value']}")
    else:
        print("  No digital inputs data")
    
    # Digital Outputs with values  
    print(f"\n{monitor.language_manager.get_text('TABLETITLE', 7)}:")
    do_values = monitor.get_digital_output_values()
    if do_values:
        for do in do_values:
            print(f"  {do['name']:30} {do['formatted_value']}")
    else:
        print("  No digital outputs data")
    
    # Special Protections with values
    print(f"\n{monitor.language_manager.get_text('TABLETITLE', 8)}:")
    sp_values = monitor.get_special_protection_values()
    if sp_values:
        for sp in sp_values:
            print(f"  {sp['formatted_status']:15} {sp['name']}")
    else:
        print("  No active special protections")
    
    print("\n" + "=" * 70)

def main():
    DEVICE_IP = "192.168.0.100"  # Change to your device IP
    
    # InfluxDB
    INFLUXDB_URL = "http://localhost:8086"  
    INFLUXDB_TOKEN = "OUR_INFLUX_TOKEN"     
    INFLUXDB_ORG = "OUR_INFLUX_ORG"             
    INFLUXDB_BUCKET = "OUR_INFLUX_BUCKET"   
    influx_manager = InfluxDBManager(
        url=INFLUXDB_URL,
        token=INFLUXDB_TOKEN,
        org=INFLUXDB_ORG,
        bucket=INFLUXDB_BUCKET
    )
    
    monitor = CompressorMonitor(DEVICE_IP, influx_manager)
    
    print("=== ELEKTRONIKON COMPRESSOR MONITOR ===\n")

    print("REPORT IN ENGLISH:")
    monitor.set_language("English") 
    print_detailed_report(monitor)
    
    # Collect data to InfluxDB
    print("\nStarting continuous data collection to InfluxDB...")
    monitor.start_data_collection(interval=5)
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping data collection...")
        monitor.stop_data_collection()
        print("Program terminated.")

if __name__ == "__main__":
    main()
