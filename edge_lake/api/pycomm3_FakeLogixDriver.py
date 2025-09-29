import argparse
import random
import json
from datetime import datetime

# Simulated pycomm3.CommError
class CommError(Exception):
    """Simulated pycomm3.CommError"""
    pass

# Helper functions
def random_bool(): return random.choice([True, False])
def random_int(min_val, max_val): return random.randint(min_val, max_val)
def random_float(min_val, max_val): return round(random.uniform(min_val, max_val), 2)
def random_string(length=8): return ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz', k=length))
def random_struct(): return {'Temp': random_float(0, 100), 'Status': random_bool()}
def random_array(dtype, length):
    if dtype == 'int': return [random_int(-100, 100) for _ in range(length)]
    elif dtype == 'bool': return [random_bool() for _ in range(length)]
    elif dtype == 'string': return [random_string() for _ in range(length)]
    return []
def random_timer_or_counter(): return {'ACC': random_int(0, 1000), 'PRE': random_int(1000, 5000)}
def random_date_time(): return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

class LogixDriver:
    def __init__(self, path, *args, init_tags: bool = None, init_program_tags: bool = None):
        self.ip = path
        self.connected = False
        self.mock_data = {
            "SystemReadyFlag": lambda: random_bool(),  # BOOL
            "SmallRangeInt": lambda: random_int(-128, 127),  # SINT
            "MediumRangeInt": lambda: random_int(-32768, 32767),  # INT
            "LargeRangeInt": lambda: random_int(-2147483648, 2147483647),  # DINT
            "TemperatureReading": lambda: random_float(20.0, 30.0),  # REAL
            "OperatorName": lambda: random_string(8),  # STRING
            "SensorStatusStruct": lambda: random_struct(),  # STRUCT
            "PressureReadings": lambda: random_array('int', 5),  # ARRAY_INT
            "InputFlags": lambda: random_array('bool', 4),  # ARRAY_BOOL
            "StatusMessages": lambda: random_array('string', 3),  # ARRAY_STRING
            "DelayTimer": lambda: random_timer_or_counter(),  # TIMER
            "CycleCounter": lambda: random_timer_or_counter(),  # COUNTER
            "Timestamp": lambda: random_date_time(),  # DATE_TIME
            "ATSNormalRdyDI": lambda: random_bool(),
            "CombinedChlorinatorAI": lambda: {"PV": random_float(0.0, 10.0)},
            "FreeChlorinatorAI": lambda: {"PV": random_float(0.0, 5.0)},
        }
        print(f"[MockPLC] Pretending to connect to PLC at {self.ip}")

    def __enter__(self): return self
    def __exit__(self, exc_type, exc_value, traceback): print("[MockPLC] Closing mock connection")

    def open(self):
        if self.ip == "fail-open":
            raise CommError(f"Simulated failure during open() to {self.ip}")
        self.connected = True
        print(f"[MockPLC] Connection opened to {self.ip}")
        return self.connected

    def close(self):
        self.connected = False
        print(f"[MockPLC] Connection closed to {self.ip}")

    def read(self, *tags):
        if self.ip == "fail-read":
            raise CommError("Simulated communication failure during read")

        class Result:
            def __init__(self, tag_name, value, error=None):
                self.tag = tag_name
                self.value = value
                self.error = error

        results = []
        for tag in tags:
            try:
                if '.' in tag:
                    base_tag, *subkeys = tag.split('.')
                    value = self.mock_data[base_tag]()
                    for key in subkeys:
                        value = value[key]
                    results.append(Result(tag, value))
                else:
                    value = self.mock_data[tag]() if callable(self.mock_data[tag]) else self.mock_data[tag]
                    results.append(Result(tag, value))
            except KeyError:
                results.append(Result(tag, None, error=f"Invalid tag: {tag}"))
            except Exception as e:
                results.append(Result(tag, None, error=str(e)))
        return results[0] if len(results) == 1 else results

    def write(self, tag, value):
        if self.ip == "fail-write":
            raise CommError("Simulated communication failure during write")

        print(f"[MockPLC] Writing {value} to {tag}")

        class Result:
            def __init__(self, error=None):
                self.error = error

        try:
            if tag not in self.mock_data:
                self.mock_data[tag] = value if isinstance(value, int) else lambda: value
            else:
                self.mock_data[tag] = value if isinstance(value, int) else lambda: value
            return Result()
        except Exception as e:
            return Result(error=f"Error writing {tag}: {str(e)}")

    @property
    def tags(self):
        output = {}
        for tag, generator in self.mock_data.items():
            sample = generator() if callable(generator) else generator
            if isinstance(sample, dict):
                internal_tags = {key: {'data_type': self._infer_type(value)} for key, value in sample.items()}
                output[tag] = {'data_type': {'internal_tags': internal_tags}}
            else:
                output[tag] = {'data_type': self._infer_type(sample)}
        return output

    def _infer_type(self, value):
        if isinstance(value, bool): return 'BOOL'
        elif isinstance(value, int): return 'DINT'
        elif isinstance(value, float): return 'REAL'
        elif isinstance(value, str): return 'STRING'
        elif isinstance(value, list): return 'ARRAY'
        elif isinstance(value, dict): return 'STRUCT'
        else: return 'UNKNOWN'

    def get_tag_list(self):
        # Simulated tag list (name -> inferred data type)
        return {
            f"{tag} ({self._get_code(tag)})": self._infer_type(value() if callable(value) else value)
            for tag, value in self.mock_data.items()
        }

    def _get_code(self, tag):
        ROCKWELL_TAG_TYPE_CODES = {
            'BOOL': 193, 'SINT': 194, 'INT': 195, 'DINT': 196, 'REAL': 202,
            'STRING': 307, 'TIMER': 762, 'COUNTER': 763, 'CONTROL': 765,
            'DATE_TIME': 358, 'STRUCT': 881
        }
        return ROCKWELL_TAG_TYPE_CODES.get(tag.split('_')[0], 'None')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('tags', metavar='T', type=str, nargs='+', help="Tags to read from the PLC (e.g., BOOL, SINT)")
    parser.add_argument('--plc-ip', type=str, default='192.168.1.10', help='PLC IP address')
    args = parser.parse_args()

    data = {}
    try:
        with LogixDriver(args.plc_ip) as plc:
            plc.write('STATIC_INT', 1000)
            tag_list = plc.get_tag_list()
            print(json.dumps(tag_list, indent=2))

            value = plc.read(*args.tags)
            if isinstance(value, list):
                for result in value:
                    data[result.tag] = {"value": result.value, "error": result.error}
            else:
                data[value.tag] = {"value": value.value, "error": value.error}
            print(json.dumps(data, indent=2))
    except CommError as ce:
        print(f"PLC Communication Error: {ce}")
    except Exception as e:
        print(f"General Error: {e}")

if __name__ == '__main__':
    main()
