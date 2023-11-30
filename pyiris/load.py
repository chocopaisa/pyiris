from dataclasses import dataclass
import json

@dataclass
class LoadOption():
    validate_csv:bool = False
    validate_unique:bool = False
    validate_unique_by_name:bool = False
    use_zlib:bool = False
    skip_rows:int = 0

    def __str__(self):
        dump_data = {
            'is_csv_dat': self.validate_csv,
            'is_unique_load': self.validate_unique,
            'unique_load_name': self.validate_unique_by_name,
            'use_zlib': self.use_zlib,
            'skip_rows': self.skip_rows
        }

        return json.dumps(dump_data)
