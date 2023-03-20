from typing import Dict
import toml

def read_config(config_file) -> Dict:
    ### read from toml file
    with open(config_file, "r") as f:
        content = toml.load(f)
        ACS_API_KEY = content["ACS_API_KEY"]
    return dict(
        ACS_API_KEY=ACS_API_KEY
    )