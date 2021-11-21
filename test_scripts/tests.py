from pydantic import ValidationError

import yaml

import sys
sys.path.insert(0, '../moderatelyhelpfulbot/moderatelyhelpfulbot')

from models.settings import MainSettings

with open("./test_scripts/test_settings.yaml", "r", encoding="utf-8") as stream:
    try:
        lmao = MainSettings(**yaml.safe_load(stream))  # type: ignore
    except ValidationError:
        print("this is an error lmao")
