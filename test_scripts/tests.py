# flake8: noqa E402
# pylint: disable=wrong-import-position
import sys
sys.path.insert(0, '../moderatelyhelpfulbot/moderatelyhelpfulbot')

from models.settings.base_settings import BaseSettings
from pydantic import ValidationError
import yaml

print("Real example")
with open("./test_scripts/real_settings.yaml", "r", encoding="utf-8") as stream:
    try:
        BaseSettings(**yaml.safe_load(stream))
    except ValidationError as e:
        print(e)
