import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import yaml
from beancount.core import amount, data, flags
from identify import identify


def format_amount(amount: str) -> Decimal:
    """Removes German thousands separator and converts decimal point to US."""
    return Decimal(amount.replace(".", "").replace(",", "."))


def create_posting(account: str, units: amount, meta: dict) -> data.Posting:
    """Create beancount posting with cost price and flag set to None"""
    return data.Posting(
        account=account,
        units=units,
        cost=None,
        price=None,
        flag=None,
        meta=meta,
    )


def create_transaction(
    postings: list[data.Posting], date: datetime, meta: dict, payee: str, narration: str
) -> data.Transaction:
    """Create beancount transaction with default values for flag tags and links"""
    return data.Transaction(
        meta=meta,
        date=date,
        flag=flags.FLAG_OKAY,
        payee=payee,
        narration=narration,
        tags=data.EMPTY_SET,
        links=data.EMPTY_SET,
        postings=postings,
    )


class AccountMapper:
    """Read mapping from specific narration or payee from a yaml or json file and return the details"""

    _mappings = {}

    def __init__(self, mapping_file: str = None):
        if mapping_file is None:
            return
        file_path = Path(mapping_file)
        if not file_path.is_file():
            raise FileNotFoundError(f"File {mapping_file} does not exist")
        file_tags = identify.tags_from_path(mapping_file)
        with open(file_path) as f:
            if "yaml" in file_tags:
                self._mappings = yaml.safe_load(f)
            elif "json" in file_tags:
                self._mappings = json.load(f)
            else:
                raise ValueError(
                    f"Format of file {mapping_file} has not been recognized. Make sure it is YAML or JSON"
                )

    def payee(self, key: str):
        if not self.known(key) or "payee" not in self._mappings[key]:
            return None
        else:
            return self._mappings[key]["payee"]

    def account(self, key: str):
        if self.known(key):
            return self._mappings[key]["account"]
        else:
            return None

    def narration(self, key: str):
        if not self.known(key) or "narration" not in self._mappings[key]:
            return None
        else:
            return self._mappings[key]["narration"]

    def known(self, key: str) -> bool:
        if key in self._mappings:
            return True
        return False
