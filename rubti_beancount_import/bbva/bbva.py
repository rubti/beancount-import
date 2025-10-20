from datetime import datetime
from decimal import Decimal
from pathlib import Path

import beangulp
import pandas as pd
from beancount.core import amount, data
from beangulp.testing import main

import rubti_beancount_import.utils as utils

IGNORE_DESCRIPTION = ("Pago con tarjeta", "Otros")


class BBVAImporter(beangulp.Importer):
    """Beancount importer for Excel sheet for checking accounts from Spanish BBVA bank"""

    _expected_header = pd.Index(
        [
            "Unnamed: 0",
            "Fecha",
            "F.Valor",
            "Concepto",
            "Movimiento",
            "Importe",
            "Divisa",
            "Disponible",
            "Divisa.1",
            "Observaciones",
        ],
        dtype="object",
    )
    _new_expected_header = pd.Index(
        [
            "Unnamed: 0",
            "F.Valor",
            "Fecha",
            "Concepto",
            "Movimiento",
            "Importe",
            "Divisa",
            "Disponible",
            "Divisa.1",
            "Observaciones",
        ],
        dtype="object",
    )
    ledger_account: str
    account_number: str
    currency: str
    _excel_header_line: int = 4
    _acc_map: utils.AccountMapper

    def __init__(
        self,
        account: str,
        account_number: str,
        account_mapping: str = None,
        currency: str = "EUR",
        tags: data.Set = data.EMPTY_SET,
    ) -> None:
        self.ledger_account = account
        self.account_number = account_number
        self.currency = currency
        self._acc_map = utils.AccountMapper(account_mapping)
        self.tags = tags

    def identify(self, filepath) -> bool:
        if Path(filepath).suffix.lower() != ".xlsx":
            return False
        try:
            raw_content = pd.read_excel(filepath, header=self._excel_header_line)
        except:
            return False
        c = raw_content.columns
        return c.equals(self._expected_header) or c.equals(self._new_expected_header)

    def account(self, filepath):
        return self.ledger_account

    def extract(self, filepath, existing=None):
        if existing:
            entries = existing
        entries = []
        raw_content = pd.read_excel(filepath, header=self._excel_header_line)
        for ind, row in raw_content.iterrows():
            meta = data.new_metadata(filename=filepath, lineno=row.name)
            units = amount.Amount(
                Decimal(str(round(row["Importe"], 2))), currency=self.currency
            )
            postings = [utils.create_posting(self.ledger_account, units, meta)]

            payee = row["Concepto"]
            search_key = payee

            if self._acc_map.known(search_key):
                postings.append(
                    utils.create_posting(
                        self._acc_map.account(search_key), -units, None
                    )
                )

            if self._acc_map.payee(search_key):
                payee = self._acc_map.payee(search_key)

            narration = row["Movimiento"]
            if narration in IGNORE_DESCRIPTION:
                narration = None
            if self._acc_map.narration(search_key):
                narration = self._acc_map.narration(search_key)
            if narration is None:
                narration = payee
                payee = None

            try:
                date = row["Fecha"].date()
            except AttributeError:
                date = datetime.strptime(row["Fecha"], "%d/%m/%Y").date()
            entries.append(
                utils.create_transaction(
                    postings,
                    date,
                    meta,
                    payee,
                    narration,
                    tags=self.tags,
                )
            )
        return entries

    def file_name(self, file):
        return f"BBVA_{self.account_number}.xlsx"

    def file_date(self, file):
        return max(map(lambda entry: entry.date, self.extract(file)))


if __name__ == "__main__":
    importer = BBVAImporter(
        "Assets:ES:BBVA:Checking",
        "ES12345678901234567890",
        "../test_mapping.yaml",
        tags={"share-Example"},
    )
    main(importer)
