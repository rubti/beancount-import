from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd
from beancount.core import amount, data
from beancount.ingest.importer import ImporterProtocol

import rubti_beancount_import.utils as utils

IGNORE_DESCRIPTION = ("Pago con tarjeta", "Otros")


class BBVAImporter(ImporterProtocol):
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
    account: str
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
        self.account = account
        self.account_number = account_number
        self.currency = currency
        self._acc_map = utils.AccountMapper(account_mapping)
        self.tags = tags

    def name(self) -> str:
        return "BBVA Checking"

    def identify(self, file) -> bool:
        if Path(file.name).suffix.lower() != ".xlsx":
            return False
        try:
            raw_content = pd.read_excel(file.name, header=self._excel_header_line)
            print(raw_content.columns)
        except:
            return False
        c = raw_content.columns
        return c.equals(self._expected_header) or c.equals(self._new_expected_header)

    def file_account(self, file):
        return self.account

    def extract(self, file, existing_entries=None):
        if existing_entries:
            entries = existing_entries
        entries = []
        raw_content = pd.read_excel(file.name, header=self._excel_header_line)
        for ind, row in raw_content.iterrows():
            meta = data.new_metadata(filename=file.name, lineno=row.name)
            units = amount.Amount(
                Decimal(str(round(row["Importe"], 2))), currency=self.currency
            )
            postings = [utils.create_posting(self.account, units, meta)]

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
