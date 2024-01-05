import csv
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

from beancount.core import amount, data
from beancount.ingest import importer

import rubti_beancount_import.utils as utils

DEFAULT_FIELDS = (
    "Auftragskonto",
    "Buchungstag",
    "Valutadatum",
    "Buchungstext",
    "Verwendungszweck",
    "Glaeubiger ID",
    "Mandatsreferenz",
    "Kundenreferenz (End-to-End)",
    "Sammlerreferenz",
    "Lastschrift Ursprungsbetrag",
    "Auslagenersatz Ruecklastschrift",
    "Beguenstigter/Zahlungspflichtiger",
    "Kontonummer/IBAN",
    "BIC (SWIFT-Code)",
    "Betrag",
    "Waehrung",
    "Info",
)


class SpkGiroImporter(importer.ImporterProtocol):
    _txn_infos: dict = {}
    _fields: Sequence[str]
    iban: str
    account: str
    currency: str
    date_format: str
    file_encoding: str
    _acc_map: utils.AccountMapper

    def __init__(
        self,
        iban: str,
        account: str,
        currency: str = "EUR",
        date_format: str = "%d.%m.%y",
        file_encoding: str = "ISO-8859-1",
        account_mapping: Path = None,
    ):
        self.iban = iban
        self.account = account
        self.currency = currency
        self.date_format = date_format
        self.file_encoding = file_encoding
        self._fields = DEFAULT_FIELDS
        self._acc_map = utils.AccountMapper(account_mapping)

    def identify(self, file):
        if Path(file.name).suffix.lower() != ".csv":
            return False
        with open(file.name, encoding=self.file_encoding) as f:
            header = f.readline().strip()
            csv_row = f.readline().strip()

        expected_header = ";".join([f'"{field}"' for field in self._fields])

        header_match = header == expected_header
        iban_match = csv_row.split(";")[0].replace('"', "") == self.iban
        return header_match and iban_match

    def extract(self, file, existing_entries=None):
        if existing_entries:
            entries = existing_entries
        else:
            entries = []
        index = 0
        with open(file.name, encoding=self.file_encoding) as f:
            for index, row in enumerate(
                csv.DictReader(f, delimiter=";", quotechar='"')
            ):
                meta = data.new_metadata(filename=file.name, lineno=index)
                date: datetime = datetime.strptime(
                    row["Buchungstag"], self.date_format
                ).date()
                payee = row["Beguenstigter/Zahlungspflichtiger"]
                narration = row["Verwendungszweck"].strip()
                units = amount.Amount(
                    utils.format_amount(row["Betrag"]), currency=self.currency
                )
                postings = [utils.create_posting(self.account, units, meta)]

                if len(payee) > 0:
                    search_key = payee
                else:
                    search_key = narration

                if self._acc_map.known(search_key):
                    postings.append(
                        utils.create_posting(
                            self._acc_map.account(search_key), -units, None
                        )
                    )
                if self._acc_map.payee(search_key):
                    payee = self._acc_map.payee(search_key)
                if self._acc_map.narration(search_key):
                    payee = self._acc_map.narration(search_key)

                entries.append(
                    utils.create_transaction(
                        postings,
                        date,
                        meta,
                        payee,
                        narration,
                    )
                )
        return entries

    def file_account(self, file):
        return self.account

    def file_name(self, file):
        return f"{self.iban}.csv"

    def file_date(self, file):
        return max(map(lambda entry: entry.date, self.extract(file)))
