import csv
from datetime import datetime
from pathlib import Path

import beangulp
from beancount.core import amount, data
from beangulp import mimetypes
from beangulp.testing import main

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


class SpkGiroImporter(beangulp.Importer):
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
        self.ledger_account = account
        self.currency = currency
        self.date_format = date_format
        self.file_encoding = file_encoding
        self._fields = DEFAULT_FIELDS
        self._acc_map = utils.AccountMapper(account_mapping)

    def identify(self, filepath):
        mimetype, encoding = mimetypes.guess_type(filepath)
        if mimetype != "text/csv":
            return False
        with open(filepath, encoding=encoding) as f:
            header = f.readline().strip()
            csv_row = f.readline().strip()

        expected_header = ";".join([f'"{field}"' for field in DEFAULT_FIELDS])

        header_match = header == expected_header
        iban_match = csv_row.split(";")[0].replace('"', "") == self.iban
        return header_match and iban_match

    def extract(self, filepath, existing=None):
        if existing:
            entries = existing
        else:
            entries = []
        index = 0
        with open(filepath, encoding=self.file_encoding) as f:
            for index, row in enumerate(
                csv.DictReader(f, delimiter=";", quotechar='"')
            ):
                meta = data.new_metadata(filename=filepath, lineno=index)
                date: datetime = datetime.strptime(
                    row["Buchungstag"], self.date_format
                ).date()
                payee = row["Beguenstigter/Zahlungspflichtiger"]
                narration = row["Verwendungszweck"].strip()
                units = amount.Amount(
                    utils.format_amount(row["Betrag"]), currency=self.currency
                )
                postings = [utils.create_posting(self.ledger_account, units, meta)]

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

    def account(self, filepath):
        return self.ledger_account

    def filename(self, filepath):
        return f"{self.iban}.csv"

    def date(self, filepath):
        return max(map(lambda entry: entry.date, self.extract(filepath)))


if __name__ == "__main__":
    importer = SpkGiroImporter(
        iban="DE12345678901234567890",
        account="Assets:DE:SpkCGW:Checking",
        account_mapping="./tests/test_mapping.yaml",
    )
    main(importer)
