import csv
from datetime import datetime

import beangulp
from beancount.core import amount, data
from beangulp import mimetypes
from beangulp.testing import main

import rubti_beancount_import.utils as utils

DEFAULT_FIELDS = (
    "Umsatz getätigt von",
    "Belegdatum",
    "Buchungsdatum",
    "Originalbetrag",
    "Originalwährung",
    "Umrechnungskurs",
    "Buchungsbetrag",
    "Buchungswährung",
    "Transaktionsbeschreibung",
    "Transaktionsbeschreibung Zusatz",
    "Buchungsreferenz",
    "Gebührenschlüssel",
    "Länderkennzeichen",
    "BAR-Entgelt+Buchungsreferenz",
    "AEE+Buchungsreferenz",
    "Abrechnungskennzeichen",
)


class SpkMasterCardImporter(beangulp.Importer):
    last_four_digits: str
    ledger_account: str
    currency: str
    _date_format = "%d.%m.%y"
    _file_encoding = "ISO-8859-1"
    _acc_map: utils.AccountMapper

    def __init__(
        self,
        account: str,
        last_four_digits: str,
        account_mapping: str = None,
        currency: str = "EUR",
    ) -> None:
        self.ledger_account = account
        self.last_four_digits = last_four_digits
        self.currency = currency
        self._acc_map = utils.AccountMapper(account_mapping)

    def identify(self, filepath) -> bool:
        mimetype, encoding = mimetypes.guess_type(filepath)
        if mimetype != "text/csv":
            return False

        with open(filepath, encoding=self._file_encoding) as f:
            header = f.readline().strip()
            csv_row = f.readline().strip()

        expected_header = ";".join([f'"{field}"' for field in DEFAULT_FIELDS])

        header_match = header == expected_header
        card_number_match = (
            csv_row.split(";")[0].replace('"', "")[-4:] == self.last_four_digits
        )
        return header_match and card_number_match

    def account(self, filepath):
        return self.ledger_account

    def extract(self, filepath, existing=None):
        if existing:
            entries = existing
        else:
            entries = []
        index = 0
        with open(filepath, encoding=self._file_encoding) as f:
            for index, row in enumerate(
                csv.DictReader(f, delimiter=";", quotechar='"')
            ):
                meta = data.new_metadata(filename=filepath, lineno=index)
                date: datetime = datetime.strptime(
                    row["Buchungsdatum"], self._date_format
                ).date()
                narration = row["Transaktionsbeschreibung"]
                units = amount.Amount(
                    utils.format_amount(row["Buchungsbetrag"]), currency=self.currency
                )
                postings = [utils.create_posting(self.ledger_account, units, meta)]

                if self._acc_map.known(narration):
                    postings.append(
                        utils.create_posting(
                            self._acc_map.account(narration), -units, None
                        )
                    )

                payee = None
                if self._acc_map.payee(narration):
                    payee = self._acc_map.payee(narration)
                if self._acc_map.narration(narration):
                    narration = self._acc_map.narration(narration)

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

    def filename(self, filepath):
        return f"MasterCard_{self.last_four_digits}.csv"

    def date(self, filepath):
        return max(map(lambda entry: entry.date, self.extract(filepath)))


if __name__ == "__main__":
    importer = SpkMasterCardImporter(
        "Liabilities:DE:MasterCard:Silver-4932",
        "4932",
        account_mapping="../../test_mapping.yaml",
    )
    main(importer)
