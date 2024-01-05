import csv
from datetime import datetime
from pathlib import Path

from beancount.core import amount, data
from beancount.ingest.importer import ImporterProtocol

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


class SpkMasterCardImporter(ImporterProtocol):
    last_four_digits: str
    account: str
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
        self.account = account
        self.last_four_digits = last_four_digits
        self.currency = currency
        self._fields = DEFAULT_FIELDS
        self._acc_map = utils.AccountMapper(account_mapping)

    def name(self) -> str:
        return "Sparkasse MasterCard"

    def identify(self, file) -> bool:
        if Path(file.name).suffix.lower() != ".csv":
            return False

        with open(file.name, encoding=self._file_encoding) as f:
            header = f.readline().strip()
            csv_row = f.readline().strip()

        expected_header = ";".join([f'"{field}"' for field in self._fields])

        header_match = header == expected_header
        card_number_match = (
            csv_row.split(";")[0].replace('"', "")[-4:] == self.last_four_digits
        )
        return header_match and card_number_match

    def file_account(self, file):
        return self.account

    def extract(self, file, existing_entries=None):
        if existing_entries:
            entries = existing_entries
        else:
            entries = []
        index = 0
        with open(file.name, encoding=self._file_encoding) as f:
            for index, row in enumerate(
                csv.DictReader(f, delimiter=";", quotechar='"')
            ):
                meta = data.new_metadata(filename=file.name, lineno=index)
                date: datetime = datetime.strptime(
                    row["Buchungsdatum"], self._date_format
                ).date()
                narration = row["Transaktionsbeschreibung"]
                units = amount.Amount(
                    utils.format_amount(row["Buchungsbetrag"]), currency=self.currency
                )
                postings = [utils.create_posting(self.account, units, meta)]

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

    def file_name(self, file):
        return f"MasterCard_{self.last_four_digits}.csv"

    def file_date(self, file):
        return max(map(lambda entry: entry.date, self.extract(file)))
