import csv
import decimal
import json
import os
import re
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

from beancount.core import amount, data
from beancount.ingest.importer import ImporterProtocol


class EdenredImporter(ImporterProtocol):
    card_number: str
    card_type: str
    account: str
    currency: str
    date_format: str
    file_encoding: str
    _fields: Sequence[str]
    _txn_infos: dict = {}

    def __init__(
        self,
        account: str,
        card_number: str,
        account_mapping: Path = None,
        currency: str = "EUR",
        date_format: str = "%d/%m/%Y %H:%M:%S",
        file_encoding: str = "UTF-8",
    ) -> None:
        self.account = account
        self.card_number = card_number
        self.currency = currency
        self.date_format = date_format
        self.file_encoding = file_encoding
        if account_mapping is not None:
            f = open(account_mapping)
            self._txn_infos = json.load(f)
            f.close()

    def _fmt_amount(self, amount: str) -> decimal.Decimal:
        """Removes Spanish thousands separator and converts decimal point to US."""
        dec_amount = decimal.Decimal(amount.replace(".", "").replace(",", "."))
        return dec_amount

    def name(self) -> str:
        return "Edenred Ticket"

    def identify(self, file) -> bool:
        if Path(file.name).suffix.lower() != ".csv":
            return False

        act_card_number: str = ""
        with open(file.name, encoding=self.file_encoding) as f:
            csv_reader = csv.reader(f, delimiter=";")
            rows = []
            try:
                rows = list(csv_reader)
            except UnicodeDecodeError:
                return False

            act_card_number = re.findall(r"\d+", rows[4][0])[0]

        return self.card_number == act_card_number

    def file_account(self, file):
        return self.account

    def extract(self, file, existing_entries=None):
        entries = []
        with open(file.name, encoding=self.file_encoding) as fr:
            lines = fr.readlines()

            with open(file.name + ".tmp", "w", encoding=self.file_encoding) as fw:
                index = 0
                for line in lines:
                    if index >= 10:
                        fw.write(line)
                    index += 1

        with open(file.name + ".tmp", encoding=self.file_encoding) as f:
            index = 0
            for index, row in enumerate(
                csv.DictReader(f, delimiter=";", quotechar='"')
            ):
                meta = data.new_metadata(filename=file.name, lineno=index)
                date: datetime = datetime.strptime(
                    row["Fecha"], self.date_format
                ).date()
                payee = row["Detalle transacción"]
                units = amount.Amount(
                    self._fmt_amount(row["Importe"]), currency=self.currency
                )
                postings = [
                    data.Posting(
                        self.account,
                        units=units,
                        cost=None,
                        price=None,
                        flag=None,
                        meta=None,
                    )
                ]
                txn_payee = payee
                narration = ""
                if payee in self._txn_infos:
                    postings.append(
                        data.Posting(
                            account=self._txn_infos[payee]["account"],
                            units=-units,
                            price=None,
                            flag=None,
                            meta=None,
                            cost=None,
                        )
                    )
                    if "payee" in self._txn_infos[payee]:
                        txn_payee = self._txn_infos[payee]["payee"]
                    if "narration" in self._txn_infos[payee]:
                        narration = self._txn_infos[payee]["narration"]

                txn = data.Transaction(
                    meta=meta,
                    date=date,
                    flag=self.FLAG,
                    payee=txn_payee,
                    narration=narration,
                    tags=data.EMPTY_SET,
                    links=data.EMPTY_SET,
                    postings=postings,
                )
                entries.append(txn)
        os.remove(file.name + ".tmp")
        return entries

    def file_name(self, file):
        card_type: str = ""
        with open(file.name, encoding=self.file_encoding) as f:
            csv_reader = csv.reader(f, delimiter=";")
            rows = list(csv_reader)
            card_type = (
                str(rows[3][0]).replace("Tipo de producto: ", "").replace(" ", "")
            )
        return f"{card_type}_{self.card_number}.csv"

    def file_date(self, file):
        return max(map(lambda entry: entry.date, self.extract(file)))
