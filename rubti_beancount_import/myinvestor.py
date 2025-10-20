import csv
from datetime import datetime
from os import path

import beangulp
from beancount.core import amount, data
from beangulp import mimetypes
from beangulp.testing import main

import rubti_beancount_import.utils as utils


class MyInvestor(beangulp.Importer):
    def __init__(
        self,
        account: str,
    ) -> None:
        self.ledger_account = account

    def identify(self, filepath):
        mimetype, encoding = mimetypes.guess_type(filepath)
        if mimetype != "text/csv":
            return False
        with open(filepath, encoding="utf-8") as f:
            header = f.readline().strip()
        return header.startswith(
            "Fecha de operación;Fecha de valor;Concepto;Importe;Divisa"
        )

    def extract(self, filepath, existing=None):
        if existing:
            entries = existing
        else:
            entries = []
        index = 0
        with open(filepath, encoding="utf-8") as f:
            for index, row in enumerate(csv.DictReader(f, delimiter=";")):
                meta = data.new_metadata(filename=filepath, lineno=index)
                date: datetime = datetime.strptime(
                    row["Fecha de valor"], "%d/%m/%Y"
                ).date()
                narration = row["Concepto"]
                units = amount.Amount(
                    utils.format_amount(row["Importe"]), currency=row["Divisa"]
                )
                postings = [utils.create_posting(self.ledger_account, units, meta)]

                entries.append(
                    utils.create_transaction(
                        postings,
                        date,
                        meta,
                        None,
                        narration,
                    )
                )
        return entries

    def filename(self, filepath):
        return "myinvestor." + path.basename(filepath)

    def account(self, filepath):
        return self.ledger_account

    def date(self, filepath):
        return max(map(lambda entry: entry.date, self.extract(filepath)))


if __name__ == "__main__":
    main(MyInvestor("Assets:ES:MyInvestor"))
