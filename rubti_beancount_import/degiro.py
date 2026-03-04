from datetime import datetime, timedelta

import beangulp
import pandas as pd
from beancount.core import amount, data
from beangulp import mimetypes
from beangulp.testing import main

import rubti_beancount_import.utils as utils


class Degiro(beangulp.Importer):
    NEW_HEADER = [
        "Date",
        "Time",
        "Valuta date",
        "Product",
        "ISIN",
        "Description",
        "Unnamed: 6",
        "FX",
        "Amount",
        "Currency",
        "Balance",
        "Order-ID",
    ]
    ORIGINAL_HEADER = [
        "Datum",
        "Uhrze",
        "Valutadatum",
        "Produkt",
        "ISIN",
        "Beschreibung",
        "FX",
        "Änderung",
        "Unnamed: 8",
        "Saldo",
        "Unnamed: 10",
        "Order-ID",
    ]
    DROP_DESCRIPTION = (
        "Degiro Cash Sweep Transfer",
        "Auszahlung von Ihrem Geldkonto bei der flatexDEGIRO Bank:",
    )

    def __init__(self, account: str, currency: str = "EUR") -> None:
        self.ledger_account = account
        self.currency = currency

    def identify(self, filepath):
        mimetype, _ = mimetypes.guess_type(filepath)
        if mimetype != "text/csv":
            return False
        with open(filepath, encoding="utf-8") as fd:
            try:
                fd.readline().strip()
            except UnicodeDecodeError:
                return False
        df = pd.read_csv(filepath, delimiter=",")
        if list(df.columns) != self.ORIGINAL_HEADER:
            return False
        return True

    def extract(self, filepath, existing=None):
        if existing:
            entries = existing
        else:
            entries = []
        balances = []

        # Read CSV and rename columns
        df = pd.read_csv(filepath, delimiter=",")
        df.columns = self.NEW_HEADER

        # Filter out rows to drop
        df = df[~df["Description"].str.startswith(self.DROP_DESCRIPTION)]

        for index, row in df.iterrows():
            # Skip rows with empty amount
            if pd.isna(row["Amount"]) or row["Amount"] == "":
                continue

            meta = data.new_metadata(filename=filepath, lineno=index + 2)
            date_obj = datetime.strptime(row["Date"], "%d-%m-%Y").date()

            # Build narration from Product | Description | ISIN | Order-ID
            narration_parts = []
            for field in ["Product", "Description", "ISIN", "Order-ID"]:
                if pd.notna(row[field]) and str(row[field]).strip():
                    narration_parts.append(str(row[field]))

            narration = " | ".join(narration_parts)

            # Parse amount
            units = amount.Amount(
                utils.format_amount(row["Amount"]), currency=self.currency
            )
            postings = [utils.create_posting(self.ledger_account, units, meta)]

            entries.append(
                utils.create_transaction(
                    postings,
                    date_obj,
                    meta,
                    None,
                    narration,
                )
            )
            balance = amount.Amount(
                utils.format_amount(row["Balance"]), currency=self.currency
            )
            balances.append(
                data.Balance(
                    meta,
                    date_obj + timedelta(days=1),
                    self.ledger_account,
                    balance,
                    None,
                    None,
                )
            )
        entries.append(balances[0])
        return entries

    def filename(self, filepath):
        return "degiro.csv"

    def account(self, filepath):
        return self.ledger_account

    def date(self, filepath):
        return max(map(lambda entry: entry.date, self.extract(filepath)))


if __name__ == "__main__":
    main(Degiro("Assets:DE:Degiro", "EUR"))
