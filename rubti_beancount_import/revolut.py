from os import path

from beangulp import mimetypes
from beangulp.importers import csvbase
from beangulp.testing import main


class Revolut(csvbase.Importer):
    date = csvbase.Date("Completed Date", "%Y-%m-%d %H:%M:%S")
    narration = csvbase.Column("Description")
    amount = csvbase.Amount("Amount")
    balance = csvbase.Amount("Balance")

    def identify(self, filepath):
        mimetype, encoding = mimetypes.guess_type(filepath)
        if mimetype != "text/csv":
            return False
        with open(filepath) as fd:
            try:
                head = fd.readline().strip()
            except UnicodeDecodeError:
                return False
        return head.startswith(
            "Type,Product,Started Date,Completed Date,Description,Amount,Fee,Currency,State,Balance"
        )

    def filename(self, filepath):
        return "revolut.csv"


if __name__ == "__main__":
    main(Revolut("Assets:ES:Revolut", "EUR"))
