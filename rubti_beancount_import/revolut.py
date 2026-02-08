from os import path

from beangulp import mimetypes
from beangulp.importers import csvbase
from beangulp.testing import main


class Importer(csvbase.Importer):
    date = csvbase.Date("Completed Date", "%Y-%m-%d %H:%M:%S")
    narration = csvbase.Column("Description")
    amount = csvbase.Amount("Amount")
    balance = csvbase.Amount("Balance")

    def identify(self, filepath):
        mimetype, encoding = mimetypes.guess_type(filepath)
        if mimetype != "text/csv":
            return False
        with open(filepath) as fd:
            head = fd.read(1024)
        return head.startswith(
            "Type,Product,Started Date,Completed Date,Description,Amount,Fee,Currency,State,Balance"
        )

    def filename(self, filepath):
        return "revolut.csv"


if __name__ == "__main__":
    main(Importer("Assets:ES:Revolut", "EUR"))
