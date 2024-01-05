import re

from beancount import loader
from beancount.core import data
from beancount.ingest import importer


class BeanFileSorter(importer.ImporterProtocol):
    """This class is not a normal importer
    but it rather files away the generated bean files"""

    def identify(self, file):
        if re.search(".bean$", file.name) is not None:
            return True

    def file_account(self, file):
        """Returns the month and the date of the first transaction
        in the given bean file bean-file
        and will create a directory structure with year/month
        """
        for entry in loader.load_file(file.name)[0]:
            if isinstance(entry, data.Transaction):
                return f"{entry.date.year}:{entry.date.month}"

    def file_date(self, file):
        """The timestamp in the file name represents the creation time of the file"""
        return None
