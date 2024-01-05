from os import path

from beancount.ingest import regression_pytest

from rubti_beancount_import import SpkMasterCardImporter

importer = SpkMasterCardImporter(
    "Liabilities:DE:MasterCard:Silver-4932", "4932", account_mapping="test_mapping.yaml"
)
directory = path.dirname(__file__)


@regression_pytest.with_importer(importer)
@regression_pytest.with_testdir(directory)
class TestImporter(regression_pytest.ImporterTestBase):
    pass
