from os import path

from beancount.ingest import regression_pytest

from rubti_beancount_import import SpkGiroImporter

importer = SpkGiroImporter(
    iban="DE12345678901234567890",
    account="Assets:DE:SpkCGW:Checking",
    account_mapping="test_mapping.yaml",
)

directory = path.dirname(__file__)


@regression_pytest.with_importer(importer)
@regression_pytest.with_testdir(directory)
class TestImporter(regression_pytest.ImporterTestBase):
    pass
