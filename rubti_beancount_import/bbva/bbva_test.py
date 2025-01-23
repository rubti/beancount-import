from os import path

from beancount.ingest import regression_pytest

from rubti_beancount_import import BBVAImporter

directory = path.dirname(__file__)
importer = BBVAImporter(
    "Assets:ES:BBVA:Checking",
    "ES12345678901234567890",
    "test_mapping.yaml",
    tags={"share-Example"},
)


@regression_pytest.with_importer(importer)
@regression_pytest.with_testdir(directory)
class TestImporter(regression_pytest.ImporterTestBase):
    pass
