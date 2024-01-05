# Beancount Importer

This repository contains [beancount](https://beancount.github.io/docs/) importers to automatically import banking data from the following sources:

* [Sparkasse Germany](https://www.sparkasse.de/)
    * Checking accounts
    * MasterCard
* [BBVA](https://www.bbva.es/) checking accounts in Excel .xlsx format 
* [Edenred Spain](https://www.edenred.es/) bonus cards

The majority of the importers have regression tests in place which are built with the [beancount regression test plugin](https://beancount.github.io/docs/importing_external_data.html#regression-testing-your-importers).

## Importing transactions

In order to import transactions from german Sparkasse, you need to download your transactions in CSV format.

* Log in to your online banking account and select the bank account that you want to import data from
* **Select the time span of interest to filter transactions**
* In case of credit card transactions there is only the option *CSV-Export*. Select this option and download the file
* On checking accounts you may choose the format. The importer expects the ``CSV-CAMT V2`` format. Download your transactions in this format
