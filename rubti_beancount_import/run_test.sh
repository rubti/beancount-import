#!/bin/bash
# Run all the regression tests.
DATA=./tests
python3 giro.py   test $DATA/spk_giro
python3 master_card.py test $DATA/spk_mastercard
python3 bbva.py test $DATA/bbva
python3 myinvestor.py test $DATA/myinvestor