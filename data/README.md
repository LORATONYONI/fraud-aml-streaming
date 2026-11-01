# Data

The pipeline runs on **synthetic data by default**, so you don't need anything here to start.

To replay **PaySim** instead:

1. Create a free account at [kaggle.com](https://www.kaggle.com).
2. Download the dataset from <https://www.kaggle.com/datasets/ealaxi/paysim1>. It contains one CSV
   (about 470 MB, 6.3 million rows) named `PS_20174392719_1491204439457_log.csv`.
3. Unzip it and save the CSV here as `data/paysim.csv`. This folder is git-ignored, so the file
   never ends up on GitHub.
4. Run `make demo-paysim`.

PaySim is a synthetic dataset generated from a month of real logs of a mobile-money service in an
African country (Lopez-Rojas, Elmir & Axelsson, 2016). Its `isFraud` column becomes the
`paysim_fraud` ground-truth label. The producer still injects the AML scenarios (structuring, mule
networks, velocity bursts) on top, because PaySim itself only contains account-takeover fraud.
