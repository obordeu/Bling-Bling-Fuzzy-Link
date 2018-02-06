## Bling Bling Fuzzy Link
by Nano Barahona & Olivia Bordeu

This repository is dedicated to develop an algorithm that allows us to match different databases. We have two versions:

* Python 2.7 ~ blink-link.py
* Pyspark ~ sparkly-link.py

Command options:
* --file1: file with first database
* --file2: file with second database
* -v: string with list of variable names

In the data folder you can find test files. 

A example of how to run the command in python:
```bash
python blink-link.py --file1=../data/test1.txt --file2=../data/test2.txt -v "nombre1 nombre2 apellido1 apellido2 provincia annoNac"
```

To run the comand using [Apache Spark](https://spark.apache.org/) use:
```bash
spark-submit sparkly-link.py --file1=../data/test1.txt --file2=../data/test2.txt -v "nombre1 nombre2 apellido1 apellido2 provincia annoNac"
```

The algorithm will separate the list of variables in strings and numeric variables. For string variables the program will allow fuzzy merge by calculating the Levenshtein distance between strings. 

# Considerations:
* Each dataset should have a id variable, named 'id'
* Both for python and pyspark version you need to install [python-Levenshtein](https://pypi.python.org/pypi/python-Levenshtein)