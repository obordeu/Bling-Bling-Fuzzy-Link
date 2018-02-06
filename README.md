## Bling Bling Fuzzy Link
by Nano Barahona & Olivia Bordeu

This repository is dedicated to develop an algorithm that allows us to match different databases. We have two versions:

* Python 2.7 ~ blink-link.py
* Pyspark ~ sparkly-link.py

Command options:
* --file1: file with first database
* --file2: file with second database
* -v: string with list of variable names
* -s: _sharp_ list of variable names
* -p: number of partitions (only available in sparkly-link)

In the data folder you can find test files. 

A example of how to run the command in python:
```bash
python blink-link.py --file1=../data/test1.txt --file2=../data/test2.txt -v "nombre1 nombre2 apellido1 apellido2 annoNac" -s "provincia"
```

To run the comand using [Apache Spark](https://spark.apache.org/) use:
```bash
spark-submit sparkly-link.py --file1=../data/test1.txt --file2=../data/test2.txt -v "nombre1 nombre2 apellido1 apellido2 annoNac" -s "provincia"
```

The algorithm will separate the list of variables in strings and numeric variables. For string variables the program will allow fuzzy merge by calculating the Levenshtein distance between strings. The algorithm will allways consider the list of sharp variables and will only allow links that are consisten within those variables. 

# Considerations:
* Each dataset should have a id variable, named 'id'
* Both for python and pyspark version you need to install [python-Levenshtein](https://pypi.python.org/pypi/python-Levenshtein)

# Nest steps:
* Create user option for python-Levenshtein versus regular python distance package
* Stop requiring id variable
* Allow the user to define steps
