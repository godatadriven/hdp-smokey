# smokey

Hadoop smoke testing framework

## What is a smoketest

A smoke test is a subset of test cases that cover the most important functionality of a component or system, to ascertain if crucial functions of the software work correctly. [Wikipedia](https://en.wikipedia.org/wiki/Smoke_testing_(software))

## What is this project

A framework for easily creating smoketests for several services from the large Hadoop Ecosystem.
For example (but not limited to):

- Ranger
- Hdfs
- Spark
- Mapreduce
- Zookeeper
- Hive
etc.


Project Organization
------------

    │
    ├── data/               <- The original, immutable data dump. 
    │
    ├── figures/            <- Figures saved by scripts or notebooks.
    │
    ├── notebooks/          <- Jupyter notebooks. Naming convention is a short `-` delimited 
    │                         description, a number (for ordering), and the creator's initials,
    │                        e.g. `initial-data-exploration-01-hg`.
    │
    ├── output/             <- Manipulated data, logs, etc.
    │
    ├── tests/              <- Unit tests.
    │
    ├── smokey/      <- Python module with source code of this project.
    │
    ├── environment.yml     <- conda virtual environment definition file.
    │
    ├── LICENSE
    │
    ├── Makefile            <- Makefile with commands like `make environment`
    │
    ├── README.md           <- The top-level README for developers using this project.
    │
    └── tox.ini             <- tox file with settings for running tox; see tox.testrun.org


--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>.</p>


Set up
------------

Install the virtual environment with conda and activate it:

```bash
$ conda env create -f environment.yml
$ source activate example-project 
```

Or with a pip virtualenv
```bash
mkvirtualenv smoketest --python /usr/local/bin/python3
pip install requests paramiko pywebhdfs requests_kerberos
```

Install `smokey` in the virtual environment:
```bash
$ pip install --editable .
```

Test Data
-------------

for the HdfsDatanode test a file with a known md5 is needed.
this can be generated in the following way:

```bash
dd if=<(openssl enc -aes-256-ctr -pass pass:"$(dd if=/dev/urandom bs=128 count=10 2>/dev/null | tr -dc A-Za-z0-9 | base64)" -nosalt < /dev/zero | tr -dc A-Za-z0-9 | fold -w 75) of=>(tee >(md5sum | awk '{ print $1 }' > md5_of_hdfs_dn_test_file_with_known_md5.txt) >(hdfs dfs -put - /user/smoketest/hdfs_smoketest/hdfs_dn_test_file_with_known_md5.txt) >/dev/null) bs=128M count=3 iflag=fullblock
```
The md5 hash from the file md5_of_hdfs_dn_test_file_with_known_md5.txt needs to be set in the hdfs_verifiers.py


TODO
------------
Describe how to run this on the Hortonworks Sandbox
Document Environment variables Used
Document installation of the library
Implement Kafka smoketesting
Implement Hbase smoketesting
Implement Knox smoketesting

