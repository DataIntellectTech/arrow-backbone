-----
Goal
-----

This repo contains Jupyter notebook scripts of queries on NYSE TAQ data in three seperate technologies; DuckDB, Polars and Vaex

-----
Setup
-----

Homer is currently running python version 2.7.17, whch for te purpose of this project was too low a version of python. Instead we set up a virtual enviroment to run python 3.11. To set up the virtual enviroment follow the steps below or follow this link for extra documentation: 

python3 -m venv <path_to_new_env>

The virtual environment can be activated with the following:

source <my_env_name>/bin/activate

For the requirements of this project install the modules with the following inside the virtual environment:

-- pip install duckdb

-- pip install polars

-- pip install vaex

-- pip install pyarrow



-----
Arrow
-----

Arrow is just a memory format through, implementation of the arrow bindings in python are handled by pyarrow.  Pyarrow is used as a replacement for the likes of numpy; which is famously efficient in its handling of strings, arrays and nulls. Pyarrow skips all the nasty serialisation problems when passing between modules (mainly written in C) and python itself. There will always be a price to pay for running in pythons general interpreter lock, but arrow and the query frameworks built on top of it allow us to sidestep a majority of the performance costs. 

Loading and saving tables is done in pyarrow with query and analysis frameworks being built separately. Development in the area is fast and recently a new datasets method has been added which is currently described as experimental. It’s worth revisiting at a later point when the api has matured as we ran into some problems using it.

-----
DuckDB
-----

DuckDB is an in-process SQL Online analytical processing (OLAP) database management system (DBMS), filling the previously empty gap in DBMS, combining pros of popular systems such as SQLite (embedded) and Teradata (OLAP). This allows DuckDB to run complex, long running queries that process significant portions of large-datasets. To achieve this, while also reducing the CPU memory footprint, DuckDB contains a columnar-vectorised query engine whilst utilisating the CPU cache, which means that queries can be interpreted as a large batch of values (or a “vector”) processed in one operation. This can vastly improve the performance of large-scale queries when compared to online transaction processing systems (OLTP) that is used in systems like SQLite, in which databases are implemented as row-oriented. 

DuckDB is free, open source, has no external dependencies and is easily integrated into other build processes with a simplified deployment process: The entire source tree of DuckDB is compiled into two files, a header and an implementation file.  

As DuckDB runs completely embedded within a host process, data can be transferred to and from the database at high-speeds. This allows DuckDB to process foreign data without copying, for example in the DuckDB Python package, that can run queries directly on Pandas data without ever copying or importing any of the data. This flexibility can be extended to a multitude of different libraries, with DuckDB also integrating with frameworks such as IBIS, POLARS, VAEX and more. If the output is preferred to match the inputted data, for example Pandas, the zero-copy integration allows for the data to be easily transferred for the desired output with no extra memory overhead! 

Now you need to load in both the trade and quote .arrow files. For this we used the pyarrow.dataset module from the pyarrow library, utilising the scanner module to construct a scanner to the database. A connection to a database can be created using the DuckDB connect function – by default as an in-memory database:

-----
Polars
-----

Similar to DuckDB
