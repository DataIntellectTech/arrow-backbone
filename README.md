Goal
-----

This repo contains Jupyter notebook scripts of queries on NYSE TAQ data in three seperate technologies; DuckDB, Polars and Vaex

Setup
-----

Homer is currently running python version 2.7.17, whch for te purpose of this project was too low a version of python. Instead we set up a virtual enviroment to run python 3.11. To set up the virtual enviroment follow the steps below or follow this link for extra documentation: 
```
python3 -m venv <path_to_new_env>
```
The virtual environment can be activated with the following:
```
source <my_env_name>/bin/activate
```
For the requirements of this project install the modules with the following inside the virtual environment (Polars comes with a small list of optional dependancies that should be downloaded along with polars, however only a few are utilised in this project:):
```
pip install duckdb
pip install 'polars[all]'
pip install vaex
pip install pyarrow
```
Arrow
--- 

Arrow is just a memory format through, implementation of the arrow bindings in python are handled by pyarrow.  Pyarrow is used as a replacement for the likes of numpy; which is famously efficient in its handling of strings, arrays and nulls. Pyarrow skips all the nasty serialisation problems when passing between modules (mainly written in C) and python itself. There will always be a price to pay for running in pythons general interpreter lock, but arrow and the query frameworks built on top of it allow us to sidestep a majority of the performance costs. 

Loading and saving tables is done in pyarrow with query and analysis frameworks being built separately. Development in the area is fast and recently a new datasets method has been added which is currently described as experimental. It’s worth revisiting at a later point when the api has matured as we ran into some problems using it.

DuckDB
---

DuckDB is an in-process SQL Online analytical processing (OLAP) database management system (DBMS), filling the previously empty gap in DBMS, combining pros of popular systems such as SQLite (embedded) and Teradata (OLAP). This allows DuckDB to run complex, long running queries that process significant portions of large-datasets. To achieve this, while also reducing the CPU memory footprint, DuckDB contains a columnar-vectorised query engine whilst utilisating the CPU cache, which means that queries can be interpreted as a large batch of values (or a “vector”) processed in one operation. This can vastly improve the performance of large-scale queries when compared to online transaction processing systems (OLTP) that is used in systems like SQLite, in which databases are implemented as row-oriented. 

DuckDB is free, open source, has no external dependencies and is easily integrated into other build processes with a simplified deployment process: The entire source tree of DuckDB is compiled into two files, a header and an implementation file.  

We carried out our queries in a Jupyter notebook, with the following modules imported:
```
import pyarrow as pa
import pyarrow.dataset as ds
import duckdb
```
Now you need to load in both the trade and quote .arrow files. For this we used the pyarrow.dataset module from the pyarrow library, utilising the scanner module to construct a scanner to the database:
```
trade = ds.dataset("~/data/trade_taq2019.arrow", format = "arrow")
quote = ds.dataset("~/data/quote_taq2019.arrow", format = "arrow")
```
A connection to a database can be created using the DuckDB connect function – by default as an in-memory database:
```
con = duckdb.connect()
```
When running a query, con.query, con.execute or con.sql can be added as a prefix with the query in SQL enclosed inbetween explanation marks and brackets (here con is arbitrary and is the variable name assigned to the duckdb databse connection showed above):
```
con.query("query in SQL")
```
As DuckDB runs completely embedded within a host process, data can be transferred to and from the database at high-speeds. This allows DuckDB to process foreign data without copying, for example in the DuckDB Python package, that can run queries directly on Pandas data without ever copying or importing any of the data. This flexibility can be extended to a multitude of different libraries, with DuckDB also integrating with frameworks such as IBIS, POLARS, VAEX and more. If the output is preferred to match the inputted data, for example Pandas, the zero-copy integration allows for the data to be easily transferred for the desired output with no extra memory overhead! To specify the desired output, an extension of the framework can be added to the end of the query, for example .df() for a pandas DataFrame, .pl() for a polars DataFrame, .fetchall() for the native format and more. If no returned format is specified, the query will return a preview of the result (effectively a top and tail of the output) with a significant decrease in the queries memory usage. This can be extremely useful for checking if the output is correct without fulling loading in the output, useing less memory and taking less time.
```
con.query("query in SQL").df()
```

Polars
---

Polars is an open source DataFrame library completely written in Rust, allowing it to perform many operations in parallel. Polars does not use an index for the DataFrame, instead repersenting data internally using Apache Arrow arrays resulting in effecient load time, memory usage and computation. Polars also supports lazy evaluation, meaning that Polars examines your queries, optimises them, and looks for ways to accelerate the query or reduce memory. While DuckDB and Polars share many similarities, DuckDB is focussed more on providing an in-process OLAP Sqlite alternative, whereas Polars is more focussed on providing a scalable DataFrame interface to many languages. Those different objectives lead to different optimisation strategies and different algorithm prioritisation. Aside from their differences however, interoperability allows them to work well together with data able to be copied between them with zero overhead in memory.

Polars data storage is aso column oriented, sharing the same benefits as discussed in the DuckDB section. Another cool trick that Plars has is “Out of Core” processing allowing many queries to be executed completely out of core (this means that datasets can be processed that are larger than the RAM) and Arrow/IPC files can be memory mapped with the same stragegy that vaex uses.

Data can be imported similarly to DuckDB, however a scanner must also be used to tell Polars that we are working in lazy mode. This allows the query optimiser to push down predicates and projections to the scan level, thereby optimising the query and reducing memory overhead:
```
trade = ds.dataset("~/data/trade_taq2019.arrow", format = "arrow")
trade = pl.scan_ds(trade, allow_pyarrow_filter=True)

quote = ds.dataset("~/data/quote_taq2019.arrow", format = "arrow")
quote = pl.scan_ds(quote, allow_pyarrow_filter=True)
```
For queries, a keyword on what type of operation is added as a prefix to the query with .collect() added on the end to fetch the result. It should be noted that for an aggregration query, the data must first be sorted by time, even if the data has already been sorted. This mayb be something that patched in the future. The query structure follows a similar syntax to the Pandas Dataframe:
```
trade.filter("query").collect

trade = trade.sort("Time")
trade.groupby_dynamic("query").collect
```
Vaex
---

Vaex is an open source DataFrame library in python with incredible processing power, without loading the entire dataset into memory. Vaex shares some similar advantages to Polars including “Out of Core” processing, however they work differently. Vaex’s method of “Out of Core” analysis is memory mapping files, for example parquet or csv files first need to be read and converted into a file format that can be memory mapped - this is great for operating such as filters or aggregrations, however operations that need a full data shuffle, such as sorts, don’t have a great performance on memory mapped data. Polars however tackles “Out of Core” processing differently, as it does not rely on memory mapping, but on streaming the data in batches (and spiling to disk if needed) which gives control to which data is held in memory.

The vaex.open method can be used to read an arrow or CSV file. If it is reading a CSV file, this method allows Vaex to ‘lazily’ read the CSV file (i.e the data from the CSV file will be streamed when computations need to be executed, which is all powered by Apache Arrow under the hood. This allows you to work with large CSV files without having to care about the RAM!). When opening a CSV file this way, Vaex will do a quick scan on the file to determine some basic metadata such as the number of rows, column names and their data types:
```
trade = vaex.open("~/file_path")
quote = vaex.open("~/file_path")
```
Vaex query syntax is very similar to Polars, both having a strong resemblance to pandas syntax. When doing a filter query, the syntax is extremely similar, except instead of using a .filter keyword, square brackets are used instead. For an aggregration opertation, a .groupby keyword is used. When experimenting with a time binning operation, we found that binning on a one minute time bucket would break the whole method, and there was very little documentation online to help - however binning on one hour intervals works okay, so likely this is something that will be patched in the future:
```
trade[query]
trade.groupby(query)
```
