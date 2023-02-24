# arrow-backbone

Arrow, DuckDB, Polars and Vaex 

 

DuckDB notes 

Background/overview: 

- Database management system (DBMS) 

- Relational (table-oriented) DBMS - Supports SQL 

- Designed to support analytical query workloads (online analytcial processing (OLAP)) - large scale changes to big data sets 

- Contains a columnar-vectorised query execution engine 

- Large batch of vectors are processed in one operation 

- Has no external dependencies 

- For releases, the entire source tree is compiled into two files; a header and an implementation file - simplifies deployment and integration into other builds - for building all that is required is a C++11 compiler 

- DuckDB does not run as a separate process, but completely embedded within a host process 

- DuckDB can process foreign data without copying - DuckDB python package can run queries* directly on Pandas data without ever importing or copying any data 

- Data can be stored in single-file databases 

- Supports secondary indexes to speed up queries trying to find a single table entry 

Setup/functions: 

- A connection to a database can be created using the connect function – by default as an in-memory  database --- con = duckdb.connect() 

 

----- 

BLOG 

----- 

  

--INTRO AND ADDRESS THE PROBLEM 

At aquaQ we're well used to working with in memory columnar data structures. Years of KDB experience show that the speed and efficenicy of this approach to data storage is unmatched... until now. KDB+ uses its own proprietry file format - unusable outside of the KX ecosystem. Apache arrow was announced in 2016, it uses many of the same optimisations that KDB does and so enjoys the same benefits, constant random access time, vectorisation optimisation etc; all while being open source. Having an open source file format as the backbone of an analysis pipeline allows for flexibility of processing, query your data in python, rust or GO without copying or converting once. Columnar databases for the masses! 

  

---ARROW OVERVIEW 

First a bit on arrow. Arrow is an open source columnar memory standard. When we think about tables we intuituvely know that they are two dimensional - rows and columns. Memory however is linear. This leaves us a choice - do we write each row consecutively in memory, or, column by column? We've been wading through the row era for years now. And while it makes sense to have each record of a table follow the next. By switching to a column layout - we can be a bit more streamlined. For a full description of what makes arrow so great take a look at LINK. 

  

---PYTHON INTEGRATION 

Arrow is just a memory format through, no query language built in like KDB. Python is currently the most widley used language in the data science community. Modules that interface with arrow are being developed at break neck pace. Here we will investigate three of the big players in the forward thinking data science landscape - Duckdb, Polars and Vaex.  

  

---METHOD FOR TESTING 

To get a nice comparison going we have two simple queries. A filter query that selects the opening apple trade, and an aggregate query that sums the trade volume per sym and minute window of time. We're going to compare each module including data import and query structure, taking a look at the memory is also worthwhile so weve thrown that in too.  

  

---Pyarrow 

Pyarrow is the unified handler for the arrow/parquet bindings in python. Data loading and writing is handled wonderfully and the library is under constant development. Most recent of which is the dataset feature which allows for handling of massive larger than memory partitioned files.   

  

  

Duckdb 

----- 

DuckDB is a relational (table oriented) database management system, designed to support analytical query workloads, which are characterised by complex, long-running queries that process significant portions of large-scale datasets.  Examples of these queries are aggregations over entire tables or joins between several large tables – resulting in large-scale changes to the data, with large portions of tables being changed, added or appended at the same time.  

 

For such operations on large datasets, it is imperative to reduce the amount of CPU cycles that are expended per individual value. To achieve this, DuckDB contains a columnar-vectorised query execution engine, where queries can be interpreted as a large batch of values (a “vector”) processed in one operation. This greatly reduces the overhead that is present systems which process each row sequentially, such as PostgreSQL, MySQL or SQLite – resulting in far better performance in online analytical processing queries.  

DuckDB has no external dependencies and is easily integrated into other build processes with a simplified deployment process: The entire source tree of DuckDB is compiled into two files, a header and an implementation file.   

DuckDB runs completely embedded within a host process, allowing for high speed  

  

  

  

----- 

  

---Polars 

Pandas is *THE* weapon of choice in the data science community. At 15 years old now, shes starting to show her age. The new and fiercer polars doesnt eat bamboo - it eats data. Developed on the same dataframe backbone as pandas, polars is built from the group up in rust. Polars uses all available cores, has a built in query optimisation engine, a powerful expression API and more.  

  

Data ingest is again handled by pyarrow    

  

  

  

  

  

 

 
