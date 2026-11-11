# 🚀 Complete PySpark Tutorial: From Beginner to Advanced

Welcome to the **Complete PySpark Step-by-Step Tutorial Series**! This repository is designed to teach you Apache Spark with Python from absolute fundamentals up to enterprise-level distributed data engineering and machine learning.

Every topic is explained in **simple, friendly, and point-to-point language** with realistic, copy-pasteable, and runnable examples.

---

## 📌 Critical Memory Rule: `spark.stop()`
> [!IMPORTANT]
> **Every single notebook ends with `spark.stop()`.**
> Apache Spark runs a Java Virtual Machine (JVM) background process with worker threads. If you leave Spark sessions running in background notebook kernels, they will continuously hold onto your computer's RAM and CPU cores. Calling `spark.stop()` releases all memory and frees your system resources immediately.

---

## 📚 Curriculum Roadmap

```
Beginner Level (Core Foundations)
├── 01_introduction_and_architecture.ipynb   ──► Cluster Architecture, SparkSession, Lazy Evaluation
├── 02_rdd_fundamentals.ipynb                ──► RDDs, Narrow vs Wide Transformations, Word Count
├── 03_dataframe_basics_and_schemas.ipynb    ──► DataFrames, StructType Schemas, withColumn
└── 04_data_manipulation_and_transformations ──► Filtering, Nulls, Duplicates, Strings, Dates, Arrays

Intermediate Level (Data Engineering & Analytics)
├── 05_aggregations_and_grouping.ipynb       ──► GroupBy, Pivot, Window Functions (Rank, Lead/Lag)
├── 06_joins_and_data_combining.ipynb        ──► Inner/Outer/Semi/Anti Joins, Broadcast Optimization
├── 07_spark_sql_and_temp_views.ipynb        ──► Temp Views, ANSI SQL, CTEs, Catalog
└── 08_data_sources_and_file_formats.ipynb   ──► Parquet, CSV, JSON, Partitioning on Disk

Advanced Level (Performance, Streaming & ML)
├── 09_udfs_user_defined_functions.ipynb     ──► Standard UDFs vs Vectorized Pandas UDFs (Arrow)
├── 10_performance_tuning_and_optimization   ──► Catalyst Optimizer, Explain Plans, Repartition vs Coalesce, Caching, AQE
├── 11_structured_streaming_intro.ipynb      ──► Real-Time Streaming, Unbounded Tables, Watermarking, Memory Sink
└── 12_mllib_machine_learning_pipeline.ipynb ──► Feature Engineering, VectorAssembler, ML Pipeline, Logistic Regression
```

---

## 📖 Notebook Breakdown

### 🟢 Part 1: Beginner Foundations

#### [01. Introduction & Architecture](01_introduction_and_architecture.ipynb)
* **What is Apache Spark & PySpark?** The chef in the kitchen analogy.
* **When to use PySpark vs Pandas**: Memory limits, distributed compute vs single-core.
* **Cluster Architecture**: Driver Program, Cluster Manager, Worker Nodes, and Executors.
* **Core Concepts**: In-memory computing, Lazy Evaluation, Directed Acyclic Graph (DAG), Transformations vs Actions.
* **Setting up `SparkSession`**: `master("local[*]")`, `appName()`, setting clean log levels (`WARN`).
* **Spark Web UI**: Monitoring jobs at `http://localhost:4040`.
* **Clean Exit**: Stopping session with `spark.stop()`.

#### [02. RDD (Resilient Distributed Datasets) Fundamentals](02_rdd_fundamentals.ipynb)
* **What is an RDD?** Resilient, Distributed, Dataset, Immutable.
* **Creating RDDs**: `sc.parallelize()` and partition inspection with `glom()`.
* **Narrow vs Wide Transformations**: Why shuffles across nodes are expensive.
* **Transformations**: `map()`, `filter()`, `flatMap()`.
* **Key-Value Operations**: Why `reduceByKey()` outperforms `groupByKey()`.
* **Actions**: `collect()`, `count()`, `take()`, `first()`, `reduce()`.
* **End-to-End Project**: Classic Distributed Word Count.
* **RDDs vs DataFrames**: Modern guidelines.

#### [03. DataFrame Basics & Schemas](03_dataframe_basics_and_schemas.ipynb)
* **What is a DataFrame?** RDD + Schema + Catalyst Optimizer.
* **Creating DataFrames**: From lists, dictionaries, RDDs, and Pandas.
* **Schema Definition**: Inferred vs Explicit Schemas (`StructType`, `StructField`, `StringType`, `IntegerType`, `DoubleType`).
* **Inspection**: `show()`, `printSchema()`, `describe()`, `dtypes`, `columns`.
* **Column Operations**: `select()`, `col()`, `alias()`, `withColumnRenamed()`.
* **Transformations**: Adding columns (`withColumn`), casting types (`cast`), dropping columns (`drop`).

#### [04. Data Manipulation, Filtering & Built-in Functions](04_data_manipulation_and_transformations.ipynb)
* **Filtering Rows**: `filter()` and `where()`, compound conditions (`&`, `|`, `~`), `isin()`.
* **Conditional Logic**: `when()` and `otherwise()` (PySpark's CASE WHEN / IF-ELSE).
* **Missing Data**: Checking nulls (`isNull`), dropping nulls (`dropna`), filling nulls (`fillna`).
* **Duplicates**: `distinct()` vs `dropDuplicates(subset=[...])`.
* **String Operations**: `upper()`, `lower()`, `trim()`, `concat_ws()`.
* **Date & Time Operations**: `current_date()`, `datediff()`, `date_add()`, `year()`, `to_date()`.
* **Array Operations**: `split()`, `explode()`, `array_contains()`.
* **Sorting**: `orderBy()`, `sort()`, `asc()`, `desc()`.

---

### 🟡 Part 2: Intermediate Data Engineering & Analytics

#### [05. Aggregations, Grouping & Window Functions](05_aggregations_and_grouping.ipynb)
* **Global Aggregations**: `count()`, `sum()`, `avg()`, `min()`, `max()`, `countDistinct()`.
* **Grouped Aggregations**: `groupBy().agg(...)` with multiple aliased metrics.
* **Pivoting**: Reshaping rows into columns with `.pivot()`.
* **Window Functions Demystified**: `Window.partitionBy().orderBy()`.
* **Ranking Functions**: `row_number()`, `rank()`, `dense_rank()` with side-by-side comparison.
* **Top N Per Category**: Finding top performers per department.
* **Offset & Analytics**: `lead()` and `lag()` for interval and growth metrics.
* **Running Totals**: Cumulative sums using `rowsBetween()`.

#### [06. Joins, Unions & Data Combining](06_joins_and_data_combining.ipynb)
* **Relational Joins**: `inner`, `left`, `right`, `outer`, `left_semi`, `left_anti`, `cross`.
* **Avoiding Ambiguity**: Safe column joining patterns and DataFrame aliases.
* **Set Operations**: Positional `union()` vs schema-safe `unionByName(..., allowMissingColumns=True)`.
* **Broadcast Joins**: Eliminating network shuffles with `broadcast(small_df)` for dramatic performance gains.

#### [07. Spark SQL & Temporary Views](07_spark_sql_and_temp_views.ipynb)
* **What is Spark SQL?** Running ANSI SQL over distributed DataFrames.
* **View Registration**: `createOrReplaceTempView()` (local) vs `createGlobalTempView()` (global).
* **Executing Queries**: `spark.sql("SELECT ...")`.
* **Advanced SQL**: CTEs (`WITH` clauses), Window functions, Subqueries.
* **Two-Way Interoperability**: Mixing SQL queries with Python DataFrame functions.
* **Catalog Management**: Listing and dropping views via `spark.catalog`.

#### [08. Data Sources, File Formats & Disk Partitioning](08_data_sources_and_file_formats.ipynb)
* **Storage Formats**: Row-based (CSV, JSON) vs Columnar (Parquet).
* **Why Parquet is King**: Column pruning, predicate pushdown, Snappy compression.
* **Reading & Writing**: Options, schemas, header handling.
* **SaveModes**: `overwrite`, `append`, `ignore`, `errorIfExists`.
* **Disk Partitioning**: Organizing folders with `partitionBy()`.
* **Partition Pruning**: How Spark skips unneeded folders on disk.
* **The Small Files Problem**: What happens when you over-partition, and how to avoid it.

---

### 🔴 Part 3: Advanced Optimization, Real-Time Streaming & ML

#### [09. User-Defined Functions (UDFs) & Pandas UDFs](09_udfs_user_defined_functions.ipynb)
* **The Function Hierarchy**: Built-ins >>> Pandas UDFs >>> Standard Python UDFs.
* **Standard Python UDFs**: `udf()`, `@udf` decorator, return types.
* **SQL Registration**: Exposing Python functions to SQL queries via `spark.udf.register()`.
* **The Serialization Bottleneck**: Explaining the JVM $\leftrightarrow$ Python worker serialization penalty.
* **Vectorized Pandas UDFs**: Apache Arrow zero-copy memory batch processing with `@pandas_udf`.

#### [10. Performance Tuning, Optimization & Execution Plans](10_performance_tuning_and_optimization.ipynb)
* **Catalyst Optimizer**: Analysis $\rightarrow$ Logical Optimization $\rightarrow$ Physical Planning $\rightarrow$ Tungsten bytecode.
* **Reading Execution Plans**: `df.explain(extended=True)` parsed, analyzed, optimized, and physical stages.
* **Partition Tuning**: `repartition()` (Full Shuffle) vs `coalesce()` (No Full Shuffle).
* **Caching & Persistence**: `cache()`, `persist(StorageLevel)`, and always remembering `unpersist()`.
* **Adaptive Query Execution (AQE)**: Dynamic partition coalescing, join switching, and skew join handling.
* **Production Tuning Checklist**: 8 essential rules for real-world Spark jobs.

#### [11. Structured Streaming: Real-Time Stream Processing](11_structured_streaming_intro.ipynb)
* **Streaming Paradigm**: Batch (bounded) vs Streaming (unbounded table).
* **Core Components**: Source $\rightarrow$ Transformation $\rightarrow$ Sink.
* **Hands-on Stream**: Setting up a `rate` source stream.
* **Output Modes**: `append`, `complete`, and `update`.
* **Real-time Queries**: Writing to an in-memory sink and querying live snapshots with Spark SQL.
* **Fault Tolerance**: Triggers, Watermarking for late data, and Checkpoint directories.
* **Safe Termination**: `query.stop()` followed by `spark.stop()`.

#### [12. PySpark MLlib: Machine Learning Pipelines at Scale](12_mllib_machine_learning_pipeline.ipynb)
* **Why MLlib?** Training models on terabytes of data across a distributed cluster.
* **Transformers vs Estimators**: Definitions and API lifecycle (`fit` vs `transform`).
* **Feature Engineering**:
  * `StringIndexer` (encoding categorical strings).
  * `OneHotEncoder` (binary one-hot vectors).
  * `VectorAssembler` (bundling all features into the required Spark ML vector).
* **End-to-End Pipeline**: Chaining stages into a single leak-free `Pipeline`.
* **Training & Evaluation**: Train-test split (`randomSplit`), `LogisticRegression`, and `BinaryClassificationEvaluator` (ROC AUC).
* **Persistence**: Saving and reloading trained pipeline models with `model.save()` and `PipelineModel.load()`.

---

## 🛠️ Prerequisites & Setup

### 1. Requirements
* **Python**: 3.10+ (Python 3.12 recommended; verified with PySpark 4.2.0 on Windows ARM64)
* **Java (JDK)**: Spark requires Java to run on the JVM (Java 8, 11, or 17 recommended).
  * Check if Java is installed:
    ```bash
    java -version
    ```
  * If Java is not installed:
    * **Windows**: Download OpenJDK from [Adoptium (Eclipse Temurin)](https://adoptium.net/) and ensure `JAVA_HOME` is set.
    * **macOS**: `brew install openjdk@11`
    * **Linux**: `sudo apt install openjdk-11-jdk`

### 2. Install PySpark
Open your terminal or command prompt and install PySpark:
```bash
pip install pyspark
```

*(Optional for advanced vectorized UDFs & ML)*:
```bash
pip install pandas pyarrow numpy
```

> [!TIP]
> **Windows Tip (`PYSPARK_PYTHON`)**:
> Set these variables before creating the first `SparkSession`, so local Spark workers use the notebook's Python interpreter:
> ```python
> import os, sys
> os.environ["PYSPARK_PYTHON"] = sys.executable
> os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
> ```
> If you still get a `SocketTimeoutException` while using Python 3.15, switch the notebook kernel to Python 3.12, install PySpark in that environment if needed, and restart the kernel. Python 3.12.10 with PySpark 4.2.0 was verified on Windows ARM64 for this tutorial.

### 3. Launching the Notebooks
You can open these notebooks directly in:
* **VS Code / Cursor / PyCharm**: Simply open any `.ipynb` file and choose your Python kernel.
* **Jupyter Notebook**:
  ```bash
  cd pyspark_tutorial
  jupyter notebook
  ```
* **JupyterLab**:
  ```bash
  cd pyspark_tutorial
  jupyter lab
  ```

---

## 💡 Quick Reference Cheat Sheet

| Task | PySpark Code |
| :--- | :--- |
| **Start Spark** | `spark = SparkSession.builder.appName("App").master("local[*]").getOrCreate()` |
| **Stop Spark** | `spark.stop()` |
| **Read CSV** | `spark.read.option("header", True).schema(schema).csv("path")` |
| **Read Parquet** | `spark.read.parquet("path")` |
| **Filter Rows** | `df.filter((col("age") > 25) & (col("salary") > 50000))` |
| **Add / Edit Column** | `df.withColumn("tax", col("salary") * 0.2)` |
| **Conditional Column** | `df.withColumn("tier", when(col("amount") > 100, "High").otherwise("Low"))` |
| **Group & Aggregate** | `df.groupBy("dept").agg(avg("salary").alias("avg_sal"), count("*").alias("count"))` |
| **Window Ranking** | `dense_rank().over(Window.partitionBy("dept").orderBy(col("salary").desc()))` |
| **Join Tables** | `df1.join(broadcast(df2), "id", "inner")` |
| **SQL Query** | `df.createOrReplaceTempView("t"); spark.sql("SELECT * FROM t")` |
| **Safe Union** | `df1.unionByName(df2, allowMissingColumns=True)` |
| **Reduce Partitions** | `df.coalesce(2)` |
| **Cache DataFrame** | `df.cache()`; when done: `df.unpersist()` |

---

## 🎓 Happy Learning!
Work through each notebook in sequential order from `01` to `12`. Run every cell, experiment with the exercises, and master distributed big data processing with PySpark!
