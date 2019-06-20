# notMNIST
Exploring notMNIST Dataset through SCALA



##### First Step is to untar the file using python
```
import tarfile
import sys
filename = r"notMNIST_small.tar.gz"
tar = tarfile.open(filename)
sys.stdout.flush()
tar.extractall()
tar.close()
```

##### install scala through sbt
  * through "sbt console" tried basics of scala syntax
  
##### IntelliJ installation for easy project management
 * post installation search for plugins in help tab and install scala.

##### In Databricks
* I could read the images dir through
```
df = spark.read.format("image") \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

Output:
root
 |-- image: struct (nullable = true)
 |    |-- origin: string (nullable = true)
 |    |-- height: integer (nullable = true)
 |    |-- width: integer (nullable = true)
 |    |-- nChannels: integer (nullable = true)
 |    |-- mode: integer (nullable = true)
 |    |-- data: binary (nullable = true)
```
* So far its been pyspark, let us try for scala : https://dzone.com/articles/working-on-apache-spark-on-windows
* you would also need the guideline for windows as in : https://stackoverflow.com/questions/27618843/why-does-spark-submit-and-spark-shell-fail-with-failed-to-find-spark-assembly-j/27628786
