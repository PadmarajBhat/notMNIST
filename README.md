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

* Databricks files are access through : os.path.isfile("/dbfs/FileStore/tables/images/Q291bnRyeXNpZGUgQmxhY2sgU1NpIEV4dHJhIEJvbGQudHRm.png")

* to cast : https://alvinalexander.com/scala/how-to-cast-objects-class-instance-in-scala-asinstanceof
  *  or we can access directly the substructure through:
  ```
  df.select("image.origin").show()
  Note that df["image"]["origin"].show() would not work. Panda way of accessing data is not possible.
  ```
  
  * unfortunately, databricks do not let us import any of the packages they support like sparkdl. So, going back to installing scala on local and trying to run the "Hello World"
    * https://www.youtube.com/watch?v=A2c4mDDn-QM indicates the scala setup instructions.
  
  * Goind ahead with image conversion at databricks itself.
  ```
  import java.io.File
  import javax.imageio.ImageIO
  import java.awt.image.BufferedImage
  val photo1 = ImageIO.read(new File("/dbfs/FileStore/tables/images/Q291bnRyeXNpZGUgQmxhY2sgU1NpIEV4dHJhIEJvbGQudHRm.png"))
  ```
  Above piece of code reads the file and then gives the output photo1:
  
  ```
  res0: java.awt.image.BufferedImage = BufferedImage@31679bff: type = 10 ColorModel: #pixelBits = 8 numComponents = 1 color space = java.awt.color.ICC_ColorSpace@50be2132 transparency = 1 has alpha = false isAlphaPre = false ByteInterleavedRaster: width = 28 height = 28 #numDataElements 1 dataOff[0] = 0
  ```
