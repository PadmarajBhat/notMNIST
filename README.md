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
  
  * Going ahead with image conversion at databricks itself. Thanks to : http://otfried.org/scala/image.html
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
  
  * Now that we have read the image binary, we can use BufferedImage for further processing
  ```
    for (x <- 0 until 28)
    for (y <- 0 until 28)
      println(photo1.getRGB(x,y).toHexString, (photo1.getRGB(x,y) & 0xffffff).toHexString)
  ```
  Here 0xffffff acts like a complementing binary value. i.e. to convert a negative number to positive number facilitating our future calculations.
  
  * Here is how data is moved to array. Silly but my first time :)
  ```
  val arr = Array.ofDim[Int](28, 28)  

  for (x <- 0 until 28)
      for (y <- 0 until 28)
        //println(photo1.getRGB(x,y).toHexString, (photo1.getRGB(x,y) & 0xffffff).toHexString)
        arr(x)(y) = photo1.getRGB(x,y) & 0xffffff

  println(arr)
  ```
  
  * Arithmetic operation: normalizing the image.
  ```
   arr(x)(y) = ((photo1.getRGB(x,y) & 0xffffff) - (pixel_depth.toDouble/2))/pixel_depth.toDouble
  ```

 * To read all the files in directory:
 ```
 import java.io.File
 def recursiveListFiles(f: File): Array[File] = {
   val these = f.listFiles
   these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
 }

 recursiveListFiles(new File("/dbfs/FileStore/tables/images/"))
 ```
 * Unfortunately below code fails
 ```
 val dataset_a =  (new File("/dbfs/FileStore/tables/images/")).listFiles.map(x=>{  
  val photo1 = ImageIO.read(new File(x.toString))
  val arr = Array.ofDim[Double](28, 28)  
  val pixel_depth = 255

  for (x <- 0 until 28)
     for (y <- 0 until 28)
      //println(photo1.getRGB(x,y).toHexString, (photo1.getRGB(x,y) & 0xffffff).toHexString)
      //arr(x)(y) = photo1.getRGB(x,y) & 0xffffff
      arr(x)(y) = ((photo1.getRGB(x,y) & 0xffffff) - (pixel_depth.toDouble/2))/pixel_depth.toDouble
arr
})
 ```
* Following fixed above issue:
```
def imageToArray(imageName : String): Either[Array[Array[Double]], Int] = {
  //println(imageName)
  val photo1 = ImageIO.read(new File(imageName))
  val arr = Array.ofDim[Double](28, 28)  
  val pixel_depth = 255
  
  try{
    for (x <- 0 until 28)
     for (y <- 0 until 28)
      //println(photo1.getRGB(x,y).toHexString, (photo1.getRGB(x,y) & 0xffffff).toHexString)
      //arr(x)(y) = photo1.getRGB(x,y) & 0xffffff
      arr(x)(y) = ((photo1.getRGB(x,y) & 0xffffff) - (pixel_depth.toDouble/2))/pixel_depth.toDouble    
    Left(arr)
  } catch {
    case ex: NullPointerException =>{

      println("NullPointerException Exception for " + imageName)
      Right(0)
    }
  }
  

}


val dataset_a =  (new File("/dbfs/FileStore/tables/images/")).listFiles.map(x=>{ val output = imageToArray(x.toString) 
                                                                                if (output.isLeft)
                                                                                  output.left.get
                                                                                else
                                                                                println("Ingoring : "+ x)
                                                                               })
```
  * However, here problem is that the dataset_a is of type Array[Any] and hence dataset_a(0)(0)(0) fails
  * Interestingly, ``` dataset_a(0).asInstanceOf[Array[Array[Double]]](0)``` displays the array of first image first row of 28x28 matrix
  * But this ```dataset_a.asInstanceOf[Array[Array[Array[Double]]]](0)(0)(0)``` fails with error ```java.lang.ClassCastException: [Ljava.lang.Object; cannot be cast to [[[D```
* Image df interesting failure
```
val image_val = image_df.select("image.data").rdd.map(photo =>{
  
  val photo1 = photo.asInstanceOf[java.awt.image.BufferedImage]
  val arr = Array.ofDim[Double](28, 28)
  val pixel_depth = 255
  
  try{
    for (x <- 0 until 28)
     for (y <- 0 until 28)
      {
        println(photo1.getRGB(x,y).toHexString, (photo1.getRGB(x,y) & 0xffffff).toHexString)
        //arr(x)(y) = photo1.getRGB(x,y) & 0xffffff
        arr(x)(y) = ((photo1.getRGB(x,y) & 0xffffff) - (pixel_depth.toDouble/2))/pixel_depth.toDouble    
      }
    arr
  } catch {
    case ex: NullPointerException =>{

      println("NullPointerException Exception for ")
      0
    }
  
}})

image_val.take(2)
```
* It fails with :
```
org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 0.0 failed 1 times, most recent failure: Lost task 0.0 in stage 0.0 (TID 0, localhost, executor driver): java.lang.ClassCastException: org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema cannot be cast to java.awt.image.BufferedImage
```

* https://stackoverflow.com/questions/10866639/difference-between-a-seq-and-a-list-in-scala
```
val data = Seq("Hello", "World!"); data(0).contains("He");
```
* Here it returns boolean result of true

* Need to try pickling in scala: https://stackoverflow.com/questions/17539375/storing-an-object-to-a-file
  * https://github.com/scala/pickling : both these link did not work in databricks notebook. Could not do the library import too. may be downloading jar and uploading might work.

* Next is to convert the 3d array to dataframe for org.apache.spark.ml libraries
  * dataset_a.toDF() fails ```value toDF is not a member of Array[Any] ```
  * Idea is to have the spark DF built like this:
   ```
   val feature : Array[Array[Int]] = Array(Array(1,2,3,4),Array(2,3,4,5))
   val lable: Int = 10

   val row = List((feature,lable))

   val row_rdd = spark.sparkContext.parallelize(row)


   row_rdd.toDF("features","labels").show(truncate=false)

   ```
   
   * Approach 2: simulating our multidimension case
   ```
   import scala.collection.mutable.ArrayBuffer
   val feature : Array[Array[Array[Int]]] = Array(Array(Array(1,2,3,4),Array(2,3,4,5)),Array(Array(1,2,3,4),Array(2,3,4,5)),Array(Array(1,2,3,4),Array(2,3,4,5)))
   val lable: Array[Int] = Array(10,20,30)

   val row = new ArrayBuffer[(Array[Array[Int]], Int)]
   for (f <- feature; l <- lable)
     row += ((f,l))
   //val row = Array((feature,lable))

   val row_rdd = spark.sparkContext.parallelize(row)
   row_rdd.toDF("features","labels").show(truncate=false)
   ```
   
   * Output:
   ```
   +----------------------------+------+
   |features                    |labels|
   +----------------------------+------+
   |[[1, 2, 3, 4], [2, 3, 4, 5]]|10    |
   |[[1, 2, 3, 4], [2, 3, 4, 5]]|20    |
   |[[1, 2, 3, 4], [2, 3, 4, 5]]|30    |
   |[[1, 2, 3, 4], [2, 3, 4, 5]]|10    |
   |[[1, 2, 3, 4], [2, 3, 4, 5]]|20    |
   |[[1, 2, 3, 4], [2, 3, 4, 5]]|30    |
   |[[1, 2, 3, 4], [2, 3, 4, 5]]|10    |
   |[[1, 2, 3, 4], [2, 3, 4, 5]]|20    |
   |[[1, 2, 3, 4], [2, 3, 4, 5]]|30    |
   +----------------------------+------+

   import scala.collection.mutable.ArrayBuffer
   feature: Array[Array[Array[Int]]] = Array(Array(Array(1, 2, 3, 4), Array(2, 3, 4, 5)), Array(Array(1, 2, 3, 4), Array(2, 3, 4, 5)), Array(Array(1, 2, 3, 4), Array(2, 3, 4, 5)))
   lable: Array[Int] = Array(10, 20, 30)
   row: scala.collection.mutable.ArrayBuffer[(Array[Array[Int]], Int)] = ArrayBuffer((Array(Array(1, 2, 3, 4), Array(2, 3, 4, 5)),10), (Array(Array(1, 2, 3, 4), Array(2, 3, 4, 5)),20), (Array(Array(1, 2, 3, 4), Array(2, 3, 4, 5)),30), (Array(Array(1, 2, 3, 4), Array(2, 3, 4, 5)),10), (Array(Array(1, 2, 3, 4), Array(2, 3, 4, 5)),20), (Array(Array(1, 2, 3, 4), Array(2, 3, 4, 5)),30), (Array(Array(1, 2, 3, 4), Array(2, 3, 4, 5)),10), (Array(Array(1, 2, 3, 4), Array(2, 3, 4, 5)),20), (Array(Array(1, 2, 3, 4), Array(2, 3, 4, 5)),30))
   row_rdd: org.apache.spark.rdd.RDD[(Array[Array[Int]], Int)] = ParallelCollectionRDD[30] at parallelize at command-1740298291896335:10
   ```
     * Here, I am using ArrayBuffered as List in python
     * tuple creation through ```(())```
     * can we create the tuple during our initial map to avoid and code related issue
   
   * Failed Again:
     ```
     val listBuffer = new ArrayBuffer[(Array[Array[Double]],String)]

     for (e <- dataset_a)
     //println(e.asInstanceOf[(Array[Array[Double]],Char)]._1.asInstanceOf[Array[Array[Double]]])
     //println(e.asInstanceOf[(Array[Array[Double]],Char)])
       listBuffer += ((e.asInstanceOf[(Array[Array[Double]],String)]._1, e.asInstanceOf[(Array[Array[Double]],String)]._2))
     ```
    * following worked :)
     ```
     val row_1 = new ArrayBuffer[(Array[Array[Double]], String)]

     for (file <- (new File("/dbfs/FileStore/tables/images/")).listFiles){
       val output = imageToArray(file.toString) 
       if (output.isLeft)
         row_1 +=((output.left.get , "A"))
       else
         println("Ingoring : "+ file)
     }
     val df = spark.sparkContext.parallelize(row_1).toDF("feature", "label")
     df.show()
     ```
    * introduced little randomness for the ease of access
      ```
      val row_1 = new ArrayBuffer[(Array[Array[Double]], String)]
      val listLables = List("A","B","C","D")
      val r = scala.util.Random

      for (file <- (new File("/dbfs/FileStore/tables/images/")).listFiles){


        val output = imageToArray(file.toString) 
        if (output.isLeft)
          row_1 +=((output.left.get , listLables(r.nextInt(4))))
        else
          println("Ingoring : "+ file)
       }
      ```
     * Missing panda... df[df['label'] == 'B'] but iteresting scala syntax to filter rows from dataframe
     ```
     df.filter(df("label") === "B").show()
     df.where($"label" === "B").show()
     df.filter("label = 'B'").show()
     df.where("label = 'B'").show()
     ```
     
     * oops.... to concatenate strings
     ```
     import org.apache.spark.sql.functions.{concat, lit}
     df.withColumn("label_new", concat($"label",lit("_new"))).show()
     
     Output:
     +--------------------+-----+---------+
     |             feature|label|label_new|
     +--------------------+-----+---------+
     |[[62954.370588235...|    A|    A_new|
     |[[5675.7588235294...|    B|    B_new|
     |[[-0.5, -0.5, -0....|    B|    B_new|
     |[[-0.5, -0.5, -0....|    B|    B_new|
     |[[-0.5, -0.5, -0....|    D|    D_new|
     |[[-0.5, -0.5, -0....|    C|    C_new|
     |[[-0.5, -0.5, -0....|    A|    A_new|
     |[[-0.5, -0.5, -0....|    B|    B_new|
     ```
     
     * Again the same problem:
     ```
     import org.apache.spark.sql.functions.{col,udf}



     val calculateAverage = udf((multiArray: Array[Array[Double]])=>{
       1
     })
     df.withColumn("avg_feature", calculateAverage(col("feature"))).take(1)

     /*

     val calculateAverage = udf((label: String)=>{
       1
     })
     df.withColumn("avg_c", calculateAverage(col("label"))).take(1)

     */
     ```
     Output:
     ```
     org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 53.0 failed 1 times, most recent failure: Lost task 0.0 in stage 53.0 (TID 763, localhost, executor driver): org.apache.spark.SparkException: Failed to execute user defined function($anonfun$1: (array<array<double>>) => int)
     
     Caused by: java.lang.ClassCastException: scala.collection.mutable.WrappedArray$ofRef cannot be cast to [[D
     ```
  * the fundamental approach has to be changed.....we have to map the 2d image 1d image.
    * First hurdle: 
     ```val (rowSize, colSize )= (28,10)
        val arr = Array.ofDim[Double](rowSize, 28)
        println(arr.size, arr(0).size)
        
        Output:
        (28,28)
        ```
        
   * We do not need 2d array
    ```
    val int_arr = new Array[Int](10)
    var k = 0
    for (i <- (0 to 2)){
      for (j <- (0 to 2 )){
        int_arr(k) = i
        k += 1
      }

    }
    Output:
    int_arr: Array[Int] = Array(0, 0, 0, 1, 1, 1, 2, 2, 2, 0)
    k: Int = 9
    ```
* to sort an array : https://stackoverflow.com/questions/1131925/how-do-i-sort-an-array-in-scala
 ```
 val array = Array((for(i <- 0 to 10) yield scala.util.Random.nextInt): _*)
 scala.util.Sorting.quickSort(array)
 ```
* https://alvinalexander.com/scala/how-to-sort-map-in-scala-key-value-sortby-sortwith
   * to sort map values

* to find an median : http://fruzenshtein.com/scala-median-funciton/
   ```
   val aSet = Seq(3, 5, 10, 11, 19)
   val bSet = Seq(1, 5, 14, 16, 17, 20)


   def medianCalculator(seq: Seq[Int]): Int = {
     //In order if you are not sure that 'seq' is sorted
     val sortedSeq = seq.sortWith(_ < _)

     if (seq.size % 2 == 1) sortedSeq(sortedSeq.size / 2)
     else {
       val (up, down) = sortedSeq.splitAt(seq.size / 2)
       (up.last + down.head) / 2
     }
   }

   medianCalculator(aSet) //10
   medianCalculator(bSet) //15
   ```
   But this logic does not work for Double type
   
   * tweaked code to support Double type
   ```
   val aDSet = Array(30.0, 5.0, 10.0, 11.0, 19.0)
   val bDSet = Array(1.0, 5.0, 14.0, 16.0, 17.0, 20.0)


   def medianDCalculator(seq: Array[Double]): (Double,Double,Double,Double) = {
     //In order if you are not sure that 'seq' is sorted
     //val sortedSeq = seq.sortWith(_ :Double < _:Double)

     scala.util.Sorting.quickSort(seq)

     if (seq.size % 2 == 1) ((seq(seq.size / 2), seq.sum / seq.size, seq.min, seq.max))
     else {
       val (up, down) = seq.splitAt(seq.size / 2)
       (((up.last + down.head) / 2, seq.sum / seq.size, seq.min, seq.max))
     }
   }

   medianDCalculator(aDSet) //10
   medianDCalculator(bDSet) //15 
   ```
   *  ```(Double, Double, Double, Double) = (15.0,12.166666666666666,1.0,20.0)``` array does have numpy like function in them. Scala is not that bad. If it could only have mean and median in them, I would have called it good.
   
   * display(df.where("label = 'C'").select("mean")) : played around plot options in databricks notebook 
      * display(df.where("label = 'C'").select("mean","median"))  : sigmoid like shape was observed. Does that mean that 2 ends are outlier?
* Key commands for observations
  ```
  df.groupBy("label").avg().select("avg(median)").withColumnRenamed("avg(median)", "median_avg").show()
  df.groupBy("label").avg("median").show()
  df.groupBy("label").avg().show()
  
  Output:
  +-----+------------------+------------------+----------------+-----------------+
  |label|       avg(median)|         avg(mean)|        avg(min)|         avg(max)|
  +-----+------------------+------------------+----------------+-----------------+
  |    B|19910.745056320397| 27437.68394421599|            -0.5|65717.33454317896|
  |    C|19337.877743989357|27376.765464085478|            -0.5|65740.39746270074|
  |    A|18633.379023126377|   27158.447598599|21.8077055944727|65670.43896938874|
  +-----+------------------+------------------+----------------+-----------------+
  ```
  
 * Attempt to scale the values
 ```
 //https://stackoverflow.com/questions/33924842/minmax-normalization-in-scala
import org.apache.spark.ml.feature.MinMaxScaler

val scaler = new MinMaxScaler()
    .setInputCol("median")
    .setOutputCol("medianScaled")
    .setMax(1.0)
    .setMin(-1.0)

scaler.setInputCol("median").setOutputCol("scaledMedian").fit(df).transform(df).show

Output:
java.lang.IllegalArgumentException: requirement failed: Column median must be of type struct<type:tinyint,size:int,indices:array<int>,values:array<double>> but was actually double.
 ```
 * Vectorizing has resulted in the 1d array of each cell post scaling which is not expected :(
 ```
 +------------------+-----------------------+
|median            |vscaled                |
+------------------+-----------------------+
|33541.029411764706|[-0.1306203600678536]  |
|65792.5           |[0.46896983626093824]  |
|-0.5              |[-0.7160680736187367]  |
 ```

 * need to covert to array as indicated in : https://stackoverflow.com/questions/32196207/derive-multiple-columns-from-a-single-column-in-a-spark-dataframe
   * but it looks too length; need to find easy way out.
 
 * vectorIndexer also do not converts to the indexed value : https://spark.apache.org/docs/latest/ml-features.html#vectorindexer
