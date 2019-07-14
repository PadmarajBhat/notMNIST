* problem with the below approach is that there are multiple rows
```
val qrt_a_median = df.filter(df("label") === "A").stat.approxQuantile("median", Array(.25,.5,.75), 0)
val qrt_b_median = df.filter(df("label") === "B").stat.approxQuantile("median", Array(.25,.5,.75), 0)

val qrt_a_mean = df.filter(df("label") === "A").stat.approxQuantile("mean", Array(.25,.5,.75), 0)
val qrt_b_mean = df.filter(df("label") === "B").stat.approxQuantile("mean", Array(.25,.5,.75), 0)


val df_A = df.filter(df("label") === "A" ).filter(df("median") >= qrt_a_median(0) and df("median") <= qrt_a_median(2))
val df_B = df.filter(df("label") === "B" ).filter(df("median") >= qrt_b_median(0) and df("median") <= qrt_b_median(2))

val df_A_mean = df.filter(df("label") === "A" ).filter(df("mean") >= qrt_a_mean(0) and df("mean") <= qrt_a_mean(2))
val df_B_mean = df.filter(df("label") === "B" ).filter(df("mean") >= qrt_b_mean(0) and df("mean") <= qrt_b_mean(2))
```

* drop duplicate logic for the above
```
val df_new = df_A.union(df_B).union(df_A_mean).union(df_B_mean)
val df_curr = df_new.dropDuplicates()
Output:
df count: 2269, df_cuurr count: 1610

```
    * that is the drastric change in count is not so favorable but do we know for sure if it is outlier free.

* changed the data structure to include file name 
   ```
   for (file <- (new File("/dbfs/FileStore/tables/noMNIST_small/A/")).listFiles){
     val output = imageToArray(file.toString) 
     if (output.isLeft){
       val temp_array = output.left.get
       val (lMedian, lmean, lmin, lmax) = medianDCalculator(temp_array)
       row_1 +=((temp_array,lMedian, lmean, lmin, lmax, "A", file.toString))
     }
     else
       println("Ingoring : "+ file)
   }

   val df = spark.sparkContext.parallelize(row_1).toDF("feature", "median", "mean", "min", "max", "label", "file_name")
   df.show()
   ```
   * Idea is to visually display the images the and double check the approach
   
 * https://spark.apache.org/docs/latest/ml-classification-regression.html#multinomial-logistic-regression
 ```
   java.lang.IllegalArgumentException: Field "features" does not exist.
 ```
 
 * https://stackoverflow.com/questions/44950897/field-features-does-not-exist-sparkml covers the issue but it not only misses the line below but also leading me to one more exception
 ```
 https://stackoverflow.com/questions/44950897/field-features-does-not-exist-sparkml
 ```
 Output:
 ```
 error: value summary is not a member of org.apache.spark.ml.PipelineModel
 ```

* error with pipeline model
   ```
   val pipeline = new Pipeline().setStages(Array(assembler, lr))
   ```
   this does not have "lr" functions. Wondering why last transform has not given pipeline its function.
   
* I found easy transform function : https://elbauldelprogramador.com/en/how-to-convert-column-to-vectorudt-densevector-spark/
```
https://elbauldelprogramador.com/en/how-to-convert-column-to-vectorudt-densevector-spark/
```
however, assembler fails to vectorize the double data type
   
* got a hint : ("feature", "median", "mean") dint work but ( "median", "mean") worked 

* https://stackoverflow.com/a/47543887/8693106 : to convert from array of double to vectors
```
def convertArrayToVector = udf((features: mutable.WrappedArray[Double]) => Vectors.dense(features.toArray))
```
   Ideas is to create a udf to convert from wrapped Array to vector dence. Note here if we change the type to only "Array" and not "WrappedArray", the code fails in execution (though compilation is successfull)
   
* Logistic Regression fails at compile time with Exception: requirement failed- Column label must be of type numeric but was actually of type string.
   * now it is debatable to have the numberical representation  through simple text to numeric encoding or one hot encoding. I believe for classification it has to be one hot encoding.

* https://spark.apache.org/docs/latest/ml-features.html#vectorindexer : VideoIndexer as described in the link is not applicable in our case because
      * we have "flattened" the 2d image data to 1d array
      * vector transformation has merged the columns mean and median to the image data (1d array)
      * post executing the vectorindexer, it accidentally pointed out the image data to have categorical values.
      
* For current image dataset we do not require any vector indexer as it does not have categorical data. Hence, skipping this exercise for now and will take up fo other dataset. For now let us focus on the label encoding which is the next exception in our code.
      * https://spark.apache.org/docs/latest/ml-features.html#onehotencoderestimator
      ```
      val assembler = new VectorAssembler()
        .setInputCols( Array("categoryVec1"))
        .setOutputCol("cv1")

      val out = assembler.transform(encoded)
      
      Output:
      +--------------+--------------+-------------+-------------+
      |categoryIndex1|categoryIndex2| categoryVec1| categoryVec2|
      +--------------+--------------+-------------+-------------+
      |           0.0|           1.0|(2,[0],[1.0])|(2,[1],[1.0])|
      |           1.0|           0.0|(2,[1],[1.0])|(2,[0],[1.0])|
      |           2.0|           1.0|    (2,[],[])|(2,[1],[1.0])|
      |           0.0|           2.0|(2,[0],[1.0])|    (2,[],[])|
      |           0.0|           1.0|(2,[0],[1.0])|(2,[1],[1.0])|
      |           2.0|           0.0|    (2,[],[])|(2,[0],[1.0])|
      +--------------+--------------+-------------+-------------+

      +--------------+--------------+-------------+-------------+---------+
      |categoryIndex1|categoryIndex2| categoryVec1| categoryVec2|      cv1|
      +--------------+--------------+-------------+-------------+---------+
      |           0.0|           1.0|(2,[0],[1.0])|(2,[1],[1.0])|[1.0,0.0]|
      |           1.0|           0.0|(2,[1],[1.0])|(2,[0],[1.0])|[0.0,1.0]|
      |           2.0|           1.0|    (2,[],[])|(2,[1],[1.0])|(2,[],[])|
      |           0.0|           2.0|(2,[0],[1.0])|    (2,[],[])|[1.0,0.0]|
      |           0.0|           1.0|(2,[0],[1.0])|(2,[1],[1.0])|[1.0,0.0]|
      |           2.0|           0.0|    (2,[],[])|(2,[0],[1.0])|(2,[],[])|
      +--------------+--------------+-------------+-------------+---------+
      ```
      cv1 - 3rd and 4th row confuses me. Would it create problem when building model ?
      
      
* mystery of one hot encoding

```
import org.apache.spark.ml.feature.OneHotEncoderEstimator

val df = spark.createDataFrame(Seq(
  (0.0, 1.0),
  (1.0, 0.0),
  (2.0, 1.0),
  (0.0, 20.0),
  (0.0, 1.0),
  (3.0, 0.0)
)).toDF("categoryIndex1", "categoryIndex2")

val encoder = new OneHotEncoderEstimator()
  .setInputCols(Array("categoryIndex1", "categoryIndex2"))
  .setOutputCols(Array("categoryVec1", "categoryVec2"))
val model = encoder.fit(df)

val encoded = model.transform(df)
encoded.show()
encoded.printSchema


val assembler = new VectorAssembler()
  .setInputCols( Array("categoryVec1"))
  .setOutputCol("cv1")

val out = assembler.transform(encoded)

println(out.show())

val assembler2 = new VectorAssembler()
  .setInputCols( Array("categoryVec2"))
  .setOutputCol("cv2")

val out2 = assembler2.transform(out)

println(out2.show())
```

   * Output:
   ```
   +--------------+--------------+-------------+--------------+-------------+
   |categoryIndex1|categoryIndex2| categoryVec1|  categoryVec2|          cv1|
   +--------------+--------------+-------------+--------------+-------------+
   |           0.0|           1.0|(3,[0],[1.0])|(20,[1],[1.0])|[1.0,0.0,0.0]|
   |           1.0|           0.0|(3,[1],[1.0])|(20,[0],[1.0])|[0.0,1.0,0.0]|
   |           2.0|           1.0|(3,[2],[1.0])|(20,[1],[1.0])|[0.0,0.0,1.0]|
   |           0.0|          20.0|(3,[0],[1.0])|    (20,[],[])|[1.0,0.0,0.0]|
   |           0.0|           1.0|(3,[0],[1.0])|(20,[1],[1.0])|[1.0,0.0,0.0]|
   |           3.0|           0.0|    (3,[],[])|(20,[0],[1.0])|    (3,[],[])|
   +--------------+--------------+-------------+--------------+-------------+


   +--------------+--------------+-------------+--------------+-------------+--------------+
   |categoryIndex1|categoryIndex2| categoryVec1|  categoryVec2|          cv1|           cv2|
   +--------------+--------------+-------------+--------------+-------------+--------------+
   |           0.0|           1.0|(3,[0],[1.0])|(20,[1],[1.0])|[1.0,0.0,0.0]|(20,[1],[1.0])|
   |           1.0|           0.0|(3,[1],[1.0])|(20,[0],[1.0])|[0.0,1.0,0.0]|(20,[0],[1.0])|
   |           2.0|           1.0|(3,[2],[1.0])|(20,[1],[1.0])|[0.0,0.0,1.0]|(20,[1],[1.0])|
   |           0.0|          20.0|(3,[0],[1.0])|    (20,[],[])|[1.0,0.0,0.0]|    (20,[],[])|
   |           0.0|           1.0|(3,[0],[1.0])|(20,[1],[1.0])|[1.0,0.0,0.0]|(20,[1],[1.0])|
   |           3.0|           0.0|    (3,[],[])|(20,[0],[1.0])|    (3,[],[])|(20,[0],[1.0])|
   +--------------+--------------+-------------+--------------+-------------+--------------+
   ```
   
   However, slight change in dataset gives different result
   ```
   val df = spark.createDataFrame(Seq(
     (0.0, 1.0),
     (1.0, 3.0),
     (2.0, 1.0),
     (0.0, 2.0),
     (0.0, 1.0),
     (3.0, 3.0)
      )).toDF("categoryIndex1", "categoryIndex2")
      
   +--------------+--------------+-------------+-------------+-------------+-------------+
   |categoryIndex1|categoryIndex2| categoryVec1| categoryVec2|          cv1|          cv2|
   +--------------+--------------+-------------+-------------+-------------+-------------+
   |           0.0|           1.0|(3,[0],[1.0])|(3,[1],[1.0])|[1.0,0.0,0.0]|[0.0,1.0,0.0]|
   |           1.0|           3.0|(3,[1],[1.0])|    (3,[],[])|[0.0,1.0,0.0]|    (3,[],[])|
   |           2.0|           1.0|(3,[2],[1.0])|(3,[1],[1.0])|[0.0,0.0,1.0]|[0.0,1.0,0.0]|
   |           0.0|           2.0|(3,[0],[1.0])|(3,[2],[1.0])|[1.0,0.0,0.0]|[0.0,0.0,1.0]|
   |           0.0|           1.0|(3,[0],[1.0])|(3,[1],[1.0])|[1.0,0.0,0.0]|[0.0,1.0,0.0]|
   |           3.0|           3.0|    (3,[],[])|    (3,[],[])|    (3,[],[])|    (3,[],[])|
   +--------------+--------------+-------------+-------------+-------------+-------------+

   ```
   
 * found databricks to be slow when we load the image and hence was exploring colab option as indicated in blog : https://medium.com/@shadaj/machine-learning-with-scala-in-google-colaboratory-e6f1661f1c88
      ```
      /bin/bash: line 8: /usr/local/share/jupyter/kernels/scala/kernel.json: No such file or directory
      ---------------------------------------------------------------------------
      CalledProcessError                        Traceback (most recent call last)
      <ipython-input-1-bf16215afe62> in <module>()
      ----> 1 get_ipython().run_cell_magic('shell', '', 'echo "{\n  \\"language\\" : \\"scala\\",\n  \\"display_name\\" : \\"Scala\\",\n  \\"argv\\" : [\n    \\"bash\\",\n    \\"-c\\",\n    \\"env LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libpython3.6m.so:\\$LD_PRELOAD java -jar /usr/local/share/jupyter/kernels/scala/launcher.jar --connection-file {connection_file}\\"\n  ]\n}" > /usr/local/share/jupyter/kernels/scala/kernel.json')

      2 frames
      /usr/local/lib/python3.6/dist-packages/google/colab/_system_commands.py in check_returncode(self)
          136     if self.returncode:
          137       raise subprocess.CalledProcessError(
      --> 138           returncode=self.returncode, cmd=self.args, output=self.output)
          139 
          140   def _repr_pretty_(self, p, cycle):  # pylint:disable=unused-argument

      CalledProcessError: Command 'echo "{
        \"language\" : \"scala\",
        \"display_name\" : \"Scala\",
        \"argv\" : [
          \"bash\",
          \"-c\",
          \"env LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libpython3.6m.so:\$LD_PRELOAD java -jar /usr/local/share/jupyter/kernels/scala/launcher.jar --connection-file {connection_file}\"
        ]
      }" > /usr/local/share/jupyter/kernels/scala/kernel.json' returned non-zero exit status 1.
      ```
      
      
* pyspark on colab worked just mentioned in https://towardsdatascience.com/pyspark-in-google-colab-6821c2faf41c

* we need to have stringindexer before to onehotencoding as per spark manual:https://spark.apache.org/docs/latest/ml-features.html#stringindexer

* onehotencoding did result in the required shape but the vector representation of the last value is still not clear.
   * (2, [], []) Here it might indicate that it is a vector of size 2 and there are 2 null/none/empty entries.
      - would it impact the ml algo ?
* tried multi class problem as in : https://spark.apache.org/docs/2.2.0/ml-classification-regression.html#multinomial-logistic-regression
   * spark could load the local file (as indicated in the above link) and the "show" indicated that the label it expect is not the vector but not the one hot encoded vector.
      - Now this is debatable that since the label is not a continuous value, it has to be one hot encoded. However, multinominal logistic regresion could handle it correctly.
* I was able to build the multinomial logistic regression and got the precession around 0.86. Need to expand the same for different letters in the dataset and need to check if precision goes down. Most importantly, mean and median helped us or not
   ```
   accuracy: Double = 0.8250330542089026
   falsePositiveRate: Double = 0.8250330542089026
   truePositiveRate: Double = 0.8250330542089026
   fMeasure: Double = 0.745936671083828
   precision: Double = 0.68067954053727
   recall: Double = 0.8250330542089026
   ```

* I ran LR with the dataset and with no outlier removal
   ```
   accuracy: Double = 0.8250330542089026
   falsePositiveRate: Double = 0.8250330542089026
   truePositiveRate: Double = 0.8250330542089026
   fMeasure: Double = 0.745936671083828
   precision: Double = 0.68067954053727
   recall: Double = 0.8250330542089026
   ```
   
 * The arrray of double is actually causing problem even when dataframe write to a file. It expects it to have vectors instead of array. Hence changed logic in the imageArray to save ml.linalg.Vector instead of Array.
   ```org.apache.spark.sql.AnalysisException: CSV data source does not support array<double> data type.;```
   
 * Saved the 3 class label loaded dataframe in the json format through the file check trick mentioned in : https://stackoverflow.com/a/21178667/8693106
   - Idea is to build once and save it as "ImageData.json"
   - In next subsequent run of the program check for the file exists (refer link) and if present it load it directly.

 * with 3 classes the LR statistically reduced:
   ```
   accuracy: Double = 0.4651545036160421
   falsePositiveRate: Double = 0.4651545036160421
   truePositiveRate: Double = 0.4651545036160421
   fMeasure: Double = 0.2953527586343727
   precision: Double = 0.21636871223428653
   recall: Double = 0.4651545036160421
   ```
