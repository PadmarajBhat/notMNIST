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
   
   
