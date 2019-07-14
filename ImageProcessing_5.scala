// Databricks notebook source
// DBTITLE 1,Imports
import java.io.File
import javax.imageio.ImageIO
import java.awt.image.BufferedImage
import org.apache.spark.ml.feature.StringIndexer
import java.nio.file.{Paths, Files}
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.{Pipeline}//, PipelineModel}
import org.apache.spark.ml.linalg.{Vector, Vectors}

import java.nio.file.{Paths, Files}

// COMMAND ----------

// DBTITLE 1,Globals
val row_1 = new ArrayBuffer[(Array[Double], Double, Double, Double, Double, String, String)]

// COMMAND ----------

// DBTITLE 1,Utilities Function
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

def imageToArray(imageName : String): Either[Array[Double], Int] = {
  //println(imageName)
  try{
    val photo1 = ImageIO.read(new File(imageName))

    if (! (photo1.getHeight == 28 && photo1.getWidth == 28)){
      Right(0)
    }


    val arr = new Array[Double](photo1.getWidth * photo1.getHeight) 

    //println("array size", arr.size)
    var arr_i = 0
    
    val pixel_depth = 255

  
    for (x <- 0 until 28)
     for (y <- 0 until 28){
      arr(arr_i) = ((photo1.getRGB(x,y) & 0xffffff) - (pixel_depth.toDouble/2))/pixel_depth.toDouble  
      arr_i += 1
     }

    Left(arr)
  } catch {
    case ex: NullPointerException =>{

      println("NullPointerException Exception for " + imageName)
      Right(0)
    }
  }
}

def buildRow(label: String) ={
  println("/dbfs/FileStore/tables/noMNIST_small/"+label+"/")
  for (file <- (new File("/dbfs/FileStore/tables/noMNIST_small/"+label+"/")).listFiles){
    val output = imageToArray(file.toString) 
    if (output.isLeft){
      val temp_array = output.left.get
      val (lMedian, lmean, lmin, lmax) = medianDCalculator(temp_array)
      row_1 +=((temp_array,lMedian, lmean, lmin, lmax, label, file.toString))
    }
    else
      println("Ingoring : "+ file)
  }
}

// COMMAND ----------

// DBTITLE 1,Build the DataFrame
val file_name = "ImageData_2.json"
if (Files.exists(Paths.get("/dbfs/"+file_name)) == false){
  for (l <- List("A","B","C")){
    buildRow(l)
  }
  
  var df = spark.sparkContext.parallelize(row_1).toDF("feature", "median", "mean", "min", "max", "label", "file_name")
  
  df.write.json(file_name)
} 


val df = spark.read.json("/"+file_name)
df.show()
df.count()
df.printSchema

// COMMAND ----------

df.select("label").distinct.show()

// COMMAND ----------

def quartileChecks(label: String ) ={
  val qrt_a_median = df.filter(df("label") === label).stat.approxQuantile("median", Array(.25,.5,.75), 0)
  val qrt_a_mean = df.filter(df("label") === label).stat.approxQuantile("mean", Array(.25,.5,.75), 0)
  val df_A = df.filter(df("label") === label ).filter(df("median") >= qrt_a_median(0) and df("median") <= qrt_a_median(2))
  val df_A_mean = df.filter(df("label") === label ).filter(df("mean") >= qrt_a_mean(0) and df("mean") <= qrt_a_mean(2))
  df_A.union(df_A_mean)
}


val df_new = quartileChecks("A").union(quartileChecks("B")).union(quartileChecks("C"))
//val df_new = df


df_new.count()


val df_curr = df_new.dropDuplicates()
df_curr.count()



// COMMAND ----------

var training = df_curr

//import org.apache.spark.mllib.linalg.Vectors
def convertArrayToVector = udf((features: scala.collection.mutable.WrappedArray[Double]) => Vectors.dense(features.toArray))
training = training.withColumn("feature_v", convertArrayToVector($"feature"))

training.printSchema()
println("attempting second transform....")

//creating features column
val assembler = new VectorAssembler()
  .setInputCols( Array("median", "mean","feature_v"))
  .setOutputCol("features")

val out = assembler.transform(training)
println(out)

val indexer = new StringIndexer()
  .setInputCol("label")
  .setOutputCol("label_new")

var indexed = indexer.fit(out).transform(out)

indexed = indexed.withColumnRenamed("label","label_old")
indexed = indexed.withColumnRenamed("label_new","label")
indexed.show()

val lr = new LogisticRegression()
//val lr = new LinearRegressionModel()
  .setMaxIter(10)
  .setRegParam(0.3)
  .setElasticNetParam(0.8)

// Fit the model
val lrModel = lr.fit(indexed)

//  Print the coefficients and intercept for multinomial logistic regression
println(s"Coefficients: \n${lrModel.coefficientMatrix}")
println(s"Intercepts: \n${lrModel.interceptVector}")

val trainingSummary = lrModel.summary

// Obtain the objective per iteration
val objectiveHistory = trainingSummary.objectiveHistory
println("objectiveHistory:")
objectiveHistory.foreach(println)

// for multiclass, we can inspect metrics on a per-label basis
println("False positive rate by label:")
trainingSummary.falsePositiveRateByLabel.zipWithIndex.foreach { case (rate, label) =>
  println(s"label $label: $rate")
}

println("True positive rate by label:")
trainingSummary.truePositiveRateByLabel.zipWithIndex.foreach { case (rate, label) =>
  println(s"label $label: $rate")
}

println("Precision by label:")
trainingSummary.precisionByLabel.zipWithIndex.foreach { case (prec, label) =>
  println(s"label $label: $prec")
}

println("Recall by label:")
trainingSummary.recallByLabel.zipWithIndex.foreach { case (rec, label) =>
  println(s"label $label: $rec")
}

println("F-measure by label:")
trainingSummary.fMeasureByLabel.zipWithIndex.foreach { case (f, label) =>
  println(s"label $label: $f")
}

val accuracy = trainingSummary.accuracy
val falsePositiveRate = trainingSummary.weightedFalsePositiveRate
val truePositiveRate = trainingSummary.weightedTruePositiveRate
val fMeasure = trainingSummary.weightedFMeasure
val precision = trainingSummary.weightedPrecision
val recall = trainingSummary.weightedRecall
println(s"Accuracy: $accuracy\nFPR: $falsePositiveRate\nTPR: $truePositiveRate\n" +
  s"F-measure: $fMeasure\nPrecision: $precision\nRecall: $recall") 

// COMMAND ----------

df_curr.printSchema
