import java.io.File
import javax.imageio.ImageIO
import java.awt.image.BufferedImage

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

    //println("Image shape : ", photo1.getHeight, photo1.getWidth, (photo1.getWidth * photo1.getHeight))


    if (! (photo1.getHeight == 28 && photo1.getWidth == 28)){
      Right(0)
    }


    val arr = new Array[Double](photo1.getWidth * photo1.getHeight) 

    //println("array size", arr.size)
    var arr_i = 0
    
    val pixel_depth = 255

  
    for (x <- 0 until 28)
     for (y <- 0 until 28){
      //println(photo1.getRGB(x,y).toHexString, (photo1.getRGB(x,y) & 0xffffff).toHexString)
      //arr(x)(y) = photo1.getRGB(x,y) & 0xffffff
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

val row_1 = new ArrayBuffer[(Array[Double],Double, Double, Double, Double, String)]
val listLables = List("A","B","C","D")
val r = scala.util.Random

for (file <- (new File("/dbfs/FileStore/tables/images/")).listFiles){
  
  
  val output = imageToArray(file.toString) 
  if (output.isLeft){
    val temp_array = output.left.get
    val (lMedian, lmean, lmin, lmax) = medianDCalculator(temp_array)
    row_1 +=((temp_array,lMedian, lmean, lmin, lmax, listLables(r.nextInt(3))))
  }
  else
    println("Ingoring : "+ file)
}

val df = spark.sparkContext.parallelize(row_1).toDF("feature", "median", "mean", "min", "max", "label")
df.show()
