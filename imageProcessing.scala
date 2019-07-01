import java.io.File
import javax.imageio.ImageIO
import java.awt.image.BufferedImage

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
