import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode

object 实验八实训一 {
  case class Product(id:Int,features:Vector)
 case class Data(id:Int,features:Vector,result:Int)

  def main(args: Array[String]): Unit =
  {
    val conf = new SparkConf().setMaster("local").setAppName("test")
    val sc = new SparkContext(conf)
    val spark = new SQLContext(sc) //定义spark
    import spark.implicits._  //定义spark的隐式函数，给后面的rdd使用DF做准备，没有这个后面的DF会报错，无法将rdd转变为dataframe

    val filePath="E:\\学习文件\\spark\\实验十实训一\\business_circle.txt"
    val input= sc.textFile(filePath)
    val vector=input.map{x=>val line=x.split("\t");(Product(line(0).toInt,Vectors.dense(line(1).toDouble,line(2).toDouble,line(3).toDouble,line(4).toDouble)))}.toDF("id","features")
    //   vector.foreach(println)

    val scaler=new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures")
    val scalerModel=scaler.fit(vector)
    val scaledData=scalerModel.transform(vector)
    print("zhege his scala dataframe")
 //   scaledData.foreach(println)
    val a=scaledData.select("id","scaledFeatures").rdd.map(_.toString.replace("[","").replace("]","").split(','))  //将dataframe中的需要的值转变为rdd
    val KMeans_input=a.map(x=>(x(0).toInt,Vectors.dense(x(1).toDouble,x(2).toDouble,x(3).toDouble))).cache()

    val numClusters=3
    val numIterations=200
    val clusters=KMeans.train(KMeans_input.map(x=>x._2),numClusters,numIterations,runs = 10)
    val predict=KMeans_input.map(x=>(x._1,x._2,clusters.predict(x._2)))
    val Ws=clusters.computeCost(KMeans_input.map(_._2))
    println("width sum Reeors="+Ws)
    val mymodel="E:\\学习文件\\spark\\实验十实训一\\mymodel"
    clusters.save(sc,mymodel)
    val sameModel=KMeansModel.load(sc,mymodel)
    predict.collect.foreach(println)   //预测结果
    predict.saveAsTextFile("E:\\学习文件\\spark\\实验十实训一\\结果")
    print("wswswsws",Ws)   //聚类中心
    clusters.clusterCenters.foreach(println("clusters",_))  //模型误差
    val predict_dataframe=predict.map(x=>Data(x._1,x._2,x._3)).toDF("id","features","result")
    predict_dataframe.save("E:\\学习文件\\spark\\实验十实训一\\last_dataframe","json",SaveMode.Overwrite)

    /*
    val new_last=last_Array_String.map(_.toString.replace("[","").replace("]","").split(','))
    new_last.foreach(println)  //将dataframe中的需要的值转变为rdd
    val last_out=new_last.map(x=>(x(0).toInt,Vectors.dense(x(1).toDouble,x(2).toDouble,x(3).toDouble),x(4).toInt))
    print("last_outlast_outlast_outlast_outlast_outlast_out")
    last_out.take(10).foreach(println)
    last_out.saveAsTextFile("E:\\学习文件\\spark\\实验十实训一\\结果_last")
*/

    //    val b=a.replace("[","").replace("]","")
    //    b.foreach(println)
    //    val b=a.map(_(0))
    // a.take(10).foreach(println)
    // input_k.take(10).foreach(println)
    // val Kmeans_input=scaledData.map(x=>(x(2)))
    /*  Kmeans_input.take(10).foreach(println)

      val Kmeans_input=a.map(x=>(x(0).split('[').map(_(1)),x(1).toDouble,x(2).split(']').map(_(0))))


  //    Kmeans_input.take(10).foreach(println)

      //a.take(10).foreach(println)


    //  val Kmeans_input=input_k.map(x=>Vectors.dense(x(0).toString.toDouble,x(1).toString.toDouble,x(2).toString.toDouble))
    //  Kmeans_input.take(10).foreach(println)

  */
    //input.map{x=>val line=x.split(" ");LabeledPoint(line(0).toDouble,Vectors.dense(line(1)).map(_.))}

  }
}
println c杜国强是猪猪