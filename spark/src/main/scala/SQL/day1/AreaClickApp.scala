package SQL.day1

/**
 * description: AreaClickApp 
 * date: 2020/6/12 20:56 
 * author: nogc
 * version: 1.0 
 */
import org.apache.spark.sql.SparkSession

object AreaClickApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[2]")
      .appName("AreaClickApp")
      .enableHiveSupport()
      .getOrCreate()
    spark.sql("use sparkpractice200213")
    // 0 注册自定义聚合函数
    spark.udf.register("city_remark", new AreaClickUDAF)
    // 1. 查询出所有的点击记录,并和城市表产品表做内连接
    spark.sql(
      """
        |select
        |    c.*,
        |    v.click_product_id,
        |    p.product_name
        |from user_visit_action v join city_info c join product_info p on v.city_id=c.city_id and v.click_product_id=p.product_id
        |where click_product_id>-1
            """.stripMargin).createOrReplaceTempView("t1")

    // 2. 计算每个区域, 每个产品的点击量
    spark.sql(
      """
        |select
        |    t1.area,
        |    t1.product_name,
        |    count(*) click_count,
        |    city_remark(t1.city_name)
        |from t1
        |group by t1.area, t1.product_name
            """.stripMargin).createOrReplaceTempView("t2")

    // 3. 对每个区域内产品的点击量进行倒序排列
    spark.sql(
      """
        |select
        |    *,
        |    rank() over(partition by t2.area order by t2.click_count desc) rank
        |from t2
            """.stripMargin).createOrReplaceTempView("t3")

    // 4. 每个区域取top3

    spark.sql(
      """
        |select
        |    *
        |from t3
        |where rank<=3
            """.stripMargin).show


  }
}
