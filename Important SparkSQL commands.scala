


import scala.collection.mutable
import org.apache.spark.mllib.fpm._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
    //pivot tables

      val transactions = sql("SELECT * FROM mrm.mrm_cust_mrch_dtl")

      var tran_cnt_pivot = transactions.groupBy($"crd_id").pivot("mrch_id").agg(sum($"tran_cnt"))
      val rows = transactions.agg(countDistinct($"crd_id")).collect.toArray()(0)
      var tranList: Array[Tuple3[Int, Int, Double]] = transactions.select($"crd_id",$"mrch_id",$"tran_cnt").collect.map( row => (row(0).toString.toInt,row(1).toString.toInt,row(2).toString.toDouble))

val rowCount = rows(0)(0).toString.toInt
      var tran_amt_pivot = transactions.groupBy($"crd_id").pivot("mrch_id").agg(sum($"tran_amt"))
Matrices.sparse(rows(0),tran_cnt_pivot.columns.size,)
Matrices.sparse(10, )

      val conf = new SparkConf().setAppName("test").setMaster("yarn")
      val sc = new SparkContext(conf)
      val data = sc.textFile("hdfs://nameservice1/projects/mdp/data/mrm/mrm_cust_mrch_dtl/*01/*", minPartitions = 1200)
      val cols = data.map(x => x.split('\t')).map(y => (y(0),y(1))).distinct
      var mat = new CoordinateMatrix(cols.map {
        x => MatrixEntry(x._1.toString.toLong, x._2.toString.toLong, 1)
      }).toRowMatrix()
      val k =10000
      var mat_reduced = mat.computePrincipalComponents(k)

      var results = mat.rows.map( x => x.toArray.mkString(","))
      results.saveAsObjectFile("/hive/mrm.db/cust_mrch_dtl_sampl_3")

      var combos = data.map(x => x.split('\t')).map(y => y(0)+" "+y(1)).distinct
        var crdIdList = combos.map(x => x.split(" ")(0)).collectAsMap
      var mrchIdList = combos.map(x => x.split(" ")(1)).collect


      //generating user ids
      var crd_nums = sc.textFile("/hive/mrm.db/mrm_tran_dtl_prt/*/*").map(x => x.split('\t')(0))
      var crd_ids = crd_nums.distinct.zipWithIndex.map(x => x._2.toString+"\t"+x._1)





      //collab filtering steps
        var ds = sql("select * from mrm.mrm_cust_stor_dtl_3 where dt = '20171111'")
        var totals = ds.groupBy("mrch_id").agg(countDistinct("crd_id")).select($"mrch_id".alias("mrch_num"),$"count(DISTINCT crd_id)".alias("total_tran_cnt"))
        var ds2 = ds.select($"crd_id".alias("crd_id_2"),$"mrch_id".alias("mrch_id_2"), $"tran_cnt".alias("tran_cnt_2"))
        var mrch_pairs = ds.join(ds2,ds("crd_id") === ds2("crd_id_2")).groupBy("mrch_id","mrch_id_2").agg(countDistinct("crd_id"))
        var mrch_join = mrch_pairs.join(totals, mrch_pairs("mrch_id") === totals("mrch_num") or mrch_pairs("mrch_id_2") === totals("mrch_num"))
        var mrch_pair_totals = mrch_join.select($"mrch_id".alias("mrch1"), $"mrch_id_2".alias("mrch2"), $"count(DISTINCT crd_id)".alias("mrch_pair_total"), $"total_tran_cnt".alias("mrch_total")).groupBy("mrch1","mrch2","mrch_pair_total").sum("mrch_total")
        var results_stg = mrch_pair_totals.select($"mrch1", $"mrch2", $"mrch_pair_total", $"sum(mrch_total)".alias("mrch_total"), ($"mrch_pair_total"/$"sum(mrch_total)").alias("tran_ratio"))

        var results = results_stg.groupBy("mrch1").pivot("mrch2").agg(sum("tran_ratio"))

        results.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("/projects/mdp/data/mrm/mrch_corr_matrix")
        results_stg.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("mrch_corr_matrix_stg")




        var ds1 = sc.textFile("/hive/mrm.db/mrm_cust_stor_cnt/*")
        var ds2 = sc.textFile("/projects/mdp/data/mrm/mrm_cust_stor_dtl/*/*")

        var cust_stor_cnt = ds1.map(x => (x.split('\001')(0),x.split('\001')))
        var cust_stor_dtl = ds2.map(x => (x.split('\001')(0),x.split('\001')))



    // Merchant corr matrix
    val mrch_corr_matrix = sql("select * from mrm.mrm_mrch_corr_matrix")

    val results = data.groupBy("mrch1").pivot("mrch2").agg(sum("tran_ratio"))
    results.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("mrch_corr_matrix")






    //collab filtering
    val mrm_usr_mrch_cf_coeffts = sql("select * from mrm.mrm_usr_mrch_cf_coeffts_stg_1")

    val mrch_corr_matrix = spark.read.format("csv").option("header", "true").load("mrch_corr_matrix/*.csv")
    var res = mrm_usr_mrch_cf_coeffts.join(mrch_corr_matrix, $"mrch_id" ===$"mrch1")


    var mrch_corr_matrix = sql("select * from mrm.mrm_mrch_corr_matrix_enr_filtered")
    var res1 = mrm_usr_mrch_cf_coeffts.join(mrch_corr_matrix, $"mrch_id" ===$"mrch1").select($"crd_id",$"mrch1",$"mrch2",$"tran_cnt"/$"total_tran_cnt"*$"tran_ratio".alias("coefft"))
    var res2 = mrm_usr_mrch_cf_coeffts.join(mrch_corr_matrix, $"mrch_id" ===$"mrch2").select($"crd_id",$"mrch2",$"mrch1",$"tran_cnt"/$"total_tran_cnt"*$"tran_ratio".alias("coefft"))
    res1.write.option("sep","\t").csv("/hive/mrm.db/mrm_usr_mrch_cf_coeffts_stg_spk/1")
    res2.write.option("sep","\t").csv("/hive/mrm.db/mrm_usr_mrch_cf_coeffts_stg_spk/2")


    var nums = sc.parallelize(res.columns.filter(x => x.matches("254")))
    res=res.withColumn("coefft",($"tran_cnt"/$"total_tran_cnt"))
    var count=0
    nums.foreach { x => {
      //count += 1
      //if (count % 10 == 0)
      //println(count + " columns completed")
      res(x) = $"coefft" * res(x)
    }
    }

    val keys = res.select($"crd_id",$"mrch_id",($"tran_cnt"/$"total_tran_cnt").alias("coefft"))
    val values  = res.select(nums:_)
    res.write.option("sep","\t").csv("/projects/mdp/data/mrm/mrm_usr_mrch_cf_coeffts_stg_spk")


    // month wise transactions

    val usr_catg = sql("select * from mrm.mrm_usr_catg")
    val tran_dtl = sql("select * from mrm.mrm_cust_catg_stor_dtl")



    //potential merchants
    val mrm_usr_top_corrltd_mrchts = sql("select * from mrm.mrm_usr_top_corrltd_mrchts_bktd_tmp").select($"crd_id".alias("corr_crd_id"), $"mrch_id".alias("corr_mrch_id"), $"mrch_rank".alias("corr_mrch_rank")).repartition(10000)
    val mrch_ids = sql("select * from mrm.mrm_store_ids").select($"mrch_id".alias("store_id"), $"mrch_grp_nm", $"mrch_nrmlz_nm").repartition(1000)
    val mrm_usr_top_corrltd_mrchts_agg = mrm_usr_top_corrltd_mrchts.join(mrch_ids, $"corr_mrch_id" === $"store_id").select($"corr_crd_id", $"corr_mrch_id", $"corr_mrch_rank", $"mrch_grp_nm".alias("corr_mrch_grp_nm"), $"mrch_nrmlz_nm".alias("corr_mrch_nm")).repartition(10000)
    val mrm_cust_mrch_dtl_agg = sql("select * from mrm.mrm_mnthly_usr_mrch_stats_bktd_tmp").join(mrch_ids, $"mrch_id" === $"store_id").select($"crd_id",$"mrch_id",$"mnth_id", $"tran_cnt", $"tran_amt", $"mrch_grp_nm").repartition(10000)
    val mrm_pot_revenue_stg_tmp = mrm_cust_mrch_dtl_agg.join(mrm_usr_top_corrltd_mrchts_agg, $"crd_id" === $"corr_crd_id" && $"mrch_grp_nm" === $"corr_mrch_grp_nm" && $"mrch_id" != $"corr_mrch_id").repartition(10000).groupBy("corr_crd_id", "corr_mrch_id", "corr_mrch_grp_nm", "mnth_id").agg(avg("tran_cnt"), avg("tran_amt"))
    val res = mrm_pot_revenue_stg_tmp.repartition(10000)
    res.write.option("sep","\t").csv("/hive/mrm.db/mrm_pot_revenue_mnthly_stg_tmp")
    var tmp = mrm_usr_top_corrltd_mrchts.join(mrm_cust_mrch_dtl_agg, $"corr_crd_id" === $"crd_id" && $"corr_mrch_id" != $"mrch_id")
    val tmp2 = tmp.join(mrch_ids, $"corr_mrch_id" === $"store_id").select($"corr_crd_id",  $"corr_mrch_id",$"corr_mrch_rank", $"orig_mrch_id", $"orig_tran_cnt", $"orig_tran_amt", $"mrch_grp_nm".alias("corr_mrch_grp_nm"), $"mrch_nrmlz_nm".alias("corr_mrch_nm"))
    val tmp3 = tmp2.join(mrch_ids, $"orig_mrch_id" === $"store_id" && $"corr_mrch_grp_nm" == $"mrch_grp_mn").groupBy("corr_crd_id","corr_mrch_id","corr_mrch_grp_nm").agg(avg("orig_tran_cnt"), avg("orig_tran_amt")).groupBy("corr_mrch_id","corr_mrch_grp_nm").agg(sum("avg(orig_tran_cnt)"), sum("avg(orig_tran_amt)"))

    val tmp = mrm_usr_top_corrltd_mrchts.join(mrm_usr_top_mrch_set, $"corr_crd_id" === $"crd_id")
    val results = results_stg.groupBy("mrch_id","mrch_nm", "corr_mrch_id", "corr_mrch_nm").agg(sum("tran_cnt"), sum("tran_amt")).select($"mrch_id",$"mrch_nm", $"corr_mrch_id", $"corr_mrch_nm", $"sum(tran_cnt)".alias("tran_cnt"), $"sum(tran_amt)".alias("tran_amt"))





//hackathon

val initialSet = mutable.HashSet.empty[(Int,Double)]
val addToSet = (s: mutable.HashSet[(Int,Double)], v:(Int,Double)) => s += v
val mergePartitionSets = (p1: mutable.HashSet[(Int,Double)], p2: mutable.HashSet[(Int,Double)]) => p1 ++= p2
val data=sc.textFile("aaa")
val mrchCnt = data.map(x => x.split("\001")(1).toInt).max+1
val mrchCnt=5325
val mappedData = data.map(x => (x.split("\001")(0).trim.toInt,(x.split("\001")(1).trim.toInt,1.0)))
val mappedData = data.map(x => (x.split("\001")(0).trim.toInt,(x.split("\001")(1).trim.toInt,x.split("\001")(1).trim.toDouble)))
val mappedData = data.map(x => (x.split("\001")(0).trim.toInt,(x.split("\001")(1).trim.toInt,x.split("\001")(2).trim.toDouble)))
val aggs = mappedData.aggregateByKey(initialSet)(addToSet,mergePartitionSets)
val ratings = aggs.map(x => Vectors.sparse(mrchCnt, x._2.toSeq)).cache()

val clusters = KMeans.train(ratings, 10, 10)
val results = clusters.predict(ratings)
clusters.save(sc, "clusters_10_10")


val data = table("mpaa.tbl_tran_dtl_masked")
val aggs = data.groupBy($"crd_id", $"mrch_nrmlz_id", $"mrch_catg_cd").agg(sum($"cs_tran_cnt"), sum($"cs_tran_amt")).select($"crd_id", $"mrch_nrmlz_id", $"mrch_catg_cd", $"sum(cs_tran_cnt)".alias("cs_tran_cnt"), $"sum(cs_tran_amt)".alias("cs_tran_amt"))
val mrch_ids = sql("select * from mpaa.mrch_ids").select($"mrch_nrmlz_id".alias("nrmlz_id"),$"mrch_id")
val cust_mrch_catg_dtl_filtered = aggs.join(mrch_ids, $"mrch_nrmlz_id" === $"nrmlz_id" ).select($"crd_id", $"mrch_id", $"mrch_catg_cd", $"cs_tran_cnt", $"cs_tran_amt")
cust_mrch_catg_dtl_filtered.write.bucketBy(10000,"crd_id").saveAsTable("cust_mrch_catg_dtl_filtered")