package denik.samples.spark

import org.apache.spark.{SparkContext, SparkConf}
import org.slf4j.LoggerFactory
import org.apache.spark.rdd._

/**
 * Created by denik on 09.12.2015.
 */
object DummyClustering {

  val log = LoggerFactory.getLogger(DummyClustering.getClass)

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("Dummy clustering of addresses by transactions")

    SparkContext.jarOfClass(this.getClass).map { x =>
      sparkConf.setJars(Seq(x))
    }

    val sparkContext = new SparkContext(sparkConf)

    val txInputs = sparkContext.textFile("/some/path/to/inputs").map {
      line =>
        //TODO convert line to TxInput
        TxInput()
    }

    val txs = sparkContext.textFile("/some/path/to/txs").map {
      line =>
        //TODO convert line to TxInput
        Tx()
    }

    val addressToClusterIds = txInputs.groupBy(_.txId).join(txs.map { tx => (tx.id, tx) }).map {
      joinedData =>
        val inputs = joinedData._2._1.toList.sortBy(_.addressId)
        val addressKey =
          inputs.map(_.addressId).foldLeft(new StringBuilder()) {
            (sb, address) =>
              sb.append(',').append(address)
              sb
          }
        (addressKey.toString(), joinedData._2._2)
    }.groupBy(_._1).map {
      clusterCandidat =>
        //cluster_id is the lowest tx_id
        val txs = clusterCandidat._2.map(_._2).toList.sortBy(_.id)
        (clusterCandidat._1, txs)
    } /*filter out cluster from 1 element*/
      .filter(_._2.size > 1)
      /*transforming to final result (cluster)*/
      .flatMap {
      cluster =>
        val clusterId = cluster._2(0).id
        cluster._1.substring(1).split(",").map {
          address =>
            (address, clusterId)
        }
    }

    addressToClusterIds.saveAsTextFile("/path/to/result")

  }

}


case class TxInput(var id: Int = 0, var addressId: String = "", var value: BigDecimal = BigDecimal("0.0"), var txId: Long = 0)

case class TxOutputs(var id: Int = 0, var addressId: String = "", var value: BigDecimal = BigDecimal("0.0"), var txId: Long = 0)

case class Tx(var id: Long = 0, var timestampBlock: Long = System.currentTimeMillis())

case class AddressToCluster(var addressId: String, var clusterId: Long)

