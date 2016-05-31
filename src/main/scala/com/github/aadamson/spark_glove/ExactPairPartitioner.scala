package com.github.aadamson.spark_glove

import org.apache.spark.Partitioner;

class ExactPairPartitioner(
    partitions: Int,
    elements: Int)
  extends Partitioner {

  def getPartition(key: Any): Int = {
    def getKey(ke: Any): Long = ke match {
      case (x: Long, y: Long) => x
      case _ => 0
    }
    val k = getKey(key)
    // `k` is assumed to go continuously from 1 to elements.
    return ((k-1) * partitions / elements).toInt
  }
  
  def numPartitions(): Int = partitions;
}