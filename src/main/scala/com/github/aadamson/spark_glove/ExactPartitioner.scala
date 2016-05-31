package com.github.aadamson.spark_glove

import org.apache.spark.Partitioner;

class ExactPartitioner(
    partitions: Int,
    elements: Int)
  extends Partitioner {

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Long]
    // `k` is assumed to go continuously from 1 to elements.
    return ((k-1) * partitions / elements).toInt
  }
  
  def numPartitions(): Int = partitions;
}