package com.github.aadamson.spark_glove

import org.apache.spark.{SparkConf, SparkContext};
import org.apache.spark.rdd.RDD;
import org.apache.spark.Partitioner;
import org.apache.spark.mllib.random.RandomRDDs;
import breeze.linalg._;
import breeze.numerics._;
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry, RowMatrix, BlockMatrix, IndexedRowMatrix, IndexedRow};
import org.apache.spark.mllib.linalg.{Vectors, Vector};


// object GloveParameters {
//   implicit def gloveParamtersToGloveParametersMatrix(p: GloveParameters): GloveParametersMatrix = 
//     new GloveParametersMatrix(p);
// }

class GloveParameters(sc: SparkContext,
                      vectorSize: Int,
                      numRows: Int) extends java.io.Serializable { 
  var W: RDD[(Long, DenseVector[Float])] = sc.parallelize(1L to numRows).map((_, convert(DenseVector.rand(vectorSize), Float)));
  var Wc: RDD[(Long, DenseVector[Float])] = sc.parallelize(1L to numRows).map((_, convert(DenseVector.rand(vectorSize), Float)));
  var b: RDD[(Long, Float)] = sc.parallelize(1L to numRows).map((_, 0.0f));
  var bc: RDD[(Long, Float)] = sc.parallelize(1L to numRows).map((_, 0.0f));
  
  def cacheAll() {
    W = W.cache();
    Wc = Wc.cache();
    b = b.cache();
    bc = bc.cache();
  }
  
  def partitionAll(partitioner: Partitioner) {
    W = W.partitionBy(partitioner);
    Wc = Wc.partitionBy(partitioner);
    b = b.partitionBy(partitioner);
    bc = bc.partitionBy(partitioner);
  }
}

class GloveParametersMatrix(sc: SparkContext,
                            vectorSize: Int,
                            numRows: Int) extends java.io.Serializable {
  var wRows: RDD[IndexedRow] = RandomRDDs.normalVectorRDD(sc, numRows, vectorSize, 4, 8).zipWithIndex().map { case (v, i) => new IndexedRow(i, v) }
  var wcRows: RDD[IndexedRow] = RandomRDDs.normalVectorRDD(sc, numRows, vectorSize, 4, 8).zipWithIndex().map { case (v, i) => new IndexedRow(i, v) }

  var W: BlockMatrix = (new IndexedRowMatrix(wRows)).toBlockMatrix();
  var Wc: BlockMatrix = (new IndexedRowMatrix(wcRows)).toBlockMatrix();
  var b: Vector = Vectors.zeros(numRows);
  var bc: Vector = Vectors.zeros(numRows);

  def cacheAll() {
    W = W.cache();
    Wc = Wc.cache();
    // b = b.cache();
    // bc = bc.cache();
  }
  
  def partitionAll(partitioner: Partitioner) {
    // W = W.partitionBy(partitioner);
    // Wc = Wc.partitionBy(partitioner);
    // b = b.partitionBy(partitioner);
    // bc = bc.partitionBy(partitioner);
  }
}