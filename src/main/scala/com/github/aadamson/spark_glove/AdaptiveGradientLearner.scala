package com.github.aadamson.spark_glove

import org.apache.spark.{SparkConf, SparkContext};
import org.apache.spark.rdd.RDD;
import breeze.linalg._;
import breeze.numerics._;

class AdaptiveGradientLearner(sc: SparkContext,
                              vectorSize: Int, 
                              numRows: Int) extends java.io.Serializable {
  // Build gradient accumulators
  var Wgrad: RDD[(Long, DenseVector[Float])] = sc.parallelize(1L to numRows).map(x => (x, DenseVector.ones[Float](vectorSize)));
  var Wcgrad: RDD[(Long, DenseVector[Float])] = sc.parallelize(1L to numRows).map(x => (x, DenseVector.ones[Float](vectorSize)));
  var bgrad: RDD[(Long, Float)] = sc.parallelize(1L to numRows).map(x => (x, 1.0f));
  var bcgrad: RDD[(Long, Float)] = sc.parallelize(1L to numRows).map(x => (x, 1.0f));
  
  def update(p: GloveParameters, g: RDD[((Long, Long), Float)], eta: Float) {
    val grads: RDD[((Long, Long), Float)] = g.mapValues(_*eta).cache();
    
    val gradsWrtW: RDD[(Long, DenseVector[Float])] = grads.map{ case ((i,j), grad) => (j, (i, grad)) }
                                                          .join(p.Wc)
                                                          .map { case (j, ((i,grad), wcj)) => (i, DenseVector.fill(vectorSize){grad} :* wcj) }
                                                          .cache();
    
    val gradsWrtWc: RDD[(Long, DenseVector[Float])] = grads.map { case ((i,j), grad) => (i, (j, grad)) }
                                                          .join(p.W)
                                                          .map { case (i, ((j,grad), wi)) => (i, DenseVector.fill(vectorSize){grad} :* wi) }
                                                          .cache();
    
    // word vector parameter updates
    p.W = p.W.join(gradsWrtW.reduceByKey(_+_))
             .join(Wgrad)
             .mapValues { case ((curr, grad), rate) => curr - grad / sqrt(rate) };
    p.Wc = p.Wc.join(gradsWrtWc.reduceByKey(_+_))
               .join(Wcgrad)
               .mapValues { case ((curr, grad), rate) => curr - grad / sqrt(rate) };
    
    // word vector gradient updates
    Wgrad = Wgrad.join(gradsWrtW.mapValues(pow(_,2)).reduceByKey(_+_)).mapValues { case (a,b) => a + b }
    Wcgrad = Wcgrad.join(gradsWrtWc.mapValues(pow(_,2)).reduceByKey(_+_)).mapValues { case (a,b) => a + b }
    
    val gradsWrtb: RDD[(Long, Float)] = grads.map { case ((i,j), fd) => (i, fd) };
    val gradsWrtbc: RDD[(Long, Float)] = grads.map { case ((i,j), fd) => (j, fd) };
    
    // bias parameter updates
    p.b = p.b.join(gradsWrtb.reduceByKey(_+_))
             .join(bgrad)
             .mapValues { case ((curr, grad), rate) => curr - grad / sqrt(rate) }
             .cache()
    p.bc = p.bc.join(gradsWrtbc.reduceByKey(_+_))
               .join(bcgrad)
               .mapValues { case ((curr, grad), rate) => curr - grad / sqrt(rate) }
               .cache()
    
    // bias gradient updates
    bgrad = bgrad.join(gradsWrtb.mapValues(pow(_,2))).mapValues { case (a,b) => a + b }
    bcgrad = bcgrad.join(gradsWrtbc.mapValues(pow(_,2))).mapValues { case (a,b) => a + b }
  }
}