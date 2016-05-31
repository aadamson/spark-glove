package com.github.aadamson.spark_glove

import org.apache.spark.{SparkConf, SparkContext};
import org.apache.spark.rdd.RDD;
import breeze.linalg._;
import breeze.numerics._;
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry, RowMatrix, BlockMatrix};
import org.apache.spark.mllib.linalg.{Matrices, Matrix};

class GloveTrainer(sc: SparkContext,
                   cooccurrenceEntries: RDD[((Long, Long), Float)],
                   vectorSize: Int,
                   numRows: Int) extends java.io.Serializable {
  val numWorkers = 4;
  val rowPartitioner: ExactPartitioner = new ExactPartitioner(numWorkers, numRows);
  val pairPartitioner: ExactPairPartitioner = new ExactPairPartitioner(numWorkers, numRows);
  
  val cooccurrencePairs: RDD[(Long, Long)] = cooccurrenceEntries.flatMap(x => List((x._1._1, x._1._2),
                                                                                   (x._1._2,x._1._1)))
                                                                .partitionBy(rowPartitioner).cache();
  val parameters = new GloveParametersMatrix(sc, vectorSize, numRows);
  val learner = new AdaptiveGradientLearner(sc, vectorSize, numRows);

  def getGrads(p: GloveParametersMatrix, logcr: BlockMatrix, fs: BlockMatrix): (BlockMatrix, BlockMatrix) = {
    val bmat = Utils.broadcastVector(p.b, numRows, sc);
    val bcmat = Utils.broadcastVector(p.bc, numRows, sc);

    var grads: BlockMatrix = p.W.multiply(p.Wc.transpose);

    println(s"Shape of grads: (${grads.numRows}, ${grads.numCols})");
    println(s"Shape of bmat: (${bmat.numRows}, ${bmat.numCols})");
    println(s"Shape of bcmat: (${bcmat.numRows}, ${bcmat.numCols})");
    assert(grads.numRows == numRows);
    assert(grads.numCols == numRows);
    assert(bmat.numRows == numRows);
    assert(bmat.numCols == numRows);
    assert(bcmat.numRows == numRows);
    assert(bcmat.numCols == numRows);

    // grads = grads.add(bmat.transpose);
    // grads = grads.add(bcmat);
    // grads = grads.add(logcr);
    val fgrads: BlockMatrix = Utils.elementwiseProduct(fs, grads);
    return (grads, fgrads);
  }

  def train(eta: Float, xMax: Float, alpha: Float, numIters: Int) = {
    // parameters.partitionAll(rowPartitioner);
    parameters.cacheAll();
    // val logCooccurrences: RDD[((Long, Long), Float)] = cooccurrenceEntries.mapValues(-log(_)).partitionBy(pairPartitioner).cache();
    // val fs: RDD[((Long, Long), Float)] = cooccurrenceEntries.mapValues(x => if (x > xMax) 1.0f else pow(x / xMax, alpha)).partitionBy(pairPartitioner).cache();
    val logCooccurrences: RDD[((Long, Long), Float)] = cooccurrenceEntries.mapValues(-log(_)).partitionBy(pairPartitioner).cache();
    val logcrBlocks = Utils.CoordinateRDD2CoordinateMatrix(logCooccurrences).toBlockMatrix().cache();
    val fs: RDD[((Long, Long), Float)] = cooccurrenceEntries.mapValues(x => if (x > xMax) 1.0f else pow(x / xMax, alpha)).partitionBy(pairPartitioner).cache();
    val fBlocks = Utils.CoordinateRDD2CoordinateMatrix(fs).toBlockMatrix().cache();

    
    // def runIteration(p: GloveParameters): Float = {
    //   // val costs = p.getCosts()      
    //   // Gradient of cost with respect to g = w.T wc + b + bc - log c
    //   val gGrads: RDD[((Long, Long), Float)] = cooccurrencePairs.join(p.W).join(p.b).map { // Accumulate terms related to the word
    //                                                 case (i, ((j, wi), bi)) => (j, (i, wi, bi)) 
    //                                               }.join(p.Wc)
    //                                               .mapValues {
    //                                                 case ((i, wi, bi), wcj) => (i, bi + (wi dot wcj))
    //                                               }.join(p.bc)
    //                                               .mapValues {
    //                                                 case ((i, wp), bcj) => (i, bcj + wp)
    //                                               }.map { // Accumulate terms related to the context word
    //                                                 case (j, (i, wp)) => ((i,j), wp) // Apply g
    //                                               }.join(logCooccurrences).mapValues { case (a,b) => a + b } // Add log term
                                                   
  
    //   // Gradient after weighting by f
    //   val grads: RDD[((Long, Long), Float)] = fs.join(gGrads).mapValues { case (a,b) => a * b }.cache();
    //   val costs = grads.join(gGrads).mapValues { case (a,b) => 0.5f * a * b }
    //   val cost = costs.map{ case (a,b) => b }.reduce(_+_);

    //   learner.update(p, grads, eta);
    //   return cost;
    // }

    def runIteration(p: GloveParametersMatrix): Float = {
      val (grads, fgrads) = getGrads(p, logcrBlocks, fBlocks);

      // // Gradient of cost with respect to g = w.T wc + b + bc - log c
      // val gGrads: RDD[((Long, Long), Float)] = cooccurrencePairs.join(p.W).join(p.b).map { // Accumulate terms related to the word
      //                                               case (i, ((j, wi), bi)) => (j, (i, wi, bi)) 
      //                                             }.join(p.Wc)
      //                                             .mapValues {
      //                                               case ((i, wi, bi), wcj) => (i, bi + (wi dot wcj))
      //                                             }.join(p.bc)
      //                                             .mapValues {
      //                                               case ((i, wp), bcj) => (i, bcj + wp)
      //                                             }.map { // Accumulate terms related to the context word
      //                                               case (j, (i, wp)) => ((i,j), wp) // Apply g
      //                                             }.join(logCooccurrences).mapValues { case (a,b) => a + b } // Add log term
                                                   
  
      // // Gradient after weighting by f
      // val grads: RDD[((Long, Long), Float)] = fs.join(gGrads).mapValues { case (a,b) => a * b }.cache();
      // val costs = grads.join(gGrads).mapValues { case (a,b) => 0.5f * a * b }
      // val cost = costs.map{ case (a,b) => b }.reduce(_+_);

      // learner.update(p, grads, eta);
      return 0.0f;
    }
    
    for (i <- 1 to numIters) {
      val cost = runIteration(parameters);
      println(f"$i%d: $cost%2.4f");
      parameters.cacheAll();
    }
  }
}