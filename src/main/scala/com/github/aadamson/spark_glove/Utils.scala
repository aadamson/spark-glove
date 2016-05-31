package com.github.aadamson.spark_glove

import org.apache.spark.{SparkConf, SparkContext};
import org.apache.spark.mllib.linalg.{Vector, Vectors, Matrix, Matrices, DenseMatrix};
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, BlockMatrix, RowMatrix, MatrixEntry, IndexedRow, IndexedRowMatrix};
import org.apache.spark.rdd.RDD;

object Utils {
  type CoordinateRDD[T] = RDD[((Long, Long), T)];

  implicit def CoordinateRDD2CoordinateMatrix(a: CoordinateRDD[Float]): CoordinateMatrix = {
    val entries: RDD[MatrixEntry] = a.map { case ((i, j), value) => new MatrixEntry(i, j, value) };
    val mat: CoordinateMatrix = new CoordinateMatrix(entries);
    return mat;
  }

  def broadcastVector(v: Vector, numRows: Int, sc: SparkContext): IndexedRowMatrix = {
    val rows: RDD[IndexedRow] = sc.parallelize(0 to numRows-1).map(i => new IndexedRow(i, v));
    val mat: IndexedRowMatrix = new IndexedRowMatrix(rows);
    return mat;
  }

  def elementwiseProduct[T](a: T, b: T): T = (a, b) match {
    case (x: BlockMatrix, y: BlockMatrix) => {
      val aIRM = x.toIndexedRowMatrix();
      val bIRM = y.toIndexedRowMatrix();
      val rows = aIRM.rows.zip(bIRM.rows).map {
        case (aRow: IndexedRow, bRow: IndexedRow) => new IndexedRow(aRow.index, elementwiseProduct(aRow.vector, bRow.vector));
      }
      return (new IndexedRowMatrix(rows)).toBlockMatrix().asInstanceOf[T];
    }
    case (x: Vector, y: Vector) => {
      val values = Array(x.toArray, y.toArray);
      return Vectors.dense(values.transpose.map(_.sum)).asInstanceOf[T];;
    }
  }
}