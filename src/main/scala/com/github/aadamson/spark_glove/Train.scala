package com.github.aadamson.spark_glove

import org.apache.commons.cli.{Options, ParseException, PosixParser};
import org.apache.spark.{SparkConf, SparkContext};
import org.apache.spark.sql.SQLContext;
import org.apache.spark.rdd.RDD;

object Train {
  object IntParam {
    def unapply(str: String): Option[Int] = {
      try {
        Some(str.toInt)
      } catch {
        case e: NumberFormatException => None
      }
    }
  }

  val CREC_CSV = "crecCSV"
  val VECTOR_SIZE = "vectorSize"
  val NUM_ROWS = "numRows"

  val THE_OPTIONS = {
    val options = new Options()
    options.addOption(CREC_CSV, true, "Cooccurrence CSV file")
    options.addOption(VECTOR_SIZE, true, "Vector Size")
    options.addOption(NUM_ROWS, true, "Num Rows")
    options
  }

  def parseCommandLineOptions(args: Array[String]) = {
    val parser = new PosixParser
    try {
      val cl = parser.parse(THE_OPTIONS, args)
      // System.setProperty("glove.crecCSV", cl.getOptionValue(CREC_CSV))
      // System.setProperty("glove.vectorSize", cl.getOptionValue(VECTOR_SIZE))
      // System.setProperty("glove.numRows", cl.getOptionValue(NUM_ROWS))
      cl.getArgList.toArray
    } catch {
      case e: ParseException =>
        System.err.println("Parsing failed.  Reason: " + e.getMessage)
        System.exit(1)
    }
  }

  def main(args: Array[String]) {
    val crecCSV = "/Users/alex/GloVe-1.2/overflow_0000.bin";
    val vectorSize = 4;
    val numRows = 32000;
    // val Array(crecCSV, IntParam(vectorSize),  IntParam(numRows)) =
    //   parseCommandLineOptions(args)

    println("Initializing Spark Context...")
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
                              .set("spark.executor.memory", "4g")
                              .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    conf.registerKryoClasses(Array(classOf[GloveParametersMatrix], classOf[GloveTrainer]));
    val sc = new SparkContext(conf);
    val sqlContext = new SQLContext(sc);

    val df = sqlContext.read
                       .format("com.databricks.spark.csv")
                       .option("header", "false") // Use first line of all files as header
                       .option("inferSchema", "true") // Automatically infer data types
                       .option("delimiter", " ")
                       .load(crecCSV.toString)

    val cooccurrenceEntries: RDD[((Long, Long), Float)] = df.sample(false, 0.01, 8)
                                .map(x => ((x.getInt(0).toLong, x.getInt(1).toLong), x.getDouble(2).toFloat))
                                .cache();

    val trainer = new GloveTrainer(sc, cooccurrenceEntries, vectorSize, numRows);
    trainer.train(0.05f, 100.0f, 0.75f, 3);
  }
}