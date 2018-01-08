package com.buaa.first.getDomainsCorpus

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

//import com.chanct.tldextract.TLDExtract

object getDomainsCorpus{
	def main(args: Array[String]) = {
		//val date = args(0)
		
		val conf = new SparkConf().setAppName("getDomainsCorpus")
		val sc = new SparkContext(conf)
		val sqlctx = new HiveContext(sc)
		sqlctx.sql("use cdns")
		//val res = getDomains(sc, sqlctx, date, mode, intable)
		//res.filter("length > 2").saveAsTable(outtable)
	}
}