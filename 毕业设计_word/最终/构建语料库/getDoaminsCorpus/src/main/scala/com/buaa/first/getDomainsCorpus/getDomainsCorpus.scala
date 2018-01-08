package com.buaa.first.getDomainsCorpus

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

import com.chanct.tldextract.TLDExtract

object getDomainsCorpus{
	def extractSecDomain(domain: String) = {
		var (_, sec, tld) = TLDExtract.split(domain)
		if(tld.startsWith("InternetDomainName")){
			tld = tld.substring(24, tld.length-1)
		}
		sec.toString+"."+tld.toString
	}

	def getDomainsSequence(s: String) = {
		var arr = s.split(" ")
		var len = arr.length
		if (len%2 == 1){
			""
		} else{
			var (a1, a2) = arr.splitAt(len/2)
			(a2 zip a1).toList.sorted.unzip._2.filter(x => !x.endsWith(".")).mkString(" ")		
		}
	}

	def getDomansNum(s: String) = {
		s.split(" ").length
	}
	
	def getDomains(sc: SparkContext, sqlctx: HiveContext, date: String, mode: String, intable: String) = {
		import sqlctx.implicits._
		
		sqlctx.udf.register("getSecDomain", (domain: String) => {extractSecDomain(domain)})
		sqlctx.udf.register("getSecDomainSequence", (domains: String) => {getDomainsSequence(domains)})
		sqlctx.udf.register("getDomainsNum", (domains: String) => {domains.split(" ").length})
		sqlctx.udf.register("filterDomain", (domain: String) => {domain=="localhost" || domain.endsWith("in-addr.arpa") || domain.endsWith(".localdomain") || domain.size < 5 || TLDExtract.split(domain)._3.size==0})
		sqlctx.udf.register("filterNoise", (domain: String) => {
			val res = domain.toList.map(x => Character.isDigit(x) || Character.isAlphabetic(x) || x=='.').filter(x => x!= true)
			res.size == 0
		})
		
		sqlctx.sql("use cdns")
		
		val getsql = mode match {
			case "AllDomains" =>  "select A.sip, A.domains, getDomainsNum(A.domains) as length from (select sip, getSecDomainSequence(concat_ws(' ', collect_list(getSecDomain(qdomain)), collect_list(time))) as domains from gddx_dnsr2c where dt = " + date + " and qtype = 'A' and group by sip,hour)A"
			case "NxDomains" => "select A.sip, A.domains, getDomainsNum(A.domains) as length from (select sip, getSecDomainSequence(concat_ws(' ', collect_list(getSecDomain(qdomain)), collect_list(time))) as domains from gddx_dnsr2c where dt = " + date + " and qtype = 'A' and rcode = '3' group by sip,hour)A"
			case "ExDomains" => "select A.sip, A.domains, getDomainsNum(A.domains) as length from (select sip, getSecDomainSequence(concat_ws(' ', collect_list(getSecDomain(qdomain)), collect_list(time))) as domains from gddx_dnsr2c where dt = " + date + " and qtype = 'A' and rcode = '0' group by sip,hour)A"
		}
		
		val res = sqlctx.sql("getsql")
		res
		
	}
	
	/*******************************************************************
	args: 
		date: 20171101, 20171102, ...
		mode: AllDomains, NxDomains, ExDomains
		intable: gddx_dnsr2c, gddx_dnsr2c_new, ...
		outtable:
	*******************************************************************/
	def main(args: Array[String]) = {
		val date = args(0)
		val mode = args(1)
		val intable = args(2)
		val outtable = args(3)
		
		val conf = new SparkConf().setAppName("getDomainsCorpus")
		val sc = new SparkContext(conf)
		val sqlctx = new HiveContext(sc)
		
		//val res = getDomains(sc, sqlctx, date, mode, intable)
		//res.filter("length > 2").saveAsTable(outtable)
	}
}