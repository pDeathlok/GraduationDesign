#qc cc nc nxdr 均值 方差

import org.apache.spark.sql.hive.HiveContext

import scala.math._
import com.chanct.tldextract.TLDExtract

def extractSecDomain(domain: String) = {
	var (_, sec, tld) = TLDExtract.split(domain)
	if(tld.startsWith("InternetDomainName")){
		tld = tld.substring(24, tld.length-1)
	}
	sec.toString+"."+tld.toString
}

def extractSecDomain(domain: String) = {
	var (_, sec, tld) = TLDExtract.split(domain)
	if(tld.startsWith("InternetDomainName")){
		tld = tld.substring(24, tld.length-1)
	}
	sec.toString+"."+tld.toString
}

def filterDomain(domain: String) = {
	var res = true
	try {
		if (domain.endsWith("in-addr.arpa") || domain.size < 4 || TLDExtract.split(domain)._3.size == 0){
			res = false
		} else if (domain.toList.map(x => Character.isDigit(x) || Character.isAlphabetic(x) || x == '.' || x == '-').filter(x => x != true).size != 0){
			res = false
		}
	}catch {
		case e: NullPointerException => res = false
	}
	res
}

val sqlctx = new HiveContext(sc)

sqlctx.udf.register("getSecDomain", (domain: String) => {extractSecDomain(domain)})
sqlctx.udf.register("islegalDomain", (domain: String) => {filterDomain(domain)})
sqlctx.sql("use cdns")
sqlctx.sql("select qdomain, sip, getSecDomain(qdomain) as SecDomain, qtype, rcode from gddx_dnsr2c_new where dt = '20171127' and hour >= '13' and hour <= '23' and islegalDomain(qdomain)").registerTempTable("tmp")
var res = sqlctx.sql("select SecDomain, sum(case when qtype = 'A' then 1 else 0 end) as qac, sum(case when qtype = 'CNAME' then 1 else 0 end) as qcnamec, sum(case when qtype = 'TXT' then 1 else 0 end) as qtxtc, sum(case when qtype = 'MX' then 1 else 0 end) as qmxc, sum(case when qtype = 'NS' then 1 else 0 end) as qnsc, sum(case when qtype = 'PTR' then 1 else 0 end) as qptrc, sum(case when qtype = 'X25' then 1 else 0 end) as qx25c, sum(case when qtype = 'SSFP' then 1 else 0 end) as qssfpc, sum(case when qtype = 'DNSKEY' then 1 else 0 end) as qdnskeyc, sum(case when qtype = 'ZXFR' then 1 else 0 end) as qzxfrc, sum(case when qtype = 'AAAA' then 1 else 0 end) as qaaaac, sum(case when qtype = 'SOA' then 1 else 0 end) as qsoac, sum(case when qtype = 'SPF' then 1 else 0 end) as qspfc, sum(case when qtype = 'SRV' then 1 else 0 end) as qsrvc, sum(case when qtype = 'DS' then 1 else 0 end) as qdsc, sum(case when qtype = 'NAPTR' then 1 else 0 end) as qnaptrc, sum(case when rcode = 0 and qtype = 'A' then 1 else 0 end) as successc, sum(case when rcode = 1 and qtype = 'A' then 1 else 0 end) as wrongqueryc, sum(case when rcode = 2 and qtype = 'A' then 1 else 0 end) as wrongserverc, sum(case when rcode = 3 and qtype = 'A' then 1 else 0 end) as nonexistc, count(distinct(sip)) as sipc, count(distinct(qdomain)) as domainc from tmp group by SecDomain")

sqlctx.sql("select qdomain, sip, getSecDomain(qdomain) as SecDomain, qtype, rcode from gddx_dnsr2c_new where dt = '20171127' and hour = '13' and islegalDomain(qdomain)").registerTempTable("tmp")
var tmp = sqlctx.sql("select SecDomain, sum(case when qtype = 'A' then 1 else 0 end) as qac_13, sum(case when qtype = 'CNAME' then 1 else 0 end) as qcnamec_13, sum(case when qtype = 'TXT' then 1 else 0 end) as qtxtc_13, sum(case when qtype = 'MX' then 1 else 0 end) as qmxc_13, sum(case when qtype = 'NS' then 1 else 0 end) as qnsc_13, sum(case when qtype = 'PTR' then 1 else 0 end) as qptrc_13, sum(case when qtype = 'X25' then 1 else 0 end) as qx25c_13, sum(case when qtype = 'SSFP' then 1 else 0 end) as qssfpc_13, sum(case when qtype = 'DNSKEY' then 1 else 0 end) as qdnskeyc_13, sum(case when qtype = 'ZXFR' then 1 else 0 end) as qzxfrc_13, sum(case when qtype = 'AAAA' then 1 else 0 end) as qaaaac_13, sum(case when qtype = 'SOA' then 1 else 0 end) as qsoac_13, sum(case when qtype = 'SPF' then 1 else 0 end) as qspfc_13, sum(case when qtype = 'SRV' then 1 else 0 end) as qsrvc_13, sum(case when qtype = 'DS' then 1 else 0 end) as qdsc_13, sum(case when qtype = 'NAPTR' then 1 else 0 end) as qnaptrc_13, sum(case when rcode = 0 and qtype = 'A' then 1 else 0 end) as successc_13, sum(case when rcode = 1 and qtype = 'A' then 1 else 0 end) as wrongqueryc_13, sum(case when rcode = 2 and qtype = 'A' then 1 else 0 end) as wrongserverc_13, sum(case when rcode = 3 and qtype = 'A' then 1 else 0 end) as nonexistc_13, count(distinct(sip)) as sipc_13, count(distinct(qdomain)) as domainc_13 from tmp group by SecDomain")
res = res.join(tmp, res("SecDomain") === tmp("SecDomain"), "left")
res = res.join(tmp, Seq("SecDomain", "SecDomain"), "left")

res.count()
sqlctx.sql("select SecDomain, sum(case when qtype = 'A' then 1 else 0 end) as qac, sum(case when qtype = 'CNAME' then 1 else 0 end) as qcnamec, count(distinct(sip)) as sipc from tmp group by SecDomain")
sqlctx.sql("select domain, sip, scity, getSecDomain(m_dnsc2r.domain) as SecDomain, hour2fragment(hour) as fragment from cdns.m_dnsc2r").registerTempTable("c2r_w")
