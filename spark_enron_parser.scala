//import stuff for tests
import org.apache.spark.sql.SparkSession
val ss = SparkSession.
builder().
master("local").
appName("Spark in Motion Example").
config("spark.config.option", "some-value").
enableHiveSupport().
getOrCreate()

import ss.implicits._
import org.apache.spark.sql.functions._
import java.util.Date;

def parseEmail(email: String): (String, String, String, String, String) = {
	val fields = email.split("Subject:")(0).split('\n')
	val messageId = fields(0).split(":")(1).trim
	val date = fields(2).split(":")(1).trim
	val from = fields(1).split(":")(1).trim
	val toStr = ""

	if (fields.length > 3) {
		val toArr = fields.slice(3, fields.length).map(x => x.trim)
		toStr = toArr.mkString(":")(1).trim
	}
	(messageId,date,from,toStr,email)
}

val files = sc.WholeTextFiles("file:///data/enron/maildir/*/*/*")
val emails = files.map(x => x._2)
val results = emails.map(x => parseEmail(x))
spark.createDataFrame(results).write.csv("file:///data.assignment2/output2")