import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author Rafael Reyes Lopez
 * @email rafareyeslopez@gmail.com
 * @date 2021-03-03
 *
 */
public class Main {

	public static void main(final String[] args) {

		// Set logging level to WARN messages
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		// Set Hadoop directory
		System.setProperty("hadoop.home.dir", "C:\\Users\\Rafa\\eclipse-workspace\\hadoop-2.8.1\\");

		// Load the data from database
		SparkConf conf = (new SparkConf()).setAppName("startingSpark").setMaster("local[*]")
				.set("spark.executor.cores", "5").set("spark.executor.memory", "8g");

		conf.set("spark.cassandra.connection.host", "159.65.84.29");
		conf.set("spark.cassandra.input.consistency.level", "ALL");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SparkSession spark = SparkSession.builder().appName("Spark calculation of average days subscribed").config(conf)
				.master("local[*]").config("spark.executor.cores", "5").getOrCreate();

		// Query database for the desired data, note the connection details
		final Dataset<Row> load = spark.read().format("jdbc")
				.option("url", "jdbc:mysql://139.59.164.134:3306?user=idbroot&password=20root17")
				.option("query",
						"SELECT cc.*  FROM dot.customer_care cc  WHERE cc.op_id='21' AND cc.date between '2021-06-01' and '2021-06-16'")
				.load();

		System.out.println("RAFAAA " + load.count());

		List<Row> collectAsList = load.collectAsList();

		collectAsList.forEach(System.out::println);

//		Calendar calendarFrom = Calendar.getInstance();
//		calendarFrom.set(2017, 3, 2);
//		Calendar calendarTo = Calendar.getInstance();
//		calendarTo.set(2021, 3, 6);
//
//		CassandraTableScanJavaRDD<CassandraRow> select = CassandraJavaUtil.javaFunctions(spark.sparkContext())
//				.cassandraTable("click", "clicks")
//				.select(new String[] { "timestamp", "ip", "isp", "query_string", "request_header_info", "campaign_id" })
//				.limit(10L);
////		JavaRDD<CassandraRow> filter = select.filter(row -> (row.getDate("timestamp") != null
////				&& row.getInt("campaign_id") != null && row.getDate("timestamp").after(calendarFrom.getTime())
////				&& row.getDate("timestamp").before(calendarTo.getTime())));
//		JavaRDD<CassandraRow> filter = select
//				.filter(row -> collectAsList.contains(row.getUUID("click_uuid").toString()));
//
//		filter.foreach(System.out::println);

//		BufferedWriter writer = new BufferedWriter(new FileWriter("brandformers.txt"));
//		filter.collect().writer.close();
		spark.close();
		sc.close();

//		// Convert the Dataset into a JavaRDD
//		final JavaRDD<Tuple3<String, Timestamp, Timestamp>> usersMap = load.toJavaRDD()
//				.map(row -> new Tuple3<>(row.getString(0), row.getTimestamp(1), row.getTimestamp(2)));
//
//		// Get for each user how many days has been into service
//		final JavaPairRDD<String, Long> mapToPair = usersMap.mapToPair(row -> new Tuple2<>(row._1(),
//				TimeUnit.MILLISECONDS.toDays(
//
//						row._3() == null ? Calendar.getInstance().getTimeInMillis() - row._2().getTime()
//								: row._3().getTime() - row._2().getTime()
//
//				)));
//
//		// Get the total of users for calculate average
//		final long count = mapToPair.count();
//
//		// Get the sum of days in service
//		final Long reduce = mapToPair.values().reduce((value1, value2) -> value1 + value2);
//
//		// Print out the average
//		System.out.println("Average days subscribed " + reduce / count);

	}

}
