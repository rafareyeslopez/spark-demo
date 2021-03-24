import java.sql.Timestamp;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import scala.Tuple3;

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
		System.setProperty("hadoop.home.dir", "c:/hadoop");

		// Load the data from database
		final SparkSession spark = SparkSession.builder().appName("Spark calculation of average days subscribed")
				.master("local[*]").getOrCreate();

		// Query database for the desired data, note the connection details
		final Dataset<Row> load = spark.read().format("jdbc")
				.option("url", "jdbc:mysql://database_ip:3306?user=username&password=password")
				.option("query",
						"SELECT user_id,subscription_timestamp,unsubscription_timestamp  FROM service.subscriptions")
				.load();

		// Convert the Dataset into a JavaRDD
		final JavaRDD<Tuple3<String, Timestamp, Timestamp>> usersMap = load.toJavaRDD()
				.map(row -> new Tuple3<>(row.getString(0), row.getTimestamp(1), row.getTimestamp(2)));

		// Get for each user how many days has been into service
		final JavaPairRDD<String, Long> mapToPair = usersMap.mapToPair(row -> new Tuple2<>(row._1(),
				TimeUnit.MILLISECONDS.toDays(

						row._3() == null ? Calendar.getInstance().getTimeInMillis() - row._2().getTime()
								: row._3().getTime() - row._2().getTime()

				)));

		// Get the total of users for calculate average
		final long count = mapToPair.count();

		// Get the sum of days in service
		final Long reduce = mapToPair.values().reduce((value1, value2) -> value1 + value2);

		// Print out the average
		System.out.println("Average days subscribed " + reduce / count);

	}

}
