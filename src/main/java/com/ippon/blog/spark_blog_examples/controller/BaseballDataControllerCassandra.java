package com.ippon.blog.spark_blog_examples.controller;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import com.ippon.blog.spark_blog_examples.cassandra.reader.GameScheduleRowReader.GameScheduleRowReaderFactory;
import com.ippon.blog.spark_blog_examples.cassandra.writer.DayGamesWriter.DayGamesRowWriterFactory;
import com.ippon.blog.spark_blog_examples.model.GameSchedule;

public class BaseballDataControllerCassandra {

	private static GameScheduleRowReaderFactory gameScheduleReader = new GameScheduleRowReaderFactory();
	private static DayGamesRowWriterFactory dayGameWriter = new DayGamesRowWriterFactory();

	
	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Please provide an input output and team as arguments");
			System.exit(0);
		}

		SparkConf conf = new SparkConf().setAppName("Boston Red Sox Scheduled Day Games");
		conf.set("spark.cassandra.connection.host", args[0]);
		
		
		JavaSparkContext context = new JavaSparkContext(conf);
		
		CassandraTableScanJavaRDD<GameSchedule> schedules = CassandraJavaUtil.javaFunctions(context)
				.cassandraTable("baseball_examples", "game_schedule", gameScheduleReader);

		// Filter out away games played by the team being examined.
		JavaRDD<GameSchedule> awayGames = schedules.filter(gameSchedule -> gameSchedule.getVisitingTeam().equals(args[1]) && gameSchedule.getTimeOfDay().equals("d"));

		// Filter out home games played by the team being examined.
		JavaRDD<GameSchedule> homeGames = schedules.filter(gameSchedule -> gameSchedule.getHomeTeam().equals(args[1]) && gameSchedule.getTimeOfDay().equals("d"));

		// Save back to Cassandra
		CassandraJavaUtil.javaFunctions(awayGames).writerBuilder("baseball_examples", "away_day_games", dayGameWriter).saveToCassandra();

		// Save back to Cassandra
		CassandraJavaUtil.javaFunctions(homeGames).writerBuilder("baseball_examples", "home_day_games", dayGameWriter).saveToCassandra();

		context.close();
	}

}
