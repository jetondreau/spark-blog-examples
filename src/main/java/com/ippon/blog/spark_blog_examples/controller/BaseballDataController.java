package com.ippon.blog.spark_blog_examples.controller;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class BaseballDataController {

	// Function to transform string array into a comma seperated String
	private static final Function<String[], String> APPEND_GAME = new Function<String[], String>() {

		private static final long serialVersionUID = -6442311685154704731L;

		@Override
		public String call(String[] game) throws Exception {
			StringBuffer gameString = new StringBuffer();
			if (game.length != 0) {
				gameString.append(game[0]);
				for (int i = 1; i < game.length; i++) {
					gameString.append(",");
					gameString.append(game[i]);
				}
			}

			return gameString.toString();
		}

	};

	public static void main(String[] args) {
		if (args.length < 3) {
			System.err.println("Please provide an input output and team as arguments");
			System.exit(0);
		}

		SparkConf conf = new SparkConf().setAppName("Boston Red Sox Scheduled Day Games");
		JavaSparkContext context = new JavaSparkContext(conf);
		// context.
		JavaRDD<String> schedules = context.textFile(args[0]);
		// Break up the lines based on the comma delimiter.
		JavaRDD<String[]> mappedFile = schedules.map(line -> line.split(",", -1));

		// Filter out away games played by the team being examined.
		JavaRDD<String[]> awayGames = mappedFile.filter(line -> line[3].equals(args[2]) && line[9].equals("d"));

		//Map array back to a String
		JavaRDD<String> mappedAwayGames = awayGames.map(APPEND_GAME);

		// Filter out home games played by the team being examined.
		JavaRDD<String[]> homeGames = mappedFile.filter(line -> line[6].equals(args[2]) && line[9].equals("d"));

		//Map array back to a String
		JavaRDD<String> mappedHomeGames = homeGames.map(APPEND_GAME);

		// Save back to HDFS
		mappedAwayGames.saveAsTextFile(args[1] + "/awayGames");

		// Save back to HDFS
		mappedHomeGames.saveAsTextFile(args[1] + "/homeGames");

		context.close();
	}

}
