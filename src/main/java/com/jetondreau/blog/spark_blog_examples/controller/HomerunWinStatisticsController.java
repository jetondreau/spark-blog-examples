package com.jetondreau.blog.spark_blog_examples.controller;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.util.StatCounter;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import scala.Tuple2;

public class HomerunWinStatisticsController {

	/**
	 * Function that will manipulate the date so we can determine winning
	 * percentage and homeruns per team per season.
	 */
	private static String[] manipulateDate(String[] game) {
		// Thread local variable containing each thread's date format
	     final ThreadLocal<DateTimeFormatter> dtf =
	         new ThreadLocal<DateTimeFormatter>() {
	             @Override protected DateTimeFormatter initialValue() {
	                 return DateTimeFormat.forPattern("yyyymmdd");
	         }
	     };
	     DateTime gameDate = dtf.get().parseDateTime(game[0].replace("\"", ""));
	     String [] gameLog = game.clone();
	     gameLog[0] = String.valueOf(gameDate.getYear());

		return gameLog;
	}

	/**
	 * This function will parse out the winning percentage for each team in a
	 * single season.
	 */
	private static Iterable<Double> getWinningPercentage(Double [] t) {
		return Arrays.asList(t[1]);
	}

	/**
	 * This function will parse out the homerun total for each team in a single
	 * season.
	 */
	private static Iterable<Double> getHomerun(Double [] t) {
		return Arrays.asList(t[0]);
	}

	/**
	 * This function will parse out and calculate the total number of homeruns,
	 * games won and games played for a team in a single season. Once this is
	 * calculated the team, homerun total and winning percentage is returned.
	 * Tuple is made up Tuple2<teamKey, Tuple2<gamesAsVisitor, gamesAsHome>>
	 */
	private static Double[] calculateByTeam(Tuple2<String, Tuple2<Iterable<String[]>, Iterable<String[]>>> v1) {
		double teamHomeRuns = 0;
		double teamWins = 0;
		double gamesPlayed = 0;
		/**
		 * Grab and loop through the games played as a visitor. Parse out
		 * the number of homeruns, and determine if the game was a win or
		 * loss. Also increment the total games played.
		 */
		for(String [] visitingGames : v1._2._1) {
			double visitingTeamScore = Double.parseDouble(visitingGames[9]);
			double homeTeamScore = Double.parseDouble(visitingGames[10]);
			if (visitingTeamScore > homeTeamScore) {
				teamWins++;
			}
			teamHomeRuns += Integer.parseInt(visitingGames[25]);
			gamesPlayed++;
		}
		
		/**
		 * Grab and loop through the games played at home. Parse out the
		 * number of homeruns, and determine if the game was a win or loss.
		 * Also increment the total games played.
		 */
		for(String [] homeGames : v1._2._2) {
			double visitingTeamScore = Double.parseDouble(homeGames[9]);
			double homeTeamScore = Double.parseDouble(homeGames[10]);
			if (homeTeamScore > visitingTeamScore) {
				teamWins++;
			}
			teamHomeRuns += Integer.parseInt(homeGames[53]);
			gamesPlayed++;
		}

		Double[] gameInformation = { teamHomeRuns,
				teamWins / gamesPlayed };
		return gameInformation;
	}

	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Please provide an input location");
			System.exit(0);
		}

		SparkConf conf = new SparkConf().setAppName("Blog Example: Statistics and Correlation");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// context.
		JavaRDD<String> gameLogs = sc.textFile(args[0]);
		// Break up the lines based on the comma delimiter.
		JavaRDD<String[]> mappedFile = gameLogs.map(line -> line.split(",", -1));
		// Parse each line to transform the date into only a year to be able to
		// determine what game was in what season.
		JavaRDD<String[]> parsedDate = mappedFile.map(HomerunWinStatisticsController::manipulateDate);
		// Group by the home team and year played in.
		JavaPairRDD<String, Iterable<String[]>> mappedByVisitingTeam = parsedDate
				.groupBy(line -> (line[3] + "," + line[0]));
		// Group by the visiting team and year played in.
		JavaPairRDD<String, Iterable<String[]>> mappedByHomeTeam = parsedDate
				.groupBy(line -> (line[6] + "," + line[0]));
		// Join the visiting team and home team RDD together to get a whole
		// season of game logs for each team
		JavaPairRDD<String, Tuple2<Iterable<String[]>, Iterable<String[]>>> joined = mappedByVisitingTeam
				.join(mappedByHomeTeam);
		// Using the map function to transform the data
		JavaRDD<Double[]> mappedTo = joined.map(HomerunWinStatisticsController::calculateByTeam);

		JavaDoubleRDD homeruns = mappedTo.flatMapToDouble(HomerunWinStatisticsController::getHomerun);

		JavaDoubleRDD winningPercentage = mappedTo.flatMapToDouble(HomerunWinStatisticsController::getWinningPercentage);

		Double correlation = Statistics.corr(JavaDoubleRDD.toRDD(homeruns), JavaDoubleRDD.toRDD(winningPercentage),
				"pearson");

		// temporarily print to console
		System.out.println("**************Pearson coefficiant for homeruns to winning percentage " + correlation);

		// List of the main statistics for homeruns;
		StatCounter homerunStats = homeruns.stats();
		// temporarily print out to console some example statistics that are
		// included in StatCounter - see Javadocs for complete list
		System.out.println("**************Mean of homeruns " + homerunStats.mean());
		System.out.println("**************Standard deviation of homeruns " + homerunStats.stdev());
		System.out.println("**************Variance of homeruns " + homerunStats.variance());

		// List of the main statistics for winning percentage.
		StatCounter winningPercentageStats = winningPercentage.stats();
		// temporarily print out to console some example statistics that are
		// included in StatCounter - see Javadocs for complete list
		System.out.println("**************Mean of winning percentage " + winningPercentageStats.mean());
		System.out.println("**************Standard deviation of winning percentage " + winningPercentageStats.stdev());
		System.out.println("**************Variance of winning percentage " + winningPercentageStats.variance());

		sc.close();
	}

}
