package com.jetondreau.blog.spark_blog_examples.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class GameSchedule implements Serializable {

	private static final long serialVersionUID = -5767263494876444456L;

	private String gameDate;
	private String gameNumber;
	private String dayOfWeek;
	private String visitingTeam;
	private String visitingLeague;
	private String visitorGameNum;
	private String homeTeam;
	private String homeLeague;
	private String homeGameNum;
	private String timeOfDay;
	private String postponed;
	private String makeupDate;

	public GameSchedule(final String gameDate, final String visitingTeam, final String homeTeam, final String dayOfWeek,
			final String gameNumber, final String homeGameNum, final String homeLeague, final String makeupDate,
			final String postponed, final String timeOfDay, final String visitingLeague, final String visitorGameNum) {
		this.gameDate = gameDate;
		this.gameNumber = gameNumber;
		this.dayOfWeek = dayOfWeek;
		this.visitingTeam = visitingTeam;
		this.visitingLeague = visitingLeague;
		this.visitorGameNum = visitorGameNum;
		this.homeTeam = homeTeam;
		this.homeLeague = homeLeague;
		this.homeGameNum = homeGameNum;
		this.timeOfDay = timeOfDay;
		this.postponed = postponed;
		this.makeupDate = makeupDate;
	}

	public String getGameDate() {
		return gameDate;
	}

	public void setGameDate(String gameDate) {
		this.gameDate = gameDate;
	}

	public String getGameNumber() {
		return gameNumber;
	}

	public void setGameNumber(String gameNumber) {
		this.gameNumber = gameNumber;
	}

	public String getDayOfWeek() {
		return dayOfWeek;
	}

	public void setDayOfWeek(String dayOfWeek) {
		this.dayOfWeek = dayOfWeek;
	}

	public String getVisitingTeam() {
		return visitingTeam;
	}

	public void setVisitingTeam(String visitingTeam) {
		this.visitingTeam = visitingTeam;
	}

	public String getVisitingLeague() {
		return visitingLeague;
	}

	public void setVisitingLeague(String visitingLeague) {
		this.visitingLeague = visitingLeague;
	}

	public String getVisitorGameNum() {
		return visitorGameNum;
	}

	public void setVisitorGameNum(String visitorGameNum) {
		this.visitorGameNum = visitorGameNum;
	}

	public String getHomeTeam() {
		return homeTeam;
	}

	public void setHomeTeam(String homeTeam) {
		this.homeTeam = homeTeam;
	}

	public String getHomeLeague() {
		return homeLeague;
	}

	public void setHomeLeague(String homeLeague) {
		this.homeLeague = homeLeague;
	}

	public String getHomeGameNum() {
		return homeGameNum;
	}

	public void setHomeGameNum(String homeGameNum) {
		this.homeGameNum = homeGameNum;
	}

	public String getTimeOfDay() {
		return timeOfDay;
	}

	public void setTimeOfDay(String timeOfDay) {
		this.timeOfDay = timeOfDay;
	}

	public String getPostponed() {
		return postponed;
	}

	public void setPostponed(String postponed) {
		this.postponed = postponed;
	}

	public String getMakeupDate() {
		return makeupDate;
	}

	public void setMakeupDate(String makeupDate) {
		this.makeupDate = makeupDate;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

	//The columns in order from the database
	public static List<String> columns() {
		List<String> columns = new ArrayList<String>();
		columns.add("game_date");
		columns.add("visiting_team");
		columns.add("home_team");
		columns.add("day_of_week");
		columns.add("game_number");
		columns.add("home_game_num");
		columns.add("home_league");
		columns.add("makeup_date");
		columns.add("postponed");
		columns.add("time_of_day");
		columns.add("visiting_league");
		columns.add("visitor_game_num");
		return columns;
	}

	@Override
	public String toString() {
		return "game date " + this.getGameDate() + " game number " + this.getGameNumber() + " days of week "
				+ this.getDayOfWeek() + " visiting team " + this.getVisitingTeam() + " visiting league "
				+ this.getVisitingLeague() + " visitor game " + this.getVisitorGameNum() + " home team "
				+ this.getHomeTeam() + " home league " + this.getHomeLeague() + " home game num "
				+ this.getHomeGameNum() + " time of day " + this.getTimeOfDay() + " postponed " + this.getPostponed()
				+ " makeup date " + this.getMakeupDate();

	}

}
