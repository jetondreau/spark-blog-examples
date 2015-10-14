package com.jetondreau.blog.spark_blog_examples.cassandra.writer;

import java.io.Serializable;

import com.datastax.spark.connector.ColumnRef;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.writer.RowWriter;
import com.datastax.spark.connector.writer.RowWriterFactory;
import com.jetondreau.blog.spark_blog_examples.model.GameSchedule;

import scala.collection.IndexedSeq;
import scala.collection.JavaConversions;
import scala.collection.Seq;

public class DayGamesWriter implements RowWriter<GameSchedule> {

	private static final long serialVersionUID = -1845581188887609455L;

	@Override
	public Seq<String> columnNames() {
		return JavaConversions.asScalaBuffer(GameSchedule.columns()).toList();
	}

	@Override
	public void readColumnValues(GameSchedule gameSchedule, Object[] buffer) {
		buffer[0] = gameSchedule.getGameDate();
		buffer[1] = gameSchedule.getVisitingTeam();
		buffer[2] = gameSchedule.getHomeTeam();
		buffer[3] = gameSchedule.getDayOfWeek();
		buffer[4] = gameSchedule.getGameNumber();
		buffer[5] = gameSchedule.getHomeGameNum();
		buffer[6] = gameSchedule.getHomeLeague();
		buffer[7] = gameSchedule.getMakeupDate();
		buffer[8] = gameSchedule.getPostponed();
		buffer[9] = gameSchedule.getTimeOfDay();
		buffer[10] = gameSchedule.getVisitingLeague();
		buffer[11] = gameSchedule.getVisitorGameNum();
	}

	// Factory
	public static class DayGamesRowWriterFactory implements RowWriterFactory<GameSchedule>, Serializable {
		private static final long serialVersionUID = 1L;

		@Override
		public RowWriter<GameSchedule> rowWriter(TableDef arg0, IndexedSeq<ColumnRef> arg1) {
			return new DayGamesWriter();
		}
	}

}
