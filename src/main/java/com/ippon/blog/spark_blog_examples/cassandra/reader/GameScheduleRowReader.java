package com.ippon.blog.spark_blog_examples.cassandra.reader;

import java.io.Serializable;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;
import com.datastax.spark.connector.ColumnRef;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.rdd.reader.RowReader;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;
import com.ippon.blog.spark_blog_examples.model.GameSchedule;

import scala.collection.IndexedSeq;

public class GameScheduleRowReader extends GenericRowReader<GameSchedule> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3300316251395492962L;

	@Override
	public GameSchedule read(Row row, String[] columnNames, ProtocolVersion protocolVersion) {
		GameSchedule gameSchedule = new GameSchedule(row.getString(0), row.getString(1), row.getString(2),
				row.getString(3), row.getString(4), row.getString(5), row.getString(6), row.getString(7),
				row.getString(8), row.getString(9), row.getString(10), row.getString(11));
		return gameSchedule;
	}

	public static class GameScheduleRowReaderFactory implements RowReaderFactory<GameSchedule>, Serializable {
		private static final long serialVersionUID = 1L;

		@Override
		public Class<GameSchedule> targetClass() {
			return GameSchedule.class;
		}

		@Override
		public RowReader<GameSchedule> rowReader(TableDef arg0, IndexedSeq<ColumnRef> arg1) {
			return new GameScheduleRowReader();
		}
	}

}
