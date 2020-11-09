package com.datastax.tcf.samples.utils;

import java.sql.Date;
import java.text.DecimalFormat;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.Random;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;

public class LoadData {

	public static long getRandomeTS() {
		Random r = new Random();
		  int Low = 1;
		  int High = 24;
		  int Result = r.nextInt(High-Low) + Low;

		  Calendar calendar = Calendar.getInstance();
		  calendar.add(Calendar.MONTH, - Result);
		

		  java.sql.Timestamp ts = new java.sql.Timestamp(calendar.getTimeInMillis());
		  return ts.getTime();
	}

	public static void main(String[] args) {

		int addition_count = 100;

		DseCluster cluster = null;
		

		// int pages_displayed =0;

		try {

			cluster = DseCluster.builder() // (1)
					.addContactPoint("127.0.0.1").withQueryOptions(new QueryOptions()).build();
			DseSession session = cluster.connect();

			LinkedList<SimpleStatement> statements = new LinkedList<SimpleStatement>();

			for (int i = 0; i < addition_count; i++) {

				new Date(getRandomeTS());
				
			LocalDate localDate =  LocalDate.fromMillisSinceEpoch(getRandomeTS());

				statements.add(new SimpleStatement("INSERT INTO demo.account_deposit_transaction"
						+ " (account_nbr,source_application_cd,bank_nbr,transaction_year,transaction_month,transaction_dt,transaction_id)"
						+ " values (?,?,?,?,?,?,?) ", "account_1", "source_app_1", "bank_1",String.valueOf( localDate.getYear()),
						String.valueOf(new DecimalFormat("00").format(localDate.getMonth())), localDate, String.valueOf(i)));

			}

			BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED).addAll(statements);

			session.execute(batch);

		} finally {
			if (cluster != null)
				cluster.close(); // (5)
		}
	}

	public LoadData() {
		// TODO Auto-generated constructor stub
	}

}
