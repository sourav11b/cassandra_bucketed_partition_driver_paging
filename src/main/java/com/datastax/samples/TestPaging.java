package com.datastax.samples;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.LinkedList;

import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;

public class TestPaging {

	public static void main(String[] args) throws IOException {

		int count = 0;
		int displayed_rows_count = 0;

		DseCluster cluster = null;
		int page_size = 5;
		SimpleStatement statement = null;
		ResultSet rs = null;
		boolean current_rs_empty = true;
		int run_for_current_rs = 0;
		PagingState pagingState = null;

		// int pages_displayed =0;

		try {

			cluster = DseCluster.builder() // (1)
					.addContactPoint("127.0.0.1").withQueryOptions(new QueryOptions()).build();
			DseSession session = cluster.connect();

			LinkedList<SimpleStatement> statements = new LinkedList<SimpleStatement>();

			for (int i = 12; i > 0; i-- ) {

				statements.add(new SimpleStatement(
						"SELECT account_nbr,source_application_cd,bank_nbr,transaction_year,transaction_month,transaction_dt,transaction_id"
								+ " FROM demo.account_deposit_transaction "
								+ " WHERE account_nbr = 'account_1'  and source_application_cd = 'source_app_1'  and bank_nbr = 'bank_1'"
								+ "  and transaction_year ='2020' and transaction_month = '" +String.valueOf(new DecimalFormat("00").format(i)) + "'"
								+ " and transaction_dt >= '2019-01-01' and transaction_dt <= '2020-12-01' "));
			}
			for (int i = 12; i > 0; i-- ) {

				statements.add(new SimpleStatement(
						"SELECT account_nbr,source_application_cd,bank_nbr,transaction_year,transaction_month,transaction_dt,transaction_id"
								+ " FROM demo.account_deposit_transaction "
								+ " WHERE account_nbr = 'account_1'  and source_application_cd = 'source_app_1'  and bank_nbr = 'bank_1'"
								+ "  and transaction_year ='2019' and transaction_month = '" +String.valueOf(new DecimalFormat("00").format(i)) + "'"
								+ " and transaction_dt >= '2019-01-01' and transaction_dt <= '2020-12-01' "));
			}

			while (!statements.isEmpty()) {
				if (current_rs_empty) {

					// System.out.println("gettign new statement");
					statement = (SimpleStatement) statements.getFirst().setFetchSize(page_size - count);
					rs = session.execute(statement);
					current_rs_empty = false;
					run_for_current_rs = 0;
				}
				while (!current_rs_empty) {

					// System.out.println("run_for_current_rs 1: " + run_for_current_rs);
					// System.out.println("count 1: " + count);
//

					for (Row row : rs) {
						System.out.println("****" + row);
						displayed_rows_count++;
						if (displayed_rows_count % 5 == 0) {
							System.out.println("displayed_rows_count : "+displayed_rows_count);
							System.out.println(" ------------ DISPLAY PAGE ENDS HERE ----------------");
						}

						count++;
						if (rs.getAvailableWithoutFetching() == 0 && !rs.isFullyFetched()) {
							// System.out.println("count 1.0: " + count);

							// System.out.println("fetching");
							count = 0;
							if (run_for_current_rs > 0) {
								rs.fetchMoreResults();

							} else {
								pagingState = rs.getExecutionInfo().getPagingState();
								break;
							}
						}
					}

					if (run_for_current_rs == 0 && !rs.isExhausted()) {
						// System.out.println("resetting statement statement");

						statement.setFetchSize(page_size).setPagingState(pagingState);
						rs = session.execute(statement);
						// System.out.println("resetting statement
						// statement------"+rs.getAvailableWithoutFetching());

					}
					run_for_current_rs++;

					// PagingState pagingState = rs.getExecutionInfo().getPagingState();
					if (rs.isExhausted()) {
						// System.out.println("count 1: " + count);
						current_rs_empty = true;
						statements.removeFirst();

					}

				}

				// System.out.println("run_for_current_rs 2 :" + run_for_current_rs);
				// System.out.println("page_count 2 :" + run_for_current_rs);

			}
			// System.out.println("final page count : " + count);

			// System.out.println("statements left : " + statements.size());
			System.out.println(" ------------FINAL DISPLAY PAGE ENDS HERE ----------------");
			System.out.println("final "
					+ "displayed_rows_count : "+displayed_rows_count);


		} finally {
			if (cluster != null)
				cluster.close(); // (5)
		}
	}

	public TestPaging() {
		// TODO Auto-generated constructor stub
	}

}
