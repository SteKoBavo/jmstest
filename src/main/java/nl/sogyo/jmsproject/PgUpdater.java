package nl.sogyo.jmsproject;

import java.util.Random;
import java.sql.*;

public class PgUpdater implements Runnable {
	private java.sql.Connection pgConnection;
	private Random rand = new Random();
	
	public PgUpdater() {
		try {
			// Connect to PostGreSQL
			Class.forName("org.postgresql.Driver");
			this.pgConnection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/testdb", "stefan", "password");
			this.initializeTable();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void initializeTable() throws SQLException {
		Statement st = this.pgConnection.createStatement();
		try {
		st.execute("CREATE TABLE IF NOT EXISTS testschema.trafficlights (direction CHAR(5) PRIMARY KEY, color CHAR(5) NOT NULL);");
		st.execute("INSERT INTO testschema.trafficlights VALUES( 'North', 'Red');");
		st.execute("INSERT INTO testschema.trafficlights VALUES( 'East', 'Red');");
		st.execute("INSERT INTO testschema.trafficlights VALUES( 'South', 'Green');");
		st.execute("INSERT INTO testschema.trafficlights VALUES( 'West', 'Red');");
		st.execute("CREATE OR REPLACE FUNCTION notifyfunc() RETURNS trigger AS $$ BEGIN NOTIFY mychannel; RETURN NEW; END; $$ LANGUAGE 'plpgsql';");
		st.execute("CREATE TRIGGER notifier BEFORE INSERT OR UPDATE ON testschema.trafficlights FOR EACH STATEMENT EXECUTE PROCEDURE notifyfunc();");
		} catch (Exception e) {
			//Do Nothing. An Exception is thrown when the table and the rows are already created.
		}
		st.close();
	}
	
	public void run() {
		try {
			while (true) {
				this.setRandomTrafficLight();
				Thread.sleep(2000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			this.close();
		}
	}
	
	private void setRandomTrafficLight() throws SQLException {
		Statement st = this.pgConnection.createStatement();
		st.execute("UPDATE testschema.trafficlights SET color = 'Red';");
		int randomNumber = this.rand.nextInt(4);
		switch (randomNumber) {
			case 0:	st.execute("UPDATE testschema.trafficlights SET color = 'Green' WHERE direction = 'North';");
					break;
			case 1:	st.execute("UPDATE testschema.trafficlights SET color = 'Green' WHERE direction = 'East';");
					break;
			case 2:	st.execute("UPDATE testschema.trafficlights SET color = 'Green' WHERE direction = 'South';");
					break;
			case 3:	st.execute("UPDATE testschema.trafficlights SET color = 'Green' WHERE direction = 'West';");
					break;
		}
		st.close();
	}
	
	public void close() {
		try {
			this.pgConnection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}