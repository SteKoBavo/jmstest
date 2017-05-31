package nl.sogyo.jmsproject;

import java.util.Random;
import java.sql.*;

public class PgUpdater {
	java.sql.Connection pgConnection;
	
	public PgUpdater() throws Exception {		
		// Connect to PostGreSQL
		Class.forName("org.postgresql.Driver");
		this.pgConnection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/testdb", "stefan", "password");
		
		// Create the trafficlights table
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
			//Do Nothing. Exception is thrown when the table and the rows are already created.
		}
		st.close();
	}
	
	public void update() throws Exception {
		Random rand = new Random();
		while (true) {
			Statement st = this.pgConnection.createStatement();
			st.execute("UPDATE testschema.trafficlights SET color = 'Red';");
			int  randomNumber = rand.nextInt(4);
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
	
			Thread.sleep(2000);
		}
	}
	
	public void close() throws Exception {
		this.pgConnection.close();
	}
	
	public static void main (String[] args) {
		try {
			PgUpdater pgUpdater = new PgUpdater();
			pgUpdater.update();
			pgUpdater.close();
        } catch (Exception e) {
			// Just printStackTrace for all Exceptions for simplicity
            e.printStackTrace();
        }
	}
}