package nl.sogyo.jmsproject;

import org.junit.Assert;
import org.junit.Test;
import nl.sogyo.jmsproject.cassandra.*;

public class JMSProjectTest {

    @Test
    public void onMessageResultsInASQLStatementOnCassandra() {
		CassandraMock cMock = new CassandraMock();
		CassandraListener cas = new CassandraListener(new JMSTopicMock(), cMock);
		cas.onMessage("a;b");
		String lastSQL = cMock.getLastSQL();
		String expectedResult = "INSERT INTO trafficlights (direction, color) VALUES ('a', 'b')";
        Assert.assertEquals(lastSQL, expectedResult);
    }
}