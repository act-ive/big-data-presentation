package trials.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.testng.annotations.Test;

public class CassandraStage {
  @Test
  public void readFromCassandra() {

    Cluster cluster = null;
    try {
      cluster = Cluster.builder()
        .addContactPoint("192.168.1.11")
        .addContactPoints("192.168.1.7", "192.168.1.8")
        .withPort(9042)
        .build();
      Session session = cluster.connect();

      ResultSet rs = session.execute("select group_name from insights_report.heartbeat_stats");
      for (Row groupName : rs.all()) {
        System.out.println("Groups: " + groupName);
      }
    } finally {
      if (cluster != null) cluster.close();
    }

  }

  @Test
  public void insertIntoCassandra() {

    Cluster cluster = null;
    try {
      cluster = Cluster.builder()
        .addContactPoint("192.168.1.11")
        .addContactPoints("192.168.1.7", "192.168.1.8")
        .withPort(9042)
        .build();
      Session session = cluster.connect();

      ResultSet rs = session.execute("INSERT INTO insights_report.heartbeat_stats " +
        "JSON '{\"export_service_id\":4, \"group_id\":\"6\", \"group_name\":\"Housesmart\", \"upload_count\":552}'; "
      );
    } finally {
      if (cluster != null) cluster.close();
    }

  }
}
