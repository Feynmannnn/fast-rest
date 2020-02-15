package hello;

import org.postgresql.jdbc.PgConnection;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class RealtimeCompasion {
    public static void main(String[] args) throws SQLException, InterruptedException {

        String innerUrl = "jdbc:postgresql://192.168.10.172:5432/";
        String innerUserName = "postgres";
        String innerPassword = "1111aaaa";

        Connection iotdbConnection = IoTDBConnection.getConnection(
                "jdbc:iotdb://101.6.15.211:6667/",
                "root",
                "root"
        );

        String pgDatabse = "root_mxw4";
        String L0tableName = "l0_mef1aedc8";
        String L1tableName = "l1_m51d0072a";
        String L2tableName = "";

        PGConnection pgtool = new PGConnection(
                innerUrl+pgDatabse,
                innerUserName,
                innerPassword
        );
        Connection pgConnection = pgtool.getConn();

        String iotdbDatabse = "root.mxw4";
        String timeseries = "s2";
        String column = "d3";
        String iotdbsql = String.format("select max_time(%s) from %s.%s", column, iotdbDatabse, timeseries);
        System.out.println(iotdbsql);


        String L0sql = String.format("select max(time) from %s", L0tableName);
        String L1sql = String.format("select max(time) from %s", L1tableName);

        Long L0latencySum = 0L;
        Long L1latencySum = 0L;
        Long count = 0L;

        while (true){
            Timestamp iotdbLatestTime;
            Statement statement = iotdbConnection.createStatement();
            ResultSet resultSet = statement.executeQuery(iotdbsql);
            resultSet.next();
            iotdbLatestTime = resultSet.getTimestamp(2);

            Timestamp L0LatestTime;
            resultSet = pgtool.query(pgConnection, L0sql);
            resultSet.next();
            L0LatestTime = resultSet.getTimestamp(1);

            Timestamp L1LatestTime;
            resultSet = pgtool.query(pgConnection, L1sql);
            resultSet.next();
            L1LatestTime = resultSet.getTimestamp(1);



            count++;
            L0latencySum += (iotdbLatestTime.getTime() - L0LatestTime.getTime());
            L1latencySum += (iotdbLatestTime.getTime() - L1LatestTime.getTime());

            System.out.println(String.format("%s,%s,%s,%s",
                    (iotdbLatestTime.getTime() - L0LatestTime.getTime()),
                    (iotdbLatestTime.getTime() - L1LatestTime.getTime()),
                    L0latencySum / count,
                    L1latencySum / count
            ));

            Thread.sleep(1000);

        }

    }
}
