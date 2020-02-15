package hello;

import org.apache.iotdb.jdbc.IoTDBSQLException;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;

public class DataImportController {

    public void dataImport(
            String url,
            String username,
            String password,
            String database,
            String timeseires,
            String column,
            String datatype,
            String encoding,
            Long startTime,
            Long interval,
            Long patchSize,
            String dbType
    ) throws SQLException {
        long time = new Date(2020 - 1900, Calendar.JANUARY, 1).getTime();

        // use iotdb as example
        Connection connection = IoTDBConnection.getConnection(url, username, password);
        if (connection == null) {
            System.out.println("get connection defeat");
            return;
        }

        // create database if not exist
        Statement statement = connection.createStatement();
        try {
            String createDatabaseSql = String.format("SET STORAGE GROUP TO %s", database);
            System.out.println(createDatabaseSql);
            statement.execute(createDatabaseSql);
        }catch (IoTDBSQLException e){
            System.out.println(e.getMessage());
        }

        // create table if not exist
        try {
            String createTableSql = String.format("CREATE TIMESERIES %s.%s.%s WITH DATATYPE=%s, ENCODING=%s;", database, timeseires, column, datatype, encoding);
            System.out.println(createTableSql);
            statement.execute(createTableSql);
        }catch (IoTDBSQLException e){
            System.out.println(e.getMessage());
        }

        // patch inserts
        int round = 1000000;
        Random r = new Random();

        long timer = System.currentTimeMillis();
        while (round > 0){
            String insertSql = "insert into %s.%s(timestamp, %s) values(%s, %s);";
            for(int i = 0; i < patchSize; i++){
                // add one insert sql
                statement.addBatch(String.format(insertSql, database, timeseires, column, time, r.nextInt(100)));
                time += 1L;
            }

            // send patch insert sql
            statement.executeBatch();
            statement.clearBatch();

            // wait for next round
//            try {
//                Thread.sleep(interval);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }

            round--;
            if(round % 1000 == 0){
                System.out.println("round: " + round + ", usedtime: " + (System.currentTimeMillis() - timer));
                timer = System.currentTimeMillis();
            }
        }

        connection.close();
    }

    public static void main(String[] args) throws SQLException {
        DataImportController dataImportController = new DataImportController();
        String url = "jdbc:iotdb://101.6.15.211:6667/";
        String username = "root";
        String password = "root";
        String database = "root.mxw";
        String timeseires = "s2";
        String column = "d3";
        String datatype = "INT32";
        String encoding = "PLAIN";
        Long startTime = null;
        Long interval = 300L;
        Long patchSize = 1000L;
        String dbType = "iotdb";
        dataImportController.dataImport(
                url, username, password, database, timeseires, column, datatype, encoding, startTime, interval, patchSize, dbType
        );
    }

}