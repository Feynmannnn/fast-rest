package hello.fast.source;

import java.sql.*;

/**
 * TimescaleDB数据库连接操作类
 */
public class PGConnection {
    private String url;
    private String username;
    private String password;
    private Connection connection = null;

    public PGConnection(String url, String username, String password){
        this.url = url;
        this.username = username;
        this.password = password;
    }

    public Connection getConn() {
        try {
            Class.forName("org.postgresql.Driver").newInstance();
            connection = DriverManager.getConnection(url, username, password);
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public ResultSet query(Connection conn, String sql) {
        PreparedStatement pStatement = null;
        ResultSet rs = null;
        try {
            pStatement = conn.prepareStatement(sql);
            rs = pStatement.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rs;
    }

    public void queryUpdate(Connection conn, String sql) {
        PreparedStatement pStatement = null;
        try {
            pStatement = conn.prepareStatement(sql);
            pStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
