package hello;

import java.sql.*;

public class PGConnection {
    private String url = "jdbc:postgresql://192.168.10.172:5432/tutorial";
    private String username = "postgres";
    private String password = "postgres";
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
            // TODO Auto-generated catch block
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

    public boolean queryUpdate(Connection conn, String sql) {
        PreparedStatement pStatement = null;
        int rs = 0;
        try {
            pStatement = conn.prepareStatement(sql);
            rs = pStatement.executeUpdate();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        if (rs > 0) {
            return true;
        }
        return false;
    }

    public static void main(String[] args) throws SQLException {
        String url = "jdbc:postgresql://192.168.10.172:5432/tutorial";
        String username = "postgres";
        String password = "postgres";
        PGConnection pgtool = new PGConnection(url, username, password);
        Connection myconn = pgtool.getConn();
        ResultSet rs = pgtool.query(myconn, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'");
        while(rs.next()){
            System.out.println(rs.getString(1));
            myconn.close();
        }
    }

}
