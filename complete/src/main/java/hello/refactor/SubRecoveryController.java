package hello.refactor;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import hello.PGConnection;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import org.springframework.util.DigestUtils;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.net.ConnectException;


//@Component
public class SubRecoveryController implements ApplicationRunner {

    @Override
    public void run(ApplicationArguments args) throws Exception {
        System.out.println("this is the SubRecoveryController");
        this.subrecover();
    }

    public void subrecover() throws IOException, SQLException, NoSuchAlgorithmException, InterruptedException {

        Thread.sleep(30000);

        BufferedReader br = new BufferedReader(new FileReader("fast.config"));
        String str = "";
        StringBuilder sb = new StringBuilder();
        while ((str = br.readLine()) != null) {
            str=new String(str.getBytes(),"UTF-8");//解决中文乱码问题
            sb.append(str);
        }
        String config = sb.toString();
        br.close();

        JSONObject jsonObject = JSONObject.parseObject(config);
        String autovisURL = jsonObject.getString("autovisURL");
        String innerUrl = jsonObject.getString("innerURL");
        String innerUserName = jsonObject.getString("innerusername");
        String innerPassword = jsonObject.getString("innerpassword");

        URL url = new URL(autovisURL);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        /**设置URLConnection的参数和普通的请求属性****start***/

        conn.setRequestProperty("accept", "*/*");
        conn.setRequestProperty("connection", "Keep-Alive");
        conn.setRequestProperty("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)");

        /**设置URLConnection的参数和普通的请求属性****end***/

        //设置是否向httpUrlConnection输出，设置是否从httpUrlConnection读入，此外发送post请求必须设置这两个
        //最常用的Http请求无非是get和post，get请求可以获取静态页面，也可以把参数放在URL字串后面，传递给servlet，
        //post与get的 不同之处在于post的参数不是放在URL字串里面，而是放在http请求的正文内。
        conn.setDoOutput(true);
        conn.setDoInput(true);

        conn.setRequestMethod("GET");//GET和POST必须全大写
        /**GET方法请求*****start*/
        /**
         * 如果只是发送GET方式请求，使用connet方法建立和远程资源之间的实际连接即可；
         * 如果发送POST方式的请求，需要获取URLConnection实例对应的输出流来发送请求参数。
         * */
        try {
            conn.connect();
        }
        catch (ConnectException e){
            System.out.println("sub recovery connection failed");
            return;
        }

        /**GET方法请求*****end*/

        //获取URLConnection对象对应的输入流
        InputStream is = conn.getInputStream();
        //构造一个字符流缓存
        br = new BufferedReader(new InputStreamReader(is));
        str = "";
        sb = new StringBuilder();
        while ((str = br.readLine()) != null) {
            str=new String(str.getBytes(),"UTF-8");//解决中文乱码问题
            sb.append(str);
        }
        System.out.println(sb.toString());
        //关闭流
        is.close();
        //断开连接，最好写上，disconnect是在底层tcp socket链接空闲时才切断。如果正在被其他线程使用就不切断。
        //固定多线程的话，如果不disconnect，链接会增多，直到收发不出信息。写上disconnect后正常一些。
        conn.disconnect();
        System.out.println("完整结束");

        String salt = "&%12345***&&%%$$#@1";

        JSONArray jsonArray = JSONArray.parseArray(sb.toString());
        if(jsonArray == null) return;
        for(int i = 0; i < jsonArray.size(); i++){
            JSONObject param = jsonArray.getJSONObject(i);
            System.out.println(param);
            String path = param.get("url").toString();
            String database = param.get("database").toString();
            String timeseries = param.get("timeseries").toString();
            String columns = param.get("columns").toString();
            String username = param.get("username").toString();
            String password = param.get("password").toString();
            String starttime = param.get("starttime").toString();
            String sample = param.get("sample").toString();
            Double percent = param.getDouble("percent");
            Double alpha = param.getDouble("alpha");
            String dbtype = param.get("dbtype").toString();
            Integer ratio = param.containsKey("ratio") ? param.getInteger("ratio") : 20;
            String query = param.containsKey("query") ? param.get("query").toString() : null;
            String ip = param.containsKey("ip") ? param.get("ip").toString() : null;
            String port = param.containsKey("port") ? param.get("port").toString() : null;


            System.out.println(path);
            System.out.println(database);
            System.out.println(timeseries);
            System.out.println(columns);
            System.out.println(dbtype);

            // iotdb is . tsdb is _
            String L0tableName = "L0" + "_M" + DigestUtils.md5DigestAsHex(String.format("%s,%s,%s,%s,%s", path, database, timeseries, columns, salt).getBytes()).substring(0, 8);
            ;
            String L1tableName = "L1" + "_M" + DigestUtils.md5DigestAsHex(String.format("%s,%s,%s,%s,%s", path, database, L0tableName, columns, salt).getBytes()).substring(0, 8);
            String L2tableName = "L2" + "_M" + DigestUtils.md5DigestAsHex(String.format("%s,%s,%s,%s,%s", path, database, L1tableName, columns, salt).getBytes()).substring(0, 8);

            String[] tables = new String[3];
            tables[0] = L2tableName;
            tables[1] = L1tableName;
            tables[2] = L0tableName;

            String newStartTime = starttime;


            PGConnection pgtool = new PGConnection(innerUrl+database.replace(".", "_").toLowerCase(), innerUserName, innerPassword);
            Connection connection = pgtool.getConn();
            if (connection == null) {
                System.out.println("get connection defeat");
                return;
            }

            for(int j = 0; j <= 2; j++){
                String sql = query != null ? query :
                    "SELECT " + "max(time) " +
                    " FROM " + tables[j];
                System.out.println(sql);
                ResultSet resultSet = pgtool.query(connection, sql);
                if (resultSet != null) {
                    if(resultSet.next()){
                        newStartTime = resultSet.getString(1);
                        if(newStartTime != null){
                            newStartTime = newStartTime.substring(0,19);
                        }
                        else {
                            newStartTime = starttime;
                        }
                        System.out.println(j);
                        break;
                    }

                }
            }

            System.out.println(newStartTime);

            connection.close();

            new LayerController().subscribe(path, username, password, database, timeseries, columns, newStartTime, sample, percent, alpha, ratio, ip, port, dbtype, 100000L);

        }
    }
}
