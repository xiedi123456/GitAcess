import java.sql.Connection;  
import java.sql.DriverManager;  
import java.sql.ResultSet;  
import java.sql.SQLException;  
import java.sql.Statement;  
import org.apache.log4j.Logger;  
public class HiveJdbcClient {  
    //private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";  
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";  
    private static String url = "jdbc:hive2://master:10000/default";  
    private static String user = "root";  
    private static String password = "123456";  
    private static String sql;  
    private static ResultSet res;  
    private static final Logger log = Logger.getLogger(HiveJdbcClient.class);  
    public static void main(String[] args) {  
        try {  
            Class.forName(driverName);  
            Connection conn = DriverManager.getConnection(url, user, password);  
            // 默认使用端口10000, <span style="font-family: Arial, Helvetica, sans-serif;">使用数据库qsjs2016，用户名为test，密码为123</span>  
            // Connection conn = DriverManager.getConnection(  
            // "jdbc:hive://192.168.69.69:10000/qsjs2016", "test",  
            // "123");  
            Statement stmt = conn.createStatement();  
  
            sql = "select * from ljh_emp";//显示全部表  
            System.out.println("Running:" + sql);  
            res = stmt.executeQuery(sql);  
            System.out.println("执行结果:");  
            while (res.next()) {  
                System.out.println(res.getString(1));  
            }  
  
            conn.close();  
            conn = null;  
        } catch (ClassNotFoundException e) {  
            e.printStackTrace();  
            log.error(driverName + " not found!", e);  
            System.exit(1);  
        } catch (SQLException e) {  
            e.printStackTrace();  
            log.error("Connection error!", e);  
            System.exit(1);  
        }  
  
    }  
}  