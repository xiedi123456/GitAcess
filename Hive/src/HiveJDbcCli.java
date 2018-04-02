import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
public class HiveJDbcCli {
	 private static String driverName = "org.apache.hive.jdbc.HiveDriver";  
	    private static String url = "jdbc:hive2://master:10000/default";  
	    private static String user = "root";  
	    private static String password = "123456";    
	    public static Connection getConn() throws Exception{
	    	  Class.forName(driverName);
	    	  Connection connection=DriverManager.getConnection(url,user,password);
	    	  return connection;
	    }
	   public static void  createTable(Statement statement) throws SQLException{
		   statement.execute("CREATE TABLE IF NOT EXISTS "
			         +" tableClass (   key int, value String)"
			         +" ROW FORMAT DELIMITED"
			         +" FIELDS TERMINATED BY '\t'"
			         +" LINES TERMINATED BY '\n'"
			         +" STORED AS TEXTFILE");

	   }
 public static void selectData (Statement statement) throws SQLException{
	 String sql="select * from tableClass";
	 System.out.println("running"+sql);
	 ResultSet resultSet=statement.executeQuery(sql);
	 while(resultSet.next()){
		 System.out.println(resultSet.getInt(1)+"----"+resultSet.getString(2));
	 }
 }
	   public static void insertData(Statement statement ) throws SQLException{
		      statement.execute("LOAD DATA LOCAL INPATH '/usr/hive/HiveJDbcCli'" + "OVERWRITE INTO TABLE tableClass");
	   }
	    public static void main(String args[]) throws Exception{
	    		 Connection connection=getConn();
	    		  Statement statement=connection.createStatement();
	    //		  createTable(statement );
	    //		  insertData(statement );
	               selectData(statement);
	    
	    }
	    }
