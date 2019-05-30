package com.infosys.juniper.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import com.infosys.juniper.constant.MetadataDBConstants;

public class ConnectionUtils {
	static Connection connection = null;
	
	
	 public static Connection connectToOracle(String ipPortDb,String user,String password) throws SQLException {

		 
		 try {
				
				if (connection == null || connection.isClosed()) {
					Class.forName(MetadataDBConstants.SYBASE_DRIVER);
					////"jdbc:sybase:Tds:${hostname}:{port}/${databasename}"
					String jdbc = "jdbc:sybase:Tds:"+ipPortDb;
					//String jdbc = "jdbc:oracle:thin:"+ipPortDb;
					connection = DriverManager.getConnection(jdbc, user, password);
				}
		 } catch (Exception e) {
			   e.printStackTrace();
				throw new SQLException("Exception occured while connecting to source oracle database");
			}
	 
	
	 	System.out.println("connection succeeded");
		return connection;
	 
 }	 

}
