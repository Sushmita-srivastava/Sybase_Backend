package com.infosys.juniper.util;

import java.sql.Connection;

import java.sql.DriverManager;
import java.sql.SQLException;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.infosys.juniper.constant.MetadataDBConstants;

@Component
public class MetadataDBConnectionUtils {
	
	
	
	private static String oracle_ip_port;
	@Value("${oracle.ip.port.sid}")
	public void setSid(String value) {
		this.oracle_ip_port=value;
	}
	
	
	private static String oracle_jdbc_url;
	@Value("${oracle.jdbc.url}")
	public void setJdbcUrl(String value) {
		this.oracle_jdbc_url=value;
	}
	
	private static String oracle_user_name;
	@Value("${oracle.user.name}")
	public void setDBName(String value) {
		this.oracle_user_name=value;
	}
	
	private static String oracle_decrypt_pwd;
	@Value("${oracle.encrypt.pwd}")
	public void setPassword(String value) {
		this.oracle_decrypt_pwd=value;
	}
	
	private static String master_key_path;
	@Value("${master.key.path}")
	public void setMasterKeyPath(String value) {
		this.master_key_path=value;
	}
	
	public static Connection getOracleConnection() throws ClassNotFoundException, SQLException,Exception {
		
		Class.forName(MetadataDBConstants.ORACLE_DRIVER);
		String content = EncryptUtils.readFile(master_key_path);
		String connectionUrl = oracle_jdbc_url.replaceAll("#orcl_ip", oracle_ip_port);
		System.out.println("connection url is "+connectionUrl);
		byte[] base_pwd=org.apache.commons.codec.binary.Base64.decodeBase64(oracle_decrypt_pwd);
		String orcl_decoded_pwd=EncryptUtils.decryptText(base_pwd, EncryptUtils.decodeKeyFromString(content));
		Connection conn = DriverManager.getConnection(connectionUrl, oracle_user_name, orcl_decoded_pwd);
		return conn;
	}

}
