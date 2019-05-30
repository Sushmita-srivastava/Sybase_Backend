package com.infosys.juniper.repository;

import java.sql.Connection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.infosys.juniper.dao.JuniperOnPremExtractOracleDao;
import com.infosys.juniper.dto.ConnectionDto;
import com.infosys.juniper.dto.TempTableInfoDto;
import com.infosys.juniper.util.ConnectionUtils;
import com.infosys.juniper.util.MetadataDBConnectionUtils;

@Component
public class JuniperOnPremExtractOracleRepositoryImpl implements JuniperOnPremExtractOracleRepository {

	@Autowired
	JuniperOnPremExtractOracleDao Dao;
	@Override
	public String testOracleConnection(ConnectionDto connDto) {
		Connection conn=null;
		try {
			if(connDto.getServiceName()==null||connDto.getServiceName().isEmpty()) {
				//34.98.98.765:5000/pubs2, username, passwd
				conn=ConnectionUtils.connectToOracle(connDto.getHostName()+":"+connDto.getPort()+":"+connDto.getDbName(), connDto.getUserName(), connDto.getPassword());
			}else {
				System.out.println("Connecting to Source Database"+connDto.getHostName() );
				
				conn=ConnectionUtils.connectToOracle(connDto.getHostName()+":"+connDto.getPort()+"/"+connDto.getServiceName(), connDto.getUserName(), connDto.getPassword());
				System.out.println("Connection established");
			}
			conn.close();
			
		}catch(Exception e) {
			
			return "Failed";
			
		}
	
		return "success";
	}
	
	@Override
	public String addOracleConnectionDetails(ConnectionDto connDto) {
		Connection conn=null;
		try {
			conn=MetadataDBConnectionUtils.getOracleConnection();
			
		}catch(Exception e) {
			e.printStackTrace();
			return "Failed to connect to Metadata database";
		}
		
		System.out.println("connection established to Metadata DB");
		return Dao.insertOracleConnectionDetails(conn, connDto);
	}
	
	@Override
	public String updateOracleConnectionDetails(ConnectionDto connDto) {
		Connection conn=null;
		try {
			conn=MetadataDBConnectionUtils.getOracleConnection();
			
		}catch(Exception e) {
			e.printStackTrace();
			return "Failed to connect to Metadata database";
		}
		return Dao.updateOracleConnectionDetails(conn, connDto);
	}

	@Override
	public String editTempTableDetails(String feed_id, String src_type) {
		Connection conn=null;
		try {
			conn=MetadataDBConnectionUtils.getOracleConnection();
			
		}catch(Exception e) {
			e.printStackTrace();
			return "Failed to connect to Metadata database";
		}
		return Dao.deleteTempTableMetadata(conn, feed_id,src_type);
	}

	@Override
	public String addTempTableDetails(TempTableInfoDto tempTableInfoDto) {
		Connection conn=null;
		try {
			conn=MetadataDBConnectionUtils.getOracleConnection();
			
		}catch(Exception e) {
			e.printStackTrace();
			return "Failed to connect to Metadata database";
		}
		return Dao.insertTempTableMetadata(conn, tempTableInfoDto);
	}

	@Override
	public String metaDataValidate(String feed_sequence, String project_id) {
		Connection conn=null;
		try {
			conn=MetadataDBConnectionUtils.getOracleConnection();
			
		}catch(Exception e) {
			e.printStackTrace();
			return "Failed to connect to Metadata database";
		}
		return Dao.metadataValidate(conn,feed_sequence,project_id);
	}

	@Override
	public String updateAfterMetadataValidate(String feed_sequence, String project_id) {
		Connection conn=null;
		try {
			conn=MetadataDBConnectionUtils.getOracleConnection();
			
		}catch(Exception e) {
			e.printStackTrace();
			return "Failed to connect to Metadata database";
		}
		return Dao.updateAfterMetadataValidate(conn, feed_sequence, project_id);
	}

	@Override
	public String deleteTempTableMetadata(String feed_sequence, String project_id) {
		Connection conn=null;
		try {
			conn=MetadataDBConnectionUtils.getOracleConnection();
			
		}catch(Exception e) {
			e.printStackTrace();
			return "Failed to connect to Metadata database";
		}
		return Dao.deleteTempTableMetadata(conn, feed_sequence, project_id);
	}

}
