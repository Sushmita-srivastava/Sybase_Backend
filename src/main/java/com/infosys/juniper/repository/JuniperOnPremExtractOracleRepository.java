package com.infosys.juniper.repository;

import java.sql.Connection;

import com.infosys.juniper.dto.ConnectionDto;
import com.infosys.juniper.dto.TempTableInfoDto;

public interface JuniperOnPremExtractOracleRepository {

	
	public String testOracleConnection(ConnectionDto connDto);
	public String addOracleConnectionDetails(ConnectionDto connDto);
	public String updateOracleConnectionDetails(ConnectionDto connDto);
	public String editTempTableDetails(String feed_id, String src_type);
	public String addTempTableDetails(TempTableInfoDto tempTableInfoDto);
	public String metaDataValidate(String feed_sequence, String project_id);
	public String updateAfterMetadataValidate(String feed_sequence, String project_id);
	public String deleteTempTableMetadata(String feed_sequence, String project_id);
	
	
	
	
}
