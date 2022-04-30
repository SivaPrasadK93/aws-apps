package com.aws.businesscomponents;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aws.utils.AWSDynamoDBUtils;

public class DataExtractionDynamoDBTable {
	
	//Filter Using Scan
	public List<Map<String, Object>> getDatafromTable() throws Exception{

		List<Map<String, Object>> filterResults =null;
		AWSDynamoDBUtils ddbutils = new AWSDynamoDBUtils("Provide AWS Profile");
		ddbutils.establishConnection();
		ddbutils.setTableName("Provide TableName");
		String filterCondition = "#ID = :i";
		HashMap<String, String> nameMap = new HashMap<>();
		nameMap.put("#ID", "COLUMN_NAME in DB");
		Map<String, Object> valueMap = new HashMap<>();
		valueMap.put(":i", "COLUMN_VALUE to filter in DB");
		filterResults = ddbutils.filterTable(filterCondition,nameMap,valueMap);
		
		return filterResults;
		
		
	}
	
	//Querying table
	public Map<String, Object> getDatafromTableByQuery(String queryExpression, HashMap<String,String> nameMap, HashMap<String, Object> valueMap){
		AWSDynamoDBUtils ddbutils = new AWSDynamoDBUtils("Provide AWS Profile");
		ddbutils.establishConnection();
		ddbutils.setTableName("Provide TableName");
		return  ddbutils.executeQuery(queryExpression, nameMap, valueMap);
			
	}
	                               

}
