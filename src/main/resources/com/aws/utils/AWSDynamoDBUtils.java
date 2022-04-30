package com.aws.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;

public class AWSDynamoDBUtils {

	private AmazonDynamoDB adb;
	private DynamoDB dynamoDB;
	private String profile;
	private String tableName;

	private HashMap<String, String> nameMap;
	private HashMap<String, String> valueMap;

	public AWSDynamoDBUtils(String profile) {
		this.profile = profile;
	}

	public AmazonDynamoDB getAdb() {
		return adb;
	}

	public void setAdb(AmazonDynamoDB adb) {
		this.adb = adb;
	}

	public DynamoDB getDynamoDB() {
		return dynamoDB;
	}

	public void setDynamoDB(DynamoDB dynamoDB) {
		this.dynamoDB = dynamoDB;
	}

	public String getProfile() {
		return profile;
	}

	public void setProfile(String profile) {
		this.profile = profile;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public HashMap<String, String> getNameMap() {
		return nameMap;
	}

	public void setNameMap(HashMap<String, String> nameMap) {
		this.nameMap = nameMap;
	}

	public HashMap<String, String> getValueMap() {
		return valueMap;
	}

	public void setValueMap(HashMap<String, String> valueMap) {
		this.valueMap = valueMap;
	}

	public void establishConnection() {

		adb = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.EU_WEST_1)
				.withCredentials(new ProfileCredentialsProvider(getProfile())).build();
		dynamoDB = new DynamoDB(adb);

	}

	public List<Map<String, Object>> filterTable(String filterCondition, HashMap<String, String> nameMap2,
			Map<String, Object> valueMap2) throws Exception {
		Table table = dynamoDB.getTable(getTableName());
		List<Map<String, Object>> querymap = new ArrayList<Map<String, Object>>();
		Iterator<Item> it;
		Item item;
		ItemCollection<ScanOutcome> itemCollection = table.scan(filterCondition, nameMap2, valueMap2);
		it = itemCollection.iterator();
		while (it.hasNext()) {
			item = it.next();
			querymap.add(item.asMap());
		}

		if (querymap.size() == 0) {
			throw new Exception("Exception Occured");
		}
		return querymap;
	}

	public Map<String,Object> executeQuery(String queryExpression, Map<String, String> nameMap2, Map<String, Object> valueMap2) {

		Table table = dynamoDB.getTable(getTableName());
		ItemCollection<QueryOutcome> items = null;
		Map<String, Object> queryMap = null ;
		Item item = null;
		QuerySpec querySpec = new QuerySpec().withKeyConditionExpression(queryExpression).withNameMap(nameMap2)
				.withValueMap(valueMap2);
		items = table.query(querySpec);
		
		Iterator<Item> it = items.iterator();
		while(it.hasNext()){
			item=it.next();
			queryMap = item.asMap();
			
		}

		return queryMap;
	}

}
