package com.aws.sampleAws;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class getListOftables {

	public static void main(String[] args) throws InterruptedException {
		 AmazonDynamoDBClient client = new AmazonDynamoDBClient().withEndpoint("http://localhost:8000");
		 DynamoDB dynamoDB = new DynamoDB(client);
		
        String tablename = "Movies";
        //createtableFunction(tablename,dynamoDB);
        //getListOftables(dynamoDB);
        getTableDescription(tablename,dynamoDB);
      
	}
	public static void createtableFunction(String tablename,DynamoDB dynamoDB) throws InterruptedException{
		System.out.println("Attempting to create table");
		Table table = dynamoDB.createTable(tablename,
				Arrays.asList(
	                    new KeySchemaElement("year", KeyType.HASH),  //Partition key
	                    new KeySchemaElement("title", KeyType.RANGE)), //Sort key
	                    Arrays.asList(
	                        new AttributeDefinition("year", ScalarAttributeType.N),
	                        new AttributeDefinition("title", ScalarAttributeType.S)), 
	                    new ProvisionedThroughput(10L, 10L)
				);
		table.waitForActive();
	}
	public static void getListOftables(DynamoDB dynamoDB){
		TableCollection<ListTablesResult> listoftables = dynamoDB.listTables();
	 Iterator<Table> itr = listoftables.iterator();
	 while(itr.hasNext()){
		 Table table = itr.next();
		 System.out.println(table.getTableName());
	 }
	 
	}
	public static void getTableDescription(String tablename,DynamoDB dynamoDB){
		TableDescription tabledescription = dynamoDB.getTable(tablename).describe();
		//System.out.println(tabledescription);
		//System.out.println(tabledescription.getTableName());
		//System.out.println(tabledescription.getTableStatus());
		//System.out.println(tabledescription.getAttributeDefinitions());
		//System.out.println(tabledescription.getProvisionedThroughput().getReadCapacityUnits());
		System.out.println(tabledescription.getProvisionedThroughput().getWriteCapacityUnits());
	 }
	public static void loadData(DynamoDB dynamoDB,String filename) throws JsonParseException, IOException{
		JsonParser parser = new JsonFactory().createParser(new File("moviedata.json"));
		JsonNode rootNode = new ObjectMapper().readTree(parser);
		Iterator<JsonNode> itr = rootNode.iterator();
		ObjectNode currentNode;
		while(itr.hasNext()){
			
		}
	}

}
