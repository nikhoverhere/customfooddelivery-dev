package com.nikh.sftp.setup;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
//import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;


public class DynamoPutRecords {


	public void putRecords(int load_date, int modified_datetime, int processed_datetime, String file) {
		
		String access_key = "";
		String secret_key = "";
		System.setProperty("aws.accessKeyId", access_key);
		System.setProperty("aws.secretKey", secret_key);

		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
				.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
						"https://dynamodb.##-######-#.amazonaws.com", "##-######-#"))
				.withCredentials(new DefaultAWSCredentialsProviderChain()).build();

		DynamoDB dynamoDB = new DynamoDB(client);
		System.out.println("Dynamo Client Established");
		Table table = dynamoDB.getTable("FILE_STATUS");
		System.out.println("Table Fetched");
        Item item = new Item()
        	    .withNumber("file_load_date", load_date)
        		.withNumber("file_modified_datetime", modified_datetime)
        		.withNumber("file_processed_datetime", processed_datetime)
        		.withString("filename", file);
        System.out.println("Item Prepared to Insert");
       
        table.putItem(item);
        System.out.println("Inserted");
	}
	
	public String getBucket() {
		return new SimpleDateFormat("yyyyMMdd").format(new Date());
		
	}

}
