package com.aws.businesscomponents;

import java.io.File;
import java.nio.file.Paths;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class S3Bucket {
	
	static String bucketName;
	static String fileName;
	final String bucketNameQA= "Bucket1";
	final String bucketNameUAT = "Bucket1";
	final String bucketNameSTG = "Bucket1";

	public static void main(String[] args) {
		
		
		
		

	}
	
	public String getBucketName(String Env) {
		
		switch(Env){
		case "QA" :
			bucketName =bucketNameQA;
			break;
		case "UAT" :
			bucketName =bucketNameUAT;
			break;
		case "STG" :
			bucketName =bucketNameSTG;
			break;
		}
		return bucketName;
		
	}
	
	public String getFile(){
		try{
		File file = new File("Path of the file location");
		File[] files = file.listFiles();
		
		for(File f: files){
			if(f.getName().contains("File Name"));
			fileName=f.getName();
		}
		
		}catch(Exception e){
			e.printStackTrace();
		}
		return fileName;
	}
	
	public void uploadFileintoS3(){
		try{
			bucketName = getBucketName("Pass Environment as argument");
			fileName =getFile();
		}catch(Exception e){
			e.printStackTrace();
		}
		
		String key_name = Paths.get(fileName).getFileName().toString();
		
		//Establish S3 connection to correct region
		final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.EU_WEST_1).build();
		
		try{
			s3.putObject(bucketName,key_name,new File(fileName));
		}catch(AmazonServiceException e){
			System.err.println(e.getErrorMessage());
			
		}
		
		System.out.println("File Upload Done");
		
	}
	

}
