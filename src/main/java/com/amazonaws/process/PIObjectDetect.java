package com.amazonaws.process;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.Collectors;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

public class PIObjectDetect  {

	//main function runs the application
	public static void main(String[] args) throws IOException {

		String filename = args[0];
		
		// Set required parameters
		Regions clientRegion = Regions.US_EAST_1;
		String outputBucketName = "cse546output";
		
		// Create S3 Client
		AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(clientRegion).build();	
		
		System.out.println("Running Thread...");
		upload(s3Client, outputBucketName, filename, performObjectDetection(filename)) ; //uploads prediction to output bucket
		
	}
	
	//function Uploads to outputBucket
	private static void upload(AmazonS3 s3Client, String bucketName, String keyName, String prediction) {
		// create meta-data for your folder and set content-length to 0
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(0);
		
		// create empty content
		InputStream emptyContent = new ByteArrayInputStream(new byte[0]);
		
		PutObjectRequest request = new PutObjectRequest(bucketName, "{ " + keyName + ", " + prediction + " }", emptyContent, metadata);
		s3Client.putObject(request);
		System.out.println("Uploaded Result");
	}
	
	//function runs darknet and performs object detection 
	private static String performObjectDetection(String keyName) {
		String[] bashScript = new String[] { "./darknet", "detector", "demo", "cfg/coco.data", "cfg/yolov3-tiny.cfg",
				"yolov3-tiny.weights", "videos/"+keyName };

		System.out.println("Running Command $ " + Arrays.toString(bashScript));
		String prediction = executeCommand(bashScript);
		writeStatus("finished");
		System.out.println("Done running");
		
		//delete file
		final File file = new File("/home/pi/darknet/videos/"+keyName);
		file.delete();
		
		return prediction;
	}
	
	public static void writeStatus(String status) {
		try {
		      FileWriter myWriter = new FileWriter("statusfile.txt");
		      myWriter.write(status);
		      myWriter.close();
		      System.out.println("Successfully wrote to the file.");
		    } catch (IOException e) {
		      System.out.println("An error occurred.");
		      e.printStackTrace();
		    }
	}
	
	//function executes command on the terminal 
		public static String executeCommand(String[] command) {
			String line;
			//results in hashmap
			HashMap<String, String> map 
	        = new HashMap<>(); 
			try {
				ProcessBuilder builder;

				builder = new ProcessBuilder(command);

				builder.redirectErrorStream(true);
				Process p = builder.start();
				BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
			
				
				while (true) {
					line = r.readLine();
					if (line == null) {
						break;
					}
					if(line.contains("%") && line.split(":").length > 1) {
						String pred = line.split(":")[0];
						String accuracy = line.split(":")[1];
						map.put(pred, accuracy); 
					}
					System.out.println(line);
				}
			} catch (IOException e) {
				System.out.println("Exception = " + e.getMessage());
			}
			String result = map.entrySet().stream()
					   .map(e -> encode(e.getKey()))
					   .collect(Collectors.joining(", "));
			return result;
		}
		
		public static String encode(String s){
		    try{
		        return java.net.URLEncoder.encode(s, "UTF-8");
		    } catch(UnsupportedEncodingException e){
		        throw new IllegalStateException(e);
		    }
		}
	
}
