package com.amazonaws.process;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;


import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.util.EC2MetadataUtils;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.Datapoint;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsRequest;
import com.amazonaws.services.cloudwatch.model.GetMetricStatisticsResult;




public class MasterController {

	//main function runs the application
	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
		
		// Set required parameters
		
		Regions clientRegion = Regions.US_EAST_1;
		String inputBucketName = args[0]; //S3 Input Bucket Name";
		String myQueueUrl = args[1]; // SQS URL  
		String folderName =  args[2];  // Name of folder in Master Node "/home/ubuntu/darknet/videos"
		
		//final AmazonCloudWatch cw = AmazonCloudWatchClientBuilder.defaultClient();

		// Create EC2, SQS and S3 Client
		final AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard().withRegion(clientRegion).build();
		final AmazonSQS sqs = AmazonSQSClientBuilder.standard().withRegion(clientRegion).build();
		final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(clientRegion).build();
		
		// Folder containing videos
		final File folder = new File(folderName);
		
		// Get all EC2 instances
		ArrayList<Instance> all_instances = getInstanceIds(ec2); // returns all instances created

		int index = 0;
		
		while(true) {
			System.out.println("Checking for file..");
			//	TODO: Get utilization for each instance and add to priority queue, with least utilized at top of the queue
			//PriorityQueue <Instance> priorityQueue = new PriorityQueue <Instance>();

			for (final File fileEntry : folder.listFiles()) {
				System.out.println("Found file " + fileEntry.getName());			
	
				String filename = fileEntry.getName(); //The name of the video file

				System.out.println("Uploading file to input bucket");
				uploadInput(s3Client, inputBucketName, filename, fileEntry); //uploads file to input bucket
			    
				
				if(index == all_instances.size()) { // iterator to wrap around instance list
					index = 0;
				}
				
				// Get the state of the instance
				Instance instance = all_instances.get(index);
				String state = instance.getState().getName();
				System.out.println("Instance is " + state);
				
				//three states: running, stopping, stopped.
				
				// If it is running then assign it task and send message to SQS
				if(state.equals("running")) {
					System.out.println("Sending message...");
					sendMessage(sqs, filename, instance.getInstanceId(), myQueueUrl); // sends message to sqs
					//delete file
					fileEntry.delete();
					System.out.println("message sent.");
				}
				else if (state.equals("stopped")) {
					// If it is not running then start up the instance with its instance id
					startInstance(ec2, instance.getInstanceId());
					System.out.println("Sending message...");
					sendMessage(sqs, filename, instance.getInstanceId(), myQueueUrl); // sends message to sqs
					//delete file
					fileEntry.delete();
					System.out.println("message sent.");
				}
				index += 1;
			}
		}
		
	}// main

	
 private double getInstanceAverageLoad(AmazonCloudWatchClient cloudWatchClient, String instanceId) {

     long offsetInMilliseconds = 1000 * 60 * 60;
     GetMetricStatisticsRequest request = new GetMetricStatisticsRequest()
             .withStartTime(new Date(new Date().getTime() - offsetInMilliseconds))
             .withNamespace("AWS/EC2")
             .withPeriod(60 * 60)
             .withDimensions(new Dimension().withName("InstanceId").withValue(instanceId))
             .withMetricName("CPUUtilization")
             .withStatistics("Average", "Maximum")
             .withEndTime(new Date());
     GetMetricStatisticsResult getMetricStatisticsResult = cloudWatchClient.getMetricStatistics(request);

     double avgCPUUtilization = 0;
     List dataPoint = getMetricStatisticsResult.getDatapoints();
     for (Object aDataPoint : dataPoint) {
         Datapoint dp = (Datapoint) aDataPoint;
         avgCPUUtilization = dp.getAverage();
     }

     return avgCPUUtilization;
 }
	
	//function uploads to inputBucket 
	private static void uploadInput(AmazonS3 s3Client, String bucketName, String keyName, File inputFile){
		PutObjectRequest request = new PutObjectRequest(bucketName, keyName, inputFile);
		s3Client.putObject(request);
		System.out.println("Uploaded Video File");
		
	}
	

	
	//function returns an array of instance Ids
	private static ArrayList<Instance> getInstanceIds(AmazonEC2 ec2){
		ArrayList<Instance> arrInstances = new ArrayList<Instance>(); // array holds all the instances 
		boolean done = false;
		DescribeInstancesRequest request = new DescribeInstancesRequest();
		
		while(!done) {
		    DescribeInstancesResult response = ec2.describeInstances(request);

		    for(Reservation reservation : response.getReservations()) {
		        for(Instance instance : reservation.getInstances()) {
		        	// If message instance id is this EC2 instance, then download and process video.
					String currInstanceId = EC2MetadataUtils.getInstanceId();
					if (!instance.getInstanceId().equals(currInstanceId)) {
						arrInstances.add(instance); 
					}
		        	}
		        }

		    request.setNextToken(response.getNextToken());

		    if(response.getNextToken() == null) {
		        done = true;
		    }
		}
		return arrInstances;
	}
	
	//function starts up an instance
	private static void startInstance(AmazonEC2 ec2, String Id){
        StartInstancesRequest startInstancesRequest = new StartInstancesRequest().withInstanceIds(Id);
        ec2.startInstances(startInstancesRequest);
	}
		
	
	//function sends a message to the sqs
	private static void sendMessage(AmazonSQS sqs, String filename, String id, String queueUrl) {
		String message = filename + ":" + id;
		SendMessageRequest send_msg_request = new SendMessageRequest()
		        .withQueueUrl(queueUrl)
		        .withMessageBody(message)
		        .withDelaySeconds(0);
		sqs.sendMessage(send_msg_request);
		
	}
		
}