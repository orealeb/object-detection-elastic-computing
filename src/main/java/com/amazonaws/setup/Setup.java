package com.amazonaws.setup;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.ec2.model.StopInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.util.EC2MetadataUtils;
import com.amazonaws.util.IOUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class Setup {


	public static void main(String[] args) throws IOException, InterruptedException {
		// Set required parameters
		final AmazonEC2 ec2 = AmazonEC2ClientBuilder.defaultClient();

		try {
			//start instance before running scp commands
			startAllInstances();
			System.out.println("Instances started");
			//loop through each instance and execute transfer
			//run all scp commands to transfer aws credential files
			//scp .aws/credentials pi@169.254.163.130:~/.aws/credentials
			
			boolean done = false;
			Thread.sleep(2000);
			DescribeInstancesRequest request = new DescribeInstancesRequest();
			while(!done) {
			    DescribeInstancesResult response = ec2.describeInstances(request);

			    for(Reservation reservation : response.getReservations()) {
			        for(Instance instance : reservation.getInstances()) {
		        		System.out.println(instance.getInstanceId() + " " + instance.getState().getName() + " " + instance.getPublicDnsName());
			        	if(!instance.getPublicDnsName().trim().equals("")) {
				        	//-i C:\Users\Ore\Downloads\AWSCSE546.pem
							String destinationServer = instance.getPublicDnsName(); //instance public DNS
							String[] bashScript = new String[] { "scp", "-o", "StrictHostKeyChecking=no", "-i", "C:\\Users\\Ore\\Downloads\\AWSCSE546.pem", "C:\\Users\\Ore\\.aws\\credentials", "ubuntu@"+destinationServer + ":~/.aws/credentials" };
							System.out.println("Running Command $ " + Arrays.toString(bashScript));
							executeCommand(bashScript);
							
							//transfer jar file
							String[] bashScript2 = new String[] { "scp", "-o", "StrictHostKeyChecking=no", "-i", "C:\\Users\\Ore\\Downloads\\AWSCSE546.pem", "C:\\Users\\Ore\\eclipse-workspace\\Upload\\EC2Controller.jar", "ubuntu@"+destinationServer + ":/home/ubuntu/darknet/EC2Controller.jar" };
							//System.out.println("Running Command $ " + Arrays.toString(bashScript2));
							//executeCommand(bashScript2);
							
							//transfer start up script
							String[] bashScript3 = new String[] { "scp", "-o", "StrictHostKeyChecking=no", "-i", "C:\\Users\\Ore\\Downloads\\AWSCSE546.pem", "C:\\Users\\Ore\\eclipse-workspace\\Upload\\startup.sh", "ubuntu@"+destinationServer + ":/home/ubuntu/darknet/startup.sh" };
							//System.out.println("Running Command $ " + Arrays.toString(bashScript3));
							//executeCommand(bashScript3);
			        	}
			        }
			    }

			    request.setNextToken(response.getNextToken());

			    if(response.getNextToken() == null) {
			        done = true;
			    }
			}
			
			Thread.sleep(2000);
			//stop all instance after transfering credential files
			//stopAllInstances();
			System.out.println("Instances stopped");
			//printUsage();
			
		} catch (AmazonServiceException e) {
			// The call was transmitted successfully, but Amazon S3 couldn't process
			// it, so it returned an error response.
			e.printStackTrace();
		} catch (SdkClientException e) {
			// Amazon S3 couldn't be contacted for a response, or the client
			// couldn't parse the response from Amazon S3.
			e.printStackTrace();
		}
	}

	private static void printUsage() {
		  OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
		  for (Method method : operatingSystemMXBean.getClass().getDeclaredMethods()) {
		    method.setAccessible(true);
		    if (method.getName().startsWith("get")
		        && Modifier.isPublic(method.getModifiers())) {
		            Object value;
		        try {
		            value = method.invoke(operatingSystemMXBean);
		        } catch (Exception e) {
		            value = e;
		        } // try
		        System.out.println(method.getName() + " = " + value);
		    } // if
		  } // for
		}
	


	private static void startAllInstances() {		
		final AmazonEC2 ec2 = AmazonEC2ClientBuilder.defaultClient();
		boolean done = false;

		DescribeInstancesRequest describeRequest = new DescribeInstancesRequest();
		while(!done) {
		    DescribeInstancesResult response  = ec2.describeInstances(describeRequest);

		    for(Reservation reservation : response.getReservations()) {
		        for(Instance instance : reservation.getInstances()) {
		        	if(instance.getState().getName().equals("stopped")) {
			        	// start instance
			        	String instance_id = instance.getInstanceId();
			    		StartInstancesRequest startRequest = new StartInstancesRequest().withInstanceIds(instance_id);
	
			        	ec2.startInstances(startRequest);
		        	}
		        }
		    }

		    describeRequest.setNextToken(response.getNextToken());

		    if(response.getNextToken() == null) {
		        done = true;
		    }
		}
	}
	
	private static void stopAllInstances() {		
		final AmazonEC2 ec2 = AmazonEC2ClientBuilder.defaultClient();
		boolean done = false;

		DescribeInstancesRequest describeRequest = new DescribeInstancesRequest();
		while(!done) {
		    DescribeInstancesResult response  = ec2.describeInstances(describeRequest);

		    for(Reservation reservation : response.getReservations()) {
		        for(Instance instance : reservation.getInstances()) {
		        	if(instance.getState().getName().equals("running")) {
			        	// start instance
			        	String instance_id = instance.getInstanceId();
			    		StopInstancesRequest stopRequest = new StopInstancesRequest().withInstanceIds(instance_id);
	
			        	ec2.stopInstances(stopRequest);
		        	}
		        }
		    }

		    describeRequest.setNextToken(response.getNextToken());

		    if(response.getNextToken() == null) {
		        done = true;
		    }
		}
	}

	public static String executeCommand(String[] command) {
		String line;
		String resultat = "";
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
				resultat = line; // final prediction
				System.out.println(line);
			}
		} catch (IOException e) {
			System.out.println("Exception = " + e.getMessage());
		}
		return resultat;
	}
}
