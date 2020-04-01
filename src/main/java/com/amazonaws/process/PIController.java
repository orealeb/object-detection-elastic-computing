package com.amazonaws.process;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
//import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

//import javax.management.MBeanServerConnection;
//import com.sun.management.OperatingSystemMXBean ;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.elasticloadbalancing.model.InstanceState ;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest ;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.util.IOUtils;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;





public class PIController implements Callable<Integer> {

	private AmazonS3 s3 ;
	private String output = null;
	private String filename = null;
	
	public PIController(AmazonS3 s3Client, String bucketName, String keyName) {
		System.out.println("In constructor, params: " + bucketName + " " +  keyName);
		this.s3 =  s3Client ;
		this.output = bucketName ;
		this.filename = keyName ;
	   
   }
   
   public void run() {
	   System.out.println("Run Thread for" + this.filename);
	   try {
			performObjectDetectionInBackground(this.filename);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	   //upload(this.s3, this.output, this.filename, pred) ;
 catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
   
	@Override
	public Integer call() throws Exception {
		performObjectDetectionInBackground(this.filename);
		return 0;
	}

	//main function runs the application
	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
		// Set required parameters
		Regions clientRegion = Regions.US_EAST_1;
		String inputBucketName = "cse546input";
		String outputBucketName = "cse546output";
		String myQueueUrl = "https://sqs.us-east-1.amazonaws.com/603754723521/Test";
		
		final AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard().withRegion(clientRegion).build();
		// Create SQS and S3 Client
		AmazonSQS sqs = AmazonSQSClientBuilder.standard().withRegion(clientRegion).build();
		AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(clientRegion).build();
		
		
		final File folder = new File("/home/pi/darknet/videos");
		ArrayList<Instance> all_instances = getInstanceIds(ec2) ; // returns all instances created
		
		int index = 0 ;

		int piWorkload = 0;
		HashMap<String, String> map 
        = new HashMap<>(); 
				
		while(true) {
			System.out.println("Checking for file..");
		for (final File fileEntry : folder.listFiles()) {
			System.out.println("Found file " + fileEntry.getName());
			if(index == all_instances.size()) {
				index = 0;
			}
			
			String status = readFile();
			System.out.println("Status of Pi " + status);
			if(status.equals("running")){
				piWorkload = 1;
			}
			else { //finished
				piWorkload = 0;
			}

			String filename = fileEntry.getName(); //The name of the video file
			if(map.containsKey(filename)) {
				if(map.get(filename).equals("processing")) {
					System.out.println("Already processing " + filename + " do not delete") ;
					break;
				}
			}
			else {
				System.out.println("Adding as available to delete file " + filename) ;
				map.put(filename, "available");
			}
			
			System.out.println("Uploading file to input bucket") ;
			uploadInput(s3Client,inputBucketName, filename,fileEntry) ; //uploads file to input bucket
		    
			Double piUtil = PiUtil();
		    piUtil = (double)Math.round(piUtil * 10d) / 10d;
			System.out.println("CPU Util " + piUtil);
			
			if(piUtil > 40.0 && piWorkload == 1){
				System.out.println("CPU Util is is big") ;
				//for(Instance instance: all_instances) {
				System.out.println("Using Instance at index :" + index) ;
				Instance instance = all_instances.get(index) ;
					String state = instance.getState().getName() ;
					System.out.println(state) ;
					
					if(state.equals("running")) {
						System.out.println("sending message...") ;
						sendMessage(sqs, filename, instance.getInstanceId(), myQueueUrl) ; // sends message to sqs
						//delete file
						fileEntry.delete() ;
						System.out.println("message sent.") ;
						index += 1 ;
					}
					else if (state.equals("stopped")) {
						// Starts up an instance with instance id
						startInstance(ec2, instance.getInstanceId()) ;
						System.out.println("sending message...") ;
						sendMessage(sqs, filename, instance.getInstanceId(), myQueueUrl) ; // sends message to sqs
						//delete file
						fileEntry.delete() ;
						System.out.println("message sent.") ;
						index += 1 ;
						}
					else {
						index += 1 ;
					}

					//}
				}else {
					writeStatus("running");
					System.out.println("Starting Thread for " + fileEntry.getName());
					//piWorload = 1;
					ExecutorService service = Executors.newFixedThreadPool(1);
					map.put(filename, "processing");
					service.submit(new PIController(s3Client, outputBucketName, filename));					//upload(s3Client, outputBucketName, filename, performObjectDetection(filename)) ; //uploads prediction to output bucket
					//performObjectDetectionInBackground(filename);
					//Thread pi = new Thread( new PIController(s3Client, outputBucketName, filename)) ;
					//pi.start();
					//delete file
					//fileEntry.delete() ;
		
					}//else
			}// for
		}
		
	}// main

	private static void writeStatus(String status) {
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
	
 private static String readFile() {
     String data = "";
	 try {
	      File myObj = new File("statusfile.txt");
	      Scanner myReader = new Scanner(myObj);
	      while (myReader.hasNextLine()) {
	        data = myReader.nextLine();
	      }
	      myReader.close();
	    } catch (FileNotFoundException e) {
	      System.out.println("An error occurred.");
	      e.printStackTrace();
	    }
	 return data;
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
	
	//function uploads to inputBucket 
	private static void uploadInput(AmazonS3 s3Client, String bucketName, String keyName, File inputFile){
		PutObjectRequest request = new PutObjectRequest(bucketName, keyName, inputFile);
		s3Client.putObject(request);
		System.out.println("Uploaded Video File");
		
	}
	
//	public static double getInstanceUtil(AmazonEC2 ec2, String instanceID) {
//		double instanceUtil = 0.0 ;
//		if(getState(instanceID) == "running") {
//			
//			//
//		}else {
//			switchInstance(ec2, instanceID) ;
//		}
//		
//		return instanceUtil ;
//	}
//	
	
	//function returns the CPU Utilization of the Pi 
	private static double PiUtil() throws IOException {
		
		 double result = 0.0;
		 OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
		  for (Method method : operatingSystemMXBean.getClass().getDeclaredMethods()) {
		    method.setAccessible(true);
		    if (method.getName().startsWith("get") && Modifier.isPublic(method.getModifiers())) {
		            Object value;
		        try {
		            value = method.invoke(operatingSystemMXBean);
		        } catch (Exception e) {
		            value = e;
		        } // try
		        if(method.getName().toString().equals("getSystemCpuLoad")) {
		        	System.out.println(method.getName() + " = " + value);
		        	result = (double) value;
		        }
		       // System.out.println(method.getName() + " = " + value);
		    } // if
		  } // for
		 return result * 100 ;
		
	}
	
	//function returns an array of instance Ids
	private static ArrayList<Instance> getInstanceIds(AmazonEC2 ec2){
		ArrayList<Instance> arrInstances = new ArrayList<Instance>() ; // array holds all the instances 
		boolean done = false;
		DescribeInstancesRequest request = new DescribeInstancesRequest();
		
		while(!done) {
		    DescribeInstancesResult response = ec2.describeInstances(request);

		    for(Reservation reservation : response.getReservations()) {
		        for(Instance instance : reservation.getInstances()) {
		        	arrInstances.add(instance) ; 
		        	}
		        }

		    request.setNextToken(response.getNextToken());

		    if(response.getNextToken() == null) {
		        done = true;
		    }
		}
		return arrInstances ;
	}
	
	//function starts up an instance
	private static void startInstance(AmazonEC2 ec2, String Id){
        StartInstancesRequest startInstancesRequest = new StartInstancesRequest().withInstanceIds(Id);
        ec2.startInstances(startInstancesRequest);
	}
		
	
	//function sends a message to the sqs
	private static void sendMessage(AmazonSQS sqs, String filename, String id, String queueUrl) {
		String message = filename + ":" + id ;
		SendMessageRequest send_msg_request = new SendMessageRequest()
		        .withQueueUrl(queueUrl)
		        .withMessageBody(message)
		        .withDelaySeconds(0);
		sqs.sendMessage(send_msg_request);
		
	}
	
	
	//function runs darknet and performs object detection 
	private static String performObjectDetection(String keyName) {
		String[] bashScript = new String[] { "./darknet", "detector", "demo", "cfg/coco.data", "cfg/yolov3-tiny.cfg",
				"yolov3-tiny.weights", "videos/"+keyName };

		System.out.println("Running Command $ " + Arrays.toString(bashScript));
		String prediction = executeCommand(bashScript);
		System.out.println("Done running");
		return prediction;
	}
	
	//function runs darknet and performs object detection 
	private static void performObjectDetectionInBackground(String keyName) throws IOException, InterruptedException {
		System.out.println("Log file: detectionOutput"+keyName+".log");
		String[] bashScript = new String[] {"java", "-jar", "PIObjectDetect.jar", keyName};// "&>", "detectionOutput"+keyName+".log" };//./darknet", "detector", "demo", "cfg/coco.data", "cfg/yolov3-tiny.cfg",
		ProcessBuilder builder;
		System.out.println("Running Command $ " + Arrays.toString(bashScript));

		builder = new ProcessBuilder(bashScript);

		//builder.redirectErrorStream(true);
		Process p = builder.start();
		//builder.redirectOutput(new File("detectionOutput"+keyName+".log"));
		builder.redirectErrorStream(true);
		InputStream stdout = new BufferedInputStream(p.getInputStream());
		FileOutputStream stdoutFile = new FileOutputStream("detectionOutput"+keyName+".log");
		IOUtils.copy(stdout, stdoutFile);
		
		stdout.close();
		stdoutFile.close();
		
		//p.waitFor();
		//String prediction = executeCommand(bashScript);
		//System.out.println("Done running");
		//return prediction;
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