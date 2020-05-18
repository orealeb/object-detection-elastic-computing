package com.amazonaws.process;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.StopInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
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
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.stream.Collectors;

public class WorkerController {

	// Create timer and schedule shutdown after 2 minutes
	static Timer timer = new Timer();

	public static void main(String[] args) throws IOException, InterruptedException {
		// Set required parameters
		Regions clientRegion = Regions.US_EAST_1;
		String inputBucketName = args[0]; //Input bucket;
		String outputBucketName = args[1]; //Output bucket;
		String myQueueUrl = args[2]; //SQS Queue URL "https://sqs.us-east-1.amazonaws.com/603754723521/Test";

		// Create SQS and S3 Client
		AmazonSQS sqs = AmazonSQSClientBuilder.standard().withRegion(clientRegion).build();

		AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(clientRegion).build();

		try {
			//use timestamp to track execution times
			String timeStamp = new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime());
			System.out.println("About to Receive Messages, Time " + timeStamp);
			
			// Receive messages.
			System.out.println("Receiving messages from MyQueue.\n");
			List<Message> messages;
			while (true) {
				ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
				messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
				// check if message is more than one
				if (messages.size() > 0) {
					for (Message message : messages) {

						//use timestamp to track execution times
						timeStamp = new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime());
						System.out.println("About to process message, Time " + timeStamp);
						
						printMessage(message);

						String keyName = message.getBody().split(":")[0];
						String instance_id = message.getBody().split(":")[1];

						// If message instance id is this EC2 instance, then download and process video.
						String currInstanceId = EC2MetadataUtils.getInstanceId();

						if (instance_id.equals(currInstanceId)) {
							//use timestamp to track execution times
							timeStamp = new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime());
							System.out.println("Current instance id == message id, Time " + timeStamp);
							
							// Reset timer and wait until this finishes running to start again.
							pauseTimer();

							//use timestamp to track execution times
							timeStamp = new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime());
							System.out.println("Deleted Message, Time " + timeStamp);
				
							//use timestamp to track execution times
							timeStamp = new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime());
							System.out.println("About to download and save video, Time " + timeStamp);
							
					
							// Get an object from s3 and save its contents to file.
							downloadAndSave(s3Client, inputBucketName, keyName);

							//use timestamp to track execution times
							timeStamp = new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime());
							System.out.println("About to run darknet, Time " + timeStamp);
				
							// Run darknet command for object detection.
							String prediction = performObjectDetection(keyName);

							//use timestamp to track execution times
							timeStamp = new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime());
							System.out.println("About to upload video, Time " + timeStamp);
			
							// Upload file and prediction to s3.
							upload(s3Client, outputBucketName, keyName, prediction);

							//use timestamp to track execution times
							timeStamp = new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime());
							System.out.println("Finished uploading video, Time " + timeStamp);

							// Delete message from queue.
							System.out.println("Deleting message.\n");
							String messageReceiptHandle = message.getReceiptHandle();
							sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, messageReceiptHandle));
				
							// Restart timer.
							timer = new Timer();
							restartTimer(currInstanceId);
						}
					}
					System.out.println();
				}
			}
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

	private static String performObjectDetection(String keyName) {
		String[] bashScript = new String[] { "./darknet", "detector", "demo", "cfg/coco.data", "cfg/yolov3-tiny.cfg",
				"yolov3-tiny.weights", keyName };

		System.out.println("Running Command $ " + Arrays.toString(bashScript));
		String prediction = executeCommand(bashScript);
		System.out.println("Done running");
		return prediction;
	}

	private static void printMessage(Message message) {
		System.out.println("  Message");
		System.out.println("    MessageId:     " + message.getMessageId());
		System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
		System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
		System.out.println("    Body:          " + message.getBody());
	}

	private static void downloadAndSave(AmazonS3 s3Client, String bucketName, String keyName) throws IOException {
		S3Object fullObject = null;
		System.out.println("Downloading an object");
		fullObject = s3Client.getObject(new GetObjectRequest(bucketName, keyName));
		System.out.println("Content-Type: " + fullObject.getObjectMetadata().getContentType());
		System.out.println("Content: ");
		save(fullObject.getObjectContent(), keyName);
	}

	private static void pauseTimer() {
		timer.cancel();
	}

	private static void restartTimer(String instance_id) {
		//use timestamp to track execution times
		String timeStamp = new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime());
		System.out.println("Swtiching off instance, Time " + timeStamp);

		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				stopInstance(instance_id);
			}
		}, 2 * 60 * 1000);
	}

	private static void stopInstance(String instance_id) {
		final AmazonEC2 ec2 = AmazonEC2ClientBuilder.defaultClient();

		StopInstancesRequest request = new StopInstancesRequest().withInstanceIds(instance_id);

		ec2.stopInstances(request);
	}

	private static void save(InputStream in, String fname) throws IOException {
		try (FileOutputStream out = new FileOutputStream(new File(fname))) {
			IOUtils.copy(in, out);
		}
	}

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
