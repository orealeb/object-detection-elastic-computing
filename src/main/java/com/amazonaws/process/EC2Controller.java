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
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class EC2Controller {

	// Create timer and schedule shutdown after 2 minutes
	static Timer timer = new Timer();

	public static void main(String[] args) throws IOException, InterruptedException {
		// Set required parameters
		Regions clientRegion = Regions.US_EAST_1;
		String inputBucketName = "cse546input";
		String outputBucketName = "cse546output";
		String myQueueUrl = "https://sqs.us-east-1.amazonaws.com/603754723521/Test";

		// Create SQS and S3 Client
		AmazonSQS sqs = AmazonSQSClientBuilder.standard().withRegion(clientRegion).build();

		AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(clientRegion).build();

		try {
			// Receive messages.
			System.out.println("Receiving messages from MyQueue.\n");
			List<Message> messages;
			while (true) {
				ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
				messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
				// check if message is more than one
				if (messages.size() > 0) {
					for (Message message : messages) {

						printMessage(message);

						String keyName = message.getBody().split(":")[0];
						String instance_id = message.getBody().split(":")[1];

						// If message instance id is this EC2 instance, then download and process video.
						String currInstanceId = EC2MetadataUtils.getInstanceId();

						if (instance_id.equals(currInstanceId)) {
							// Reset timer and wait until this finishes running to start again.
							pauseTimer();

							// Get an object from s3 and save its contents to file.
							downloadAndSave(s3Client, inputBucketName, keyName);

							// Run darknet command for object detection.
							String prediction = performObjectDetection(keyName);

							// Upload file and prediction to s3.
							upload(s3Client, outputBucketName, keyName, prediction);

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
		PutObjectRequest request = new PutObjectRequest(bucketName, "{ " + keyName + ", " + prediction + " }",
				new File(keyName));
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
