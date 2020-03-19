package com.amazonaws.process;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
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
import java.util.List;


public class PrcoessVideo {

    
	
	public static void main(String[] args) throws IOException, InterruptedException {
        Regions clientRegion = Regions.US_EAST_1;
        String bucketName = "cse546input";
        String outputBucketName = "cse546output";
       
        AmazonSQS sqs = AmazonSQSClientBuilder.standard()
                .withRegion(clientRegion)
                .build();
        
        //This code expects that you have AWS credentials set up per:
        // https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(clientRegion)
                .build();
        
        try {
        	
            // get a queue
            String myQueueUrl = "https://sqs.us-east-1.amazonaws.com/603754723521/Test";
            // Receive messages
            System.out.println("Receiving messages from MyQueue.\n");
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            // check if message is more than one
            if(messages.size() > 0) {
	            for (Message message : messages) {
	                System.out.println("  Message");
	                System.out.println("    MessageId:     " + message.getMessageId());
	                System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
	                System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
	                System.out.println("    Body:          " + message.getBody());

	                String keyName = message.getBody().split(":")[0];
       
	                //get instance id
	                String instance_id = message.getBody().split(":")[1];//"i-06a964afe85584b03";
	                
	                // if instance id == started instance, then download process video
	                String currInstanceId = EC2MetadataUtils.getInstanceId(); 
	                if(instance_id.equals(currInstanceId)) {
	                	//download from s3
	                    // Get an object and save its contents to file.
	                	S3Object fullObject = null;
	                    System.out.println("Downloading an object");
	                    fullObject = s3Client.getObject(new GetObjectRequest(bucketName, keyName));
	                    System.out.println("Content-Type: " + fullObject.getObjectMetadata().getContentType());
	                    System.out.println("Content: ");
	                    save(fullObject.getObjectContent(), keyName);
	                    
	                    //TODO: process video from terminal
	                    String darknetCommand = "./darknet detector demo cfg/coco.data cfg/yolov3-tiny.cfg yolov3-tiny.weights " + keyName;
	                    //String predictionResult = "Dog : 50%"; //call command in terminal
	                    String[] bashScript = new String[] {"/bin/bash", "-c", darknetCommand, "with", "args"};
	                    System.out.println("Running Command $ " + darknetCommand);
	                    Process proc = new ProcessBuilder(bashScript).start();		
	                    
	                    // Read the output

	                    BufferedReader reader =  
	                          new BufferedReader(new InputStreamReader(proc.getInputStream()));

	                    String line = "";
	                    String prediction = "";

	                    while((line = reader.readLine()) != null) {
	                    	if(line.contains("%")) {
	                    		prediction = line;
	                    	}
	                        System.out.print(line + "\n");
	                    }
	                    
	                    proc.waitFor(); //make thread wait
	                    
	                    //upload result to s3
	                    // Upload a file as a new object with ContentType and title specified.
	                    PutObjectRequest request = new PutObjectRequest(outputBucketName, "{ " +  keyName + ", " + prediction + " }", new File(keyName));
	                    s3Client.putObject(request);
	                    System.out.println("Uploaded Result");
			            // Delete a message
			            //System.out.println("Deleting a message.\n");
			            //String messageReceiptHandle = messages.get(0).getReceiptHandle();
			            //sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, messageReceiptHandle));		            
	                
			            //TODO: call to terminal to shut down machine after 5 minutes
	                }    
	            }
	            System.out.println();

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
	
	private static void save(InputStream in, String fname) throws IOException {
	    try (FileOutputStream out = new FileOutputStream(new File(fname))) {
	        IOUtils.copy(in, out);
	    }
	}
}
