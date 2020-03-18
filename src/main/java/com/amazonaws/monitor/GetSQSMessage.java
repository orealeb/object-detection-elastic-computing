package com.amazonaws.monitor;
import java.util.List;
import java.util.Map.Entry;

import org.json.*;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;

public class GetSQSMessage {

    public static void main(String[] args) throws Exception {

    	final String bucketName = "cse546input";

        
        ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
        try {
            credentialsProvider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (C:\\Users\\Ore\\.aws\\credentials), and is in valid format.",
                    e);
        }

        AmazonSQS sqs = AmazonSQSClientBuilder.standard()
                               .withCredentials(credentialsProvider)
                               .withRegion(Regions.US_EAST_1)
                               .build();
        
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(Regions.US_EAST_1)
                .build();
        try {
            // get a queue
            String myQueueUrl = "https://sqs.us-east-1.amazonaws.com/603754723521/Test";

            while(true) {
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

		                JSONObject obj = new JSONObject(message.getBody());
		                String keyName = obj.getJSONArray("Records").getJSONObject(0).getJSONObject("s3").getJSONObject("object").getString("key");

		                // Retrieve the object's tags.
		                GetObjectTaggingRequest getTaggingRequest = new GetObjectTaggingRequest(bucketName,keyName );
		                GetObjectTaggingResult getTagsResult = s3Client.getObjectTagging(getTaggingRequest);
		                
		                //get instance id
		                String instance_id = getTagsResult.getTagSet().get(0).getValue();//"i-06a964afe85584b03";
		                
			            //start ec2 instance if it is off
		                final AmazonEC2 ec2 = AmazonEC2ClientBuilder.defaultClient();

		                StartInstancesRequest request = new StartInstancesRequest()
		                    .withInstanceIds(instance_id);

		                ec2.startInstances(request);
			            
			            // Delete a message
			            System.out.println("Deleting a message.\n");
			            String messageReceiptHandle = messages.get(0).getReceiptHandle();
			            sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, messageReceiptHandle));		            
		            }
		            System.out.println();
	
	            }
	         }
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it " +
                    "to Amazon SQS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered " +
                    "a serious internal problem while trying to communicate with SQS, such as not " +
                    "being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }
}
