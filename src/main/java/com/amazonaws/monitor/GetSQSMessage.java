package com.amazonaws.monitor;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.StartInstancesRequest;

public class GetSQSMessage {

    public static void main(String[] args) throws Exception {

        Regions clientRegion = Regions.US_EAST_1;

        AmazonSQS sqs = AmazonSQSClientBuilder.standard()
                .withRegion(clientRegion)
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

		                //get instance id
		                String instance_id = message.getBody().split(":")[1];
		                
			            //start ec2 instance if it is off
		                final AmazonEC2 ec2 = AmazonEC2ClientBuilder.defaultClient();

		                StartInstancesRequest request = new StartInstancesRequest()
		                    .withInstanceIds(instance_id);

		                ec2.startInstances(request);
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
