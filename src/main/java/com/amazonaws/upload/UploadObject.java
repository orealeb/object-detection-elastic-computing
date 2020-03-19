package com.amazonaws.upload;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UploadObject {

    
	
	public static void main(String[] args) throws IOException {
        Regions clientRegion = Regions.US_EAST_1;

        AmazonSQS sqs = AmazonSQSClientBuilder.standard()
                .withRegion(clientRegion)
                .build();
        
        String bucketName = "cse546input";
        String fileObjKeyName = "video.h264"; //pass filename and path as args
        String fileName = "video.h264";

        try {
            //This code expects that you have AWS credentials set up per:
            // https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(clientRegion)
                    .build();

            // get a queue
            String myQueueUrl = "https://sqs.us-east-1.amazonaws.com/603754723521/Test";

            // Upload a file as a new object with ContentType and title specified.
            PutObjectRequest request = new PutObjectRequest(bucketName, fileObjKeyName, new File(fileName));
            //get list of ec2s.. select one that is off. add instance id as tag
            //if none is off then run video processing locally
            String instance_id = "i-06a964afe85584b03";
            
            //upload file
            s3Client.putObject(request);
            
            // Send a message
            sqs.sendMessage(new SendMessageRequest(myQueueUrl, fileObjKeyName + ":" + instance_id));
            System.out.println("Sent a message to MyQueue.\n");
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
}
