package com.amazonaws.upload;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.Tag;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UploadObject {

    
	
	public static void main(String[] args) throws IOException {
        Regions clientRegion = Regions.US_EAST_1;
        String bucketName = "cse546input";
        //String stringObjKeyName = "*** String object key name ***";
        String fileObjKeyName = "20200316_151914_HDR.jpg"; //pass filename and path as args
        String fileName = "C:\\Users\\Ore\\Downloads\\20200316_151914_HDR.jpg";

        try {
            //This code expects that you have AWS credentials set up per:
            // https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(clientRegion)
                    .build();

            // Upload a text string as a new object.
            //s3Client.putObject(bucketName, stringObjKeyName, "Uploaded String Object");

            // Upload a file as a new object with ContentType and title specified.
            PutObjectRequest request = new PutObjectRequest(bucketName, fileObjKeyName, new File(fileName));
            List<Tag> tags = new ArrayList<Tag>();
            //get list of ec2s.. select one that is off. add instance id as tag
            //if none is off then run video processing locally
            tags.add(new Tag("EC2", "i-06a964afe85584b03"));
            request.setTagging(new ObjectTagging(tags));
            
            //ObjectMetadata metadata = new ObjectMetadata();
            //metadata.setContentType("plain/text");
            //metadata.addUserMetadata("x-amz-meta-title", "someTitle");
            //request.setMetadata(metadata);
            s3Client.putObject(request);
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
