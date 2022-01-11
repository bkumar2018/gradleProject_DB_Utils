package util;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.HttpMethod;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class AmazonS3Util {

    protected static AmazonS3 s3client;
    protected List<String> jsonFileNames = new ArrayList<>();

    /**
     * Connect with AWS account.
     * Create aws client with access and secret key
     */
    public AmazonS3 getAmazonS3Client(String accessKey, String secretKey){
        final AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        final AmazonS3 s3client = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(Regions.US_EAST_1)
                .build();
        return s3client;
    }

    /**
     * Connect with AWS account.
     * Create aws client with credentials api key and secret
     * residing in location ~.aws/credential file.
     */
    private AmazonS3 getS3Client() {
        System.out.println("Create aws s3 client and upload");
        Regions clientRegion = Regions.US_WEST_2;
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider())
                .withRegion(clientRegion)
                .build();
        return s3Client;
    }


    //This method is to download file from S3 bucket
    public void downloadFilesFromS3Bucket(AmazonS3 amazonS3Client, String bucketName, String prefix, String fileName){
        final S3Object s3Object = amazonS3Client.getObject(bucketName, prefix+fileName);
        final S3ObjectInputStream inputStream = s3Object.getObjectContent();
        try{
            FileUtils.copyInputStreamToFile(inputStream, new File("./awss3json/" + fileName));
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    //This method is to upload file to s3 location
    public void uploadFileToS3(AmazonS3 amazonS3Client, String bucketName, String path, String fileName){

        amazonS3Client.putObject(bucketName, path, new File(System.getProperty("user.dir")+
                "//PushS3Files//"+fileName));
    }

    private final long EXPIRE_TIME_IN_MILISEC = 7 * 24 * 60 * 60 * 1000;
    /**
     * Upload file to aws bucket and generate public accessible link with
     * expire time.
     * @param file - File to upload on aws
     * @return - Public url of image uploaded on aws
     */
    public String uploadFileToS3Bucket(String bucketName, String file) {

        final String BUCKET_NAME = bucketName;

        String objectName = "random-" + System.currentTimeMillis() + ".png";
        AmazonS3 s3Client = getS3Client();
        URL url = null;
        try {
            Date expiration = new Date();
            long expTimeMillis = expiration.getTime();
            expTimeMillis += EXPIRE_TIME_IN_MILISEC;
            expiration.setTime(expTimeMillis);
            System.out.println("Upload file to AWS bucket: " + BUCKET_NAME);
            s3Client.putObject(new PutObjectRequest(
                    BUCKET_NAME, objectName, new File(file)
            ));

            System.out.println("Creating presigned url to have public access");
            GeneratePresignedUrlRequest generatePresignedUrlGetRequest = new GeneratePresignedUrlRequest(BUCKET_NAME,
                    objectName)
                    .withMethod(HttpMethod.GET)
                    .withExpiration(expiration);
            url = s3Client.generatePresignedUrl(generatePresignedUrlGetRequest);
            System.out.println("File uploaded successfully with public access.");
        } catch (AmazonServiceException e) {
            e.printStackTrace();
        } catch (SdkClientException e) {
            e.printStackTrace();
        }
        return url.toString();
    }



}
