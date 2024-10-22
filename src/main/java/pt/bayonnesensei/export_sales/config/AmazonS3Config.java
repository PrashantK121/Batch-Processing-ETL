package pt.bayonnesensei.export_sales.config;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AmazonS3Config {

    @Value("${cos.aws.s3.endpoint}")
    private String cosAWSEndpoint;

    @Value("${cos.aws.s3.credentials.access-key}")
    private String cosAWSAccessKey;

    @Value("${cos.aws.s3.credentials.secret-key}")
    private String cosAWSSecretKey;

    @Bean
    public AmazonS3 amazonS3(){
        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(cosAWSEndpoint, Regions.EU_SOUTH_1.getName()))
                .withPathStyleAccessEnabled(Boolean.TRUE)
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(cosAWSAccessKey,cosAWSSecretKey)))
                .build();
    }
}
