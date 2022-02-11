package com.vuta.reactive.filepooling.aws.configuration;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

/**
 * @author Vuta Alexandru https://vuta-alexandru.com Created at 12 nov. 2018
 *         contact email: verso.930[at]gmail.com
 */
@Configuration
public class S3ClientConfiguration {

//  @Bean
//  public AmazonS3 awsS3Client() {
//    return AmazonS3ClientBuilder.defaultClient();
//  }

  @Value("${aws.access_key_id}")
  private String accessKeyId;
  @Value("${aws.secret_access_key}")
  private String secretAccessKey;
  @Value("${aws.s3.region}")
  private String region;
  @Value("${aws.s3.endpoint}")
  private String endpoint;

  @Bean
  public AmazonS3 awsS3Client() {
    final BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(accessKeyId, secretAccessKey);
    return AmazonS3ClientBuilder
            .standard()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region))
            .withCredentials(new AWSStaticCredentialsProvider(basicAWSCredentials))
            .withPathStyleAccessEnabled(true)
            .withClientConfiguration(new ClientConfiguration().
                    withProtocol(Protocol.HTTP).
                    withMaxErrorRetry(3).
                    withConnectionTimeout(51000).
                    withSocketTimeout(51000))
            .build();
  }

}
