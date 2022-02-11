package com.vuta.reactive.filepooling.aws.service;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.vuta.reactive.filepooling.aws.beans.FileHolder;
import com.vuta.reactive.filepooling.aws.configuration.S3ClientConfiguration;

/**
 * @author Vuta Alexandru https://vuta-alexandru.com Created at 11 nov. 2018
 * contact email: verso.930[at]gmail.com
 */
@Service
public class S3FileProcessor {

    Logger logger = LoggerFactory.getLogger(S3FileProcessor.class);

    @Value("${aws.s3.bucket}")
    private String bucketName;

    @Autowired
    S3ClientConfiguration s3Client;

    @Autowired
    private FileFlux s3FileFlux;

    @PostConstruct
    public void init() {
        // subscribe the flux
        subscribe();
    }

    public void subscribe() {
        s3FileFlux.getParallelFlux().doOnSubscribe(item -> logger.info("======> Subscribed to S3 FLUX")).
                map(this::saveFile).map(this::finalizer)
                .doOnError(e -> logger.error("======> S3 ERROR: {}", e.getMessage()))
                .subscribe(value -> logger.info("=== S3 {} ===> completed", value.getFileName()));
    }

    /**
     * Save a FileHolder to S3 AWS Bucket
     *
     * @param fileHolder
     * @return
     */
    public FileHolder saveFile(FileHolder fileHolder) {
        logger.info("=== S3 {} ===> transferring...", fileHolder.getFileName());
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(fileHolder.getSize());

        try (InputStream inputStream = new ByteArrayInputStream(fileHolder.getBytes())) {
            s3Client.awsS3Client().putObject(bucketName, fileHolder.getFileName(), inputStream, objectMetadata);
        } catch (Exception e) {
            logger.error("Failed to save file {} on AWS S3. Error: {}", fileHolder.getFileName(), e.getMessage());
        }

        return fileHolder;
    }

    /**
     * Free Up memory
     *
     * @param file
     * @return
     */
    private FileHolder finalizer(FileHolder file) {
        file.setBytes(null);
        return file;
    }

}
