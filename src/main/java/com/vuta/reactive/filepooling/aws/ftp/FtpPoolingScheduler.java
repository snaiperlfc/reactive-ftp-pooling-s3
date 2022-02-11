package com.vuta.reactive.filepooling.aws.ftp;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.pool2.ObjectPool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.vuta.reactive.filepooling.aws.beans.FileHolder;
import com.vuta.reactive.filepooling.aws.configuration.FtpClientConfiguration;
import com.vuta.reactive.filepooling.aws.service.FileFlux;
import ru.lanwen.verbalregex.VerbalExpression;

import static ru.lanwen.verbalregex.VerbalExpression.regex;

/**
 * @author Vuta Alexandru https://vuta-alexandru.com Created at 12 nov. 2018
 * contact email: verso.930[at]gmail.com
 */
@Service
public class FtpPoolingScheduler {

    @Autowired
    ObjectPool<FTPClient> objectPool;

    @Autowired
    FtpClientConfiguration ftpConfiguration;

    @Autowired
    @Qualifier("ftpFileFlux")
    private FileFlux ftpFileFlux;

    private long lastFileTransferTime = 0;

    org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FtpPoolingScheduler.class);

    /**
     * Get List of files and push to the flux only the files that are added after
     * the last pool date
     *
     * @return
     * @throws Exception
     */

    @Scheduled(fixedDelayString = "${vuta.ftp.poolInterval}", initialDelay = 2000)
    public void poolFTP() {
        FTPClient client;

        try {
            // get ftp client from FTP pool
            client = objectPool.borrowObject();
            client.setFileType(FTP.BINARY_FILE_TYPE);
            client.setFileTransferMode(FTP.STREAM_TRANSFER_MODE);

            // init array
            FTPFile[] files;

            // new files counter (just for simple stats)
            int newFilesCount = 0;

            // get the list of files from FTP
            files = client.listFiles(ftpConfiguration.getPath());

            VerbalExpression expression = regex().anythingBut(" ").
                    oneOf(".jpeg", ".jpg", ".dmg").endOfLine().build();
            Pattern pattern = Pattern.compile(expression.toString(), Pattern.CASE_INSENSITIVE);

            List<FTPFile> list = Arrays.stream(files)
                    .filter(s -> pattern.matcher(s.getName()).lookingAt())
                    .sorted(Comparator.comparing(FTPFile::getTimestamp))
                    .collect(Collectors.toList());

            if (list.isEmpty())
                return;

            for (FTPFile file : list) {
                logger.debug("name: {} date: {} millis: {} ", file.getName(), file.getTimestamp().getTime(),
                        file.getTimestamp().getTimeInMillis());
                // push into sink only the files that are added between after last file
                // transferred time
                if (file.getTimestamp().getTimeInMillis() > lastFileTransferTime) {
                    ftpFileFlux.onNewItem(mapToFileHolder(file));
                    newFilesCount++;
                }
            }
            // set the last file timestamp as last pooling date for the next pooling
            lastFileTransferTime = list.get(list.size() - 1).getTimestamp().getTimeInMillis();

            logger.debug("FTP POOL: total files [{}] || new files [{}]", list.size(), newFilesCount);
            if (newFilesCount > 0)
                logger.info("FTP POOL: total files [{}] || new files [{}]", list.size(), newFilesCount);
            objectPool.returnObject(client);

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Failed to get FTP client from POOL: {}", e.getMessage());
        }

    }

    private FileHolder mapToFileHolder(FTPFile file) {

        FileHolder fileHolder = new FileHolder();
        fileHolder.setFileName(file.getName());
        fileHolder.setSize(file.getSize());
        fileHolder.setCreationDate(
                OffsetDateTime.ofInstant(Instant.ofEpochSecond(file.getTimestamp().getTimeInMillis()), ZoneId.of("UTC")));
        fileHolder.setDescription("Downloaded at " + OffsetDateTime.now());

        return fileHolder;
    }

}
