package pt.bayonnesensei.export_sales.service.impl;

import com.amazonaws.services.s3.model.ObjectMetadata;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import pt.bayonnesensei.export_sales.service.StoreFileToObjectStorageUseCase;
import pt.bayonnesensei.export_sales.service.dto.ObjectDTO;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.OptionalLong;

@Component
@RequiredArgsConstructor
@Slf4j
public class StoreFileToObjectStorageServiceImpl implements StoreFileToObjectStorageUseCase {


    private final S3TransferManager s3TransferManager;

    @Value("${cos.aws.s3.bucket.name}")
    private String bucket;

    @Override
    public String store(ObjectDTO objectDTO) {
        createBucketIfDoesNotExist(bucket);

        UploadFileRequest uploadFileRequest = UploadFileRequest.builder()
                .putObjectRequest(builder -> builder.bucket(bucket)
                        .key(objectDTO.name()))
                .source(Paths.get(objectDTO.name()))
                .build();

        FileUpload fileUpload = s3TransferManager.uploadFile(uploadFileRequest);
        printUploadProgress(fileUpload);

        return objectDTO.name();
    }


    private void printUploadProgress(FileUpload fileUpload) {
        OptionalLong remainingBytesOptional;
        do {
            remainingBytesOptional = fileUpload.progress()
                    .snapshot()
                    .remainingBytes();

            remainingBytesOptional.ifPresent(remainingBytes -> log.info("------------> Remaining byres to transfer: {}", remainingBytes));
        } while (remainingBytesOptional.orElse(0L) > 0);

        CompletedFileUpload completedFileUpload = fileUpload.completionFuture().join();
        boolean successful = completedFileUpload.response().sdkHttpResponse().isSuccessful();

        log.info("-----------> upload succeeded: {}", successful);
    }

    @Override
    public void createBucketIfDoesNotExist(final String bucketName) {
        Assert.hasText(bucketName, "The bucket name should not be blank");
//        if (!amazonS3.doesBucketExistV2(bucketName)) {
//            amazonS3.createBucket(bucketName);
//        }
    }

    public ObjectMetadata mapOjectDtoToS3ObjectMetadata(final ObjectDTO objectDTO) {
        Objects.requireNonNull(objectDTO, "the object dto should not be null");

        final var objectMetadata = new ObjectMetadata();
        objectMetadata.addUserMetadata("filename", objectDTO.name());
        objectMetadata.addUserMetadata("storeDate", LocalDateTime.now().toString());
        objectMetadata.setContentType(objectDTO.contentType());
        objectMetadata.setContentLength(objectDTO.data().length);
        return objectMetadata;
    }

}
