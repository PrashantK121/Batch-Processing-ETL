package pt.bayonnesensei.export_sales.service.impl;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import pt.bayonnesensei.export_sales.service.StoreFileToObjectStorageUseCase;
import pt.bayonnesensei.export_sales.service.dto.ObjectDTO;

import java.io.ByteArrayInputStream;
import java.time.LocalDateTime;
import java.util.Objects;

@Component
@RequiredArgsConstructor
@Slf4j
public class StoreFileToObjectStorageServiceImpl implements StoreFileToObjectStorageUseCase {

    private final AmazonS3 amazonS3;

    @Value("${cos.aws.s3.bucket.name}")
    private String bucket;

    @Override
    public String store(ObjectDTO objectDTO) {
        createBucketIfDoesNotExist(bucket);

        ObjectMetadata objectMetadata = mapOjectDtoToS3ObjectMetadata(objectDTO);
        PutObjectResult putObjectResult = amazonS3.putObject(bucket, objectDTO.name(),
                new ByteArrayInputStream(objectDTO.data()), objectMetadata);
        log.info("The object '{}' has been stored. The metadata short info is {}", objectDTO.name(), putObjectResult);
        return objectDTO.name();
    }

    @Override
    public void createBucketIfDoesNotExist(final String bucketName) {
        Assert.hasText(bucketName, "The bucket name should not be blank");
        if (!amazonS3.doesBucketExistV2(bucketName)) {
            amazonS3.createBucket(bucketName);
        }
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
