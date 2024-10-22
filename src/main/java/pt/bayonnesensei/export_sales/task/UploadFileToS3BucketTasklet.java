package pt.bayonnesensei.export_sales.task;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import pt.bayonnesensei.export_sales.service.StoreFileToObjectStorageUseCase;
import pt.bayonnesensei.export_sales.service.dto.ObjectDTO;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@Component
@RequiredArgsConstructor
@StepScope
@Slf4j
public class UploadFileToS3BucketTasklet implements Tasklet {

    private final StoreFileToObjectStorageUseCase fileToObjectStorageUseCase;

    @Value("#{jobParameters['output.file.name']}")
    private String outputFile;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        log.info("-----------> Now storing files to bucket on s3");

        var objectDTO = mapToObjectDTO();
        String fileUploaded = fileToObjectStorageUseCase.store(objectDTO);

        log.info("----------> File uploaded successfully on S3 : {}", fileUploaded);
        return RepeatStatus.FINISHED;
    }

    private ObjectDTO mapToObjectDTO() throws IOException {
        String filename = new File(outputFile).getName();
        Path filePath = Path.of(outputFile);

        String contentType = Files.probeContentType(filePath);
        long size = Files.size(filePath);
        byte[] data = Files.readAllBytes(filePath);
        return new ObjectDTO(filename, contentType, size, data);
    }
}
