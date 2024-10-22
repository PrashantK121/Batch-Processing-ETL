package pt.bayonnesensei.export_sales.service;

import pt.bayonnesensei.export_sales.service.dto.ObjectDTO;

public interface StoreFileToObjectStorageUseCase {

    String store(ObjectDTO objectDTO);

    void createBucketIfDoesNotExist(String bucketName);

}
