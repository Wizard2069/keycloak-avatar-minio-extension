package com.github.thomasdarimont.keycloak.avatar.storage.minio;

import com.github.thomasdarimont.keycloak.avatar.storage.AvatarStorageProvider;
import io.minio.GetObjectArgs;
import io.minio.PutObjectArgs;
import lombok.extern.jbosslog.JBossLog;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

@JBossLog
public class MinioAvatarStorageProvider implements AvatarStorageProvider {

    private final MinioTemplate minioTemplate;

    public MinioAvatarStorageProvider(MinioConfig minioConfig) {
        this.minioTemplate = new MinioTemplate(minioConfig);
    }

    @Override
    public void saveAvatarImage(String realmName, String userId, InputStream input) {
        String bucketName = minioTemplate.getBucketName(realmName);
        minioTemplate.ensureBucketExists(bucketName);
        
        Map<String, String> extraQueries = createQueryParams(userId);
        minioTemplate.execute(minioClient -> {
            minioClient.putObject(
                PutObjectArgs.builder()
                    .bucket(bucketName)
                    .stream(input, -1, 10485760)
                    .extraQueryParams(extraQueries)
                    .contentType("image/png")
                    .build()
            );
            
            return null;
        });
    }

    @Override
    public InputStream loadAvatarImage(String realmName, String userId) {
        String bucketName = minioTemplate.getBucketName(realmName);
        Map<String, String> extraQueries = createQueryParams(userId);
        
        return minioTemplate.execute(minioClient ->
            minioClient.getObject(GetObjectArgs.builder()
                .bucket(bucketName)
                .extraQueryParams(extraQueries)
                .build()
            )
        );
    }

    private Map<String, String> createQueryParams(String userId) {
        Map<String, String> userQuery = new HashMap<>();
        userQuery.put("userId", userId);
        
        return userQuery;
    }
    
    @Override
    public void close() {
        // NOOP
    }

}
