package com.github.thomasdarimont.keycloak.avatar.storage.minio;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;

import java.util.concurrent.TimeUnit;

import lombok.extern.jbosslog.JBossLog;
import okhttp3.OkHttpClient;

@JBossLog
public class MinioTemplate {
    private static final int TIMEOUT_SECONDS = 15;
    
    private final MinioConfig minioConfig;
    
    private OkHttpClient httpClient;
    
    public MinioTemplate(MinioConfig minioConfig) {
        this.minioConfig = minioConfig;
        httpClient = new OkHttpClient.Builder()
            .connectTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .readTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .writeTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS)
            .build();
    }
    
    public <T> T execute(MinioCallback<T> callback) {
        
        try {
            MinioClient minioClient = MinioClient.builder()
                .endpoint(minioConfig.getServerUrl())
                .credentials(minioConfig.getAccessKey(), minioConfig.getSecretKey())
                .httpClient(httpClient)
                .build();
            
            return callback.doInMinio(minioClient);
        } catch (Exception mex) {
            throw new RuntimeException(mex);
        }
    }
    
    public void ensureBucketExists(String bucketName) {
        execute(minioClient -> {
            boolean exists = minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
            if (exists) {
                log.debugf("Bucket: %s already exists", bucketName);
            } else {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
            }
            
            return null;
        });
    }
    
    
    public String getBucketName(String realmName) {
        return realmName + minioConfig.getDefaultBucketSuffix();
    }
    
}
