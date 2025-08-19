package org.drools.ansible.rulebook.integration.ha.api;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for High Availability state management
 */
public class HAConfiguration {
    
    private String dbUrl;
    private String username;
    private String password;
    private String certificatePath;
    private String privateKeyPath;
    private int connectionPoolSize = 10;
    private int connectionTimeout = 5000; // ms
    private boolean useMTLS = false;
    private Map<String, Object> additionalProperties = new HashMap<>();
    
    // Database type for factory selection
    private DatabaseType databaseType = DatabaseType.H2;
    
    public enum DatabaseType {
        H2,
        POSTGRESQL
    }
    
    public String getDbUrl() {
        return dbUrl;
    }
    
    public void setDbUrl(String dbUrl) {
        this.dbUrl = dbUrl;
    }
    
    public String getUsername() {
        return username;
    }
    
    public void setUsername(String username) {
        this.username = username;
    }
    
    public String getPassword() {
        return password;
    }
    
    public void setPassword(String password) {
        this.password = password;
    }
    
    public String getCertificatePath() {
        return certificatePath;
    }
    
    public void setCertificatePath(String certificatePath) {
        this.certificatePath = certificatePath;
    }
    
    public String getPrivateKeyPath() {
        return privateKeyPath;
    }
    
    public void setPrivateKeyPath(String privateKeyPath) {
        this.privateKeyPath = privateKeyPath;
    }
    
    public int getConnectionPoolSize() {
        return connectionPoolSize;
    }
    
    public void setConnectionPoolSize(int connectionPoolSize) {
        this.connectionPoolSize = connectionPoolSize;
    }
    
    public int getConnectionTimeout() {
        return connectionTimeout;
    }
    
    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }
    
    public boolean isUseMTLS() {
        return useMTLS;
    }
    
    public void setUseMTLS(boolean useMTLS) {
        this.useMTLS = useMTLS;
    }
    
    public DatabaseType getDatabaseType() {
        return databaseType;
    }
    
    public void setDatabaseType(DatabaseType databaseType) {
        this.databaseType = databaseType;
    }
    
    public Map<String, Object> getAdditionalProperties() {
        return additionalProperties;
    }
    
    public void setAdditionalProperties(Map<String, Object> additionalProperties) {
        this.additionalProperties = additionalProperties;
    }
    
    /**
     * Create configuration from a map (used by Python/JPY interface)
     */
    public static HAConfiguration fromMap(Map<String, Object> configMap) {
        HAConfiguration config = new HAConfiguration();
        
        if (configMap.containsKey("db_url")) {
            config.setDbUrl((String) configMap.get("db_url"));
        }
        if (configMap.containsKey("username")) {
            config.setUsername((String) configMap.get("username"));
        }
        if (configMap.containsKey("password")) {
            config.setPassword((String) configMap.get("password"));
        }
        if (configMap.containsKey("certificate_path")) {
            config.setCertificatePath((String) configMap.get("certificate_path"));
        }
        if (configMap.containsKey("private_key_path")) {
            config.setPrivateKeyPath((String) configMap.get("private_key_path"));
        }
        if (configMap.containsKey("pool_size")) {
            config.setConnectionPoolSize((Integer) configMap.get("pool_size"));
        }
        if (configMap.containsKey("use_mtls")) {
            config.setUseMTLS((Boolean) configMap.get("use_mtls"));
        }
        if (configMap.containsKey("database_type")) {
            String dbType = (String) configMap.get("database_type");
            config.setDatabaseType(DatabaseType.valueOf(dbType.toUpperCase()));
        }
        
        return config;
    }
}