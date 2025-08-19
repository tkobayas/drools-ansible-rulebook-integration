package org.drools.ansible.rulebook.integration.ha.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Factory for creating HAStateManager instances based on configuration
 */
public class HAStateManagerFactory {
    
    private static final Logger logger = LoggerFactory.getLogger(HAStateManagerFactory.class);
    
    /**
     * Create an HAStateManager instance based on configuration
     * @param configMap Configuration map from Python/JPY
     * @return HAStateManager instance
     */
    public static HAStateManager create(Map<String, Object> configMap) {
        HAConfiguration config = HAConfiguration.fromMap(configMap);
        return create(config);
    }
    
    /**
     * Create an HAStateManager instance based on configuration
     * @param config HAConfiguration object
     * @return HAStateManager instance
     */
    public static HAStateManager create(HAConfiguration config) {
        HAStateManager manager = null;
        
        try {
            switch (config.getDatabaseType()) {
                case H2:
                    // Use reflection to avoid compile-time dependency
                    Class<?> h2Class = Class.forName(
                        "org.drools.ansible.rulebook.integration.ha.h2.H2StateManager"
                    );
                    manager = (HAStateManager) h2Class.getDeclaredConstructor().newInstance();
                    break;
                    
                case POSTGRESQL:
                    // Use reflection to avoid compile-time dependency
                    Class<?> pgClass = Class.forName(
                        "org.drools.ansible.rulebook.integration.ha.postgres.PostgresStateManager"
                    );
                    manager = (HAStateManager) pgClass.getDeclaredConstructor().newInstance();
                    break;
                    
                default:
                    throw new IllegalArgumentException(
                        "Unsupported database type: " + config.getDatabaseType()
                    );
            }
            
            manager.initialize(config);
            logger.info("Created HAStateManager for database type: {}", config.getDatabaseType());
            
        } catch (Exception e) {
            logger.error("Failed to create HAStateManager", e);
            throw new RuntimeException("Failed to create HAStateManager: " + e.getMessage(), e);
        }
        
        return manager;
    }
}