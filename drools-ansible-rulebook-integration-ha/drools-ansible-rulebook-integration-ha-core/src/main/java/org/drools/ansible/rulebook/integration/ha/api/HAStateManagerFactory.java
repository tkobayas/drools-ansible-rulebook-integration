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
     * Create an H2StateManager instance without initialization
     * Used for new initializeHA API where initialization happens separately
     * @return HAStateManager instance
     */
    public static HAStateManager createH2() {
        try {
            Class<?> h2Class = Class.forName(
                "org.drools.ansible.rulebook.integration.ha.h2.H2StateManager"
            );
            HAStateManager manager = (HAStateManager) h2Class.getDeclaredConstructor().newInstance();
            logger.info("Created H2StateManager instance without initialization");
            return manager;
        } catch (Exception e) {
            logger.error("Failed to create H2StateManager", e);
            throw new RuntimeException("Failed to create H2StateManager: " + e.getMessage(), e);
        }
    }
}