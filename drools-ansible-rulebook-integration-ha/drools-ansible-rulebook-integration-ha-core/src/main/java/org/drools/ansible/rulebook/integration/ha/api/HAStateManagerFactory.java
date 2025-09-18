package org.drools.ansible.rulebook.integration.ha.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating HAStateManager instances based on configuration
 */
public class HAStateManagerFactory {

    private static final Logger logger = LoggerFactory.getLogger(HAStateManagerFactory.class);

    private HAStateManagerFactory() {
    }

    /**
     * Create an HAStateManager instance without initialization
     * Used for new initializeHA API where initialization happens separately
     *
     * @return HAStateManager instance
     */
    public static HAStateManager create() {
        try {
            // TODO: At the moment, only H2 is supported. Extend to support portgres.
            Class<?> h2Class = Class.forName(
                    "org.drools.ansible.rulebook.integration.ha.h2.H2StateManager"
            );
            HAStateManager manager = (HAStateManager) h2Class.getDeclaredConstructor().newInstance();
            logger.info("Created H2StateManager instance");
            return manager;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create H2StateManager: " + e.getMessage(), e);
        }
    }
}