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
     * Database type is determined by system property "ha.db.type":
     * - "postgres" or "postgresql": Uses PostgreSQLStateManager
     * - "h2" or any other value: Uses H2StateManager (default)
     *
     * Example: java -Dha.db.type=postgres ...
     *
     * @return HAStateManager instance
     */
    public static HAStateManager create() {
        String haDbTypeEnvValue = System.getenv("DROOLS_HA_DB_TYPE");
        if (haDbTypeEnvValue != null && !haDbTypeEnvValue.isEmpty()) {
            // Environment variable takes precedence over system property
            System.setProperty("ha.db.type", haDbTypeEnvValue);
        }

        String dbType = System.getProperty("ha.db.type", "h2");

        try {
            String className;
            String managerName;

            if ("postgres".equalsIgnoreCase(dbType) || "postgresql".equalsIgnoreCase(dbType)) {
                className = "org.drools.ansible.rulebook.integration.ha.postgres.PostgreSQLStateManager";
                managerName = "PostgreSQLStateManager";
            } else {
                className = "org.drools.ansible.rulebook.integration.ha.h2.H2StateManager";
                managerName = "H2StateManager";
            }

            Class<?> managerClass = Class.forName(className);
            HAStateManager manager = (HAStateManager) managerClass.getDeclaredConstructor().newInstance();
            logger.info("Created {} instance (ha.db.type={})", managerName, dbType);
            return manager;
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to find HAStateManager implementation for type '" + dbType + "': " + e.getMessage() +
                ". Make sure the appropriate module (ha-h2 or ha-postgres) is on the classpath.", e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create HAStateManager for type '" + dbType + "': " + e.getMessage(), e);
        }
    }
}