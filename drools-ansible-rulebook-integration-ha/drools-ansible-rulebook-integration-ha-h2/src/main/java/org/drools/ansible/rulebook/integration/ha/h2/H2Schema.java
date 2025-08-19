package org.drools.ansible.rulebook.integration.ha.h2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * H2 database schema creation and management
 */
public class H2Schema {
    
    private static final Logger logger = LoggerFactory.getLogger(H2Schema.class);
    
    public static void createSchema(DataSource dataSource) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            
            // Create event state table (now contains matching events)
            String createEventStateTable = """
                CREATE TABLE IF NOT EXISTS eda_event_state (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    session_id VARCHAR(255) NOT NULL,
                    rulebook_hash VARCHAR(64),
                    partial_matching_events CLOB,
                    time_windows CLOB,
                    clock_time TIMESTAMP,
                    session_stats CLOB,
                    matching_events CLOB,
                    version INT DEFAULT 1,
                    is_current BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    leader_id VARCHAR(255),
                    UNIQUE(session_id, version)
                )
                """;
            stmt.execute(createEventStateTable);
            logger.debug("Created/verified eda_event_state table");
            
            // Create action state table
            String createActionStateTable = """
                CREATE TABLE IF NOT EXISTS eda_action_state (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    session_id VARCHAR(255) NOT NULL,
                    me_uuid VARCHAR(36) NOT NULL,
                    ruleset_name VARCHAR(255),
                    rule_name VARCHAR(255),
                    action_data CLOB,
                    status VARCHAR(50),
                    version INT DEFAULT 1,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(session_id, me_uuid, version)
                )
                """;
            stmt.execute(createActionStateTable);
            logger.debug("Created/verified eda_action_state table");
            
            // Create indexes for better query performance
            String createEventStateIndex = """
                CREATE INDEX IF NOT EXISTS idx_event_state_session 
                ON eda_event_state(session_id, is_current)
                """;
            stmt.execute(createEventStateIndex);
            
            String createActionStateIndex = """
                CREATE INDEX IF NOT EXISTS idx_action_state_session_me 
                ON eda_action_state(session_id, me_uuid)
                """;
            stmt.execute(createActionStateIndex);
            
            // Create session stats table
            String createSessionStatsTable = """
                CREATE TABLE IF NOT EXISTS eda_session_stats (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    session_id VARCHAR(255) NOT NULL,
                    stats_data CLOB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(session_id)
                )
                """;
            stmt.execute(createSessionStatsTable);
            logger.debug("Created/verified eda_session_stats table");
            
            conn.commit();
            logger.info("H2 schema creation completed successfully");
        }
    }
    
    public static void dropSchema(DataSource dataSource) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            
            stmt.execute("DROP TABLE IF EXISTS eda_action_state");
            stmt.execute("DROP TABLE IF EXISTS eda_event_state");
            stmt.execute("DROP TABLE IF EXISTS eda_session_stats");
            
            conn.commit();
            logger.info("H2 schema dropped successfully");
        }
    }
}