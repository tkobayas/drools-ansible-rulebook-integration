package org.drools.ansible.rulebook.integration.ha.postgres;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for multi-host URL construction and target_session_attrs mapping.
 */
public class PostgreSQLMultiHostTest {

    // ── buildHostPortion ────────────────────────────────────────────────

    @Test
    public void testSingleHost() {
        assertEquals("server1:5432",
                PostgreSQLStateManager.buildHostPortion("server1", "5432"));
    }

    @Test
    public void testMultiHostSinglePort() {
        assertEquals("server1:5432,server2:5432",
                PostgreSQLStateManager.buildHostPortion("server1,server2", "5432"));
    }

    @Test
    public void testMultiHostMultiPort() {
        assertEquals("server1:5432,server2:5433",
                PostgreSQLStateManager.buildHostPortion("server1,server2", "5432,5433"));
    }

    @Test
    public void testMultiHostWithSpaces() {
        assertEquals("server1:5432,server2:5433",
                PostgreSQLStateManager.buildHostPortion(" server1 , server2 ", " 5432 , 5433 "));
    }

    @Test
    public void testThreeHostsSinglePort() {
        assertEquals("s1:5432,s2:5432,s3:5432",
                PostgreSQLStateManager.buildHostPortion("s1,s2,s3", "5432"));
    }

    @Test
    public void testThreeHostsThreePorts() {
        assertEquals("s1:5432,s2:5433,s3:5434",
                PostgreSQLStateManager.buildHostPortion("s1,s2,s3", "5432,5433,5434"));
    }

    @Test
    public void testMultiHostFewerPortsThanHosts() {
        // When fewer ports than hosts, last port is reused (libpq behavior)
        assertEquals("s1:5432,s2:5433,s3:5433",
                PostgreSQLStateManager.buildHostPortion("s1,s2,s3", "5432,5433"));
    }

    // ── mapTargetSessionAttrs ───────────────────────────────────────────

    @Test
    public void testMapTargetSessionAttrsAny() {
        assertEquals("any", PostgreSQLStateManager.mapTargetSessionAttrs("any"));
    }

    @Test
    public void testMapTargetSessionAttrsReadWrite() {
        assertEquals("primary", PostgreSQLStateManager.mapTargetSessionAttrs("read-write"));
    }

    @Test
    public void testMapTargetSessionAttrsPrimary() {
        assertEquals("primary", PostgreSQLStateManager.mapTargetSessionAttrs("primary"));
    }

    @Test
    public void testMapTargetSessionAttrsReadOnly() {
        assertEquals("secondary", PostgreSQLStateManager.mapTargetSessionAttrs("read-only"));
    }

    @Test
    public void testMapTargetSessionAttrsStandby() {
        assertEquals("secondary", PostgreSQLStateManager.mapTargetSessionAttrs("standby"));
    }

    @Test
    public void testMapTargetSessionAttrsPreferStandby() {
        assertEquals("preferSecondary", PostgreSQLStateManager.mapTargetSessionAttrs("prefer-standby"));
    }

    @Test
    public void testMapTargetSessionAttrsNull() {
        assertEquals("any", PostgreSQLStateManager.mapTargetSessionAttrs(null));
    }

    @Test
    public void testMapTargetSessionAttrsUnknown() {
        assertEquals("any", PostgreSQLStateManager.mapTargetSessionAttrs("unknown-value"));
    }
}
