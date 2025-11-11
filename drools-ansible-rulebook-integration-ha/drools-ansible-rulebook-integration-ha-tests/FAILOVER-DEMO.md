# EDA HA Failover Demo

This demo shows two nodes in an HA cluster with real failover simulation.

## Prerequisites

1. Start PostgreSQL:
   ```bash
   cd drools-ansible-rulebook-integration-ha/drools-ansible-rulebook-integration-ha-tests
   docker-compose up -d
   ```

2. Clean the database (optional, for fresh start):
   ```bash
   mvn exec:java -pl drools-ansible-rulebook-integration-ha/drools-ansible-rulebook-integration-ha-tests \
     -Dexec.mainClass="org.drools.ansible.rulebook.integration.ha.examples.CleanPostgreSQL" \
     -Dexec.classpathScope=test
   ```

## Running the Demo

### Terminal 1: Start Node-1 (Leader)

```bash
mvn exec:java -pl drools-ansible-rulebook-integration-ha/drools-ansible-rulebook-integration-ha-tests \
  -Dexec.mainClass="org.drools.ansible.rulebook.integration.ha.examples.PostgreSQLFailoverExampleNode1" \
  -Dexec.classpathScope=test
```

**What Node-1 does:**
- Initializes HA with cluster UUID "two-node-cluster"
- Becomes the leader
- **Step 1**: Sends 2 separate events:
  - Event 1: temperature=35 (partial match)
  - Event 2: humidity=55 (completes the match → creates match + action)
- **Step 2**: Sends 1 event with temperature=40 only (partial match, waiting for humidity)
- **Step 3**: Waits indefinitely (until you press Ctrl+C to simulate crash)

### Terminal 2: Start Node-2 (Standby)

```bash
mvn exec:java -pl drools-ansible-rulebook-integration-ha/drools-ansible-rulebook-integration-ha-tests \
  -Dexec.mainClass="org.drools.ansible.rulebook.integration.ha.examples.PostgreSQLFailoverExampleNode2" \
  -Dexec.classpathScope=test
```

**What Node-2 does:**
- Initializes HA with same cluster UUID "two-node-cluster"
- Runs as STANDBY (not a leader)
- Waits for your signal (press Enter) to become leader
- **Step 4**: Becomes leader and recovers state (including partial match from Node-1)
- **Step 5**: Extracts matching_uuid from async recovery messages
- **Step 6**: Manages recovered action (get/update/delete) - demonstrates action continuation after failover
- **Step 7**: Sends humidity=60 event to complete the partial match from Node-1

### Simulate Failover

1. **In Terminal 1**: Press `Ctrl+C` to stop Node-1 (simulates crash/failure)
   - Node-1 shuts down
   - 1 completed MatchingEvent with action remains in PostgreSQL
   - 1 partial match (temperature=40, waiting for humidity) persisted in SessionState

2. **In Terminal 2**: Press `Enter` to trigger Node-2 takeover
   - Node-2 becomes the leader
   - Recovers SessionState from PostgreSQL (including the partial match!)
   - Dispatches pending MatchingEvent via async channel
   - Manages the recovered action (get/update/delete)
   - Sends humidity=60 event which completes the partial match started by Node-1

## What to Observe

### Node-1 Output (before failure):
```
[Node-1] ✓ I am now the LEADER

[Node-1] Step 1: Sending 2 events that create a match...
[Node-1] ✓ Event 1 processed (temperature=35)
[Node-1]   No Match yet: []
[Node-1] ✓ Event 2 processed (humidity=55)
[Node-1] ✓ MATCH! matching_uuid: <uuid2>
[Node-1] ✓ Action added (status=1: started)

[Node-1] Step 2: Sending 1 event that creates partial match...
[Node-1] ✓ Event 3 processed (temperature=40, no humidity)
[Node-1] ✓ No match (partial match stored in session state)
[Node-1]   Waiting for humidity event to complete the match...

[Node-1] Current HA Stats: {"current_leader":"node-1","leader_switches":1,...}

[Node-1] Step 3: Ready to simulate failure...
```

### Node-2 Output (after takeover):
```
[Node-2] Step 4: Node-1 failure detected! Taking over as LEADER...
[Node-2] ✓ I am now the LEADER
[Node-2] ✓ SessionState recovered from PostgreSQL (including partial match)
[Node-2] ✓ Pending MatchingEvents dispatched via async channel

[Node-2] Step 5: Checking async recovery messages...
[Node-2] Async messages received: 1
[Node-2]   Message 1: {matching_uuid:<uuid1>, ...}
[Node-2] ✓ Extracted matching_uuid from recovery: <uuid1>

[Node-2] HA Stats after takeover: {"current_leader":"node-2","leader_switches":2,...}

[Node-2] Step 6: Managing recovered action...
[Node-2] ✓ Retrieved existing action: {"action":"send_alert","status":1,...}
[Node-2] ✓ Updated action (status=3: success)
[Node-2] ✓ Action completed and deleted

[Node-2] Step 7: Sending humidity event to complete partial match...
[Node-2] ✓ Event 3 processed (humidity=60)
[Node-2] ✓ MATCH! Partial match completed!
[Node-2]   matching_uuid: <uuid3>
[Node-2]   (temperature=40 from Node-1 + humidity=60 from Node-2)
[Node-2] ✓ Action added (status=3: success)
[Node-2] ✓ Action completed and deleted
```

**Key observations:**
- **Step 1**: Node-1 sends 2 separate events (temperature=35, then humidity=55) → Creates full match
- **Step 2**: Node-1 sends 1 event (temperature=40 only) → Creates partial match stored in SessionState
- **Step 3**: Node-1 crashes with partial match persisted in SessionState
- **Step 4**: Node-2 becomes leader and recovers state (including the partial match!)
- **Step 5**: Node-2 receives the pending MatchingEvent from Step 1
- **Step 6**: Node-2 continues action management (get/update/delete) from where Node-1 left off
- **Step 7**: Node-2 sends humidity=60 event → Completes the partial match from Step 2!
- The partial match **survives failover** and completes on the new leader!
- Leader switches from "node-1" to "node-2"
- `leader_switches` increments from 1 to 2

## Verify in PostgreSQL

```bash
docker exec -it eda-ha-postgres psql -U eda_user -d eda_ha_db
```

```sql
-- View session state versions
SELECT id, ha_uuid, rule_set_name, version, leader_id, persisted_time
FROM SessionState
ORDER BY version;

-- View matching events (should be empty after Node-2 completes all actions)
SELECT me_uuid, rule_set_name, rule_name
FROM MatchingEvent;

-- View action info (should be empty after Node-2 completes all actions)
SELECT * FROM ActionInfo;

-- View HA statistics
SELECT * FROM HAStats;
```

**Expected results:**
- SessionState: Multiple versions showing updates from both Node-1 and Node-2
  - Node-1 versions: After Step 1 (full match) and Step 2 (partial match)
  - Node-2 versions: After Step 6 (action completion) and Step 7 (partial match completion)
- MatchingEvent: Empty (all actions completed and deleted by Node-2)
- ActionInfo: Empty (all actions completed and deleted by Node-2)
- HAStats: `current_leader="node-2"`, `leader_switches=2`

## Architecture

```
┌─────────────┐                ┌─────────────┐
│   Node-1    │                │   Node-2    │
│  (Leader)   │                │  (Standby)  │
└─────┬───────┘                └─────┬───────┘
      │                              │
      │    Both connect to same      │
      └──────────────┬───────────────┘
                     │
              ┌──────▼──────┐
              │  PostgreSQL │
              │   (Shared)  │
              └─────────────┘
```

**Failover Flow (7 Steps):**
1. **Node-1 (Leader)**: Sends 2 events separately:
   - Event 1: temperature=35 (partial match)
   - Event 2: humidity=55 (completes match → full match created, action added)
2. **Node-1 (Leader)**: Sends event with temperature=40 only → Partial match stored in SessionState
3. **Node-1**: Crashes (Ctrl+C) - partial match and pending action persist in PostgreSQL
4. **Node-2**: Detects failure (Enter key), calls `enableLeader()` which:
   - Recovers SessionState from PostgreSQL (including partial match from Step 2!)
   - Dispatches pending MatchingEvent from Step 1 via async channel
5. **Node-2 (New Leader)**: Extracts matching_uuid from async recovery message
6. **Node-2 (New Leader)**: Manages recovered action from Step 1 (get/update/delete) - continues where Node-1 left off
7. **Node-2 (New Leader)**: Sends humidity=60 event → Completes the partial match from Step 2!

## Cleanup

Stop both nodes:
- Terminal 1: `Ctrl+C` (if still running)
- Terminal 2: `Ctrl+C`

Stop PostgreSQL:
```bash
docker-compose down
```

To remove data:
```bash
docker-compose down -v
```
