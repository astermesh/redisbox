# T01: CLUSTER Commands

**Status:** done

Implement CLUSTER INFO (return cluster_enabled:0), CLUSTER MYID (return consistent node ID), CLUSTER KEYSLOT key (return correct CRC16 hash slot 0-16383), CLUSTER NODES, CLUSTER SLOTS, CLUSTER SHARDS, CLUSTER COUNTKEYSINSLOT, CLUSTER GETKEYSINSLOT, CLUSTER RESET, and remaining CLUSTER subcommands as stubs. CLUSTER KEYSLOT must compute CRC16 correctly.

## Acceptance Criteria

- CLUSTER INFO returns correct non-cluster info
- CLUSTER KEYSLOT computes correct hash slots

---

[← Back](README.md)
