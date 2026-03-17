# S01: RESP2 Parser and Serializer

**Status:** done

Implement a streaming RESP2 protocol parser and serializer. The parser must handle incremental data (TCP delivers arbitrary chunks), support pipelining (multiple commands in one buffer), and handle both multibulk and inline command formats. The serializer encodes Redis responses.

---

[← Back to S01](README.md)
