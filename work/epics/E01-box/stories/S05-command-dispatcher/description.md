# S05: Command Dispatcher

Central routing layer between parsed RESP commands and type engine handlers. Validates arity, checks client state (MULTI mode, subscribe mode), routes to correct handler, and handles sub-commands.

---

[← Back to S05](README.md)
