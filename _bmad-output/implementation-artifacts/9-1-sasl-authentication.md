# Story 9.1: SASL Authentication

Status: backlog

## Story

As a broker operator,
I want clients to authenticate via SASL/PLAIN before making requests,
so that only authorized clients can produce and consume messages.

## Acceptance Criteria

1. **Given** a broker configured with `sasl.enabled=true` and `sasl.mechanism=PLAIN`
   **When** a client connects without authentication
   **Then** all requests are rejected with `SASL_AUTHENTICATION_FAILED` (error 58)

2. **Given** a broker with SASL enabled and a credentials file with `user1:password1`
   **When** a client sends valid SASL/PLAIN credentials (username=`user1`, password=`password1`)
   **Then** the connection is authenticated and subsequent requests are processed

3. **Given** a broker with SASL enabled
   **When** a client sends invalid credentials
   **Then** the connection is rejected with `SASL_AUTHENTICATION_FAILED` and the socket is closed

4. **Given** a broker with `sasl.enabled=false` (default)
   **When** any client connects
   **Then** requests are processed without authentication (backward compatible)

5. **Given** an authenticated connection
   **When** `ApiVersions` request is received
   **Then** it is processed without requiring authentication (pre-auth handshake)

## Technical Requirements

### From IMPLEMENTATION_PLAN.md (Phase 9, Step 37)

- Authentication framework in `shared/auth/`
- Plain/SASL mechanism support

### From NFRs

- **DRY:** Auth logic lives in `shared/auth/` — reusable across all handlers
- **SOLID:** `Authenticator` interface — single responsibility, pluggable mechanisms
- **Backward compatible:** SASL disabled by default

## Tasks / Subtasks

- [ ] Task 1: Create authentication framework (AC: #1, #4)
  - [ ] Subtask 1.1: Create `shared/auth/Authenticator.java` interface with `authenticate(credentials) → boolean`
  - [ ] Subtask 1.2: Create `shared/auth/SaslServerAuthenticator.java` — manages SASL handshake per connection
  - [ ] Subtask 1.3: Add `SaslConfig` section to `Config.java` (`sasl.enabled`, `sasl.mechanism`, `sasl.credentials.file`)
- [ ] Task 2: Implement SASL/PLAIN mechanism (AC: #2, #3)
  - [ ] Subtask 2.1: Create `shared/auth/PlainSaslProvider.java`
  - [ ] Subtask 2.2: Parse credentials file format: `username:password` per line
  - [ ] Subtask 2.3: Validate username/password against loaded credentials
  - [ ] Subtask 2.4: Cache credentials in memory, reload on file change
- [ ] Task 3: Integrate SASL handshake into connection handling (AC: #1, #2, #5)
  - [ ] Subtask 3.1: In `SimpleKafkaBroker.handleClient()`, check if SASL is enabled
  - [ ] Subtask 3.2: If enabled, perform SASL handshake before dispatching requests
  - [ ] Subtask 3.3: `ApiVersions` request bypasses auth (AC: #5)
  - [ ] Subtask 3.4: Attach `AuthenticatedPrincipal` to the connection session
- [ ] Task 4: Create credentials file format (AC: #2)
  - [ ] Subtask 4.1: Define `{log.dirs}/credentials.properties` format
  - [ ] Subtask 4.2: Support plain text and optionally hashed passwords
- [ ] Task 5: Add test for SASL authentication (AC: #1, #2, #3, #4)
  - [ ] Subtask 5.1: Test valid credentials → authenticated
  - [ ] Subtask 5.2: Test invalid credentials → rejected
  - [ ] Subtask 5.3: Test SASL disabled → no auth required
  - [ ] Subtask 5.4: Test ApiVersions bypasses auth

## Dev Notes

### Package Structure

```
shared/auth/
  Authenticator.java          — interface: boolean authenticate(String mechanism, byte[] credentials)
  SaslServerAuthenticator.java — manages SASL handshake per connection
  PlainSaslProvider.java       — SASL/PLAIN mechanism implementation
  AuthPrincipal.java           — value object: authenticated user identity
```

### SASL/PLAIN Handshake Flow

```
Client                              Broker
  |                                    |
  |--- ApiVersions (no auth) --------->|  (allowed without auth)
  |<-- ApiVersions Response -----------|
  |                                    |
  |--- SaslHandshake (PLAIN) --------->|
  |<-- SaslHandshake Response ---------|
  |                                    |
  |--- SASL tokens (username\0password)|
  |<-- Auth success/failure -----------|
  |                                    |
  |--- Produce/Fetch (authenticated) ->|  (now allowed)
```

### Config Properties

```properties
# SASL Configuration
sasl.enabled=true
sasl.mechanism=PLAIN
sasl.credentials.file=/etc/kafka/credentials.properties
```

### Credentials File Format

```properties
# credentials.properties
admin=admin-secret
producer=producer-secret
consumer=consumer-secret
```

## File List

- `src/main/java/com/simplekafka/shared/auth/Authenticator.java` — NEW
- `src/main/java/com/simplekafka/shared/auth/SaslServerAuthenticator.java` — NEW
- `src/main/java/com/simplekafka/shared/auth/PlainSaslProvider.java` — NEW
- `src/main/java/com/simplekafka/shared/auth/AuthPrincipal.java` — NEW
- `src/main/java/com/simplekafka/shared/Config.java` — MODIFY (SASL config fields)
- `src/main/java/com/simplekafka/broker/SimpleKafkaBroker.java` — MODIFY (auth handshake)
- `src/test/java/com/simplekafka/SaslAuthTest.java` — NEW
