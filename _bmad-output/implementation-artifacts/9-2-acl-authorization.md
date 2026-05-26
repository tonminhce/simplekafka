# Story 9.2: ACL Authorization

Status: backlog

## Story

As a broker operator,
I want to define ACLs that control which authenticated users can produce to or consume from specific topics,
so that access is restricted on a per-topic, per-operation basis.

## Acceptance Criteria

1. **Given** an ACL rule `user1 ALLOW WRITE on topic "orders"`
   **When** `user1` sends a Produce request for topic "orders"
   **Then** the request is processed normally

2. **Given** an ACL rule `user1 ALLOW WRITE on topic "orders"`
   **When** `user1` sends a Produce request for topic "payments"
   **Then** the request is rejected with `TOPIC_AUTHORIZATION_FAILED` (error 29)

3. **Given** an ACL rule `user2 ALLOW READ on topic "orders"`
   **When** `user2` sends a Fetch request for topic "orders"
   **Then** the request is processed normally

4. **Given** an ACL rule `user2 ALLOW READ on topic "orders"`
   **When** `user2` sends a Produce request for topic "orders"
   **Then** the request is rejected with `TOPIC_AUTHORIZATION_FAILED` (error 29)

5. **Given** a super-user `admin`
   **When** `admin` sends any request
   **Then** all operations are allowed regardless of ACL rules

6. **Given** no ACL rules exist (empty ACL file)
   **When** any authenticated user sends a request
   **Then** all operations are allowed (backward compatible — no ACL = open)

## Technical Requirements

### From IMPLEMENTATION_PLAN.md (Phase 9, Step 38)

- ACL storage and lookup in `shared/auth/`
- Per-request authorization check

### From NFRs

- **DRY:** Reuse `AuthPrincipal` from Story 9.1 for identity checks
- **SOLID:** `AclAuthorizer` is single responsibility — evaluate permissions. `AclEntry` is a value object.
- **DRY:** Authorization check runs once per request in `dispatchRequest()` — not duplicated in each handler

## Tasks / Subtasks

- [ ] Task 1: Create ACL data model (AC: #1, #2, #3, #4)
  - [ ] Subtask 1.1: Create `shared/auth/AclEntry.java` — value object: principal, permission (ALLOW/DENY), operation (READ/WRITE/CREATE/DESCRIBE), resource (topic name or `*`)
  - [ ] Subtask 1.2: Create `shared/auth/AclPermission.java` enum — `ALLOW`, `DENY`
  - [ ] Subtask 1.3: Create `shared/auth/AclOperation.java` enum — `READ`, `WRITE`, `CREATE`, `DESCRIBE`, `ALL`
- [ ] Task 2: Create AclAuthorizer (AC: #1, #2, #3, #4, #5, #6)
  - [ ] Subtask 2.1: Create `shared/auth/AclAuthorizer.java`
  - [ ] Subtask 2.2: `loadAcls(File)` — parse ACL file into list of `AclEntry`
  - [ ] Subtask 2.3: `isAuthorized(principal, operation, resource)` → boolean
  - [ ] Subtask 2.4: Super-user check (principals in `super.users` config)
  - [ ] Subtask 2.5: Empty ACL = allow all (AC: #6)
  - [ ] Subtask 2.6: Wildcard resource `*` matches any topic
- [ ] Task 3: Integrate authorization into dispatchRequest (AC: #1, #2)
  - [ ] Subtask 3.1: After authentication, determine operation from API key:
    - API key 0 (Produce) → WRITE
    - API key 1 (Fetch) → READ
    - API key 75 (DescribeTopicPartitions) → DESCRIBE
    - API key 18 (ApiVersions) → no auth required
  - [ ] Subtask 3.2: Extract resource (topic name) from request body
  - [ ] Subtask 3.3: Call `AclAuthorizer.isAuthorized()`
  - [ ] Subtask 3.4: Return error 29 or 31 if not authorized
- [ ] Task 4: Create ACL file format (AC: #1, #5)
  - [ ] Subtask 4.1: Define `{log.dirs}/acl.properties` format
  - [ ] Subtask 4.2: Support hot-reload on file change
- [ ] Task 5: Add test for ACL authorization (AC: #1, #2, #3, #4, #5, #6)
  - [ ] Subtask 5.1: Test ALLOW WRITE → produce succeeds
  - [ ] Subtask 5.2: Test no WRITE ACL → produce rejected
  - [ ] Subtask 5.3: Test super-user → all operations allowed
  - [ ] Subtask 5.4: Test empty ACL → all operations allowed

## Dev Notes

### Package Structure

```
shared/auth/
  AclEntry.java          — value object: principal + permission + operation + resource
  AclPermission.java     — enum: ALLOW, DENY
  AclOperation.java      — enum: READ, WRITE, CREATE, DESCRIBE, ALL
  AclAuthorizer.java     — evaluate permissions against ACL rules
```

### ACL File Format

```properties
# acl.properties — one rule per line
# Format: principal permission operation resource

admin ALLOW ALL *
producer1 ALLOW WRITE orders
producer1 ALLOW WRITE payments
consumer1 ALLOW READ orders
consumer1 ALLOW DESCRIBE *
```

### Authorization Check Flow

```java
// In SimpleKafkaBroker.dispatchRequest()
AuthPrincipal principal = connection.getPrincipal();
if (principal != null && aclAuthorizer != null) {
    AclOperation operation = mapApiKeyToOperation(header.getApiKey());
    if (operation != null) {
        String resource = extractResource(requestBody, header.getApiKey());
        if (!aclAuthorizer.isAuthorized(principal, operation, resource)) {
            short errorCode = (operation == AclOperation.CREATE || header.getApiKey() == 18)
                ? ErrorCodes.CLUSTER_AUTHORIZATION_FAILED
                : ErrorCodes.TOPIC_AUTHORIZATION_FAILED;
            return buildErrorResponse(header, errorCode);
        }
    }
}
```

### Config Properties

```properties
# ACL Configuration
acl.enabled=true
acl.file=/etc/kafka/acl.properties
super.users=admin
```

## File List

- `src/main/java/com/simplekafka/shared/auth/AclEntry.java` — NEW
- `src/main/java/com/simplekafka/shared/auth/AclPermission.java` — NEW
- `src/main/java/com/simplekafka/shared/auth/AclOperation.java` — NEW
- `src/main/java/com/simplekafka/shared/auth/AclAuthorizer.java` — NEW
- `src/main/java/com/simplekafka/shared/Config.java` — MODIFY (ACL config fields)
- `src/main/java/com/simplekafka/broker/SimpleKafkaBroker.java` — MODIFY (authorization check in dispatch)
- `src/test/java/com/simplekafka/AclAuthorizationTest.java` — NEW
