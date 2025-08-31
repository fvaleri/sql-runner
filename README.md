[![build](https://github.com/fvaleri/sql-runner/actions/workflows/main.yaml/badge.svg)](https://github.com/fvaleri/sql-runner/actions/workflows/main.yaml)
[![release](https://img.shields.io/github/release/fvaleri/sql-runner.svg)](https://github.com/fvaleri/sql-runner/releases/latest)
[![license](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)

# sql-runner

SQL Runner is a modern and efficient Java library for queryable storage.

## Features

- **Fluent Builder API** - Fluent configuration with embedded connection and query caching.
- **Batch Processing** - Batch processing of bulk writes with configurable sizes.
- **Transaction Support** - Transaction support with commit and rollback methods.
- **JDBC Implementation** - Thread-safe JDBC implementation with no runtime dependencies.

## Quick start

### Maven dependency

Add the Maven dependency to your project:

```xml
<dependency>
    <groupId>it.fvaleri.sqlrunner</groupId>
    <artifactId>jdbc</artifactId>
    <version>${sql-runnner.version}</version>
</dependency>
```

### Queries creation

Create a properties file with your fine-tuned SQL queries:

```bash
cat <<EOF >src/main/resources/users.properties
users.insert=insert into users (userid, password, email) values (?, ?, ?)
users.update=update users set password = ?, email = ? where userid = ?
users.delete=delete from users where userid = ?
users.find.all=select userid, password, email from users
users.find.by.pk=select userid, password, email from users where userid = ?
users.count=select count(1) from users
users.emails=select email from users
EOF
```

### Basic usage

Create the JDBC storage and run your write and read queries:

```java
import it.fvaleri.sqlrunner.QueryableStorage;
import it.fvaleri.sqlrunner.jdbc.JdbcQueryableStorage;

// Create JDBC storage.
try (QueryableStorage storage = JdbcQueryableStorage.builder()
        .connectionFromUrl("jdbc:postgresql://localhost:5432/mydb", "username", "password")
        .queriesFromClasspath("users.properties")
        .build()) {
        
    // Records write.
    int affected = storage.write(
        "users.insert", 
        List.of("user0", "changeit", "user0@example.com")
    );
    int affected = storage.write(
        "users.insert", 
        List.of("user1", "changeit", "user1@example.com")
    );   
    
    // Single record read with automatic type inference.
    Optional<User> user = storage.readSingle(
        "users.find.by.pk",
        List.of("user0")
     ).map(row -> new User(
         row.getValue(0, String.class),
         row.getValue(1, String.class), 
         row.getValue(2, String.class)
     ));
    
    // Stream-based processing with automatic type inference.
    List<User> users = storage.readAsStream("users.find.all")
        .map(row -> new User(
            row.getValue(0, String.class),
            row.getValue(1, String.class),
            row.getValue(2, String.class)
        )).toList();
    
    // Single value read.
    long count = storage.readSingleValue("users.count", Long.class).orElse(0L);
    
    // Column values read.
    List<String> emails = storage.readColumnValues("users.emails", String.class).toList();
}
```

## Build from source

Follow these instructions to build from source:

```bash
# Clone repository.
git clone git@github.com:fvaleri/sql-runner.git
cd sql-runner

# Build and test.
mvn install

# Run integration tests.
mvn failsafe:integration-test

# Run simple user management example.
mvn -pl examples exec:java -Dexec.mainClass="it.fvaleri.sqlrunner.examples.UsersExample"

# Run high-volume payments processing example.
mvn -pl examples exec:java -Dexec.mainClass="it.fvaleri.sqlrunner.examples.PaymentsExample"
```
