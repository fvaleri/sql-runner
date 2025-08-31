/*
 * Copyright 2018 Federico Valeri.
 * Licensed under the Apache License 2.0 (see LICENSE file).
 */
package it.fvaleri.sqlrunner.examples;

import it.fvaleri.sqlrunner.QueryableStorage;
import it.fvaleri.sqlrunner.jdbc.JdbcQueryableStorage;

import java.util.List;

import static java.lang.String.format;

/**
 * Simple example.
 */
public class UsersExample {
    public static void main(String[] args) {
        try {
            try (QueryableStorage storage = JdbcQueryableStorage.builder()
                    .connectionFromUrl("jdbc:h2:mem:test;INIT=runscript from 'classpath:/init.sql'")
                    .queriesFromClasspath("users.properties")
                    .build()) {
                UsersDao usersDao = new UsersDao(storage);

                User user0 = User.builder()
                        .userid("user0")
                        .withDefaultPassword()
                        .withExampleEmail()
                        .build();
                
                User user1 = User.builder()
                        .userid("user1")
                        .password("changeit")
                        .email("user1@foo.com")
                        .build();
                
                User user2 = User.builder()
                        .userid("user2")
                        .withDefaultPassword()
                        .withExampleEmail()
                        .build();

                usersDao.insert(user0);
                System.out.println("Created: " + usersDao.findByPk("user0"));

                usersDao.insert(user1);
                usersDao.insert(user2);
                System.out.println("Users: " + usersDao.findAll());
                System.out.println("Count: " + usersDao.count());

                user0.password("secret");
                usersDao.update(user0);
                System.out.println("User 0 exists: " + usersDao.exists("user0"));

                usersDao.delete(user2.userid());
                System.out.println("User 2 exists after deletion: " + usersDao.exists("user2"));

                System.out.print("Search result: ");
                usersDao.findByEmailPattern("%@foo.com").forEach(System.out::println);

                System.out.println("Users: " + usersDao.findAll());
                System.out.println("Count: " + usersDao.count());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class UsersDao {
        private QueryableStorage storage;

        public UsersDao(QueryableStorage storage) {
            this.storage = storage;
        }

        public int insert(User user) {
            if (user == null) {
                throw new IllegalArgumentException("Invalid user");
            }
            return storage.write("users.insert", List.of(user.userid(), user.password(), user.email()));
        }

        public int update(User user) {
            if (user == null || user.userid() == null) {
                throw new IllegalArgumentException("Invalid user");
            }
            return storage.write("users.update", List.of(user.password(), user.email(), user.userid()));
        }

        public int delete(String userid) {
            if (userid == null) {
                throw new IllegalArgumentException("Invalid userid");
            }
            return storage.write("users.delete", List.of(userid));
        }

        public long count() {
            return storage.<Long>readSingleValue("users.count").orElse(0L);
        }

        public User findByPk(String userid) {
            if (userid == null) {
                throw new IllegalArgumentException("Invalid userid");
            }
            return storage.readSingle("users.find.by.pk", List.of(userid))
                    .map(row -> new User(
                            (String) row.get(0),
                            (String) row.get(1),
                            (String) row.get(2)
                    ))
                    .orElseThrow(() -> new RuntimeException(format("User %s not found", userid)));
        }

        public boolean exists(String userid) {
            return storage.readSingle("users.find.by.pk", List.of(userid)).isPresent();
        }

        public List<User> findAll() {
            return storage.readAsStream("users.find.all")
                    .map(row -> new User(
                            (String) row.get(0),
                            (String) row.get(1),
                            (String) row.get(2)
                    ))
                    .toList();
        }

        public List<User> findByEmailPattern(String pattern) {
            if (pattern == null) {
                throw new IllegalArgumentException("Pattern cannot be null");
            }
            return storage.readAsStream("user.search.by.email", List.of(pattern))
                    .map(row -> new User(
                            (String) row.get(0),
                            (String) row.get(1),
                            (String) row.get(2)
                    ))
                    .toList();
        }
    }

    static class User {
        private String userid;
        private String password;
        private String email;

        private User(String userid, String password, String email) {
            this.userid = userid;
            this.password = hashPassword(password);
            this.email = email;
        }

        public String userid() {
            return userid;
        }

        public void userid(String userid) {
            this.userid = userid;
        }

        public String password() {
            return password;
        }

        public void password(String password) {
            this.password = hashPassword(password);
        }

        public String email() {
            return email;
        }

        public void email(String email) {
            this.email = email;
        }

        private static String hashPassword(String password) {
            // TODO: use a password hashing library such as BCrypt
            return password;
        }

        public static boolean verifyPassword(String password, String hash) {
            // TODO: use a password hashing library such as BCrypt
            return password.equals(hash);
        }

        @Override
        public String toString() {
            return "User{" +
                    "userid='" + userid + '\'' +
                    ", password='" + password + '\'' +
                    ", email='" + email + '\'' +
                    '}';
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private String userid;
            private String rawPassword;
            private String email;

            public Builder userid(String userid) {
                this.userid = userid;
                return this;
            }

            public Builder password(String password) {
                this.rawPassword = password;
                return this;
            }

            public Builder email(String email) {
                this.email = email;
                return this;
            }

            public Builder withExampleEmail() {
                if (userid != null) {
                    this.email = userid + "@example.com";
                }
                return this;
            }

            public Builder withDefaultPassword() {
                this.rawPassword = "changeit";
                return this;
            }

            public User build() {
                if (userid == null || userid.trim().isEmpty()) {
                    throw new IllegalArgumentException("User ID is required");
                }
                if (rawPassword == null || rawPassword.trim().isEmpty()) {
                    throw new IllegalArgumentException("Password is required");
                }
                if (email == null || email.trim().isEmpty()) {
                    throw new IllegalArgumentException("Email is required");
                }
                return new User(userid, rawPassword, email);
            }
        }
    }
}
