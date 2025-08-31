/*
 * Copyright 2018 Federico Valeri.
 * Licensed under the Apache License 2.0 (see LICENSE file).
 */
package it.fvaleri.sqlrunner.examples;

import it.fvaleri.sqlrunner.QueryableStorage;
import it.fvaleri.sqlrunner.StorageConfig;
import it.fvaleri.sqlrunner.jdbc.JdbcQueryableStorage;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Transactional batch processing example.
 * This is the recommended method when doing bulk writes.
 */
public class PaymentsExample {
    public static void main(String[] args) {
        long totalRows = 10_000;
        int batchSize = 100;
        StorageConfig config = StorageConfig.builder().maxStringParamLength(200).batchSize(batchSize).autoCommit(false).build();
        
        try (QueryableStorage storage = JdbcQueryableStorage.builder()
                .connectionFromUrl("jdbc:h2:mem:batch_test;INIT=runscript from 'classpath:/init.sql'")
                .queriesFromClasspath("payments.properties")
                .config(config)
                .build()) {
            PaymentsDao paymentsDao = new PaymentsDao(storage, batchSize);

            try {
                List<Payment> payments = Payment.Builder.buildSequential((int) totalRows, new BigDecimal(100));
                for (Payment payment : payments) {
                    paymentsDao.insert(payment);
                }
                storage.commit();
                System.out.println("Bulk payments store completed");
            } catch (Exception e) {
                storage.rollback();
                System.out.println("Bulk payments store rolled back due to: " + e.getMessage());
            }

            System.out.println("Sample records:");
            System.out.println("Record 1: " + paymentsDao.findByPk(1));
            System.out.println("Record 100: " + paymentsDao.findByPk(100));
            System.out.println("Record 1000: " + paymentsDao.findByPk(1_000));
            System.out.println("Record 10000: " + paymentsDao.findByPk(10_000));

            System.out.println("Statistics:");
            System.out.println("Total records: " + paymentsDao.count());
            System.out.println("Total amount: " + paymentsDao.getTotalAmount());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class PaymentsDao {
        private QueryableStorage storage;
        private int batchSize;

        public PaymentsDao(QueryableStorage storage, int batchSize) {
            this.storage = storage;
            this.batchSize = batchSize;
        }

        public int insert(Payment payment) {
            if (payment == null) {
                throw new IllegalArgumentException("Invalid payment");
            }
            return storage.write(
                    "payments.insert",
                    List.of(
                            payment.paCode(),
                            payment.paIntCode(),
                            payment.paAmount(),
                            payment.paDate(),
                            payment.paChCode(),
                            payment.paState(),
                            payment.paAccount(),
                            payment.paType()
                    ),
                    batchSize
            );
        }

        public Payment findByPk(long key) {
            if (key == 0) {
                throw new IllegalArgumentException("Invalid key");
            }
            return storage.readSingle("payments.find.by.pk", List.of(String.valueOf(key)))
            .map(row -> new Payment(
                    (Long) row.get(0),
                    (Long) row.get(1),
                    (BigDecimal) row.get(2),
                    ((java.sql.Date) row.get(3)).toLocalDate(),
                    (Long) row.get(4),
                    (Long) row.get(5),
                    (String) row.get(6),
                    (String) row.get(7)
            ))
            .orElse(null);
        }

        public long count() {
            return storage.<Long>readSingleValue("payments.count").orElse(0L);
        }

        public BigDecimal getTotalAmount() {
            return storage.<BigDecimal>readSingleValue("payments.total.amount").orElse(BigDecimal.ZERO);
        }
    }

    record Payment(
            long paCode,
            long paIntCode,
            BigDecimal paAmount,
            LocalDate paDate,
            long paChCode,
            long paState,
            String paAccount,
            String paType) {
        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private long paCode;
            private long paIntCode = 1;
            private BigDecimal paAmount;
            private LocalDate paDate;
            private long paChCode = 1;
            private long paState = 1;
            private String paAccount = "000123456";
            private String paType = "AAA";

            public Builder paCode(long paCode) {
                this.paCode = paCode;
                return this;
            }

            public Builder paIntCode(long paIntCode) {
                this.paIntCode = paIntCode;
                return this;
            }

            public Builder paAmount(BigDecimal paAmount) {
                this.paAmount = paAmount;
                return this;
            }

            public Builder paDate(LocalDate paDate) {
                this.paDate = paDate;
                return this;
            }

            public Builder paChCode(long pagChCode) {
                this.paChCode = pagChCode;
                return this;
            }

            public Builder paState(long paState) {
                this.paState = paState;
                return this;
            }

            public Builder paAccount(String paAccount) {
                this.paAccount = paAccount;
                return this;
            }

            public Builder paType(String paType) {
                this.paType = paType;
                return this;
            }

            /**
             * Set today as payment date.
             */
            public Builder withToday() {
                this.paDate = LocalDate.now();
                return this;
            }

            /**
             * Create a standard payment with common defaults.
             */
            public Builder withStandardDefaults() {
                this.paIntCode = 1;
                this.paChCode = 1;
                this.paState = 1;
                this.paAccount = "000123456";
                this.paType = "AAA";
                this.paDate = LocalDate.now();
                return this;
            }

            /**
             * Create multiple payments with sequential codes.
             */
            public static List<Payment> buildSequential(int count, BigDecimal amount) {
                return IntStream.rangeClosed(1, count)
                        .mapToObj(i -> Payment.builder()
                                .paCode(i)
                                .paAmount(amount)
                                .withStandardDefaults()
                                .build())
                        .toList();
            }

            public Payment build() {
                if (paCode <= 0) {
                    throw new IllegalArgumentException("Payment code must be positive");
                }
                if (paAmount == null || paAmount.compareTo(BigDecimal.ZERO) <= 0) {
                    throw new IllegalArgumentException("Payment amount must be positive");
                }
                if (paDate == null) {
                    paDate = LocalDate.now();
                }
                if (paAccount == null || paAccount.trim().isEmpty()) {
                    throw new IllegalArgumentException("Payment channel is required");
                }
                if (paType == null || paType.trim().isEmpty()) {
                    throw new IllegalArgumentException("Payment type is required");
                }
                return new Payment(paCode, paIntCode, paAmount, paDate,
                        paChCode, paState, paAccount, paType);
            }
        }
    }
}
