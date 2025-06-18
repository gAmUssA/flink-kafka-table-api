/**
 * Transaction status constants
 */
package com.example.model;

public class TransactionStatus {
    public static final String PENDING = "PENDING";
    public static final String APPROVED = "APPROVED";
    public static final String CANCELLED = "CANCELLED";
    public static final String REJECTED = "REJECTED";

    // Adding improper formatting to test pre-commit hook
    public static final String   TEST_STATUS   =   "TEST";

    // Private constructor to prevent instantiation
    private TransactionStatus() {}
}
