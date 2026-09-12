package io.github.balchua.datafusion.bench;

/**
 * Benchmark queries (spec §21). Each entry carries its SQL so that every output row
 * is self-describing: the CSV/JSON contain the full SQL text, and the console prints it.
 * Add new queries as new enum entries.
 */
public enum BenchmarkQuery {
    Q1_PROJECTION("q1_projection",
            "SELECT transaction_id FROM transactions"),
    Q2_NESTED_PROJECTION("q2_nested_projection",
            "SELECT transaction_id, debtor.name AS debtor_name FROM transactions"),
    Q3_NESTED_FILTER("q3_nested_filter",
            "SELECT transaction_id FROM transactions "
                    + "WHERE debtor.name = '" + SQL_CONSTANTS.MATCH + "'"),
    Q4_DEEP_NESTED_FILTER("q4_deep_nested_filter",
            "SELECT transaction_id FROM transactions "
                    + "WHERE debtor.account.iban = '" + SQL_CONSTANTS.IBAN + "'"),
    Q5_AGGREGATION("q5_aggregation",
            "SELECT currency, COUNT(*) AS n, SUM(amount) AS total "
                    + "FROM transactions GROUP BY currency"),
    Q6_JOIN("q6_join_three_tables",
            "SELECT m.message_id, r.remittance_id, COUNT(t.transaction_id) AS txn_count "
                    + "FROM payment_message m "
                    + "JOIN remittances r ON r.message_id = m.message_id "
                    + "JOIN transactions t ON t.remittance_id = r.remittance_id "
                    + "GROUP BY m.message_id, r.remittance_id"),
    Q7_VALIDATION("q7_remittance_control_validation",
            // Per-remittance check: transaction count and SUM(amount) must equal the
            // remittance's declared number_of_transactions and control_sum (pain.001 CtrlSum).
            "SELECT r.remittance_id AS rid, "
                    + "r.number_of_transactions AS declared_count, "
                    + "COUNT(t.transaction_id) AS actual_count, "
                    + "r.control_sum AS declared_sum, "
                    + "SUM(t.amount) AS actual_sum, "
                    + "(COUNT(t.transaction_id) = r.number_of_transactions "
                    + " AND SUM(t.amount) = r.control_sum) AS balanced "
                    + "FROM remittances r LEFT JOIN transactions t "
                    + "ON t.remittance_id = r.remittance_id "
                    + "GROUP BY r.remittance_id, r.number_of_transactions, r.control_sum"),
    Q8_JSON_SCAN("q8_json_column_scan",
            "SELECT transaction_id FROM transactions "
                    + "WHERE regulatory_reporting LIKE '%Dmstcd%'"),
    Q9_DEEP_STRUCT_PATH("q9_ultimate_party_filter",
            "SELECT transaction_id FROM transactions "
                    + "WHERE ultimate_debtor.account.iban = 'FR1400000430000000000'"),
    Q10_CONTROL_SUM_MISMATCHES("q10_control_sum_mismatches",
            // Validation summary: how many remittances fail the control-sum/count check.
            // Zero on a well-formed payment; non-zero flags the offending remittances.
            "SELECT COUNT(*) AS mismatched_remittances FROM ("
                    + "SELECT r.remittance_id FROM remittances r "
                    + "LEFT JOIN transactions t ON t.remittance_id = r.remittance_id "
                    + "GROUP BY r.remittance_id, r.number_of_transactions, r.control_sum "
                    + "HAVING COUNT(t.transaction_id) <> r.number_of_transactions "
                    + "OR SUM(t.amount) <> r.control_sum) x");

    private static final class SQL_CONSTANTS {
        static final String MATCH = "BBB";
        static final String IBAN = "DE02999999990000009999";
    }

    public final String name;
    public final String sql;
    /** One-line human explanation, printed by the harness. */
    public final String description;

    BenchmarkQuery(String name, String sql) {
        this.name = name;
        this.sql = sql;
        this.description = descriptionFor(name);
    }

    private static String descriptionFor(String name) {
        return switch (name) {
            case "q1_projection" -> "projection: reads one plain column from every row";
            case "q2_nested_projection" -> "nested projection: reads a field inside a Struct (debtor.name)";
            case "q3_nested_filter" -> "nested filter: WHERE on a struct field (debtor.name = 'BBB')";
            case "q4_deep_nested_filter" -> "deep nested filter: WHERE debtor.account.iban = '…' (struct in struct)";
            case "q5_aggregation" -> "aggregation: COUNT(*) + SUM(amount) GROUP BY currency (exact Decimal128)";
            case "q6_join_three_tables" -> "join: payment_message x remittances x transactions on their keys";
            case "q7_remittance_control_validation" -> "validation report: per-remittance declared vs actual count/sum";
            case "q8_json_column_scan" -> "JSON column scan: LIKE over the regulatory_reporting JSON payload";
            case "q9_ultimate_party_filter" -> "deep struct path: WHERE ultimate_debtor.account.iban = '…' (3 struct levels)";
            case "q10_control_sum_mismatches" -> "control-sum validation: count remittances whose transaction count/sum differ from the declared control sum";
            default -> "SQL benchmark query";
        };
    }
}