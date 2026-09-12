package io.github.balchua.datafusion.bench;

import io.github.balchua.datafusion.TestPayments;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.holders.TimeStampMilliHolder;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

/**
 * Synthetic benchmark data generator (spec §18). Generates the canonical payment tables
 * **directly into Arrow vectors** — no XML, no Java POJO graphs, deterministic content.
 *
 * <p>Stage separation (spec §22): {@code gen} computes column values into plain Java
 * arrays (this is where a production parser would decode XML), {@code build} writes them
 * into Arrow vectors, {@code batch} creates the VectorSchemaRoot (RecordBatch).
 *
 * <p>Profiles (spec §20): {@code normal} = canonical model + 6 structured extension
 * groups (~200 logical leaf fields); {@code wide} = 40 named domain groups (~2000 leaf
 * fields) resembling an expanded ISO 20022 structure. Extension leaves keep meaningful
 * nested names ({@code regulatory_reporting_g0.branch_3_counter}), not field0001.
 */
public final class SyntheticPaymentGenerator {

    public record SchemaStats(int topLevelColumns, int nestedStructFields, int leafFields) {}

    public record Timings(long genNanos, long buildNanos, long batchNanos) {}

    /** Dataset shape: messages x remittances-per-message x transactions-per-remittance. */
    public record Shape(int messages, int remittancesPerMessage, int transactionsPerRemittance) {
        public int remittances() { return messages * remittancesPerMessage; }
        public int transactions() { return (long) messages * remittancesPerMessage * transactionsPerRemittance > Integer.MAX_VALUE
                ? Integer.MAX_VALUE : messages * remittancesPerMessage * transactionsPerRemittance; }
    }

    public record Generated(
            VectorSchemaRoot message,
            VectorSchemaRoot remittances,
            VectorSchemaRoot transactions,
            Timings timings,
            SchemaStats transactionsStats) {}

    public static final String MESSAGE_ID = "MSG-SYNTH-1";
    public static final String MATCH_DEBTOR = "BBB";
    public static final String MATCH_IBAN = "DE02999999990000009999";

    private static final String[] DOMAIN_PREFIXES = {
            "regulatory_reporting", "tax_details", "charge_details", "fx_details",
            "sanctions_screening", "audit_trail", "party_identification", "settlement_info",
            "clearing_details", "return_reason", "remittance_extended", "invoice_reference",
            "creditor_reference", "mandate_details", "standing_order", "direct_debit",
            "card_instruction", "cheque_details", "cash_management", "corporate_action",
            "priority_instruction", "interbank_settlement", "customer_agreement",
            "regulatory_original", "compliance_check", "document_metadata", "enriched_party",
            "routing_metadata", "ledger_entry", "dispute_tracking", "fx_settlement",
            "reconciliation", "fraud_signal", "chargeback_data", "reversal_details",
            "deposit_instruction", "loan_reference", "guarantee_details", "insurance_data",
            "pension_reference"
    };

    private static final String[] CURRENCIES = {"EUR", "USD", "GBP", "CHF"};
    private static final String[] STATUSES = {"ACSC", "PDNG", "RJCT"};

    private SyntheticPaymentGenerator() {}

    /** debtor name cycles such that 'BBB' matches every 26th row offset by 1. */
    public static String partyName(int r) {
        char first = (char) ('A' + (r % 26));
        return "" + first + 'B' + 'B';
    }

    /** Transactions schema for a profile (canonical fields + extension groups). */
    public static Schema transactionSchema(String profile) {
        if (NarrowPain001.PROFILE.equals(profile)) {
            return NarrowPain001.transactionSchema();
        }
        List<Field> fields = new ArrayList<>(canonicalTransactionFields());
        if ("wide".equals(profile)) {
            // wide = canonical model + 40 domain expansion groups (~2,000 leaves)
            for (int g = 0; g < 40; g++) {
                fields.add(extensionGroup(DOMAIN_PREFIXES[g % DOMAIN_PREFIXES.length] + "_g" + g, 48));
            }
        } else {
            // normal = canonical model + six realistic payment-domain groups. These are
            // named ISO/payment blocks, not field0001-style filler; they bring the
            // transactions table above the requested 200 logical leaves.
            for (int g = 0; g < 6; g++) {
                fields.add(extensionGroup(DOMAIN_PREFIXES[g] + "_g" + g, 22));
            }
        }
        return new Schema(fields);
    }

    static List<Field> canonicalTransactionFields() {
        return io.github.balchua.datafusion.PaymentSchemas.transactionSchema().getFields();
    }

    /** {@code name: Struct{name_block: Struct{leaf...}}} with alternating leaf types. */
    static Field extensionGroup(String name, int leaves) {
        List<Field> inner = new ArrayList<>();
        for (int f = 0; f < leaves; f++) {
            String leaf = "branch_" + f + switch (f % 4) {
                case 0 -> "_code";
                case 1 -> "_status";
                case 2 -> "_reference";
                default -> "_counter";
            };
            inner.add(new Field(leaf, FieldType.nullable(leafType(f)), null));
        }
        Field block = new Field(name + "_block", FieldType.nullable(new ArrowType.Struct()), inner);
        return new Field(name, FieldType.nullable(new ArrowType.Struct()), List.of(block));
    }

    private static ArrowType leafType(int f) {
        return switch (f % 4) {
            case 1 -> new ArrowType.Int(32, true);
            case 3 -> new ArrowType.Int(64, true);
            default -> new ArrowType.Utf8();
        };
    }

    /** Logical field counting: top-level columns, nested struct fields, leaves (spec §20). */
    public static SchemaStats stats(Schema schema) {
        int[] counters = new int[3]; // top-level, nested struct fields, leaves
        for (Field f : schema.getFields()) {
            counters[0]++;
            countField(f, counters);
        }
        return new SchemaStats(counters[0], counters[1], counters[2]);
    }

    private static void countField(Field field, int[] counters) {
        if (field.getType() instanceof ArrowType.Struct) {
            counters[1]++;
            for (Field child : field.getChildren()) {
                countField(child, counters);
            }
        } else if (field.getType() instanceof ArrowType.List) {
            for (Field child : field.getChildren()) {
                countField(child, counters);
            }
        } else {
            counters[2]++;
        }
    }

    /** Back-compat: flat row count with the default 1 x (rows/25) x 25 shape. */
    public static Generated generate(String profile, int rows, BufferAllocator allocator) {
        return generate(new Shape(1, (rows + 24) / 25, 25), profile, allocator);
    }

    /** Full generation for a hierarchical shape: messages x remittances x transactions. */
    public static Generated generate(Shape shape, String profile, BufferAllocator allocator) {
        if (NarrowPain001.PROFILE.equals(profile)) {
            return generateNarrow(shape, allocator);
        }
        int rows = shape.transactions();
        long genStart = System.nanoTime();

        // ---------- gen: compute values into plain Java arrays ----------
        long[] amountCents = new long[rows];
        int[] currencyIdx = new int[rows];
        int[] statusIdx = new int[rows];
        int[] dayOffsets = new int[rows];
        byte[][] txnIds = new byte[rows][];
        byte[][] debtorIbans = new byte[rows][];
        for (int r = 0; r < rows; r++) {
            amountCents[r] = 1000L + r % 9000;
            currencyIdx[r] = r % CURRENCIES.length;
            statusIdx[r] = r % STATUSES.length;
            dayOffsets[r] = r % 30;
            txnIds[r] = ("TXN-" + r).getBytes();
            debtorIbans[r] = (r % 1000 == 0)
                    ? MATCH_IBAN.getBytes()
                    : ("DE" + (200000000 + r) + "0000000000").getBytes();
        }
        long genEnd = System.nanoTime();

        // ---------- build: write into Arrow vectors ----------
        long buildStart = System.nanoTime();
        Schema txnSchema = transactionSchema(profile);
        VectorSchemaRoot transactions = VectorSchemaRoot.create(txnSchema, allocator);
        io.github.balchua.datafusion.TestPayments.fillTransactions(
                transactions, "MSG-", rows, shape.transactionsPerRemittance(),
                shape.remittancesPerMessage());
        fillExtensions(transactions, profile, rows);
        long buildEnd = System.nanoTime();

        // ---------- batch: finalize the roots ----------
        long batchStart = System.nanoTime();
        transactions.setRowCount(rows);
        VectorSchemaRoot messageRoot = buildMessageRoot(shape, rows, allocator);
        VectorSchemaRoot remittanceRoot = buildRemittanceRoot(shape, rows, allocator);
        long batchEnd = System.nanoTime();

        return new Generated(messageRoot, remittanceRoot, transactions,
                new Timings(genEnd - genStart, buildEnd - buildStart, batchEnd - batchStart),
                stats(txnSchema));
    }

    /**
     * Narrow profile: flat pain.001-like schema matching the java-arrow-pain benchmark
     * (36 flat columns, no Struct/List/JSON). Used only for ingestion comparisons.
     */
    private static Generated generateNarrow(Shape shape, BufferAllocator allocator) {
        long buildStart = System.nanoTime();
        VectorSchemaRoot message = NarrowPain001.buildMessage(shape, allocator);
        VectorSchemaRoot remittances = NarrowPain001.buildRemittances(shape, allocator);
        VectorSchemaRoot transactions = NarrowPain001.buildTransactions(shape, allocator);
        long buildNanos = System.nanoTime() - buildStart;
        return new Generated(message, remittances, transactions,
                new Timings(0L, buildNanos, 0L), stats(transactions.getSchema()));
    }

    /** Writes all canonical transaction columns. */
    private static void fillCanonical(VectorSchemaRoot root, int rows,
                                      byte[][] txnIds, long[] amountCents,
                                      int[] currencyIdx, int[] statusIdx, int[] dayOffsets,
                                      byte[][] debtorIbans, Shape shape) {
        VarCharVector txnId = (VarCharVector) root.getVector("transaction_id");
        VarCharVector msgId = (VarCharVector) root.getVector("message_id");
        VarCharVector remId = (VarCharVector) root.getVector("remittance_id");
        VarCharVector e2e = (VarCharVector) root.getVector("end_to_end_id");
        DecimalVector amount = (DecimalVector) root.getVector("amount");
        VarCharVector currency = (VarCharVector) root.getVector("currency");
        StructVector debtor = (StructVector) root.getVector("debtor");
        VarCharVector debtorName = (VarCharVector) debtor.getChild("name");
        StructVector debtorAccount = (StructVector) debtor.getChild("account");
        VarCharVector debtorIban = (VarCharVector) debtorAccount.getChild("iban");
        VarCharVector debtorNumber = (VarCharVector) debtorAccount.getChild("number");
        VarCharVector debtorBic = (VarCharVector) debtor.getChild("agent_bic");
        StructVector creditor = (StructVector) root.getVector("creditor");
        VarCharVector creditorName = (VarCharVector) creditor.getChild("name");
        StructVector creditorAccount = (StructVector) creditor.getChild("account");
        VarCharVector creditorIban = (VarCharVector) creditorAccount.getChild("iban");
        VarCharVector creditorNumber = (VarCharVector) creditorAccount.getChild("number");
        VarCharVector creditorBic = (VarCharVector) creditor.getChild("agent_bic");
        ListVector structured = (ListVector) root.getVector("structured_remittance");
        StructVector items = (StructVector) structured.getDataVector();
        VarCharVector code = (VarCharVector) items.getChild("code");
        VarCharVector reference = (VarCharVector) items.getChild("reference");
        DecimalVector itemAmount = (DecimalVector) items.getChild("amount");
        VarCharVector remitInfo = (VarCharVector) root.getVector("remittance_information");
        DateDayVector date = (DateDayVector) root.getVector("requested_execution_date");
        VarCharVector status = (VarCharVector) root.getVector("transaction_status");

        for (int r = 0; r < rows; r++) {
            int remittance = r / shape.transactionsPerRemittance();
            txnId.setSafe(r, txnIds[r]);
            remId.setSafe(r, ("RMT-" + remittance).getBytes());
            msgId.setSafe(r, ("MSG-" + (remittance / shape.remittancesPerMessage())).getBytes());
            e2e.setSafe(r, ("E2E-" + r).getBytes());
            amount.setSafe(r, BigDecimal.valueOf(amountCents[r], 4).setScale(4, RoundingMode.UNNECESSARY));
            currency.setSafe(r, CURRENCIES[currencyIdx[r]].getBytes());
            debtor.setIndexDefined(r);
            debtorAccount.setIndexDefined(r);
            debtorName.setSafe(r, partyName(r).getBytes());
            debtorIban.setSafe(r, debtorIbans[r]);
            debtorNumber.setSafe(r, ("ACC" + r).getBytes());
            debtorBic.setSafe(r, BIC_BYTES);
            creditor.setIndexDefined(r);
            creditorAccount.setIndexDefined(r);
            creditorName.setSafe(r, partyName(r + 7).getBytes());
            creditorIban.setSafe(r, ("GB" + (300000000 + r) + "0000000000").getBytes());
            creditorNumber.setSafe(r, ("CR-" + r).getBytes());
            creditorBic.setSafe(r, "DEUTDEFFXXX".getBytes());
            int itemCount = 1 + (r % 3);
            structured.startNewValue(r);
            int base = structured.getOffsetBuffer().getInt((long) r * 4);
            for (int i = 0; i < itemCount; i++) {
                int idx = base + i;
                items.setIndexDefined(idx);
                code.setSafe(idx, "SCOR".getBytes());
                reference.setSafe(idx, ("REF-" + r + "-" + i).getBytes());
                itemAmount.setSafe(idx, BigDecimal.valueOf(10L * i + r % 7, 4).setScale(4, RoundingMode.UNNECESSARY));
            }
            structured.endValue(r, itemCount);
            remitInfo.setSafe(r, ("INVOICE-" + remittance).getBytes());
            date.setSafe(r, 20000 + dayOffsets[r]);
            status.setSafe(r, STATUSES[statusIdx[r]].getBytes());
        }
    }

    /**
     * Writes the synthetic extension groups of the transactions root. Only the 'wide'
     * profile carries them; 'normal' is the real canonical model only.
     */
    private static void fillExtensions(VectorSchemaRoot root, String profile, int rows) {
        int groups = "wide".equals(profile) ? 40 : 6;
        int leaves = "wide".equals(profile) ? 48 : 22;
        Schema schema = root.getSchema();
        for (int g = 0; g < groups; g++) {
            String groupName = DOMAIN_PREFIXES[g % DOMAIN_PREFIXES.length] + "_g" + g;
            StructVector group = (StructVector) root.getVector(groupName);
            StructVector block = (StructVector) group.getChild(groupName + "_block");
            for (int r = 0; r < rows; r++) {
                group.setIndexDefined(r);
                block.setIndexDefined(r);
            }
            for (int f = 0; f < leaves; f++) {
                String leaf = "branch_" + f + switch (f % 4) {
                    case 0 -> "_code";
                    case 1 -> "_status";
                    case 2 -> "_reference";
                    default -> "_counter";
                };
                org.apache.arrow.vector.ValueVector child = block.getChild(leaf);
                switch (f % 4) {
                    case 1 -> {
                        IntVector iv = (IntVector) child;
                        for (int r = 0; r < rows; r++) {
                            iv.setSafe(r, (r + f + g) % 1000);
                        }
                    }
                    case 3 -> {
                        BigIntVector bv = (BigIntVector) child;
                        for (int r = 0; r < rows; r++) {
                            bv.setSafe(r, 1000L * f + r % 997);
                        }
                    }
                    case 0 -> {
                        VarCharVector cv = (VarCharVector) child;
                        String prefix = DOMAIN_PREFIXES[g % DOMAIN_PREFIXES.length].substring(0, 3);
                        for (int r = 0; r < rows; r++) {
                            cv.setSafe(r, (prefix + "-" + f + "-" + r % 100).getBytes());
                        }
                    }
                    default -> {
                        VarCharVector cv = (VarCharVector) child;
                        for (int r = 0; r < rows; r++) {
                            cv.setSafe(r, ("REF-" + g + "-" + f + "-" + r % 100).getBytes());
                        }
                    }
                }
            }
        }
    }

    private static final byte[] BIC_BYTES = "COBADEFFXXX".getBytes();

    /** Message root: one row per message. */
    public static VectorSchemaRoot buildMessageRoot(Shape shape, int transactionRows, BufferAllocator allocator) {
        VectorSchemaRoot root = VectorSchemaRoot.create(
                io.github.balchua.datafusion.PaymentSchemas.messageSchema(), allocator);
        VarCharVector id = (VarCharVector) root.getVector("message_id");
        VarCharVector type = (VarCharVector) root.getVector("message_type");
        VarCharVector version = (VarCharVector) root.getVector("version");
        TimeStampMilliVector created = (TimeStampMilliVector) root.getVector("creation_timestamp");
        VarCharVector party = (VarCharVector) root.getVector("initiating_party");
        IntVector count = (IntVector) root.getVector("number_of_transactions");
        DecimalVector sum = (DecimalVector) root.getVector("control_sum");
        int messages = shape.messages();
        int txnsPerMessage = shape.transactionsPerRemittance() * shape.remittancesPerMessage();
        for (int m = 0; m < messages; m++) {
            BigDecimal total = BigDecimal.ZERO;
            for (int t = m * txnsPerMessage; t < (m + 1) * txnsPerMessage; t++) {
                total = total.add(BigDecimal.valueOf(1000L + t % 9000, 4));
            }
            id.setSafe(m, ("MSG-" + m).getBytes());
            type.setSafe(m, "pain.001.001.11".getBytes());
            version.setSafe(m, "camt".getBytes());
            TimeStampMilliHolder createdHolder = new TimeStampMilliHolder();
            createdHolder.value = 1772995200000L;
            created.setSafe(m, createdHolder);
            party.setSafe(m, "ACME-PAY".getBytes());
            count.setSafe(m, txnsPerMessage);
            sum.setSafe(m, total.setScale(4, RoundingMode.UNNECESSARY));
        }
        root.setRowCount(messages);
        return root;
    }

    /** Remittance root: remittancesPerMessage rows per message. */
    public static VectorSchemaRoot buildRemittanceRoot(Shape shape, int transactionRows, BufferAllocator allocator) {
        VectorSchemaRoot root = VectorSchemaRoot.create(
                io.github.balchua.datafusion.PaymentSchemas.remittanceSchema(), allocator);
        VarCharVector id = (VarCharVector) root.getVector("remittance_id");
        VarCharVector msg = (VarCharVector) root.getVector("message_id");
        VarCharVector paymentInfo = (VarCharVector) root.getVector("payment_information_id");
        DateDayVector date = (DateDayVector) root.getVector("requested_execution_date");
        StructVector debtor = (StructVector) root.getVector("debtor");
        VarCharVector debtorName = (VarCharVector) debtor.getChild("name");
        StructVector debtorAccount = (StructVector) debtor.getChild("account");
        VarCharVector iban = (VarCharVector) debtorAccount.getChild("iban");
        VarCharVector number = (VarCharVector) debtorAccount.getChild("number");
        VarCharVector bic = (VarCharVector) debtor.getChild("agent_bic");
        IntVector count = (IntVector) root.getVector("number_of_transactions");
        DecimalVector sum = (DecimalVector) root.getVector("control_sum");

        int remittanceCount = shape.remittances();
        int txnsPerRemittance = shape.transactionsPerRemittance();
        for (int r = 0; r < remittanceCount; r++) {
            int first = r * txnsPerRemittance;
            int last = Math.min(first + txnsPerRemittance, transactionRows) - 1;
            int n = 0;
            BigDecimal control = BigDecimal.ZERO;
            for (int t = first; t <= last; t++) {
                n++;
                control = control.add(BigDecimal.valueOf(1000L + t % 9000, 4));
            }
            id.setSafe(r, ("RMT-" + r).getBytes());
            msg.setSafe(r, ("MSG-" + (r / shape.remittancesPerMessage())).getBytes());
            paymentInfo.setSafe(r, ("PI-" + (r % 5)).getBytes());
            date.setSafe(r, 20000 + r % 30);
            debtor.setIndexDefined(r);
            debtorAccount.setIndexDefined(r);
            debtorName.setSafe(r, partyName(first).getBytes());
            iban.setSafe(r, (first % 1000 == 0) ? MATCH_IBAN.getBytes()
                    : ("DE" + (400000000 + first) + "0000000000").getBytes());
            number.setSafe(r, ("ACC" + first).getBytes());
            bic.setSafe(r, "COBADEFFXXX".getBytes());
            count.setSafe(r, n);
            sum.setSafe(r, control.setScale(4, RoundingMode.UNNECESSARY));
        }
        root.setRowCount(remittanceCount);
        return root;
    }

}