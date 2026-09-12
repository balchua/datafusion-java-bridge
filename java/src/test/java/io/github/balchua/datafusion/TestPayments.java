package io.github.balchua.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.holders.TimeStampMilliHolder;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Builders for the canonical payment tables, used by tests and the large-data tests.
 * Deterministic content: debtor names cycle so 'BBB' matches every 26th row offset by 1;
 * currencies cycle EUR/USD/GBP/CHF; every 1000th row carries MATCH_IBAN for query Q4.
 * Fills the full canonical model including the pain.001-style extension columns:
 * typed structs (addresses, agents, ultimate parties) get fully-populated or deliberately
 * null values; JSON columns carry realistic ragged payloads.
 */
public final class TestPayments {
    private TestPayments() {}

    public static final String MATCH_DEBTOR = "BBB";
    public static final String MATCH_IBAN = "DE02999999990000009999";

    /** names cycle so that 'BBB' hits every 26th row offset by 1 (r % 26 == 1). */
    public static String partyName(int r) {
        char first = (char) ('A' + (r % 26));
        return "" + first + 'B' + 'B';
    }

    // ------------------------------------------------------------------
    private static final com.fasterxml.jackson.databind.ObjectMapper JSON_MAPPER =
            new com.fasterxml.jackson.databind.ObjectMapper();

    /** Serializes a Java map/list tree to compact JSON. */
    private static String toJson(Object tree) {
        try {
            return JSON_MAPPER.writeValueAsString(tree);
        } catch (Exception e) {
            throw new IllegalStateException("JSON payload serialization failed", e);
        }
    }

    // JSON columns: faithful JSON renderings of the corresponding ISO 20022 XML
    // blocks (element-for-element, camelCase keys, attributes as {value, ccy},
    // unbounded repeats as JSON arrays). Deterministic per row.
    // ------------------------------------------------------------------

    /**
     * RgltryRptg[1..n] rendered as faithful JSON of the XML tree:
     * DbtCdtRptgInd?, Athrty?{Nm, PstlAdr{Ctry, AdrLine[1..n], TwnNm}, Othr?{Id, SchmeNm{Cd}}},
     * Dtls[1..n]{Tp?, Cd?, Amt?{@ccy,#text}, Inf?{DbtrSts?, CdtrSts?},
     * CdtrTx?{Cd?, Agt?{FinInstnId{BICFI | ClrSysMmbId{ClrSysId{Cd}, MmbId}}}},
     * DbtrTx?{...same...}, Taxonomies?{Dmstcd{Domn, Fmly{Cd}, Sqnc1..n{Brnch{Cd}}, Smmry?}}}.
     * Serialized with Jackson so the structure cannot drift out of balance.
     */
    public static String regulatoryReportingJson(int r) {
        var dtls = new java.util.LinkedHashMap<String, Object>();
        dtls.put("Tp", r % 2 == 0 ? "TEST" : "BAL");
        dtls.put("Cd", String.format("RRC-%03d", r % 999));
        dtls.put("Amt", java.util.Map.of("@ccy", "EUR", "#text", (1000 + r % 9000) + ".25"));
        dtls.put("Inf", java.util.Map.of(
                "DbtrSts", r % 5 == 0 ? "INV" : "TRAN",
                "CdtrSts", "TRAN"));
        dtls.put("CdtrTx", java.util.Map.of(
                "Cd", "REC",
                "Agt", java.util.Map.of("FinInstnId",
                        java.util.Map.of("BICFI", "COBADEFFXXX"))));
        dtls.put("DbtrTx", java.util.Map.of(
                "Cd", "PAY",
                "Agt", java.util.Map.of("FinInstnId",
                        java.util.Map.of("ClrSysMmbId",
                                java.util.Map.of(
                                        "ClrSysId", java.util.Map.of("Cd", "ATBLZ"),
                                        "MmbId", "37040044")))));
        dtls.put("Taxonomies", java.util.Map.of("Dmstcd",
                java.util.Map.of(
                        "Domn", "FINANCIAL",
                        "Fmly", java.util.Map.of("Cd", "CASHMGMT"),
                        "Sqnc1", java.util.Map.of("Brnch", java.util.Map.of("Cd", "AWZ")))));
        var rptg = new java.util.LinkedHashMap<String, Object>();
        rptg.put("DbtCdtRptgInd", switch (r % 3) {
            case 0 -> "CRED";
            case 1 -> "DEBT";
            default -> "RES";
        });
        rptg.put("Athrty", java.util.Map.of(
                "Nm", "Deutsche Bundesbank",
                "PstlAdr", java.util.Map.of(
                        "Ctry", "DE",
                        "AdrLine", java.util.List.of("Bundesbankplatz 1"),
                        "TwnNm", "Frankfurt am Main"),
                "Othr", java.util.Map.of(
                        "Id", "DE1234AWZ",
                        "SchmeNm", java.util.Map.of("Cd", "CTR"))));
        rptg.put("Dtls", java.util.List.of(dtls));
        return toJson(java.util.List.of(rptg));
    }

    /** Tax -> Debtr{Id, Rgn, TaxPty{Tp, Cd, Rgn, Cm, Prcntg, BaseAmt}}, Rcd[1..n]{Tp, Cd, Rgn, TaxAmt} */
    public static String taxJson(int r) {
        return "{"
                + "\"Debtr\":{"
                + "  \"Id\":{\"PrvtId\":{\"Othr\":[{"
                + "    \"Id\":\"TAXID-" + (r % 9973) + "\""
                + ",   \"Tp\":\"TXID\"}]}}"
                + ", \"Rgn\":\"BY\""
                + ", \"TaxPty\":{"
                + "    \"Tp\":\"TX\""
                + ",   \"Cd\":\"ELE\""
                + ",   \"Rgn\":\"BY\""
                + ",   \"Cm\":15.00"
                + ",   \"TaxPrcntg\":19.0"
                + ",   \"BaseAmt\":{\"@ccy\":\"EUR\",\"#text\":" + (1000 + r % 9000) + ".00}}}"
                + ",\"Rcd\":[{"
                + "  \"Tp\":\"VAT\""
                + ", \"Cd\":\"VAT19\""
                + ", \"Rgn\":\"BY\""
                + ", \"TaxAmt\":{\"@ccy\":\"EUR\",\"#text\":" + (r % 250) + ".50}}]"
                + "}";
    }

    /** Taxonomies -> Dmstcd{Domn, Fmly, Sqnc1..n, Smmry?} (open-world category tree). */
    public static String taxonomiesJson(int r) {
        String[] domns = {"FINANCIAL", "TRADE", "REGULATORY"};
        return "{\"Dmstcd\":{"
                + "\"Domn\":\"" + domns[r % domns.length] + "\""
                + ",\"Fmly\":{\"Cd\":\"PAYMENTS\"}"
                + ",\"Sqnc1\":{\"Brnch\":{\"Cd\":\"ISO-PAIN-" + (r % 11) + "\"}}"
                + ",\"Sqnc2\":{\"Brnch\":{\"Cd\":\"SUB-" + (r % 7) + "\"}}"
                + ",\"Smmry\":" + (r % 2 == 0)
                + "}}";
    }

    /** OrgnlGrpInfAndSts: OrgnlMsgId, OrgnlMsgNmId, OrgnlCreDtTm, OrgnlNbOfTxs, OrgnlCtrlSum, StsInf. */
    public static String originalMessageReferenceJson(int r) {
        return "{"
                + "\"OrgnlMsgId\":\"MSG-ORIG-" + r + "\""
                + ",\"OrgnlMsgNmId\":\"pain.001.001.11\""
                + ",\"OrgnlCreDtTm\":\"2026-03-08T10:15:30\""
                + ",\"OrgnlNbOfTxs\":" + (25 + r % 100)
                + ",\"OrgnlCtrlSum\":" + (25000 + r) + ".7500"
                + ",\"StsInf\":[{"
                + "  \"Sts\":\"AMENDED\""
                + ", \"Rsn\":{\"Cd\":\"AM05\"}}]"
                + "}";
    }

    /** SupplementaryData -> Envelope (opaque content + schema ref). */
    public static String supplementalDataJson(int r) {
        return "{\"Envlp\":{"
                + "\"content\":{\"jobId\":\"JOB-" + (r % 65521) + "\",\"lane\":" + (r % 8) + "}"
                + ",\"schemaRef\":\"urn:acme:supplementary:v1\""
                + "}}";
    }

    /** Transport/file metadata (application domain, not an XSD block). */
    public static String fileMetadataJson(int r) {
        return "{"
                + "\"fileName\":\"pain.001.batch-" + r + ".xml\""
                + ",\"checksum\":\"sha256:" + String.format("%016x", 0xABCDEF00L + r) + "\""
                + ",\"sizeBytes\":" + (4800 + r % 32000)
                + ",\"validatedAgainst\":\"pain.001.001.11\""
                + ",\"validationResult\":\"PASS\""
                + "}";
    }

    /** Chrgs[1..n] -> Record{Amt@ccy, Agt{FinInstnId{BICFI}}, Rcvr, Dbtr, Tp}. */
    public static String chargeDetailsJson(int r) {
        return "[{"
                + "\"Amt\":{\"@ccy\":\"EUR\",\"#text\":" + (r % 25) + ".5000}"
                + ",\"Agt\":{\"FinInstnId\":{\"BICFI\":\"DEUTDEFFXXX\"}}"
                + ",\"Rcvr\":\"DEBT\""
                + ",\"Dbtr\":\"SHAR\""
                + ",\"Tp\":{\"Cd\":\"CHRG-" + (r % 9) + "\"}"
                + "}]";
    }

    /** Payment terms extension block (app-level). */
    public static String paymentTermsJson(int r) {
        return "{"
                + "\"discount\":{\"rate\":0.75,\"dueDays\":14}"
                + ",\"partialPaymentsAllowed\":" + (r % 2 == 0)
                + ",\"rebate\":{\"@ccy\":\"EUR\",\"#text\":" + (r % 50) + ".0000}"
                + "}";
    }

    /** ChqInstr: ChqNb, DlvryMtd, PrntLctn, PymntVldtyDt. */
    public static String chequeInstructionsJson(int r) {
        return "{"
                + "\"ChqNb\":\"CHQ-" + String.format("%06d", r % 1000000) + "\""
                + ",\"DlvryMtd\":{\"Cd\":\"MLCD\"}"
                + ",\"PrntLctn\":\"HOME\""
                + ",\"PymntVldtyDt\":\"2026-04-01\""
                + "}";
    }

    /** Card direct debit instruction: CardData, AuthMtd. */
    public static String cardInstructionJson(int r) {
        return "{"
                + "\"CardData\":{"
                + "  \"mskdPAN\":\"****-****-****-" + String.format("%04d", r % 10000) + "\""
                + ", \"VldtTo\":\"2028-12\""
                + ", \"Brnd\":\"VISA\"}"
                + ",\"AuthMtd\":\"CUSTOMER_SIGNATURE\""
                + "}";
    }

    /** Invoice references with line items (application-level, unbounded). */
    public static String invoiceReferencesJson(int r) {
        return "[{"
                + "\"invoiceNumber\":\"INV-" + (r % 9973) + "\""
                + ",\"Amt\":{\"@ccy\":\"EUR\",\"#text\":" + (1000 + r % 9000) + ".0000}"
                + ",\"dueDate\":\"2026-05-15\""
                + ",\"lines\":[{"
                + "  \"position\":1,\"article\":\"SKU-" + (r % 313) + "\""
                + ", \"quantity\":" + (1 + r % 9)
                + ", \"unitPrice\":" + (10 + r % 500) + ".00"
                + "},{"
                + "  \"position\":2,\"article\":\"SKU-" + (r % 313 + 500) + "\""
                + ", \"quantity\":" + (1 + r % 3)
                + ", \"unitPrice\":99.50"
                + "}]"
                + "}]";
    }

    /** Sanctions/fraud screening output (application domain, free-form). */
    public static String riskSignalsJson(int r) {
        return "{"
                + "\"screenings\":[{"
                + "  \"provider\":\"INTERNAL\""
                + ", \"list\":\"OFAC\""
                + ", \"hit\":false"
                + ", \"score\":0." + String.format("%02d", r % 20)
                + "},{"
                + "  \"provider\":\"EXTERNAL\""
                + ", \"list\":\"EU-CONSOLIDATED\""
                + ", \"hit\":false"
                + ", \"score\":0." + String.format("%02d", 5 + r % 15)
                + "}]"
                + ",\"velocityFlags\":{\"txnsLast24h\":" + (r % 40) + ",\"windowHours\":24}"
                + ",\"riskBand\":\"" + (r % 5 == 0 ? "AMBER" : "GREEN") + "\""
                + "}";
    }

    // ------------------------------------------------------------------
    // message table
    // ------------------------------------------------------------------

    public static VectorSchemaRoot messageRoot(String messageId, int transactions, BufferAllocator allocator) {
        VectorSchemaRoot root = VectorSchemaRoot.create(PaymentSchemas.messageSchema(), allocator);
        fillMessage(root, messageId, transactions);
        return root;
    }

    public static void fillMessage(VectorSchemaRoot root, String messageId, int transactions) {
        VarCharVector id = (VarCharVector) root.getVector("message_id");
        VarCharVector type = (VarCharVector) root.getVector("message_type");
        VarCharVector version = (VarCharVector) root.getVector("version");
        TimeStampMilliVector created = (TimeStampMilliVector) root.getVector("creation_timestamp");
        VarCharVector party = (VarCharVector) root.getVector("initiating_party");
        IntVector count = (IntVector) root.getVector("number_of_transactions");
        DecimalVector sum = (DecimalVector) root.getVector("control_sum");

        id.setSafe(0, messageId.getBytes());
        type.setSafe(0, "pain.001.001.11".getBytes());
        version.setSafe(0, "camt".getBytes());
        TimeStampMilliHolder createdHolder = new TimeStampMilliHolder();
        createdHolder.value = 1772995200000L;
        created.setSafe(0, createdHolder);
        party.setSafe(0, "ACME-PAY".getBytes());
        count.setSafe(0, transactions);
        BigDecimal total = BigDecimal.ZERO;
        for (int r = 0; r < transactions; r++) {
            total = total.add(BigDecimal.valueOf(1000L + r % 9000, 4));
        }
        sum.setSafe(0, total.setScale(4, RoundingMode.UNNECESSARY));

        // -- new pain.001 columns --
        ((org.apache.arrow.vector.BitVector) root.getVector("batch_booking")).setSafe(0, 1);
        ((org.apache.arrow.vector.BitVector) root.getVector("test_indicator")).setSafe(0, 0);
        ((VarCharVector) root.getVector("settlement_method")).setSafe(0, "INDA".getBytes());
        ((VarCharVector) root.getVector("clearing_system")).setSafe(0, "SEPA".getBytes());
        fillAgent((StructVector) root.getVector("instructing_agent"), "INSTRUCT-BANK", "AAAADEFFXXX", "LEI-INSTRUCT-1");
        fillAgent((StructVector) root.getVector("receiving_agent"), "RECEIVE BANK", "BBBBUS33XXX", null); // deliberate null LEI
        ((VarCharVector) root.getVector("original_message_reference"))
                .setSafe(0, originalMessageReferenceJson(1).getBytes());
        ((VarCharVector) root.getVector("regulatory_reporting"))
                .setSafe(0, regulatoryReportingJson(2).getBytes());
        ((VarCharVector) root.getVector("message_tax"))
                .setSafe(0, taxJson(3).getBytes());
        ((VarCharVector) root.getVector("supplemental_data"))
                .setSafe(0, supplementalDataJson(4).getBytes());
        ((VarCharVector) root.getVector("file_metadata"))
                .setSafe(0, fileMetadataJson(5).getBytes());
        ((VarCharVector) root.getVector("message_hash"))
                .setSafe(0, ("hash-" + transactions).getBytes());
        root.setRowCount(1);
    }

    private static void fillAgent(StructVector agent, String name, String bic, String lei) {
        agent.setIndexDefined(0);
        ((VarCharVector) agent.getChild("name")).setSafe(0, name.getBytes());
        ((VarCharVector) agent.getChild("bic")).setSafe(0, bic.getBytes());
        ((VarCharVector) agent.getChild("clearing_member_id")).setSafe(0, ("CM-" + bic).getBytes());
        VarCharVector leiVec = (VarCharVector) agent.getChild("lei");
        if (lei != null) {
            leiVec.setSafe(0, lei.getBytes());
        } else {
            leiVec.setNull(0);
        }
    }

    private static void fillAddress(StructVector address, int seed) {
        address.setIndexDefined(0);
        ((VarCharVector) address.getChild("street")).setSafe(0, ("Street-" + seed % 89).getBytes());
        ((VarCharVector) address.getChild("house_number")).setSafe(0, Integer.toString(1 + seed % 199).getBytes());
        ((VarCharVector) address.getChild("city")).setSafe(0, ("City-" + seed % 31).getBytes());
        ((VarCharVector) address.getChild("postal_code")).setSafe(0, String.format("%05d", 10000 + seed % 89999).getBytes());
        ((VarCharVector) address.getChild("country")).setSafe(0, "DE".getBytes());
        // address_line7 deliberately null on odd seeds to exercise nullable leaves
        if (seed % 2 == 0) {
            ((VarCharVector) address.getChild("address_line7")).setSafe(0, ("line7-" + seed).getBytes());
        } else {
            ((VarCharVector) address.getChild("address_line7")).setNull(0);
        }
    }

    /**
     * Fills an ultimate-party struct; {@code nullName} makes the whole struct null
     * (exercising optional blocks in the data).
     */
    private static void fillUltimateParty(StructVector party, int seed, boolean nullName) {
        if (nullName) {
            party.setNull(seed);
            return;
        }
        party.setIndexDefined(seed);
        ((VarCharVector) party.getChild("name")).setSafe(seed, ("ULT-" + partyName(seed)).getBytes());
        ((VarCharVector) party.getChild("identifier")).setSafe(seed, ("ID-" + seed).getBytes());
        StructVector account = (StructVector) party.getChild("account");
        account.setIndexDefined(seed);
        ((VarCharVector) account.getChild("iban"))
                .setSafe(seed, ("FR" + (140000000 + seed) + "0000000000").getBytes());
        ((VarCharVector) account.getChild("number")).setSafe(seed, ("UACC-" + seed).getBytes());
        fillAddress((StructVector) party.getChild("postal_address"), seed);
    }

    // ------------------------------------------------------------------
    // remittances table
    // ------------------------------------------------------------------

    /** Builds a remittances root with full 25-transaction batches. */
    public static VectorSchemaRoot remittancesRoot(String messageId, int remittanceCount, BufferAllocator allocator) {
        return remittancesRoot(messageId, remittanceCount, remittanceCount * 25, allocator);
    }

    /** Builds a remittances root with exact transaction-row counts, including a short final batch. */
    public static VectorSchemaRoot remittancesRoot(String messageId, int remittanceCount,
                                                    int transactionRows, BufferAllocator allocator) {
        VectorSchemaRoot root = VectorSchemaRoot.create(PaymentSchemas.remittanceSchema(), allocator);
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

        for (int r = 0; r < remittanceCount; r++) {
            int first = r * 25;
            int last = Math.min(first + 25, transactionRows);
            int txCount = Math.max(0, last - first);
            BigDecimal control = BigDecimal.ZERO;
            for (int tx = first; tx < last; tx++) {
                control = control.add(BigDecimal.valueOf(1000L + tx % 9000, 4));
            }
            id.setSafe(r, ("RMT-" + r).getBytes());
            msg.setSafe(r, messageId.getBytes());
            paymentInfo.setSafe(r, ("PI-" + (r % 5)).getBytes());
            date.setSafe(r, 20000 + r);
            debtor.setIndexDefined(r);
            debtorAccount.setIndexDefined(r);
            debtorName.setSafe(r, partyName(r).getBytes());
            iban.setSafe(r, ("DE" + (100000000 + r) + "0000000000").getBytes());
            number.setSafe(r, ("ACC" + r).getBytes());
            bic.setSafe(r, "COBADEFFXXX".getBytes());
            count.setSafe(r, txCount);
            sum.setSafe(r, control.setScale(4, RoundingMode.UNNECESSARY));
        }
        root.setRowCount(remittanceCount);
        fillRemittanceExtensions(root, remittanceCount);
        return root;
    }

    /** Fills the +14 new remittance columns for rows [0, remittanceCount). */
    public static void fillRemittanceExtensions(VectorSchemaRoot root, int rows) {
        for (int r = 0; r < rows; r++) {
            ((VarCharVector) root.getVector("payment_method")).setSafe(r, "TRF".getBytes());
            ((VarCharVector) root.getVector("service_level")).setSafe(r, "NURG".getBytes());
            ((VarCharVector) root.getVector("local_instrument")).setSafe(r, ("INST-" + r % 7).getBytes());
            ((VarCharVector) root.getVector("category_purpose")).setSafe(r, "SALA".getBytes());
            ((VarCharVector) root.getVector("sequence_type")).setSafe(r, "OOFF".getBytes());
            // address present on even rows, null on odd rows
            StructVector address = (StructVector) root.getVector("debtor_postal_address");
            if (r % 2 == 0) {
                fillAddress(address, r);
            } else {
                address.setNull(r);
            }
            // ultimate debtor null on every 3rd row
            fillUltimateParty((StructVector) root.getVector("ultimate_debtor"), r, r % 3 == 0);
            fillAgent((StructVector) root.getVector("debtor_agent"), "DEBTOR BANK", "COBADEFFXXX", "LEI-DEBT-" + r);
            ((VarCharVector) root.getVector("charge_bearer")).setSafe(r, "SHAR".getBytes());
            ((DateDayVector) root.getVector("pooling_adjustment_date")).setSafe(r, 20000 + r % 30);
            TimeStampMilliHolder holder = new TimeStampMilliHolder();
            holder.value = 1772995200000L + r;
            ((TimeStampMilliVector) root.getVector("settlement_date")).setSafe(r, holder);
            ((VarCharVector) root.getVector("remittance_regulatory_reporting"))
                    .setSafe(r, regulatoryReportingJson(r).getBytes());
            ((VarCharVector) root.getVector("remittance_tax"))
                    .setSafe(r, taxJson(r).getBytes());
            ((VarCharVector) root.getVector("payment_terms"))
                    .setSafe(r, paymentTermsJson(r).getBytes());
        }
    }

    // ------------------------------------------------------------------
    // transactions table
    // ------------------------------------------------------------------

    /**
     * Builds a transactions root with {@code rows} rows; every 26th row's debtor name is
     * 'BBB' (Q3), every 1000th row carries MATCH_IBAN (Q4); 1-3 structured remittance
     * items per row; optional blocks (FX, charges, cheque, card, addresses, ultimate
     * parties) are deliberately null on rotating rows to exercise nullability at scale.
     */
    public static VectorSchemaRoot transactionsRoot(String messageId, int rows, BufferAllocator allocator) {
        VectorSchemaRoot root = VectorSchemaRoot.create(PaymentSchemas.transactionSchema(), allocator);
        fillTransactions(root, messageId, rows, 25);
        return root;
    }

    /**
     * Fills a transactions root. {@code txnsPerRemittance} transactions share one
     * remittance id, so the number of distinct remittances is ceil(rows/txnsPerRemittance).
     */
    public static void fillTransactions(VectorSchemaRoot root, String messageId, int rows, int txnsPerRemittance) {
        fillTransactions(root, messageId, rows, txnsPerRemittance, Integer.MAX_VALUE, false);
    }

    /** Fills a hierarchical dataset with indexed message ids (MSG-0, MSG-1, ...). */
    public static void fillTransactions(VectorSchemaRoot root, String messageIdPrefix, int rows,
                                        int txnsPerRemittance, int remittancesPerMessage) {
        fillTransactions(root, messageIdPrefix, rows, txnsPerRemittance, remittancesPerMessage, true);
    }

    private static void fillTransactions(VectorSchemaRoot root, String messageId, int rows,
                                         int txnsPerRemittance, int remittancesPerMessage,
                                         boolean indexedMessageIds) {
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

        String[] currencies = {"EUR", "USD", "GBP", "CHF"};
        String[] statuses = {"ACSC", "PDNG", "RJCT"};
        String[] purposes = {"SALA", "SUPP", "DIVI", "TAX " .trim(), "GDDS"};

        for (int r = 0; r < rows; r++) {
            int remittance = r / txnsPerRemittance;
            txnId.setSafe(r, ("TXN-" + r).getBytes());
            String rowMessageId = indexedMessageIds
                    ? "MSG-" + (remittance / remittancesPerMessage)
                    : messageId;
            msgId.setSafe(r, rowMessageId.getBytes());
            remId.setSafe(r, ("RMT-" + remittance).getBytes());
            e2e.setSafe(r, ("E2E-" + r).getBytes());
            amount.setSafe(r, BigDecimal.valueOf(1000L + r % 9000, 4).setScale(4, RoundingMode.UNNECESSARY));
            currency.setSafe(r, currencies[r % currencies.length].getBytes());
            debtor.setIndexDefined(r);
            debtorAccount.setIndexDefined(r);
            debtorName.setSafe(r, partyName(r).getBytes());
            if (r % 1000 == 0) {
                debtorIban.setSafe(r, MATCH_IBAN.getBytes());
            } else {
                debtorIban.setSafe(r, ("DE" + (200000000 + r) + "0000000000").getBytes());
            }
            debtorNumber.setSafe(r, ("ACC" + r).getBytes());
            debtorBic.setSafe(r, "COBADEFFXXX".getBytes());
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
            date.setSafe(r, 20000 + r % 30);
            status.setSafe(r, statuses[r % statuses.length].getBytes());

            fillTransactionExtensions(root, r, purposes);
        }
        root.setRowCount(rows);
    }

    /** Fills the +22 new transaction columns for row {@code r}. */
    private static void fillTransactionExtensions(VectorSchemaRoot root, int r, String[] purposes) {
        // ---- money: FX block null unless currency differs from EUR ----
        boolean fx = r % 4 != 0; // EUR-rows (every 4th) have no FX conversion
        DecimalVector instructed = (DecimalVector) root.getVector("instructed_amount");
        DecimalVector xchgRate = (DecimalVector) root.getVector("exchange_rate");
        DecimalVector equivalent = (DecimalVector) root.getVector("equivalent_amount");
        DecimalVector chargeAmount = (DecimalVector) root.getVector("charge_amount");
        StructVector fxDet = (StructVector) root.getVector("fx_details");
        if (fx) {
            instructed.setSafe(r, BigDecimal.valueOf(1000L + r % 9000, 4).setScale(4, RoundingMode.UNNECESSARY));
            xchgRate.setSafe(r, BigDecimal.valueOf(1.0825 + (r % 100) * 0.0001).setScale(10, RoundingMode.HALF_UP));
            equivalent.setSafe(r, BigDecimal.valueOf(1082L + r % 9000, 4).setScale(4, RoundingMode.UNNECESSARY));
            chargeAmount.setSafe(r, BigDecimal.valueOf(r % 25, 4).setScale(4, RoundingMode.UNNECESSARY));
            fxDet.setIndexDefined(r);
            ((VarCharVector) fxDet.getChild("quote_currency")).setSafe(r, "USD".getBytes());
            ((VarCharVector) fxDet.getChild("rate_type")).setSafe(r, "SPOT".getBytes());
            ((VarCharVector) fxDet.getChild("contract_id")).setSafe(r, ("FX-" + r % 311).getBytes());
        } else {
            instructed.setNull(r);
            xchgRate.setNull(r);
            equivalent.setNull(r);
            chargeAmount.setNull(r);
            fxDet.setNull(r);
        }

        ((VarCharVector) root.getVector("charge_details"))
                .setSafe(r, chargeDetailsJson(r).getBytes());

        // ---- addresses: even rows structured, odd rows null ----
        StructVector debtorAddr = (StructVector) root.getVector("debtor_postal_address");
        StructVector creditorAddr = (StructVector) root.getVector("creditor_postal_address");
        if (r % 2 == 0) {
            fillAddress(debtorAddr, r);
        } else {
            debtorAddr.setNull(r);
        }
        if (r % 2 == 1) {
            fillAddress(creditorAddr, r + 5);
        } else {
            creditorAddr.setNull(r);
        }

        // ---- ultimate parties: null on every 3rd/5th row ----
        fillUltimateParty((StructVector) root.getVector("ultimate_debtor"), r, r % 3 == 0);
        fillUltimateParty((StructVector) root.getVector("ultimate_creditor"), r, r % 7 == 0);

        fillAgent((StructVector) root.getVector("creditor_agent"), "CREDITOR BANK",
                "DEUTDEFFXXX", "LEI-CRED-" + r);

        ((VarCharVector) root.getVector("purpose_code")).setSafe(r, purposes[r % purposes.length].getBytes());
        ((VarCharVector) root.getVector("regulatory_reporting"))
                .setSafe(r, regulatoryReportingJson(r + 1).getBytes());
        ((VarCharVector) root.getVector("tax_details"))
                .setSafe(r, taxJson(r).getBytes());
        ((VarCharVector) root.getVector("taxonomies"))
                .setSafe(r, taxonomiesJson(r).getBytes());
        ((VarCharVector) root.getVector("invoice_references"))
                .setSafe(r, invoiceReferencesJson(r).getBytes());
        ((VarCharVector) root.getVector("mandate_reference")).setSafe(r, ("MNDT-" + r % 500).getBytes());
        ((VarCharVector) root.getVector("cheque_instructions"))
                .setSafe(r, chequeInstructionsJson(r).getBytes());
        ((VarCharVector) root.getVector("card_instruction"))
                .setSafe(r, cardInstructionJson(r).getBytes());
        ((VarCharVector) root.getVector("return_reason")).setSafe(r, "NONE".getBytes());
        TimeStampMilliHolder stHolder = new TimeStampMilliHolder();
        stHolder.value = 1772995200000L + r;
        ((TimeStampMilliVector) root.getVector("status_timestamp")).setSafe(r, stHolder);
        ((VarCharVector) root.getVector("risk_signals"))
                .setSafe(r, riskSignalsJson(r).getBytes());
    }
}