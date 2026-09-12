package io.github.balchua.datafusion.bench;

import io.github.balchua.datafusion.PaymentSchemas;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

/**
 * The {@code narrow} schema profile: a flat, pain.001-like model that mirrors the
 * {@code java-arrow-pain} DuckDB benchmark schema column-for-column (36 flat columns,
 * no Struct/List/JSON). It exists purely so the ingestion/load numbers can be compared
 * apples-to-apples with that project; the canonical model (normal/wide) is the default.
 *
 * <p>Tables and columns:
 * <ul>
 *   <li>message (5): msg_id, msg_cre_dt_tm, msg_nb_of_txs, msg_ctrl_sum, msg_initg_pty_nm</li>
 *   <li>remittance (16): msg_id, pmt_inf_id, pmt_mtd, nb_of_txs, ctrl_sum, svc_lvl_cd,
 *       reqd_exctn_dt, dbtr_nm, dbtr_acct_iban, dbtr_agt_bicfi, btch_bookg, instr_prty,
 *       lcl_instrm_cd, ctgy_purp_cd, chrg_br, ultmt_dbtr_nm</li>
 *   <li>transaction (15): pmt_inf_id, instr_id, end_to_end_id, instd_amt, ccy,
 *       cdtr_agt_bicfi, cdtr_nm, cdtr_acct_iban, rmt_inf_ustrd, rglty_rptg_cd,
 *       rmt_inf_strd_ref, rmt_inf_strd_ref_tp, purp_cd, ultmt_cdtr_nm, cdtr_ctry</li>
 * </ul>
 */
public final class NarrowPain001 {
    public static final String PROFILE = "narrow";
    public static final String TABLE_MESSAGE = "payment_message";
    public static final String TABLE_REMITTANCES = "remittances";
    public static final String TABLE_TRANSACTIONS = "transactions";

    private static final BigDecimal DEC_18_2 = BigDecimal.valueOf(100000, 2);
    private static final String[] CURRENCIES = {"EUR", "USD", "GBP", "CHF"};

    private NarrowPain001() {}

    public static Schema messageSchema() {
        return new Schema(List.of(
                PaymentSchemas.utf8("msg_id", false),
                PaymentSchemas.utf8("msg_cre_dt_tm", false),
                PaymentSchemas.utf8("msg_nb_of_txs", false),
                PaymentSchemas.decimal("msg_ctrl_sum", 18, 2, false),
                PaymentSchemas.utf8("msg_initg_pty_nm", false)
        ));
    }

    public static Schema remittanceSchema() {
        return new Schema(List.of(
                PaymentSchemas.utf8("msg_id", false),
                PaymentSchemas.utf8("pmt_inf_id", false),
                PaymentSchemas.utf8("pmt_mtd", false),
                PaymentSchemas.utf8("nb_of_txs", false),
                PaymentSchemas.decimal("ctrl_sum", 18, 2, true),
                PaymentSchemas.utf8("svc_lvl_cd", true),
                PaymentSchemas.date32("reqd_exctn_dt", false),
                PaymentSchemas.utf8("dbtr_nm", false),
                PaymentSchemas.utf8("dbtr_acct_iban", false),
                PaymentSchemas.utf8("dbtr_agt_bicfi", false),
                PaymentSchemas.utf8("btch_bookg", true),
                PaymentSchemas.utf8("instr_prty", true),
                PaymentSchemas.utf8("lcl_instrm_cd", true),
                PaymentSchemas.utf8("ctgy_purp_cd", true),
                PaymentSchemas.utf8("chrg_br", true),
                PaymentSchemas.utf8("ultmt_dbtr_nm", true)
        ));
    }

    public static Schema transactionSchema() {
        return new Schema(List.of(
                PaymentSchemas.utf8("pmt_inf_id", false),
                PaymentSchemas.utf8("instr_id", true),
                PaymentSchemas.utf8("end_to_end_id", false),
                PaymentSchemas.decimal("instd_amt", 18, 5, false),
                PaymentSchemas.utf8("ccy", false),
                PaymentSchemas.utf8("cdtr_agt_bicfi", true),
                PaymentSchemas.utf8("cdtr_nm", false),
                PaymentSchemas.utf8("cdtr_acct_iban", false),
                PaymentSchemas.utf8("rmt_inf_ustrd", true),
                PaymentSchemas.utf8("rglty_rptg_cd", true),
                PaymentSchemas.utf8("rmt_inf_strd_ref", true),
                PaymentSchemas.utf8("rmt_inf_strd_ref_tp", true),
                PaymentSchemas.utf8("purp_cd", true),
                PaymentSchemas.utf8("ultmt_cdtr_nm", true),
                PaymentSchemas.utf8("cdtr_ctry", true)
        ));
    }

    public static VectorSchemaRoot buildMessage(SyntheticPaymentGenerator.Shape shape,
                                                BufferAllocator allocator) {
        VectorSchemaRoot root = VectorSchemaRoot.create(messageSchema(), allocator);
        VarCharVector msgId = (VarCharVector) root.getVector("msg_id");
        VarCharVector creDtTm = (VarCharVector) root.getVector("msg_cre_dt_tm");
        VarCharVector nbOfTxs = (VarCharVector) root.getVector("msg_nb_of_txs");
        DecimalVector ctrlSum = (DecimalVector) root.getVector("msg_ctrl_sum");
        VarCharVector initgPty = (VarCharVector) root.getVector("msg_initg_pty_nm");
        long txnsPerMessage = (long) shape.remittancesPerMessage() * shape.transactionsPerRemittance();
        for (int m = 0; m < shape.messages(); m++) {
            msgId.setSafe(m, ("MSG-" + m).getBytes());
            creDtTm.setSafe(m, "2026-03-08T10:15:30".getBytes());
            nbOfTxs.setSafe(m, Long.toString(txnsPerMessage).getBytes());
            ctrlSum.setSafe(m, DEC_18_2);
            initgPty.setSafe(m, "ACME-PAY".getBytes());
        }
        root.setRowCount(shape.messages());
        return root;
    }

    public static VectorSchemaRoot buildRemittances(SyntheticPaymentGenerator.Shape shape,
                                                    BufferAllocator allocator) {
        VectorSchemaRoot root = VectorSchemaRoot.create(remittanceSchema(), allocator);
        VarCharVector msgId = (VarCharVector) root.getVector("msg_id");
        VarCharVector pmtInfId = (VarCharVector) root.getVector("pmt_inf_id");
        VarCharVector pmtMtd = (VarCharVector) root.getVector("pmt_mtd");
        VarCharVector nbOfTxs = (VarCharVector) root.getVector("nb_of_txs");
        DecimalVector ctrlSum = (DecimalVector) root.getVector("ctrl_sum");
        VarCharVector svcLvl = (VarCharVector) root.getVector("svc_lvl_cd");
        DateDayVector exctnDt = (DateDayVector) root.getVector("reqd_exctn_dt");
        VarCharVector dbtrNm = (VarCharVector) root.getVector("dbtr_nm");
        VarCharVector dbtrIban = (VarCharVector) root.getVector("dbtr_acct_iban");
        VarCharVector dbtrBic = (VarCharVector) root.getVector("dbtr_agt_bicfi");
        VarCharVector btchBookg = (VarCharVector) root.getVector("btch_bookg");
        VarCharVector instrPrty = (VarCharVector) root.getVector("instr_prty");
        VarCharVector lclInstrm = (VarCharVector) root.getVector("lcl_instrm_cd");
        VarCharVector ctgyPurp = (VarCharVector) root.getVector("ctgy_purp_cd");
        VarCharVector chrgBr = (VarCharVector) root.getVector("chrg_br");
        VarCharVector ultmtDbtr = (VarCharVector) root.getVector("ultmt_dbtr_nm");

        int count = shape.remittances();
        for (int r = 0; r < count; r++) {
            msgId.setSafe(r, ("MSG-" + (r / shape.remittancesPerMessage())).getBytes());
            pmtInfId.setSafe(r, ("PI-" + r).getBytes());
            pmtMtd.setSafe(r, "TRF".getBytes());
            nbOfTxs.setSafe(r, Integer.toString(shape.transactionsPerRemittance()).getBytes());
            ctrlSum.setSafe(r, DEC_18_2);
            svcLvl.setSafe(r, "SEPA".getBytes());
            exctnDt.setSafe(r, 20000 + r % 30);
            dbtrNm.setSafe(r, "DEBTOR-" + (r % 97).getBytes());
            dbtrIban.setSafe(r, ("DE" + (100000000 + r) + "0000000000").getBytes());
            dbtrBic.setSafe(r, "COBADEFFXXX".getBytes());
            btchBookg.setSafe(r, "true".getBytes());
            instrPrty.setSafe(r, "NORM".getBytes());
            lclInstrm.setSafe(r, "CORE".getBytes());
            ctgyPurp.setSafe(r, "SALA".getBytes());
            chrgBr.setSafe(r, "SHAR".getBytes());
            ultmtDbtr.setSafe(r, "ULT-DEBTOR".getBytes());
        }
        root.setRowCount(count);
        return root;
    }

    /** Flat transaction table; {@code transactionsPerRemittance} txns share a pmt_inf_id. */
    public static VectorSchemaRoot buildTransactions(SyntheticPaymentGenerator.Shape shape,
                                                     BufferAllocator allocator) {
        VectorSchemaRoot root = VectorSchemaRoot.create(transactionSchema(), allocator);
        VarCharVector pmtInfId = (VarCharVector) root.getVector("pmt_inf_id");
        VarCharVector instrId = (VarCharVector) root.getVector("instr_id");
        VarCharVector endToEnd = (VarCharVector) root.getVector("end_to_end_id");
        DecimalVector instdAmt = (DecimalVector) root.getVector("instd_amt");
        VarCharVector ccy = (VarCharVector) root.getVector("ccy");
        VarCharVector cdtrBic = (VarCharVector) root.getVector("cdtr_agt_bicfi");
        VarCharVector cdtrNm = (VarCharVector) root.getVector("cdtr_nm");
        VarCharVector cdtrIban = (VarCharVector) root.getVector("cdtr_acct_iban");
        VarCharVector rmtUstrd = (VarCharVector) root.getVector("rmt_inf_ustrd");
        VarCharVector rgltyCd = (VarCharVector) root.getVector("rglty_rptg_cd");
        VarCharVector strdRef = (VarCharVector) root.getVector("rmt_inf_strd_ref");
        VarCharVector strdRefTp = (VarCharVector) root.getVector("rmt_inf_strd_ref_tp");
        VarCharVector purpCd = (VarCharVector) root.getVector("purp_cd");
        VarCharVector ultmtCdtr = (VarCharVector) root.getVector("ultmt_cdtr_nm");
        VarCharVector cdtrCtry = (VarCharVector) root.getVector("cdtr_ctry");

        int rows = shape.transactions();
        int perRmt = shape.transactionsPerRemittance();
        String[] purposes = {"SALA", "SUPP", "DIVI", "GDDS"};
        for (int t = 0; t < rows; t++) {
            pmtInfId.setSafe(t, ("PI-" + (t / perRmt)).getBytes());
            instrId.setSafe(t, ("INSTR-" + t).getBytes());
            endToEnd.setSafe(t, ("E2E-" + t).getBytes());
            instdAmt.setSafe(t, BigDecimal.valueOf(1000L + t % 9000, 5).setScale(5, RoundingMode.UNNECESSARY));
            ccy.setSafe(t, CURRENCIES[t % CURRENCIES.length].getBytes());
            cdtrBic.setSafe(t, "DEUTDEFFXXX".getBytes());
            cdtrNm.setSafe(t, ("CREDITOR-" + (t % 89)).getBytes());
            cdtrIban.setSafe(t, ("GB" + (300000000 + t) + "0000000000").getBytes());
            rmtUstrd.setSafe(t, ("INVOICE-" + (t % 1000)).getBytes());
            rgltyCd.setSafe(t, ("RRC-" + (t % 97) + "\nRRC-" + (t % 53)).getBytes());
            strdRef.setSafe(t, ("REF-" + t).getBytes());
            strdRefTp.setSafe(t, "SCOR".getBytes());
            purpCd.setSafe(t, purposes[t % purposes.length].getBytes());
            ultmtCdtr.setSafe(t, "ULT-CREDITOR".getBytes());
            cdtrCtry.setSafe(t, "DE".getBytes());
        }
        root.setRowCount(rows);
        return root;
    }
}