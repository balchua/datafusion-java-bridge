package io.github.balchua.datafusion.bench.pain;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * Three Apache Arrow schemas for ISO 20022 pain.001.001.09 (message / remittance /
 * transaction), copied from the sibling {@code java-arrow-pain} project
 * ({@code com.pgw.arrow.Pain001ArrowSchema}) so the XML→Arrow comparison uses the
 * identical flat schema. See {@link JacksonPainParser}.
 *
 * <p>Message = one row per GrpHdr; remittance = one row per PmtInf; transaction = one row
 * per CdtTrfTxInf. The transaction table has 15 flat columns.
 */
public final class Pain001ArrowSchema {

    private Pain001ArrowSchema() {
    }

    private static final ArrowType UTF8 = ArrowType.Utf8.INSTANCE;
    private static final ArrowType DECIMAL_18_2 = new ArrowType.Decimal(18, 2, 128);
    private static final ArrowType DECIMAL_18_5 = new ArrowType.Decimal(18, 5, 128);
    private static final ArrowType DATE_DAY = new ArrowType.Date(DateUnit.DAY);

    // ── Message (GroupHeader) field names ───────────────────────────────────
    public static final String MSG_ID = "msg_id";
    public static final String MSG_CRE_DT_TM = "msg_cre_dt_tm";
    public static final String MSG_NB_OF_TXS = "msg_nb_of_txs";
    public static final String MSG_CTRL_SUM = "msg_ctrl_sum";
    public static final String MSG_INITG_PTY_NM = "msg_initg_pty_nm";

    // ── Remittance (PmtInf) field names ─────────────────────────────────────
    public static final String RMT_MSG_ID = "msg_id";
    public static final String RMT_PMT_INF_ID = "pmt_inf_id";
    public static final String RMT_PMT_MTD = "pmt_mtd";
    public static final String RMT_NB_OF_TXS = "nb_of_txs";
    public static final String RMT_CTRL_SUM = "ctrl_sum";
    public static final String RMT_SVC_LVL_CD = "svc_lvl_cd";
    public static final String RMT_REQD_EXCTN_DT = "reqd_exctn_dt";
    public static final String RMT_DBTR_NM = "dbtr_nm";
    public static final String RMT_DBTR_ACCT_IBAN = "dbtr_acct_iban";
    public static final String RMT_DBTR_AGT_BICFI = "dbtr_agt_bicfi";
    public static final String RMT_BTCH_BOOKG = "btch_bookg";
    public static final String RMT_INSTR_PRTY = "instr_prty";
    public static final String RMT_LCL_INSTRM_CD = "lcl_instrm_cd";
    public static final String RMT_CTGY_PURP_CD = "ctgy_purp_cd";
    public static final String RMT_CHRG_BR = "chrg_br";
    public static final String RMT_ULTMT_DBTR_NM = "ultmt_dbtr_nm";

    // ── Transaction (CdtTrfTxInf) field names ───────────────────────────────
    public static final String TX_PMT_INF_ID = "pmt_inf_id";
    public static final String TX_INSTR_ID = "instr_id";
    public static final String TX_END_TO_END_ID = "end_to_end_id";
    public static final String TX_INSTD_AMT = "instd_amt";
    public static final String TX_CCY = "ccy";
    public static final String TX_CDTR_AGT_BICFI = "cdtr_agt_bicfi";
    public static final String TX_CDTR_NM = "cdtr_nm";
    public static final String TX_CDTR_ACCT_IBAN = "cdtr_acct_iban";
    public static final String TX_RMT_INF_USTRD = "rmt_inf_ustrd";
    public static final String TX_RGLTY_RPTG_CD = "rglty_rptg_cd";
    public static final String TX_RMT_INF_STRD_REF = "rmt_inf_strd_ref";
    public static final String TX_RMT_INF_STRD_REF_TP = "rmt_inf_strd_ref_tp";
    public static final String TX_PURP_CD = "purp_cd";
    public static final String TX_ULTMT_CDTR_NM = "ultmt_cdtr_nm";
    public static final String TX_CDTR_CTRY = "cdtr_ctry";

    public static Schema createMessageSchema() {
        return new Schema(List.of(
                field(MSG_ID, UTF8, false),
                field(MSG_CRE_DT_TM, UTF8, false),
                field(MSG_NB_OF_TXS, UTF8, false),
                field(MSG_CTRL_SUM, DECIMAL_18_2, false),
                field(MSG_INITG_PTY_NM, UTF8, false)));
    }

    public static Schema createRemittanceSchema() {
        return new Schema(List.of(
                field(RMT_MSG_ID, UTF8, false),
                field(RMT_PMT_INF_ID, UTF8, false),
                field(RMT_PMT_MTD, UTF8, false),
                field(RMT_NB_OF_TXS, UTF8, false),
                field(RMT_CTRL_SUM, DECIMAL_18_2, true),
                field(RMT_SVC_LVL_CD, UTF8, true),
                field(RMT_REQD_EXCTN_DT, DATE_DAY, false),
                field(RMT_DBTR_NM, UTF8, false),
                field(RMT_DBTR_ACCT_IBAN, UTF8, false),
                field(RMT_DBTR_AGT_BICFI, UTF8, false),
                field(RMT_BTCH_BOOKG, UTF8, true),
                field(RMT_INSTR_PRTY, UTF8, true),
                field(RMT_LCL_INSTRM_CD, UTF8, true),
                field(RMT_CTGY_PURP_CD, UTF8, true),
                field(RMT_CHRG_BR, UTF8, true),
                field(RMT_ULTMT_DBTR_NM, UTF8, true)));
    }

    public static Schema createTransactionSchema() {
        return new Schema(List.of(
                field(TX_PMT_INF_ID, UTF8, false),
                field(TX_INSTR_ID, UTF8, true),
                field(TX_END_TO_END_ID, UTF8, false),
                field(TX_INSTD_AMT, DECIMAL_18_5, false),
                field(TX_CCY, UTF8, false),
                field(TX_CDTR_AGT_BICFI, UTF8, true),
                field(TX_CDTR_NM, UTF8, false),
                field(TX_CDTR_ACCT_IBAN, UTF8, false),
                field(TX_RMT_INF_USTRD, UTF8, true),
                field(TX_RGLTY_RPTG_CD, UTF8, true),
                field(TX_RMT_INF_STRD_REF, UTF8, true),
                field(TX_RMT_INF_STRD_REF_TP, UTF8, true),
                field(TX_PURP_CD, UTF8, true),
                field(TX_ULTMT_CDTR_NM, UTF8, true),
                field(TX_CDTR_CTRY, UTF8, true)));
    }

    private static Field field(String name, ArrowType type, boolean nullable) {
        return new Field(name, new FieldType(nullable, type, null), null);
    }
}
