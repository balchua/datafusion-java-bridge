package io.github.balchua.datafusion.bench.pain;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.dataformat.xml.XmlMapper;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * pain.001 parser using <b>Jackson 3 XML streaming tokens</b> (Woodstox underneath).
 * Produces three single-root Arrow tables for the flat pain.001 schema.
 *
 * <p>Jackson's XML token model differs from StAX: an element that has children emits
 * {@code START_OBJECT}/{@code END_OBJECT}, a leaf emits {@code PROPERTY_NAME} followed by a
 * value token, attributes appear as fields, and an element that has both an attribute and
 * text exposes the text under an empty property name. Repeated sibling elements are emitted
 * as repeated property names (not arrays).
 */
public final class JacksonPainParser {

    private static final XmlMapper MAPPER = XmlMapper.builder().build();

    public PainTables parse(Path xmlFile, BufferAllocator allocator) throws IOException {
        int declaredTx = declaredTransactionCount(xmlFile);

        Schema messageSchema = Pain001ArrowSchema.createMessageSchema();
        Schema remittanceSchema = Pain001ArrowSchema.createRemittanceSchema();
        Schema transactionSchema = Pain001ArrowSchema.createTransactionSchema();

        VectorSchemaRoot msgRoot = VectorSchemaRoot.create(messageSchema, allocator);
        msgRoot.allocateNew();
        VectorSchemaRoot rmtRoot = VectorSchemaRoot.create(remittanceSchema, allocator);
        rmtRoot.allocateNew();
        VectorSchemaRoot txRoot = VectorSchemaRoot.create(transactionSchema, allocator);
        if (declaredTx > 0) {
            PainVectors.presetCapacity(txRoot, declaredTx);
        }
        txRoot.allocateNew();

        int msgRow = 0;
        int rmtRow = 0;
        int txRow = 0;

        String currentMsgId = null;
        String currentPmtInfId = null;

        boolean inGrpHdr = false;
        boolean inPmtInf = false;
        boolean inCdtTrfTxInf = false;
        boolean inDbtrAgt = false;
        boolean inCdtrAgt = false;
        boolean inDbtrAcct = false;
        boolean inCdtrAcct = false;
        boolean inSvcLvl = false;
        boolean inReqdExctnDt = false;
        boolean inDbtr = false;
        boolean inCdtr = false;
        boolean inRgltryRptg = false;
        boolean inRgltryRptgDtls = false;
        boolean inPmtTpInf = false;
        boolean inLclInstrm = false;
        boolean inCtgyPurp = false;
        boolean inUltmtDbtr = false;
        boolean inUltmtCdtr = false;
        boolean inPstlAdr = false;
        boolean inPurp = false;
        boolean inStrd = false;
        boolean inCdtrRefInf = false;
        boolean inCdtrRefInfTp = false;
        boolean inCdOrPrtry = false;
        boolean inInstdAmt = false;

        String pmtMtd = null;
        String pmtNbOfTxs = null;
        BigDecimal pmtCtrlSum = null;
        String pmtSvcLvlCd = null;
        String pmtReqdExctnDt = null;
        String pmtDbtrNm = null;
        String pmtDbtrAcctIban = null;
        String pmtDbtrAgtBicfi = null;
        String pmtBtchBookg = null;
        String pmtInstrPrty = null;
        String pmtLclInstrmCd = null;
        String pmtCtgyPurpCd = null;
        String pmtChrgBr = null;
        String pmtUltmtDbtrNm = null;

        String grpCreDtTm = null;
        String grpNbOfTxs = null;
        BigDecimal grpCtrlSum = null;
        String grpInitgPtyNm = null;

        String txInstrId = null;
        String txEndToEndId = null;
        BigDecimal txInstdAmt = null;
        String txCcy = null;
        String txCdtrAgtBicfi = null;
        String txCdtrNm = null;
        String txCdtrAcctIban = null;
        StringBuilder txRmtInfUstrd = null;
        StringBuilder txRgltyRptgCd = null;
        String txUltmtCdtrNm = null;
        String txCdtrCtry = null;
        String txPurpCd = null;
        String txRmtInfStrdRef = null;
        String txRmtInfStrdRefTp = null;

        try (InputStream in = Files.newInputStream(xmlFile);
             JsonParser parser = MAPPER.createParser(in)) {

            JsonToken token;
            while ((token = parser.nextToken()) != null) {
                switch (token) {
                    case START_OBJECT -> {
                        switch (name(parser)) {
                            case "GrpHdr" -> inGrpHdr = true;
                            case "PmtInf" -> inPmtInf = true;
                            case "CdtTrfTxInf" -> inCdtTrfTxInf = true;
                            case "DbtrAgt" -> inDbtrAgt = true;
                            case "CdtrAgt" -> inCdtrAgt = true;
                            case "DbtrAcct" -> inDbtrAcct = true;
                            case "CdtrAcct" -> inCdtrAcct = true;
                            case "SvcLvl" -> inSvcLvl = true;
                            case "ReqdExctnDt" -> inReqdExctnDt = true;
                            case "PmtTpInf" -> inPmtTpInf = true;
                            case "LclInstrm" -> inLclInstrm = true;
                            case "CtgyPurp" -> inCtgyPurp = true;
                            case "InstdAmt" -> inInstdAmt = true;
                            case "Strd" -> {
                                if (inCdtTrfTxInf) {
                                    inStrd = true;
                                }
                            }
                            case "CdtrRefInf" -> {
                                if (inCdtTrfTxInf && inStrd) {
                                    inCdtrRefInf = true;
                                }
                            }
                            case "Tp" -> {
                                if (inCdtTrfTxInf && inStrd && inCdtrRefInf) {
                                    inCdtrRefInfTp = true;
                                }
                            }
                            case "CdOrPrtry" -> {
                                if (inCdtTrfTxInf && inStrd && inCdtrRefInf && inCdtrRefInfTp) {
                                    inCdOrPrtry = true;
                                }
                            }
                            case "Purp" -> {
                                if (inCdtTrfTxInf) {
                                    inPurp = true;
                                }
                            }
                            case "Dbtr" -> {
                                if (inPmtInf && !inCdtTrfTxInf) {
                                    inDbtr = true;
                                }
                            }
                            case "Cdtr" -> {
                                if (inCdtTrfTxInf) {
                                    inCdtr = true;
                                }
                            }
                            case "PstlAdr" -> {
                                if (inCdtTrfTxInf && inCdtr) {
                                    inPstlAdr = true;
                                }
                            }
                            case "UltmtDbtr" -> {
                                if (inPmtInf && !inCdtTrfTxInf) {
                                    inUltmtDbtr = true;
                                }
                            }
                            case "UltmtCdtr" -> {
                                if (inCdtTrfTxInf) {
                                    inUltmtCdtr = true;
                                }
                            }
                            case "RgltryRptg" -> {
                                if (inCdtTrfTxInf) {
                                    inRgltryRptg = true;
                                }
                            }
                            case "Dtls" -> {
                                if (inRgltryRptg) {
                                    inRgltryRptgDtls = true;
                                }
                            }
                            default -> {
                            }
                        }
                    }

                    case VALUE_STRING -> {
                        String field = name(parser);
                        String value = parser.getString().trim();

                        if (inInstdAmt && "Ccy".equals(field)) {
                            txCcy = value;
                            break;
                        }
                        if (inInstdAmt && field.isEmpty()) {
                            txInstdAmt = value.isEmpty() ? null : new BigDecimal(value);
                            break;
                        }
                        if ("InstdAmt".equals(field)) {
                            txInstdAmt = value.isEmpty() ? null : new BigDecimal(value);
                            break;
                        }

                        switch (field) {
                            case "MsgId" -> {
                                if (inGrpHdr) {
                                    currentMsgId = value;
                                }
                            }
                            case "CreDtTm" -> {
                                if (inGrpHdr) {
                                    grpCreDtTm = value;
                                }
                            }
                            case "NbOfTxs" -> {
                                if (inGrpHdr && !inPmtInf) {
                                    grpNbOfTxs = value;
                                } else if (inPmtInf && !inCdtTrfTxInf) {
                                    pmtNbOfTxs = value;
                                }
                            }
                            case "CtrlSum" -> {
                                if (inGrpHdr && !inPmtInf) {
                                    grpCtrlSum = value.isEmpty() ? null : new BigDecimal(value);
                                } else if (inPmtInf && !inCdtTrfTxInf) {
                                    pmtCtrlSum = value.isEmpty() ? null : new BigDecimal(value);
                                }
                            }
                            case "Nm" -> {
                                if (inGrpHdr && !inPmtInf) {
                                    grpInitgPtyNm = value;
                                } else if (inCdtTrfTxInf && inUltmtCdtr) {
                                    txUltmtCdtrNm = value;
                                } else if (inCdtTrfTxInf && inCdtr) {
                                    txCdtrNm = value;
                                } else if (inPmtInf && !inCdtTrfTxInf && inUltmtDbtr) {
                                    pmtUltmtDbtrNm = value;
                                } else if (inPmtInf && !inCdtTrfTxInf && inDbtr) {
                                    pmtDbtrNm = value;
                                }
                            }
                            case "PmtInfId" -> {
                                if (inPmtInf && !inCdtTrfTxInf) {
                                    currentPmtInfId = value;
                                }
                            }
                            case "PmtMtd" -> {
                                if (inPmtInf) {
                                    pmtMtd = value;
                                }
                            }
                            case "Cd" -> {
                                if (inPmtInf && !inCdtTrfTxInf && inSvcLvl) {
                                    pmtSvcLvlCd = value;
                                } else if (inPmtInf && !inCdtTrfTxInf && inLclInstrm) {
                                    pmtLclInstrmCd = value;
                                } else if (inPmtInf && !inCdtTrfTxInf && inCtgyPurp) {
                                    pmtCtgyPurpCd = value;
                                } else if (inCdtTrfTxInf && inPurp) {
                                    txPurpCd = value;
                                } else if (inCdtTrfTxInf && inStrd && inCdtrRefInf
                                        && inCdtrRefInfTp && inCdOrPrtry) {
                                    txRmtInfStrdRefTp = value;
                                } else if (inCdtTrfTxInf && inRgltryRptg && inRgltryRptgDtls
                                        && !value.isEmpty()) {
                                    txRgltyRptgCd = txRgltyRptgCd == null
                                            ? new StringBuilder(value)
                                            : txRgltyRptgCd.append('\n').append(value);
                                }
                            }
                            case "Dt" -> {
                                if (inPmtInf && !inCdtTrfTxInf && inReqdExctnDt) {
                                    pmtReqdExctnDt = value;
                                }
                            }
                            case "BtchBookg" -> {
                                if (inPmtInf && !inCdtTrfTxInf) {
                                    pmtBtchBookg = value;
                                }
                            }
                            case "InstrPrty" -> {
                                if (inPmtInf && !inCdtTrfTxInf && inPmtTpInf) {
                                    pmtInstrPrty = value;
                                }
                            }
                            case "ChrgBr" -> {
                                if (inPmtInf && !inCdtTrfTxInf) {
                                    pmtChrgBr = value;
                                }
                            }
                            case "Ctry" -> {
                                if (inCdtTrfTxInf && inCdtr && inPstlAdr) {
                                    txCdtrCtry = value;
                                }
                            }
                            case "Ref" -> {
                                if (inCdtTrfTxInf && inStrd && inCdtrRefInf) {
                                    txRmtInfStrdRef = value;
                                }
                            }
                            case "IBAN" -> {
                                if (inCdtTrfTxInf && inCdtrAcct) {
                                    txCdtrAcctIban = value;
                                } else if (inPmtInf && !inCdtTrfTxInf && inDbtrAcct) {
                                    pmtDbtrAcctIban = value;
                                }
                            }
                            case "BICFI" -> {
                                if (inCdtTrfTxInf && inCdtrAgt) {
                                    txCdtrAgtBicfi = value;
                                } else if (inPmtInf && !inCdtTrfTxInf && inDbtrAgt) {
                                    pmtDbtrAgtBicfi = value;
                                }
                            }
                            case "InstrId" -> {
                                if (inCdtTrfTxInf) {
                                    txInstrId = value;
                                }
                            }
                            case "EndToEndId" -> {
                                if (inCdtTrfTxInf) {
                                    txEndToEndId = value;
                                }
                            }
                            case "Ustrd" -> {
                                if (inCdtTrfTxInf && !value.isEmpty()) {
                                    txRmtInfUstrd = txRmtInfUstrd == null
                                            ? new StringBuilder(value)
                                            : txRmtInfUstrd.append('\n').append(value);
                                }
                            }
                            default -> {
                            }
                        }
                    }

                    case END_OBJECT -> {
                        switch (name(parser)) {
                            case "GrpHdr" -> {
                                inGrpHdr = false;
                                PainVectors.setVarChar(msgRoot, Pain001ArrowSchema.MSG_ID, msgRow, currentMsgId);
                                PainVectors.setVarChar(msgRoot, Pain001ArrowSchema.MSG_CRE_DT_TM, msgRow, grpCreDtTm);
                                PainVectors.setVarChar(msgRoot, Pain001ArrowSchema.MSG_NB_OF_TXS, msgRow, grpNbOfTxs);
                                PainVectors.setDecimal(msgRoot, Pain001ArrowSchema.MSG_CTRL_SUM, msgRow, grpCtrlSum);
                                PainVectors.setVarChar(msgRoot, Pain001ArrowSchema.MSG_INITG_PTY_NM, msgRow, grpInitgPtyNm);
                                msgRow++;
                            }
                            case "PmtInf" -> {
                                inPmtInf = false;
                                PainVectors.setVarChar(rmtRoot, Pain001ArrowSchema.RMT_MSG_ID, rmtRow, currentMsgId);
                                PainVectors.setVarChar(rmtRoot, Pain001ArrowSchema.RMT_PMT_INF_ID, rmtRow, currentPmtInfId);
                                PainVectors.setVarChar(rmtRoot, Pain001ArrowSchema.RMT_PMT_MTD, rmtRow, pmtMtd);
                                PainVectors.setVarChar(rmtRoot, Pain001ArrowSchema.RMT_NB_OF_TXS, rmtRow, pmtNbOfTxs);
                                PainVectors.setDecimal(rmtRoot, Pain001ArrowSchema.RMT_CTRL_SUM, rmtRow, pmtCtrlSum);
                                PainVectors.setVarChar(rmtRoot, Pain001ArrowSchema.RMT_SVC_LVL_CD, rmtRow, pmtSvcLvlCd);
                                PainVectors.setDateDay(rmtRoot, Pain001ArrowSchema.RMT_REQD_EXCTN_DT, rmtRow, pmtReqdExctnDt);
                                PainVectors.setVarChar(rmtRoot, Pain001ArrowSchema.RMT_DBTR_NM, rmtRow, pmtDbtrNm);
                                PainVectors.setVarChar(rmtRoot, Pain001ArrowSchema.RMT_DBTR_ACCT_IBAN, rmtRow, pmtDbtrAcctIban);
                                PainVectors.setVarChar(rmtRoot, Pain001ArrowSchema.RMT_DBTR_AGT_BICFI, rmtRow, pmtDbtrAgtBicfi);
                                PainVectors.setVarChar(rmtRoot, Pain001ArrowSchema.RMT_BTCH_BOOKG, rmtRow, pmtBtchBookg);
                                PainVectors.setVarChar(rmtRoot, Pain001ArrowSchema.RMT_INSTR_PRTY, rmtRow, pmtInstrPrty);
                                PainVectors.setVarChar(rmtRoot, Pain001ArrowSchema.RMT_LCL_INSTRM_CD, rmtRow, pmtLclInstrmCd);
                                PainVectors.setVarChar(rmtRoot, Pain001ArrowSchema.RMT_CTGY_PURP_CD, rmtRow, pmtCtgyPurpCd);
                                PainVectors.setVarChar(rmtRoot, Pain001ArrowSchema.RMT_CHRG_BR, rmtRow, pmtChrgBr);
                                PainVectors.setVarChar(rmtRoot, Pain001ArrowSchema.RMT_ULTMT_DBTR_NM, rmtRow, pmtUltmtDbtrNm);
                                rmtRow++;

                                currentPmtInfId = null;
                                pmtMtd = null;
                                pmtNbOfTxs = null;
                                pmtCtrlSum = null;
                                pmtSvcLvlCd = null;
                                pmtReqdExctnDt = null;
                                pmtDbtrNm = null;
                                pmtDbtrAcctIban = null;
                                pmtDbtrAgtBicfi = null;
                                pmtBtchBookg = null;
                                pmtInstrPrty = null;
                                pmtLclInstrmCd = null;
                                pmtCtgyPurpCd = null;
                                pmtChrgBr = null;
                                pmtUltmtDbtrNm = null;
                            }
                            case "CdtTrfTxInf" -> {
                                inCdtTrfTxInf = false;
                                PainVectors.setVarChar(txRoot, Pain001ArrowSchema.TX_PMT_INF_ID, txRow, currentPmtInfId);
                                PainVectors.setVarChar(txRoot, Pain001ArrowSchema.TX_INSTR_ID, txRow, txInstrId);
                                PainVectors.setVarChar(txRoot, Pain001ArrowSchema.TX_END_TO_END_ID, txRow, txEndToEndId);
                                PainVectors.setDecimal(txRoot, Pain001ArrowSchema.TX_INSTD_AMT, txRow, txInstdAmt);
                                PainVectors.setVarChar(txRoot, Pain001ArrowSchema.TX_CCY, txRow, txCcy);
                                PainVectors.setVarChar(txRoot, Pain001ArrowSchema.TX_CDTR_AGT_BICFI, txRow, txCdtrAgtBicfi);
                                PainVectors.setVarChar(txRoot, Pain001ArrowSchema.TX_CDTR_NM, txRow, txCdtrNm);
                                PainVectors.setVarChar(txRoot, Pain001ArrowSchema.TX_CDTR_ACCT_IBAN, txRow, txCdtrAcctIban);
                                PainVectors.setVarChar(txRoot, Pain001ArrowSchema.TX_RMT_INF_USTRD, txRow,
                                        txRmtInfUstrd != null ? txRmtInfUstrd.toString() : null);
                                PainVectors.setVarChar(txRoot, Pain001ArrowSchema.TX_RGLTY_RPTG_CD, txRow,
                                        txRgltyRptgCd != null ? txRgltyRptgCd.toString() : null);
                                PainVectors.setVarChar(txRoot, Pain001ArrowSchema.TX_RMT_INF_STRD_REF, txRow, txRmtInfStrdRef);
                                PainVectors.setVarChar(txRoot, Pain001ArrowSchema.TX_RMT_INF_STRD_REF_TP, txRow, txRmtInfStrdRefTp);
                                PainVectors.setVarChar(txRoot, Pain001ArrowSchema.TX_PURP_CD, txRow, txPurpCd);
                                PainVectors.setVarChar(txRoot, Pain001ArrowSchema.TX_ULTMT_CDTR_NM, txRow, txUltmtCdtrNm);
                                PainVectors.setVarChar(txRoot, Pain001ArrowSchema.TX_CDTR_CTRY, txRow, txCdtrCtry);
                                txRow++;

                                txInstrId = null;
                                txEndToEndId = null;
                                txInstdAmt = null;
                                txCcy = null;
                                txCdtrAgtBicfi = null;
                                txCdtrNm = null;
                                txCdtrAcctIban = null;
                                txRmtInfUstrd = null;
                                txRgltyRptgCd = null;
                                txUltmtCdtrNm = null;
                                txCdtrCtry = null;
                                txPurpCd = null;
                                txRmtInfStrdRef = null;
                                txRmtInfStrdRefTp = null;
                            }
                            case "DbtrAgt" -> inDbtrAgt = false;
                            case "CdtrAgt" -> inCdtrAgt = false;
                            case "DbtrAcct" -> inDbtrAcct = false;
                            case "CdtrAcct" -> inCdtrAcct = false;
                            case "SvcLvl" -> inSvcLvl = false;
                            case "ReqdExctnDt" -> inReqdExctnDt = false;
                            case "PmtTpInf" -> inPmtTpInf = false;
                            case "LclInstrm" -> inLclInstrm = false;
                            case "CtgyPurp" -> inCtgyPurp = false;
                            case "InstdAmt" -> inInstdAmt = false;
                            case "Purp" -> inPurp = false;
                            case "CdOrPrtry" -> inCdOrPrtry = false;
                            case "Tp" -> {
                                if (inCdtTrfTxInf && inStrd && inCdtrRefInf) {
                                    inCdtrRefInfTp = false;
                                }
                            }
                            case "CdtrRefInf" -> {
                                if (inCdtTrfTxInf && inStrd) {
                                    inCdtrRefInf = false;
                                }
                            }
                            case "Strd" -> {
                                if (inCdtTrfTxInf) {
                                    inStrd = false;
                                }
                            }
                            case "Dbtr" -> {
                                if (inPmtInf && !inCdtTrfTxInf) {
                                    inDbtr = false;
                                }
                            }
                            case "Cdtr" -> {
                                if (inCdtTrfTxInf) {
                                    inCdtr = false;
                                }
                            }
                            case "PstlAdr" -> inPstlAdr = false;
                            case "UltmtDbtr" -> {
                                if (inPmtInf && !inCdtTrfTxInf) {
                                    inUltmtDbtr = false;
                                }
                            }
                            case "UltmtCdtr" -> {
                                if (inCdtTrfTxInf) {
                                    inUltmtCdtr = false;
                                }
                            }
                            case "Dtls" -> inRgltryRptgDtls = false;
                            case "RgltryRptg" -> {
                                if (inCdtTrfTxInf) {
                                    inRgltryRptg = false;
                                }
                            }
                            default -> {
                            }
                        }
                    }

                    default -> {
                    }
                }
            }
        } catch (RuntimeException e) {
            msgRoot.close();
            rmtRoot.close();
            txRoot.close();
            throw e;
        }

        msgRoot.setRowCount(msgRow);
        rmtRoot.setRowCount(rmtRow);
        txRoot.setRowCount(txRow);
        return new PainTables(msgRoot, rmtRoot, txRoot, msgRow, rmtRow, txRow);
    }

    /**
     * Reads the declared transaction count from {@code GrpHdr/NbOfTxs} (a few KB from the
     * start of the file) so the transaction root can be pre-sized. Returns 0 if it cannot be
     * read (the root then grows as needed).
     */
    private static int declaredTransactionCount(Path xmlFile) {
        try (InputStream in = Files.newInputStream(xmlFile);
             JsonParser parser = MAPPER.createParser(in)) {
            boolean inGrpHdr = false;
            JsonToken token;
            while ((token = parser.nextToken()) != null) {
                if (token == JsonToken.START_OBJECT) {
                    if ("GrpHdr".equals(name(parser))) {
                        inGrpHdr = true;
                    }
                } else if (token == JsonToken.VALUE_STRING) {
                    if (inGrpHdr && "NbOfTxs".equals(name(parser))) {
                        long n = Long.parseLong(parser.getString().trim());
                        return (int) Math.min(Integer.MAX_VALUE, Math.max(0, n));
                    }
                } else if (token == JsonToken.END_OBJECT && "GrpHdr".equals(name(parser))) {
                    return 0;
                }
            }
        } catch (IOException | RuntimeException e) {
            return 0;
        }
        return 0;
    }

    /** Current field/element name, or "" when Jackson has no current name. */
    private static String name(JsonParser parser) {
        try {
            return parser.currentName();
        } catch (IllegalStateException e) {
            return "";
        }
    }
}
