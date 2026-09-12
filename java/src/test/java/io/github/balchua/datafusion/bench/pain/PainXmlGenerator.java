package io.github.balchua.datafusion.bench.pain;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * StAX writer for ISO 20022 pain.001.001.09 XML files, copied from the sibling
 * {@code java-arrow-pain} project ({@code com.pgw.generator.PainXmlGeneratorImpl}) so the
 * comparison uses byte-for-byte the same generated input. Logging was removed.
 */
public final class PainXmlGenerator {

    private static final String ISO20022_NAMESPACE =
            "urn:iso:std:iso:20022:tech:xsd:pain.001.001.09";
    private static final String XSI_NAMESPACE = "http://www.w3.org/2001/XMLSchema-instance";

    private static final DateTimeFormatter ISO_DATETIME_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
    private static final DateTimeFormatter ISO_DATE_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private static final String[] DEBTOR_IBANS = {
            "DE89370400440532013000", "FR7630006000011234567890189",
            "GB29NWBK60161331926819", "NL91ABNA0417164300"
    };
    private static final String[] DEBTOR_BICS = {
            "COBADEFFXXX", "BNPAFRPPXXX", "NWBKGB2LXXX", "ABNANL2AXXX"
    };
    private static final String[] CREDITOR_IBANS = {
            "IT60X0542811101000000123456", "ES9121000418450200051332",
            "AT611904300234573201", "BE68539007547034",
            "CH9300762011623852957", "PT50000201231234567890154"
    };
    private static final String[] CREDITOR_BICS = {
            "BPPIITRRXXX", "CABORKMRXXX", "BKAUATWWXXX", "BBRUBEBB010",
            "UBSWCHZH80A", "CGDIPTPLXXX"
    };
    private static final String[] CREDITOR_NAMES = {
            "Acme Corporation", "Global Industries Ltd", "Smith Trading Co",
            "EuroPayments GmbH", "Nordic Supplies AB", "Atlantic Services SA"
    };
    private static final String[] CREDITOR_COUNTRIES = {"IT", "ES", "AT", "BE", "CH", "PT"};
    private static final String[] REGULATORY_CODES = {
            "BENEFICIARY_RESIDENT", "RESIDENT", "NONRESIDENT", "REGLF001", "REGLF002"
    };
    private static final String[] CATEGORY_PURPOSE_CODES = {"SALA", "SUPP", "TAXS", "GDDS", "TRAD"};
    private static final String[] PURPOSE_CODES = {"GDDS", "SUPP", "SALA", "TAXS", "TRAD"};

    public PainXmlGenerator() {}

    /** Generates {@code spec} into {@code outputDir} and returns the file path. */
    public Path generate(PainFileSpec spec, Path outputDir)
            throws IOException, XMLStreamException {

        Files.createDirectories(outputDir);
        Path outputFile = outputDir.resolve(spec.fileName());

        XMLOutputFactory factory = XMLOutputFactory.newInstance();
        try (OutputStream fileOut = new BufferedOutputStream(
                Files.newOutputStream(outputFile), 1024 * 1024)) {
            XMLStreamWriter writer = factory.createXMLStreamWriter(fileOut, "UTF-8");
            try {
                writeDocument(writer, spec);
            } finally {
                writer.close();
            }
        }
        return outputFile;
    }

    private static void writeDocument(XMLStreamWriter writer, PainFileSpec spec)
            throws XMLStreamException {
        writer.writeStartDocument("UTF-8", "1.0");
        writer.writeStartElement("Document");
        writer.writeNamespace("", ISO20022_NAMESPACE);
        writer.writeNamespace("xsi", XSI_NAMESPACE);
        writer.writeStartElement("CstmrCdtTrfInitn");

        writeGroupHeader(writer, spec);

        BigDecimal txAmount = new BigDecimal("100.00");
        for (int pmtIdx = 0; pmtIdx < spec.numberOfPaymentInfoBlocks(); pmtIdx++) {
            writePaymentInformation(writer, spec, pmtIdx, txAmount);
        }

        writer.writeEndElement(); // CstmrCdtTrfInitn
        writer.writeEndElement(); // Document
        writer.writeEndDocument();
        writer.flush();
    }

    private static void writeGroupHeader(XMLStreamWriter writer, PainFileSpec spec)
            throws XMLStreamException {
        long totalTxns = spec.totalTransactions();
        String ctrlSumStr;
        if (spec.invalidControlSum()) {
            ctrlSumStr = "99999.99";
        } else {
            ctrlSumStr = new BigDecimal("100.00")
                    .multiply(BigDecimal.valueOf(totalTxns))
                    .setScale(2, RoundingMode.HALF_UP)
                    .toPlainString();
        }

        writer.writeStartElement("GrpHdr");
        writeElement(writer, "MsgId", "MSG-" + System.currentTimeMillis());
        writeElement(writer, "CreDtTm", LocalDateTime.now().format(ISO_DATETIME_FORMAT));
        writeElement(writer, "NbOfTxs", String.valueOf(totalTxns));
        writeElement(writer, "CtrlSum", ctrlSumStr);
        writer.writeStartElement("InitgPty");
        writeElement(writer, "Nm", "Sample Initiating Party Corp");
        writer.writeEndElement(); // InitgPty
        writer.writeEndElement(); // GrpHdr
    }

    private static void writePaymentInformation(XMLStreamWriter writer, PainFileSpec spec,
            int pmtIndex, BigDecimal txAmount) throws XMLStreamException {
        int txCount = spec.transactionsPerBlock();
        String blockCtrlSumStr;
        if (spec.invalidControlSum()) {
            blockCtrlSumStr = "0.01";
        } else {
            blockCtrlSumStr = txAmount.multiply(BigDecimal.valueOf(txCount))
                    .setScale(2, RoundingMode.HALF_UP)
                    .toPlainString();
        }

        String debtorIban = DEBTOR_IBANS[pmtIndex % DEBTOR_IBANS.length];
        String debtorBic = DEBTOR_BICS[pmtIndex % DEBTOR_BICS.length];

        writer.writeStartElement("PmtInf");
        writeElement(writer, "PmtInfId", String.format("PMT-%06d", pmtIndex + 1));
        writeElement(writer, "PmtMtd", "TRF");
        writeElement(writer, "BtchBookg", "false");
        writeElement(writer, "NbOfTxs", String.valueOf(txCount));
        writeElement(writer, "CtrlSum", blockCtrlSumStr);

        writer.writeStartElement("PmtTpInf");
        writeElement(writer, "InstrPrty", pmtIndex % 3 == 0 ? "HIGH" : "NORM");
        writer.writeStartElement("SvcLvl");
        writeElement(writer, "Cd", "SEPA");
        writer.writeEndElement(); // SvcLvl
        writer.writeStartElement("LclInstrm");
        writeElement(writer, "Cd", "CORE");
        writer.writeEndElement(); // LclInstrm
        writer.writeStartElement("CtgyPurp");
        writeElement(writer, "Cd", CATEGORY_PURPOSE_CODES[pmtIndex % CATEGORY_PURPOSE_CODES.length]);
        writer.writeEndElement(); // CtgyPurp
        writer.writeEndElement(); // PmtTpInf

        writeElement(writer, "ChrgBr", "SLEV");

        writer.writeStartElement("ReqdExctnDt");
        writeElement(writer, "Dt", LocalDate.now().plusDays(1).format(ISO_DATE_FORMAT));
        writer.writeEndElement(); // ReqdExctnDt

        writer.writeStartElement("Dbtr");
        writeElement(writer, "Nm", "Debtor Company " + (pmtIndex + 1));
        writer.writeEndElement(); // Dbtr

        writer.writeStartElement("DbtrAcct");
        writer.writeStartElement("Id");
        writeElement(writer, "IBAN", debtorIban);
        writer.writeEndElement(); // Id
        writer.writeEndElement(); // DbtrAcct

        writer.writeStartElement("DbtrAgt");
        writer.writeStartElement("FinInstnId");
        writeElement(writer, "BICFI", debtorBic);
        writer.writeEndElement(); // FinInstnId
        writer.writeEndElement(); // DbtrAgt

        writer.writeStartElement("UltmtDbtr");
        writeElement(writer, "Nm", "Ultimate Debtor " + (pmtIndex + 1));
        writer.writeEndElement(); // UltmtDbtr

        for (int txIdx = 0; txIdx < txCount; txIdx++) {
            writeCreditTransferTransaction(writer, pmtIndex, txIdx, txAmount);
        }

        writer.writeEndElement(); // PmtInf
    }

    private static void writeCreditTransferTransaction(XMLStreamWriter writer,
            int pmtIndex, int txIndex, BigDecimal amount) throws XMLStreamException {
        int credIdx = txIndex % CREDITOR_NAMES.length;

        writer.writeStartElement("CdtTrfTxInf");

        writer.writeStartElement("PmtId");
        writeElement(writer, "InstrId", String.format("INSTR-%06d-%07d", pmtIndex + 1, txIndex + 1));
        writeElement(writer, "EndToEndId", String.format("E2E-%06d-%07d", pmtIndex + 1, txIndex + 1));
        writer.writeEndElement(); // PmtId

        writer.writeStartElement("Amt");
        writer.writeStartElement("InstdAmt");
        writer.writeAttribute("Ccy", "EUR");
        writer.writeCharacters(amount.toPlainString());
        writer.writeEndElement(); // InstdAmt
        writer.writeEndElement(); // Amt

        writer.writeStartElement("CdtrAgt");
        writer.writeStartElement("FinInstnId");
        writeElement(writer, "BICFI", CREDITOR_BICS[credIdx]);
        writer.writeEndElement(); // FinInstnId
        writer.writeEndElement(); // CdtrAgt

        writer.writeStartElement("UltmtCdtr");
        writeElement(writer, "Nm", "Ultimate " + CREDITOR_NAMES[credIdx]);
        writer.writeEndElement(); // UltmtCdtr

        writer.writeStartElement("Cdtr");
        writeElement(writer, "Nm", CREDITOR_NAMES[credIdx]);
        writer.writeStartElement("PstlAdr");
        writeElement(writer, "Ctry", CREDITOR_COUNTRIES[credIdx]);
        writer.writeEndElement(); // PstlAdr
        writer.writeEndElement(); // Cdtr

        writer.writeStartElement("CdtrAcct");
        writer.writeStartElement("Id");
        writeElement(writer, "IBAN", CREDITOR_IBANS[credIdx]);
        writer.writeEndElement(); // Id
        writer.writeEndElement(); // CdtrAcct

        writer.writeStartElement("Purp");
        writeElement(writer, "Cd", PURPOSE_CODES[txIndex % PURPOSE_CODES.length]);
        writer.writeEndElement(); // Purp

        writer.writeStartElement("RmtInf");
        writeElement(writer, "Ustrd", String.format("Invoice PMT%06d-TX%07d", pmtIndex + 1, txIndex + 1));
        writeElement(writer, "Ustrd", String.format("Ref %s/%s",
                CREDITOR_NAMES[credIdx].replaceAll("\\s+", "-"), txIndex + 1));
        writer.writeStartElement("Strd");
        writer.writeStartElement("CdtrRefInf");
        writer.writeStartElement("Tp");
        writer.writeStartElement("CdOrPrtry");
        writeElement(writer, "Cd", "SCOR");
        writer.writeEndElement(); // CdOrPrtry
        writer.writeEndElement(); // Tp
        writeElement(writer, "Ref", String.format("REF%06d%07d", pmtIndex + 1, txIndex + 1));
        writer.writeEndElement(); // CdtrRefInf
        writer.writeEndElement(); // Strd
        writer.writeEndElement(); // RmtInf

        writer.writeStartElement("RgltryRptg");
        writer.writeStartElement("Dtls");
        writeElement(writer, "Cd", REGULATORY_CODES[txIndex % REGULATORY_CODES.length]);
        writer.writeEndElement(); // Dtls
        writer.writeStartElement("Dtls");
        writeElement(writer, "Cd", REGULATORY_CODES[(txIndex + 1) % REGULATORY_CODES.length]);
        writer.writeEndElement(); // Dtls
        writer.writeEndElement(); // RgltryRptg

        writer.writeEndElement(); // CdtTrfTxInf
    }

    private static void writeElement(XMLStreamWriter writer, String localName, String text)
            throws XMLStreamException {
        writer.writeStartElement(localName);
        writer.writeCharacters(text);
        writer.writeEndElement();
    }
}
