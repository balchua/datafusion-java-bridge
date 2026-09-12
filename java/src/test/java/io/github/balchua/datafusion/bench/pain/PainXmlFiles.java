package io.github.balchua.datafusion.bench.pain;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Locates/generates the pain.001 XML fixtures. Generated files live under
 * {@code bench/pain-xml} (gitignored, like the rest of {@code bench/}) and are reused
 * across runs; nothing large is committed.
 */
public final class PainXmlFiles {

    public static final Path DEFAULT_DIR = Path.of("bench", "pain-xml");

    /** Type A: 1 PmtInf × 1,000,000 CdtTrfTxInf — the 1M comparison case. */
    public static final PainFileSpec TYPE_A = new PainFileSpec(
            "Type A (1 PmtInf x 1,000,000 TxInf)",
            "pain001_type_a_1x1M.xml", 1, 1_000_000);

    /** Small fixture parsed a few times to JIT-warm the parser before measuring Type A. */
    public static final PainFileSpec WARMUP = new PainFileSpec(
            "Warmup (1 PmtInf x 10,000 TxInf)",
            "pain001_warmup_10k.xml", 1, 10_000);

    private PainXmlFiles() {}

    /** Generates {@code spec} into {@code dir} if absent/incomplete; returns its path. */
    public static Path ensureGenerated(PainFileSpec spec, Path dir)
            throws IOException, XMLStreamException {
        Files.createDirectories(dir);
        Path file = dir.resolve(spec.fileName());
        if (Files.exists(file) && isComplete(file)) {
            return file;
        }
        return new PainXmlGenerator().generate(spec, dir);
    }

    /** A generated file is complete when it ends with the closing document element. */
    static boolean isComplete(Path xmlFile) {
        try {
            long size = Files.size(xmlFile);
            if (size < 100) {
                return false;
            }
            try (RandomAccessFile raf = new RandomAccessFile(xmlFile.toFile(), "r")) {
                long seekPos = Math.max(0, size - 64);
                raf.seek(seekPos);
                byte[] tail = new byte[(int) (size - seekPos)];
                raf.readFully(tail);
                return new String(tail, StandardCharsets.UTF_8).contains("</Document>");
            }
        } catch (IOException e) {
            return false;
        }
    }
}
