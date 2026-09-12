package io.github.balchua.datafusion.bench.pain;

/**
 * Specifies a pain.001.001.09 file to generate. Copied from the sibling
 * {@code java-arrow-pain} project ({@code com.pgw.generator.PainFileSpec}).
 *
 * @param name                      human-readable label
 * @param fileName                  output file name
 * @param numberOfPaymentInfoBlocks number of PmtInf blocks
 * @param transactionsPerBlock      number of CdtTrfTxInf per PmtInf block
 * @param invalidControlSum         if true, GrpHdr CtrlSum is deliberately wrong
 */
public record PainFileSpec(
        String name,
        String fileName,
        int numberOfPaymentInfoBlocks,
        int transactionsPerBlock,
        boolean invalidControlSum) {

    public PainFileSpec(String name, String fileName,
                        int numberOfPaymentInfoBlocks, int transactionsPerBlock) {
        this(name, fileName, numberOfPaymentInfoBlocks, transactionsPerBlock, false);
    }

    public long totalTransactions() {
        return (long) numberOfPaymentInfoBlocks * transactionsPerBlock;
    }
}
