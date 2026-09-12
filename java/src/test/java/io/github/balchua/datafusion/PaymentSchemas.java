package io.github.balchua.datafusion;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * Canonical payment schemas (spec §6/§7), modeled on pain.001/pain.002 semantics.
 * The Java schema is authoritative: Rust sees exactly this schema through the Arrow
 * C Data Interface — there is no second schema definition on the Rust side.
 *
 * <p>Column census (top-level / logical leaf fields):
 * <ul>
 *   <li>payment_message: 19 / 23</li>
 *   <li>remittances: 21 / 32</li>
 *   <li>transactions: 34 / ~73</li>
 * </ul>
 *
 * <p>Type philosophy: typed Arrow Structs for everything with a predictable shape
 * (parties, accounts, postal addresses, agents); JSON strings only for genuinely
 * ragged payloads (regulatory reporting, tax, invoices, audit events); Decimal128
 * for all monetary values and FX rates — never floating point.
 */
public final class PaymentSchemas {
    public static final String TABLE_MESSAGE = "payment_message";
    public static final String TABLE_REMITTANCES = "remittances";
    public static final String TABLE_TRANSACTIONS = "transactions";

    private PaymentSchemas() {}

    // ------------------------------------------------------------------
    // primitive helpers
    // ------------------------------------------------------------------

    public static Field utf8(String name, boolean nullable) {
        return new Field(name, nullable
                ? FieldType.nullable(new ArrowType.Utf8())
                : FieldType.notNullable(new ArrowType.Utf8()), null);
    }

    public static Field int32(String name, boolean nullable) {
        return new Field(name, nullable ? FieldType.nullable(new ArrowType.Int(32, true))
                : FieldType.notNullable(new ArrowType.Int(32, true)), null);
    }

    public static Field decimal(String name, int precision, int scale, boolean nullable) {
        return new Field(name, nullable
                ? FieldType.nullable(new ArrowType.Decimal(precision, scale, 128))
                : FieldType.notNullable(new ArrowType.Decimal(precision, scale, 128)), null);
    }

    public static Field timestampMilli(String name, boolean nullable) {
        return new Field(name, nullable
                ? FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null))
                : FieldType.notNullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)), null);
    }

    public static Field date32(String name, boolean nullable) {
        return new Field(name, nullable
                ? FieldType.nullable(new ArrowType.Date(DateUnit.DAY))
                : FieldType.notNullable(new ArrowType.Date(DateUnit.DAY)), null);
    }

    public static Field bool(String name, boolean nullable) {
        return new Field(name, nullable
                ? FieldType.nullable(new ArrowType.Bool())
                : FieldType.notNullable(new ArrowType.Bool()), null);
    }

    /** JSON payload stored as Utf8 (genuinely ragged data; no json_extract in v1). */
    public static Field json(String name) {
        return utf8(name, true);
    }

    private static Field struct(String name, boolean nullable, List<Field> children) {
        return new Field(name, nullable
                ? FieldType.nullable(new ArrowType.Struct())
                : FieldType.notNullable(new ArrowType.Struct()), children);
    }

    // ------------------------------------------------------------------
    // shared nested shapes
    // ------------------------------------------------------------------

    /** account: Struct{iban, number} */
    public static Field accountField() {
        return struct("account", true, List.of(
                utf8("iban", true),
                utf8("number", true)
        ));
    }

    /** postal_address: Struct{street, house_number, city, postal_code, country, address_line7} */
    public static Field postalAddress(String name, boolean nullable) {
        return struct(name, nullable, List.of(
                utf8("street", true),
                utf8("house_number", true),
                utf8("city", true),
                utf8("postal_code", true),
                utf8("country", true),
                utf8("address_line7", true)
        ));
    }

    /** agent: Struct{name, bic, clearing_member_id, lei} */
    public static Field agent(String name, boolean nullable) {
        return struct(name, nullable, List.of(
                utf8("name", true),
                utf8("bic", true),
                utf8("clearing_member_id", true),
                utf8("lei", true)
        ));
    }

    /**
     * ultimate party: Struct{name, identifier, account{iban, number},
     * postal_address{...}} — 9 leaves, 3 struct levels deep.
     */
    public static Field ultimateParty(String name, boolean nullable) {
        return struct(name, nullable, List.of(
                utf8("name", true),
                utf8("identifier", true),
                accountField(),
                postalAddress("postal_address", true)
        ));
    }

    /** party: Struct{name, account: Struct{iban, number}, agent_bic} */
    public static Field party(String name, boolean nullable) {
        return struct(name, nullable, List.of(
                utf8("name", true),
                accountField(),
                utf8("agent_bic", true)
        ));
    }

    /** structured_remittance: List&lt;Struct{code, reference, amount: Decimal128(18,4)}&gt; */
    public static Field structuredRemittance() {
        Field item = new Field("item", FieldType.notNullable(new ArrowType.Struct()), List.of(
                utf8("code", true),
                utf8("reference", true),
                decimal("amount", 18, 4, true)
        ));
        return new Field("structured_remittance", FieldType.notNullable(new ArrowType.List()), List.of(item));
    }

    // ------------------------------------------------------------------
    // table schemas
    // ------------------------------------------------------------------

    /** Message-level facts: 19 top-level columns. */
    public static Schema messageSchema() {
        return new Schema(List.of(
                // -- original 7 --
                new Field("message_id", FieldType.notNullable(new ArrowType.Utf8()), null),
                utf8("message_type", true),
                utf8("version", true),
                timestampMilli("creation_timestamp", true),
                utf8("initiating_party", true),
                int32("number_of_transactions", false),
                decimal("control_sum", 20, 4, true),
                // -- +12 new --
                bool("batch_booking", true),
                bool("test_indicator", true),
                utf8("settlement_method", true),
                utf8("clearing_system", true),
                agent("instructing_agent", true),
                agent("receiving_agent", true),
                json("original_message_reference"),
                json("regulatory_reporting"),
                json("message_tax"),
                json("supplemental_data"),
                json("file_metadata"),
                utf8("message_hash", true)
        ));
    }

    /** Payment-group (PmtInf) facts: 21 top-level columns. */
    public static Schema remittanceSchema() {
        return new Schema(List.of(
                // -- original 7 --
                new Field("remittance_id", FieldType.notNullable(new ArrowType.Utf8()), null),
                new Field("message_id", FieldType.notNullable(new ArrowType.Utf8()), null),
                utf8("payment_information_id", true),
                date32("requested_execution_date", true),
                party("debtor", true),
                int32("number_of_transactions", false),
                decimal("control_sum", 20, 4, true),
                // -- +14 new --
                utf8("payment_method", true),
                utf8("service_level", true),
                utf8("local_instrument", true),
                utf8("category_purpose", true),
                utf8("sequence_type", true),
                postalAddress("debtor_postal_address", true),
                ultimateParty("ultimate_debtor", true),
                agent("debtor_agent", true),
                utf8("charge_bearer", true),
                date32("pooling_adjustment_date", true),
                timestampMilli("settlement_date", true),
                json("remittance_regulatory_reporting"),
                json("remittance_tax"),
                json("payment_terms")
        ));
    }

    /** Transaction (CdtTrfTxInf) facts: 34 top-level columns. */
    public static Schema transactionSchema() {
        return new Schema(List.of(
                // -- original 12 --
                new Field("transaction_id", FieldType.notNullable(new ArrowType.Utf8()), null),
                new Field("message_id", FieldType.notNullable(new ArrowType.Utf8()), null),
                new Field("remittance_id", FieldType.notNullable(new ArrowType.Utf8()), null),
                utf8("end_to_end_id", true),
                decimal("amount", 18, 4, false),
                utf8("currency", false),
                party("debtor", true),
                party("creditor", true),
                structuredRemittance(),
                utf8("remittance_information", true),
                date32("requested_execution_date", true),
                utf8("transaction_status", true),
                // -- +22 new (screening_result/audit_trail dropped in favor of typed
                //    ultimate-party structs) --
                decimal("instructed_amount", 18, 4, true),
                decimal("exchange_rate", 18, 10, true),
                struct("fx_details", true, List.of(
                        utf8("quote_currency", true),
                        utf8("rate_type", true),
                        utf8("contract_id", true)
                )),
                decimal("equivalent_amount", 18, 4, true),
                decimal("charge_amount", 18, 4, true),
                json("charge_details"),
                postalAddress("debtor_postal_address", true),
                postalAddress("creditor_postal_address", true),
                ultimateParty("ultimate_debtor", true),
                ultimateParty("ultimate_creditor", true),
                agent("creditor_agent", true),
                utf8("purpose_code", true),
                json("regulatory_reporting"),
                json("tax_details"),
                json("taxonomies"),
                json("invoice_references"),
                utf8("mandate_reference", true),
                json("cheque_instructions"),
                json("card_instruction"),
                utf8("return_reason", true),
                timestampMilli("status_timestamp", true),
                json("risk_signals")
        ));
    }
}