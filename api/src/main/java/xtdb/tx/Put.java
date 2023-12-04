package xtdb.tx;

import clojure.lang.Keyword;

import java.time.Instant;
import java.util.Map;

public final class Put extends Ops implements Ops.HasValidTimeBounds<Put> {

    private final Keyword tableName;
    private final Map<Keyword, Object> doc;
    private final Instant validFrom;
    private final Instant validTo;

    Put(Keyword tableName, Map<Keyword, Object> doc) {
        this(tableName, doc, null, null);
    }

    private Put(Keyword tableName, Map<Keyword, Object> doc, Instant validFrom, Instant validTo) {
        this.tableName = tableName;
        this.doc = doc;
        this.validFrom = validFrom;
        this.validTo = validTo;
    }

    public Keyword tableName() {
        return tableName;
    }

    public Map<Keyword, Object> doc() {
        return doc;
    }

    public Instant validFrom() {
        return validFrom;
    }

    @Override
    public xtdb.tx.Put startingFrom(Instant validFrom) {
        return new xtdb.tx.Put(tableName, doc, validFrom, validTo);
    }

    public Instant validTo() {
        return validTo;
    }

    @Override
    public xtdb.tx.Put until(Instant validTo) {
        return new xtdb.tx.Put(tableName, doc, validFrom, validTo);
    }

    @Override
    public xtdb.tx.Put during(Instant validFrom, Instant validTo) {
        return new xtdb.tx.Put(tableName, doc, validFrom, validTo);
    }

    @Override
    public String toString() {
        return String.format("[:put {tableName=%s, doc=%s, validFrom=%s, validTo=%s}]", tableName, doc, validFrom, validTo);
    }
}