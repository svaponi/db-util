package io.github.svaponi.dbutil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

/**
 * Incapsula il risultato della query.
 *
 * @author svaponi
 * @created Jul 21, 2016 13:10:30 PM
 */
public class DbResult {

    public List<List<Object>> set = new ArrayList<>();
    public List<String> columns = new Vector<>();
    public List<String> types = new Vector<>();
    public List<String> classes = new Vector<>();

    @Override
    public String toString() {
        final StringBuilder strb = new StringBuilder();
        strb.append("COLUMNS:");
        strb.append("\n" + Arrays.deepToString(this.columns.toArray()));
        strb.append("\nRECORDS:");
        for (final List<Object> row : this.set) {
            strb.append("\n" + Arrays.deepToString(row.toArray()));
        }
        return strb.toString();
    }

    public int size() {
        return this.set.size();
    }

    public Object get(final int rowId, final String colName) {
        if (null == colName || colName.trim().isEmpty()) {
            throw new IllegalArgumentException("Invalid column name");
        }
        if (!this.columns.contains(colName)) {
            throw new IllegalArgumentException("Invalid column name <" + colName + ">");
        }
        return this.get(rowId, this.columns.indexOf(colName));
    }

    public Object get(final int rowId, final int colId) {
        final List<Object> row;
        try {
            row = this.set.get(rowId);
        } catch (final Exception e) {
            throw new IllegalArgumentException("Invalid row number [" + rowId + "]");
        }
        try {
            return row.get(colId);
        } catch (final Exception e) {
            throw new IllegalArgumentException("Invalid column number [" + colId + "]");
        }
    }

}
