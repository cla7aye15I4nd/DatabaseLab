package simpledb;

import java.io.Serializable;
import java.util.*;
import java.lang.Iterable;
import java.lang.Math;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

        public boolean equals(Object o) {
            if (this == o) 
                return true;
            
            if (o instanceof TDItem) {
                TDItem rhs = (TDItem) o;
                
                if (!fieldType.equals(rhs.fieldType))
                    return false;
                                    
                if (fieldName != null)
                    return fieldName.equals(rhs.fieldName);

                return rhs.fieldName == null;
            } 
            
            return false;
        }
    }

    private TDItem[] TDItems;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        Iterator<TDItem> iterator = new Iterator<TDItem>() {
            private int i = 0;
 
            @Override
            public boolean hasNext() {
                return TDItems.length > i;
            }
 
            @Override
            public TDItem next() {
                return TDItems[i++];
            }
        };

        return iterator;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        int length = Math.min(typeAr.length, fieldAr.length);
        TDItems = new TDItem [length];

        for (int i = 0; i < length; i++) 
            TDItems[i] = new TDItem (typeAr[i], fieldAr[i]);
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        this(typeAr, new String [typeAr.length]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return TDItems.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < 0 || i >= numFields())
            throw new NoSuchElementException();

        return TDItems[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i < 0 || i >= numFields())
            throw new NoSuchElementException();

        return TDItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        int i = 0;
        for (TDItem tdItem : TDItems) {
            if (tdItem.fieldName != null && tdItem.fieldName.equals(name))
                return i;
            i++;
        }

        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        for (TDItem tdItem : TDItems)
            size += tdItem.fieldType.getLen();
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int size = td1.numFields() + td2.numFields();
        Type[] typeAr = new Type [size];
        String[] fieldAr = new String [size];

        size = td1.numFields();
        for (int i = 0; i < size; ++i) {
            typeAr[i] = td1.TDItems[i].fieldType;
            fieldAr[i] = td1.TDItems[i].fieldName; 
        }
        
        for (int i = 0; i < td2.numFields(); ++i) {
            typeAr[size + i] = td2.TDItems[i].fieldType;
            fieldAr[size + i] = td2.TDItems[i].fieldName; 
        }

        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (this == o) 
            return true;

        if (o instanceof TupleDesc) {
            TupleDesc rhs = (TupleDesc) o;
            if (rhs.numFields() != this.numFields()) {
                return false;
            }
            for (int i = 0; i < numFields(); i++) {
                if (!TDItems[i].equals(rhs.TDItems[i])) {
                    return false;
                }
            }
            return true;
        }
        
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuffer str = new StringBuffer();
        str.append("TupleDesc[");
        for (TDItem tdItem : TDItems) {
            str.append(tdItem.toString() + ",");
        }
        str.append("]");
        return str.toString();
    }
}
