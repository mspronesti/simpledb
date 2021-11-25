package simpledb;

import java.io.Serializable;
import java.util.*;
import java.util.stream.IntStream;

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

        /**
         * Two TDItem are considered equal if they have
         * the same fieldType, regardless of its name
         * @param o
         *          the Object to be compared for equality with TDItem
         * @return
         *          true if the object is equal to this TDItem
         */
        public boolean equals (Object o) {
            return o == null ?
                    fieldType == null :
                    o instanceof TDItem && ((TDItem) o).fieldType.equals(fieldType);

        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return TDItems.iterator();
    }

    private static final long serialVersionUID = 1L;
    private ArrayList<TDItem> TDItems;

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
        // some code goes here
        __tupleDesc(typeAr, fieldAr);
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
        // some code goes here
        __tupleDesc(typeAr, new String[typeAr.length]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return TDItems.size();
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
        // some code goes here
        if ( i < 0  || i >= TDItems.size())
            throw new NoSuchElementException();

        return TDItems.get(i).fieldName;
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
        // some code goes here
        if ( i < 0  || i >= TDItems.size())
            throw new NoSuchElementException();

        return TDItems.get(i).fieldType;
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
        // some code goes here
        return IntStream
                .range(0, TDItems.size())
                .filter(idx -> {
                    String itemName = TDItems.get(idx).fieldName;
                    // protecting from null string comparisons
                    // (NullPointerExceptions)
                    return itemName != null && itemName.equals(name);
                })
                .findFirst()
                .orElseThrow(NoSuchElementException::new);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        return TDItems
                .stream()
                .map(tdItem -> tdItem.fieldType.getLen())
                .reduce(0, Integer::sum);
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
        // some code goes here
        int dim = td1.numFields() + td2.numFields();
        // initializing the data structures
        Type[] typeAr = new Type[dim];
        String[] fieldAr = new String[dim];

        // adding fields coming from td1
        for(int i = 0; i < td1.numFields(); ++i){
            typeAr[i] = td1.TDItems.get(i).fieldType;
            fieldAr[i] = td1.TDItems.get(i).fieldName;
        }

        // adding fields coming from td2
        for(int i = 0; i < td2.numFields(); ++i){
            typeAr[i + td1.numFields()] = td2.TDItems.get(i).fieldType;
            fieldAr[i + td1.numFields()] = td2.TDItems.get(i).fieldName;
        }

        return new TupleDesc(typeAr,fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        return o == null ?
                // if o is null, return TDItems is null (bool)
                TDItems == null :
                // if not null, check if it's from class TupleDesc and if it is,
                // cast to it and actually compare the content
                o instanceof TupleDesc && __containSameTDItems(this, (TupleDesc)o);
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
        // some code goes here
        return TDItems
                .stream()
                .map(TDItem::toString)
                .reduce("", (s1, s2) -> s1 + s2 + ", ");
    }

    /**
     * Helper method to construct new TupleDesc object with type typeAr
     * of class Type
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    private void __tupleDesc(Type[] typeAr, String[] fieldAr) {
        TDItems = new ArrayList<>(typeAr.length);
        for(int i = 0; i < typeAr.length; ++i)
            TDItems.add(new TDItem(typeAr[i], fieldAr[i]));
    }

    /**
     * Helper method to check two TupleDesc actually share the same elements
     * according to the comparison criteria of the TDItem class,
     * regardless of the order
     * @param td1
     *          first TupleDesc
     * @param td2
     *          second TupleDesc
     * @return
     *          true if they share the same TDItems
     */
    private boolean __containSameTDItems(TupleDesc td1, TupleDesc td2){
        return td1.TDItems.containsAll(td2.TDItems) &&
                td2.TDItems.containsAll(td1.TDItems);
    }
}
