package simpledb;

import java.io.Serializable;
import java.lang.annotation.Target;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private TDItem[] tdItems;
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
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return (Iterator<TDItem>) Arrays.asList(tdItems).iterator();
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
        // some code goes here
        int l = typeAr.length;
        tdItems = new TDItem[l];
        for(int i = 0; i < l; i++){
            tdItems[i] = new TDItem(typeAr[i], fieldAr[i]);
        }

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
        int l = typeAr.length;
        tdItems = new TDItem[l];
        for(int i = 0; i < l; i++){
            tdItems[i] = new TDItem(typeAr[i], "");
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tdItems.length;
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
        if (i >= 0 && i < tdItems.length){
            return tdItems[i].fieldName;
        }
        else{
            throw new NoSuchElementException( i + " is not a valid field reference.");
        }
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
        if (i >= 0 && i < tdItems.length){
            return tdItems[i].fieldType;
        }
        else{
            throw new NoSuchElementException( i + " is not a valid field reference.");
        }
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
        int l = tdItems.length;
        for (int i = 0 ; i < l; i++){
            String fname = tdItems[i].fieldName;
            if (fname.equals(name)){
                return i;
            }
        }
       throw new NoSuchElementException("No field with a matching name is found.");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        int l = tdItems.length;
        for(int i = 0; i < l; i++){
            size += tdItems[i].fieldType.getLen();
        }
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
        // some code goes here
        int num1 = td1.numFields();
        int num2 = td2.numFields();
        Type[] typeMerge = new Type[num1 + num2];
        String[] fieldMerge = new String[num1 + num2];
        for (int i = 0; i < num1 + num2; i++){
            if (i < num1){
                typeMerge[i] = td1.tdItems[i].fieldType;
                fieldMerge[i] = td1.tdItems[i].fieldName;
            }
            else{
                int j = i - num1;
                typeMerge[i] = td2.tdItems[j].fieldType;
                fieldMerge[i] = td2.tdItems[j].fieldName;
            }
        }
        return new TupleDesc(typeMerge, fieldMerge);
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
        // some code goes here
        if(this.getClass().isInstance(o)){
            TupleDesc target = (TupleDesc) o;
            int l1 = numFields();
            int l2 = target.numFields();
            if (l1 == l2){
                for (int i = 0; i < l1; i++){
                    if (!tdItems[i].fieldType.equals(target.tdItems[i].fieldType)){
                        return false;
                    }
                }
                return true;
            }
            else{
                return false;
            }
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
        // some code goes here
        StringBuilder stringBuilder = new StringBuilder();
        int l = tdItems.length;

        for (int i = 0; i < l; i++){
            if (i != l - 1){
                stringBuilder.append(tdItems[i].fieldType + "(" + tdItems[i].fieldName + "),");
            }
            else if (i == l -1){
                stringBuilder.append(tdItems[i].fieldType + "(" + tdItems[i].fieldName + ")");
            }
        }
        return stringBuilder.toString();
    }
}
