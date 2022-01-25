package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbField;
    private final Type gbFieldType;
    private final int aField;
    private final Op operator;
    private final Map<Field, Integer> minmax, counts, sums, averages;
    private String gFieldName, aFieldName;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbField=gbfield;
        this.gbFieldType=gbfieldtype;
        this.aField=afield;
        this.operator=what;
        minmax = new HashMap<>(); //if operator= min contains the current min value; if operator = max contains the current max value
        counts = new HashMap<>(); // contains the current count of tuples
        sums = new HashMap<>(); // contains the current sum of aggregate fields
        averages = new HashMap<>(); // contains the current average of aggregate fields
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupField;
        Field aggField;
        if(gbField != Aggregator.NO_GROUPING) {
            groupField = tup.getField(gbField);
            gFieldName = tup.getTupleDesc().getFieldName(gbField);
        }
        else
            groupField = new IntField(Aggregator.NO_GROUPING);
        aggField = tup.getField(aField);
        aFieldName = tup.getTupleDesc().getFieldName(aField);
        if(operator == Op.COUNT || operator == Op.AVG) {
            if (!counts.containsKey(groupField))
                counts.put(groupField, 1);
            else
                counts.put(groupField, counts.get(groupField) + 1);
        }
        if(operator == Op.AVG || operator == Op.SUM) {
            if (!sums.containsKey(groupField))
                sums.put(groupField, aggField.hashCode());
            else
                sums.put(groupField, sums.get(groupField) + aggField.hashCode());
        }
        if(operator == Op.AVG)
            averages.put(groupField, sums.get(groupField) / counts.get(groupField));


        switch (operator){
            case COUNT:
            case SUM:
            case AVG: {
                break;
            }
            case MAX:{
                if(!minmax.containsKey(groupField))
                    minmax.put(groupField, aggField.hashCode());
                else
                    minmax.put(groupField,
                            Math.max(aggField.hashCode(), minmax.get(groupField)));
                break;
            }
            case MIN:{
                if(!minmax.containsKey(groupField))
                    minmax.put(groupField, aggField.hashCode());
                else
                    minmax.put(groupField,
                            Math.min(aggField.hashCode(), minmax.get(groupField)));
                break;
            }
            default:{
                throw new UnsupportedOperationException("Operation not supported");
            }

        }


    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> results;
        TupleDesc desc = getTupleDesc();
        switch (operator){
            case COUNT:{
                results = getTuples(counts, desc);
                break;
            }
            case SUM:{
                results = getTuples(sums, desc);
                break;
            }
            case AVG:{
                results = getTuples(averages, desc);
                break;
            }
            default:{
                results = getTuples(minmax, desc);
                break;
            }
        }
        return new TupleIterator(desc, results);
    }

    /**
     * Create a TupleDesc according to the type and field name
     * of the new tuple to merge on the aggregate.
     * if there is no grouping it is composed of one Field of type (Type.INT_TYPE)
     * and Name of aggrFieldName.
     * if there is grouping it is composed of two Field of type (groupType, Type.INT_TYPE)
     * and Name (groupFieldName, aggrFieldName).
     * @return the new TupleDesc
     */
    private TupleDesc getTupleDesc(){
        Type[] typeAr;
        String[] fieldAr;
        if(gbField == Aggregator.NO_GROUPING){
            typeAr = new Type[1];
            fieldAr = new String[1];
            typeAr[0] = Type.INT_TYPE;
            fieldAr[0] = aFieldName;
        }
        else{
            typeAr = new Type[2];
            fieldAr = new String[2];
            typeAr[0] = gbFieldType;
            fieldAr[0] = gFieldName;
            typeAr[1] = Type.INT_TYPE;
            fieldAr[1] = aFieldName;
        }
        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Helper method of iterator() to get a List of the tuples aggregated
     * @param values contains the map to transform in tuples
     * @param desc contains the new TupleDesc
     * @return the new List of tuples
     */
    private List<Tuple> getTuples(Map<Field, Integer> values, TupleDesc desc) {
        List<Tuple> tuples = new ArrayList<>();
        for(Map.Entry<Field, Integer> entry: values.entrySet()){
            Tuple tuple = new Tuple(desc);
            if(gbField == Aggregator.NO_GROUPING){
                tuple.setField(0, new IntField(entry.getValue()));
            }
            else {
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(entry.getValue()));
            }
            tuples.add(tuple);
        }
        return tuples;

    }

}
