package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

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
    private int _oppai = NO_GROUPING, _oppai2;
    private Type _myTypeOppai = null;
    private Op _oppairator;
    private ArrayList<Field> no_grouping_values;
    private HashMap<Type, ArrayList<Field>> hash;
    
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	_oppai = gbfield;
    	_myTypeOppai = gbfieldtype;
    	_oppai2 = afield;
    	no_grouping_values = new ArrayList<Field>();
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
    	Field tup_group_by, tup_aggregate;
    	if(_oppai != NO_GROUPING) {
    		tup_group_by = tup.getField(_oppai);
    		tup_aggregate = tup.getField(_oppai2);
    		if(hash.containsKey(tup_group_by.getType())){
    			hash.get(tup_group_by.getType()).add(tup_aggregate);
    		}
    		else {
    			ArrayList<Field> values = new ArrayList<Field>();
    			values.add(tup_aggregate);
    			hash.put(tup_group_by.getType(), values);
    		}
    	}
    	else {
    		tup_aggregate = tup.getField(_oppai2);
    		no_grouping_values.add(tup_aggregate);
    	}
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
    	Aggregate blah;
    	if(_oppai != NO_GROUPING)
    		blah = new Aggregate(blah, _oppai, _oppai, _oppairator);
    	else
    		blah = new Aggregate(blah, _oppai, _oppai, _oppairator);
    	return null;
        // some code goes here
    }
}
