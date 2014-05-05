package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import simpledb.Aggregator.Op;

//NOT TESTED FOR NO GROUPING!!!
/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    private int _gbfield, _afield, _count;
    private Type _gbfieldtype;
    private Op _what;
    private ArrayList<Field> no_grouping_values;
	private ArrayList<Tuple> tuplist;
    private HashMap<String, ArrayList<Field>> hash;
    private TupleDesc desc;
    ///////////////For Grouping///////////////////////
    private int[] counts;
    
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	_gbfield = gbfield;
    	_gbfieldtype = gbfieldtype;
    	_count = 0;
    	_afield = afield;
    	_what = what;
    	no_grouping_values = new ArrayList<Field>();
    	tuplist = new ArrayList<Tuple>();
    	hash = new HashMap<String,ArrayList<Field>>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	String key;
    	Field value = tup.getField(_afield);
    	/////////////////////IF THERE IS GROUPING////////////////////////
    	if(_gbfield != NO_GROUPING) {
    		key = tup.getField(_gbfield).toString();
    		if(hash.containsKey(key)){
    			hash.get(key).add(value);
    			//counts.put(key, counts.get(key)+1);
    		}
    		else {
    			ArrayList<Field> values = new ArrayList<Field>();
    			values.add(value);
    			hash.put(key, values);
    			//counts.put(key, 1);
    		}
    		int index = 0;
    		switch (_what){
    		case COUNT:
    			//index = 0;
    			counts = new int[hash.size()];
    			for(ArrayList<Field> f : hash.values()){
    				counts[index] = f.size();
    				index++;
    				/*for(Field values : f){
    					counts[index] += 1;//((IntField)values).getValue();
    				}*/
    			}
    			break;
    		default:
    			System.out.println("Invalid Operation!");
    		}
    	}
		/////////////////////IF THERE IS NO GROUPING//////////////////////
		else {
			no_grouping_values.add(value);
			switch (_what){
			case COUNT:
				_count += 1;
				break;
			default:
				System.out.println("Invalid Operation!");
			}
		}
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	DbIterator dbI;
		if(_gbfield == NO_GROUPING) {
			
	    	String[] s = {"aggregateValue"};
			Type[] t = {Type.INT_TYPE};
			desc = new TupleDesc(t, s);
			Tuple _tup = new Tuple(desc);
			IntField f;
			
			switch (_what){
			case COUNT:
				f = new IntField(_count);
				_tup.setField(0, f);
				tuplist.add(_tup);
				break;
			default:
				System.out.println("Invalid Operation!");
			}
			dbI = new TupleIterator(desc, tuplist);
		}
		else {
			//Create a new TupleDesc for our resulting tuples
	    	String[] s = {"groupValue","aggregateValue"};
			Type[] t = {_gbfieldtype,Type.INT_TYPE};
			desc = new TupleDesc(t, s);
			
			IntField f;
			Field groupbyField = null;
			Iterator<String> set = hash.keySet().iterator();
			
			if(set.hasNext()){
			switch(_what){
			case COUNT:
				for(int ea_c : counts){
					String tmp = set.next();
					if(_gbfieldtype == Type.STRING_TYPE)
						groupbyField = new StringField(tmp,tmp.length());
					if(_gbfieldtype == Type.INT_TYPE)
						groupbyField = new IntField(Integer.parseInt(tmp));
					Tuple _tup = new Tuple(desc);
					f = new IntField(ea_c);
					_tup.setField(0, groupbyField);
					_tup.setField(1, f);
					tuplist.add(_tup);
				}
				break;
			default:
				System.out.println("Invalid Operation!");
			}
			}
			dbI = new TupleIterator(desc, tuplist);
		}
		return dbI;
    }
}
