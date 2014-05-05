package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

//NOT TESTED FOR NO GROUPING!!!
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
    private int _gbfield, _afield, _count, _sum, _avg, _min, _max;
    private Type _gbfieldtype;
    private Op _what;
    private ArrayList<Field> no_grouping_values;
	private ArrayList<Tuple> tuplist;
    private HashMap<String, ArrayList<Field>> hash;
    private TupleDesc desc;
    ///////////////For Grouping///////////////////////
    private int[] counts, sums, avg, min, max;
    
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	_gbfield = gbfield;
    	_gbfieldtype = gbfieldtype;
    	_count = 0;
    	_sum = 0;
    	_avg = 0;
    	_min = Integer.MAX_VALUE;
    	_max = Integer.MIN_VALUE;
    	_afield = afield;
    	_what = what;
    	no_grouping_values = new ArrayList<Field>();
    	tuplist = new ArrayList<Tuple>();
    	hash = new HashMap<String,ArrayList<Field>>();
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
    			//((IntField)(hash.values().iterator().next().get(0))).getValue();
    			break;
    		case SUM:
    			//index = 0;
    			sums = new int[hash.size()];
    			for(ArrayList<Field> f : hash.values()){
    				for(Field values : f){
    					sums[index] += ((IntField)values).getValue();
    				}
    				index++;
    			}
    			break;
    		case AVG: //The algorithm used here can be improved, so it runs faster.
    			//index = 0;
    			avg = new int[hash.size()];
    			int tmp = 0;
    			for(ArrayList<Field> f : hash.values()){
    				tmp = 0;
    				for(Field values : f){
    					tmp += ((IntField)values).getValue();
    				}
    				avg[index] = tmp/f.size();
    				index++;
    			}
    			break;
    		case MIN:
    			//index = 0;
    			min = new int[hash.size()];
    			for(int i = 0; i < min.length; i++)
    				min[i] = Integer.MAX_VALUE;
    			for(ArrayList<Field> f : hash.values()){
    				for(Field values : f){
    					if(((IntField)values).getValue() < min[index])
    						min[index] = ((IntField)values).getValue();
    				}
    				index++;
    			}
    			break;
    		case MAX:
    			//index = 0;
    			max = new int[hash.size()];
    			for(int i = 0; i < max.length; i++)
    				max[i] = Integer.MIN_VALUE;
    			for(ArrayList<Field> f : hash.values()){
    				for(Field values : f){
    					if(((IntField)values).getValue() > max[index])
    						max[index] = ((IntField)values).getValue();
    				}
    				index++;
    			}
    			break;
    		default:
    			System.out.println("Invalid Operation!");
    			break;
    		}
    	}
    	/////////////////////IF THERE IS NO GROUPING//////////////////////
    	else {
    		no_grouping_values.add(value);
    		int insertionValue = ((IntField)value).getValue();
    		switch (_what){
    		case COUNT:
    			_count += 1;
    			break;
    		case SUM:
    			_sum += insertionValue;
    			break;
    		case AVG:
    			int sum = 0;
    			for(int i = 0; i < no_grouping_values.size(); i++)
    				sum += ((IntField)no_grouping_values.get(i)).getValue();
    			_avg = sum/no_grouping_values.size();//((_avg*(no_grouping_values.size()-1)) + insertionValue)/no_grouping_values.size();
    			break;
    		case MIN:
    			if(insertionValue < _min)
    				_min = insertionValue;
    			break;
    		case MAX:
    			if(insertionValue > _max)
    				_max = insertionValue;
    			break;
    		default:
    			System.out.println("Invalid Operation!");
    			break;
    		}
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
			case SUM:
				f = new IntField(_sum);
				_tup.setField(0, f);
				tuplist.add(_tup);
				break;
			case AVG:
				f = new IntField(_avg);
				_tup.setField(0, f);
				tuplist.add(_tup);
				break;
			case MIN:
				f = new IntField(_min);
				_tup.setField(0, f);
				tuplist.add(_tup);
				break;
			case MAX:
				f = new IntField(_max);
				_tup.setField(0, f);
				tuplist.add(_tup);
				break;
			default:
				System.out.println("Invalid Operation!");
				break;
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
			case SUM:
				for(int ea_s : sums){
					String tmp = set.next();
					if(_gbfieldtype == Type.STRING_TYPE)
						groupbyField = new StringField(tmp,tmp.length());
					if(_gbfieldtype == Type.INT_TYPE)
						groupbyField = new IntField(Integer.parseInt(tmp));
					Tuple _tup = new Tuple(desc);
					f = new IntField(ea_s);
					_tup.setField(0, groupbyField);
					_tup.setField(1, f);
					tuplist.add(_tup);
				}
				break;
			case AVG:
				for(int ea_avg : avg){
					String tmp = set.next();
					if(_gbfieldtype == Type.STRING_TYPE)
						groupbyField = new StringField(tmp,tmp.length());
					if(_gbfieldtype == Type.INT_TYPE)
						groupbyField = new IntField(Integer.parseInt(tmp));
					Tuple _tup = new Tuple(desc);
					f = new IntField(ea_avg);
					_tup.setField(0, groupbyField);
					_tup.setField(1, f);
					tuplist.add(_tup);
				}
				break;
			case MIN:
				for(int ea_min : min){
					//if(set.hasNext()){
					String tmp = set.next();
					if(_gbfieldtype == Type.STRING_TYPE)
						groupbyField = new StringField(tmp,tmp.length());
					if(_gbfieldtype == Type.INT_TYPE)
						groupbyField = new IntField(Integer.parseInt(tmp));
					Tuple _tup = new Tuple(desc);
					f = new IntField(ea_min);
					_tup.setField(0, groupbyField);
					_tup.setField(1, f);
					tuplist.add(_tup);
				}
				break;
			case MAX:
				for(int ea_max : max){
					String tmp = set.next();
					if(_gbfieldtype == Type.STRING_TYPE)
						groupbyField = new StringField(tmp,tmp.length());
					if(_gbfieldtype == Type.INT_TYPE)
						groupbyField = new IntField(Integer.parseInt(tmp));
					Tuple _tup = new Tuple(desc);
					f = new IntField(ea_max);
					_tup.setField(0, groupbyField);
					_tup.setField(1, f);
					tuplist.add(_tup);
				}
				break;
			default:
				System.out.println("Invalid Operation!");
				break;
			}
			}
			dbI = new TupleIterator(desc, tuplist);
		}
    	return dbI;
    }
}
