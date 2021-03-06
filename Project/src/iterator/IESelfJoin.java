package iterator;

import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;

import java.lang.*;
import java.util.ArrayList;
import java.io.*;

public class IESelfJoin extends Iterator implements GlobalConst {

	private AttrType _in1[], _in2[];
	private int in1_len, in2_len;
	private Iterator L11, L12, L21, L22, L31, L41; // Pointers to iterators
													// after sorting
	private int rightcount = 0;
	private int leftcount = 0;
	private Iterator left;
	private Iterator right;
	private short t1_str_sizescopy[];
	private ArrayList<Integer> arr = new ArrayList<Integer>();
	private short t2_str_sizescopy[];
	private CondExpr LeftFilter[];
	private CondExpr OutputFilter[];
	private CondExpr RightFilter[];
	private int n_buf_pgs; // # of buffer pages available.
	private boolean done, // Is the join complete
			get_from_outer; // if TRUE, a tuple is got from outer
	private Tuple right_tuple, left_tuple;
	private Tuple Jtuple; // Joined tuple
	private FldSpec perm_mat[];
	private int nOutFlds;
	private ArrayList<Integer> p; // PERMUTATION ARRAY
	private int[] bit; // BIT ARRAY
	private AttrType LeftSortFldType;
	private AttrType RightSortFldType;
	private int bTraverse = 0; //
	private int pIndex = 0;
	private int join_column1;
	private int join_column2;
	private TupleOrder LOrder, ROrder;
	private ArrayList<Tuple> rightarray = new ArrayList<Tuple>();
	private ArrayList<Tuple> leftarray = new ArrayList<Tuple>();
	private int sortF1, sortF2;
	private int amt_memory;
	private int tempIndex = 0;


	/**
	 * constructor Initialize the two relations which are joined, including
	 * relation type,
	 * 
	 * @param in1
	 *            Array containing field types of R.
	 * @param len_in1
	 *            # of columns in R.
	 * @param t1_str_sizes
	 *            shows the length of the string fields.
	 * @param in2
	 *            Array containing field types of S
	 * @param len_in2
	 *            # of columns in S
	 * @param t2_str_sizes
	 *            shows the length of the string fields.
	 * @param amt_of_mem
	 *            IN PAGES
	 * @param am1
	 *            access method for left i/p to join
	 * @param relationName
	 *            access hfapfile for right i/p to join
	 * @param outFilter
	 *            select expressions
	 * @param rightFilter
	 *            reference to filter applied on right i/p
	 * @param proj_list
	 *            shows what input fields go where in the output tuple
	 * @param n_out_flds
	 *            number of outer relation fileds
	 * @return
	 * @throws Exception
	 * @throws UnknownKeyTypeException
	 * @throws UnknowAttrType
	 * @throws LowMemException
	 * @throws PredEvalException
	 * @throws PageNotReadException
	 * @throws InvalidTypeException
	 * @throws InvalidTupleSizeException
	 * @throws IndexException
	 * @throws JoinsException
	 * @exception NestedLoopException
	 *                exception from this class
	 */

	public IESelfJoin(

			AttrType in1[], int len_in1, short s1_sizes[], AttrType in2[], int len_in2, short s2_sizes[],
			int amt_of_mem,

			int join_col_in1, int sortFld1Len, int join_col_in2, int sortFld2Len,

			TupleOrder LeftOrder, TupleOrder RightOrder,

			Iterator am1, Iterator am2, CondExpr outFilter[], FldSpec proj_list[], int n_out_flds)
					throws JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException,
					PageNotReadException, PredEvalException, LowMemException, UnknowAttrType, UnknownKeyTypeException,
					Exception {

		_in1 = new AttrType[in1.length];
		_in2 = new AttrType[in2.length];
		System.arraycopy(in1, 0, _in1, 0, in1.length);
		System.arraycopy(in2, 0, _in2, 0, in2.length);
		in1_len = len_in1;
		in2_len = len_in2;

		left = am1;
		right = am2;
		t1_str_sizescopy = s1_sizes;
		t2_str_sizescopy = s2_sizes;
		left_tuple = new Tuple();
		right_tuple = new Tuple();
		Jtuple = new Tuple();
		// LeftFilter = leftFilter;
		// RightFilter = rightFilter;

		n_buf_pgs = amt_of_mem;
		done = false;
		get_from_outer = true;

		AttrType[] Jtypes = new AttrType[n_out_flds];
		short[] t_size;

		perm_mat = proj_list;
		nOutFlds = n_out_flds;
		OutputFilter = outFilter;
		join_column1 = join_col_in1;
		join_column2 = join_col_in2;
		sortF1 = sortFld1Len;
		sortF2 = sortFld2Len;
		amt_memory = amt_of_mem;

		// Sort L1
		L11 = am1;
		L21 = am2;

		// Sort L1 in Given order
		try {
			L11 = new Sort(in1, (short) len_in1, s1_sizes, am2, join_col_in1, LeftOrder, sortFld1Len, amt_of_mem);
			L21 = new Sort(in2, (short) len_in2, s2_sizes, am1, join_col_in2, LeftOrder, sortFld2Len, amt_of_mem);
			

		} catch (Exception e) {
			throw new SortException(e, "Left Sort failed");
		}

	}

	public Tuple get_next() throws JoinsException, IndexException, PageNotReadException, TupleUtilsException,
			SortException, LowMemException, UnknownKeyTypeException, Exception {
		Iterator leftCol = L11;
		Iterator rightCol = L21;

		if ((right_tuple = rightCol.get_next()) != null) {
			System.out.println("In right");
			Tuple newtuple = new Tuple(right_tuple);

			rightarray.add(newtuple);
			arr.add(right_tuple.getIntFld(2));
			rightcount++;
		}
		if ((left_tuple = leftCol.get_next()) != null) {
			System.out.println("In left");
			Tuple newtuple = new Tuple(left_tuple);

			leftarray.add(newtuple);
			// leftarray.add(left_tuple);
			leftcount++;
		}
		for (int i = 0; i < rightarray.size(); i++)
			// System.out.println(rightarray.get(i).getIntFld(2));
			System.out.println(rightarray.get(i).getIntFld(join_column1) + " ," + leftarray.get(i).getIntFld(join_column2));

		if (left_tuple != null)
			return Jtuple;
		else
			return null;

	}

	public void joinTuples(int joincol1, int joincol2, int projcol1, int projcol2, int op)
			throws UnknowAttrType, TupleUtilsException, FieldNumberOutOfBoundException, IOException {
		AttrType a = new AttrType(AttrType.attrInteger);
		for (int i = 0; i < rightcount; i++) {
			for (int j = 0; j < leftcount; j++) {
				if (TupleUtils.CompareTupleWithTuple(a, rightarray.get(i), joincol1, leftarray.get(j), joincol2) < 0) {
					System.out.println("[" + rightarray.get(i).getIntFld(projcol1) + ","
							+ leftarray.get(j).getIntFld(projcol2) + "]");
				}

			}
		}
	}

	@Override
	public void close() throws IOException, JoinsException, SortException, IndexException {
		// TODO Auto-generated method stub

	}

}
