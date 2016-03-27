package tests;

import java.io.IOException;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.GlobalConst;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.Tuple;
import index.IndexException;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.Iterator;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.NestedLoopException;
import iterator.PredEvalException;
import iterator.SortException;
import iterator.TupleUtils;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;

public class IEJoin {

	 private AttrType      _in1[],  _in2[];
	  private   int        in1_len, in2_len;
	  private   Iterator  outer;
	  private   short t2_str_sizescopy[];
	  private   CondExpr OutputFilter[];
	  private   CondExpr RightFilter[];
	  private   int        n_buf_pgs;        // # of buffer pages available.
	  private   boolean        done,         // Is the join complete
	    get_from_outer;                 // if TRUE, a tuple is got from outer
	  private   Tuple     outer_tuple, inner_tuple;
	  private   Tuple     Jtuple;           // Joined tuple
	  private   FldSpec   perm_mat[];
	  private   int        nOutFlds;
	  private   Heapfile  hf;
	  private   Scan      inner;
	  
	  
	public IEJoin( AttrType    in1[],    
			   int     len_in1,           
			   short   t1_str_sizes[],
			   AttrType    in2[],         
			   int     len_in2,           
			   short   t2_str_sizes[],   
			   int     amt_of_mem,        
			   Iterator     am1,          
			   String relationName,      
			   CondExpr outFilter[],      
			   CondExpr rightFilter[],    
			   FldSpec   proj_list[],
			   int        n_out_flds
			   ) throws IOException,NestedLoopException {

		 _in1 = new AttrType[in1.length];
	      _in2 = new AttrType[in2.length];
	      System.arraycopy(in1,0,_in1,0,in1.length);
	      System.arraycopy(in2,0,_in2,0,in2.length);
	      in1_len = len_in1;
	      in2_len = len_in2;
	      
	      
	      outer = am1;
	      t2_str_sizescopy =  t2_str_sizes;
	      inner_tuple = new Tuple();
	      Jtuple = new Tuple();
	      OutputFilter = outFilter;
	      RightFilter  = rightFilter;
	      
	      n_buf_pgs    = amt_of_mem;
	      inner = null;
	      done  = false;
	      get_from_outer = true;
	      
	      AttrType[] Jtypes = new AttrType[n_out_flds];
	      short[]    t_size;
	      
	      perm_mat = proj_list;
	      nOutFlds = n_out_flds;
	      try {
		t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
						   in1, len_in1, in2, len_in2,
						   t1_str_sizes, t2_str_sizes,
						   proj_list, nOutFlds);
	      }catch (TupleUtilsException e){
		throw new NestedLoopException(e,"TupleUtilsException is caught by NestedLoopsJoins.java");
	      }
	      
	      
	      
	      try {
		  hf = new Heapfile(relationName);
		  
	      }
	      catch(Exception e) {
		throw new NestedLoopException(e, "Create new heapfile failed.");
	      }
	}
	  
	  


}
