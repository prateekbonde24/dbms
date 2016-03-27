package tests;
//originally from : joins.C

import iterator.*;

import heap.*;
import global.*;
import index.*;
import java.io.*;
import java.util.*;
import java.lang.*;
import diskmgr.*;
import bufmgr.*;
import btree.*;
import catalog.*;

/**
   Here is the implementation for the tests. There are N tests performed.
   We start off by showing that each operator works on its own.
   Then more complicated trees are constructed.
   As a nice feature, we allow the user to specify a selection condition.
   We also allow the user to hardwire trees together.
*/

//Define the Sailor schema
class Schema {
          public int s1;
          public int s2;
          public int s3;
          public int s4;

          public Schema (int _s1, int _s2, int _s3, int _s4) {
            s1  = _s1;
            s2  = _s2;
            s3 = _s3;
            s4 = _s4;
          }

}


class JoinsDriver implements GlobalConst {

  private boolean OK = true;
  private boolean FAIL = false;
  /*private Vector<S> arrayS;
  private Vector arrayQ;
  private Vector arrayR;*/
  private ArrayList<ArrayList<Schema>> fileArrays = new ArrayList<ArrayList<Schema>>();
  private int projcol1;
  private int projcol2;
  private int joincol1;
  private int joincol2;
  private int joincol22;
  private int joincol21;
  private int condition_code2;
  private int condition_code;
  private String condition;
  private String table1;
  private String table2;
  private int relationFlag = 0;
  private int relationFlag1 = 0;
  /** Constructor
   */
  public JoinsDriver() {

  }

  public boolean runTests() throws JoinsException, IndexException, PageNotReadException, TupleUtilsException, SortException, LowMemException, UnknownKeyTypeException, Exception {

    Disclaimer();
    /*Query1();

    Query2();
    Query3();


    Query4();
    Query5();
    Query6();*/
    //Query1();
    /*Query1a();
    fileArrays.clear();
    Query1b();*/
    Query2a();


    System.out.print ("Finished joins testing"+"\n");


    return true;
  }

  public void openFiles(ArrayList<String> tablenames)
  {
          //fileArrays = null;
          for(int tableindex=0;tableindex<tablenames.size();tableindex++)
          {
                  String filename = new String("/home/pratik/workspace3/Project/src/"+tablenames.get(tableindex)+".txt");
                  String line;
                  ArrayList<Schema> tempArray = new ArrayList<Schema>();
                  try
                        {
                                BufferedReader br = new BufferedReader(new FileReader(filename));
                                line = br.readLine();
                                String iptypeq[] = line.split(",");
                                int nq=iptypeq.length;
                                int i;
                                int temp[]=new int[nq];
                                while((line = br.readLine())!=null)
                                {
                                String arrq[]=line.split(",");

                                for(i=0;i<nq;i++)
                                {
                                        temp[i]=Integer.parseInt(arrq[i]);
                                }

                                tempArray.add(new Schema(temp[0], temp[1], temp[2], temp[3]));
                                System.out.println(temp[0]+" "+ temp[1]+" "+ temp[2]+" "+ temp[3]);
                                }
                                br.close();
                                fileArrays.add(tempArray);
                                System.out.println("________________________");
                                tempArray = null;

                        }
                  catch(Exception e)
                        {
                                e.printStackTrace();
                        }


          }

          boolean status = OK;

          ArrayList<Integer> sizes = new ArrayList<Integer>();
          ArrayList<Integer> attrs = new ArrayList<Integer>();
          for(int number = 0;number<fileArrays.size();number++)
          {
                  sizes.add(fileArrays.get(number).size());
                  attrs.add(4);
          }


            String dbpath = "/tmp/"+System.getProperty("user.name")+".minibase.jointestdb";
            String logpath = "/tmp/"+System.getProperty("user.name")+".joinlog";

            String remove_cmd = "/bin/rm -rf ";
            String remove_logcmd = remove_cmd + logpath;
            String remove_dbcmd = remove_cmd + dbpath;
            String remove_joincmd = remove_cmd + dbpath;

            try {
              Runtime.getRuntime().exec(remove_logcmd);
              Runtime.getRuntime().exec(remove_dbcmd);
              Runtime.getRuntime().exec(remove_joincmd);
            }
            catch (IOException e) {
              System.err.println (""+e);
            }

            SystemDefs sysdef = new SystemDefs( dbpath, 1000, NUMBUF, "Clock" );

            for(int number = 0; number<fileArrays.size();number++)
            {

                    AttrType [] types = new AttrType[4];
                    types[0] = new AttrType (AttrType.attrInteger);
                    types[1] = new AttrType (AttrType.attrInteger);
                    types[2] = new AttrType (AttrType.attrInteger);
                    types[3] = new AttrType (AttrType.attrInteger);

                    //SOS
                    short [] Ssizes = new short [1];
                    Ssizes[0] = 30; //first elt. is 30

                    Tuple t = new Tuple();
                    try {
                      t.setHdr((short) 4,types, Ssizes);
                    }
                    catch (Exception e) {
                      System.err.println("*** error in Tuple.setHdr() ***");
                      status = FAIL;
                      e.printStackTrace();
                    }

                    int size = t.size();

                    // inserting the tuple into file "sailors"
                    RID             rid;
                    Heapfile        f = null;
                    String filename = new String(tablenames.get(number)+".in");
                    //System.out.println("THISSS: "+filename);
                    try {
                      f = new Heapfile(filename);
                    }
                    catch (Exception e) {
                      System.err.println("*** error in Heapfile constructor ***");
                      status = FAIL;
                      e.printStackTrace();
                    }

                    t = new Tuple(size);
                    try {
                      t.setHdr((short) 4, types, Ssizes);
                    }
                    catch (Exception e) {
                      System.err.println("*** error in Tuple.setHdr() ***");
                      status = FAIL;
                      e.printStackTrace();
                    }

                    for (int i=0; i<sizes.get(number); i++) {
                      try {
                          //CHANGES HERE
                        t.setIntFld(1, ((Schema)fileArrays.get(number).get(i)).s1);
                        t.setIntFld(2, ((Schema)fileArrays.get(number).get(i)).s2);
                        t.setIntFld(3, ((Schema)fileArrays.get(number).get(i)).s3);
                        t.setIntFld(4, ((Schema)fileArrays.get(number).get(i)).s4);
                      }
                      catch (Exception e) {
                        System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
                        status = FAIL;
                        e.printStackTrace();
                      }

                      try {
                        rid = f.insertRecord(t.returnTupleByteArray());

                      }
                      catch (Exception e) {
                        System.err.println("*** error in Heapfile.insertRecord() ***");
                        status = FAIL;
                        e.printStackTrace();
                      }
                    }
                    if (status != OK) {
                      //bail out
                      System.err.println ("*** Error creating relation for sailors");
                      Runtime.getRuntime().exit(1);
                    }
            }
            //fileArrays = null;
  }

  private void query1b_CondExpr(CondExpr[] expr) {

    expr[0].next  = null;
    if(condition_code == 1)
        expr[0].op    = new AttrOperator(AttrOperator.aopLT);
    else if(condition_code == 2)
        expr[0].op    = new AttrOperator(AttrOperator.aopLE);
    else if(condition_code == 3)
        expr[0].op    = new AttrOperator(AttrOperator.aopGE);
    else if(condition_code == 4)
        expr[0].op    = new AttrOperator(AttrOperator.aopGT);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),joincol1);

    if (relationFlag == 1)
    {
            expr[0].type2 = new AttrType(AttrType.attrSymbol);
            relationFlag = 0;
            expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.outer),joincol2);
    }
    else
    {

        expr[0].type2 = new AttrType(AttrType.attrInteger);
        expr[0].operand2.integer = joincol2;
    }

    if(condition_code2 == 1)
        expr[1].op    = new AttrOperator(AttrOperator.aopLT);
    else if(condition_code2 == 2)
        expr[1].op    = new AttrOperator(AttrOperator.aopLE);
    else if(condition_code2 == 3)
        expr[1].op    = new AttrOperator(AttrOperator.aopGE);
    else if(condition_code2 == 4)
        expr[1].op    = new AttrOperator(AttrOperator.aopGT);
    expr[1].next  = null;
    expr[1].type1 = new AttrType(AttrType.attrSymbol);

    expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),joincol21);
    if (relationFlag1 == 1)
    {
            expr[1].type2 = new AttrType(AttrType.attrSymbol);
            relationFlag1 = 0;
            expr[1].operand2.symbol = new FldSpec (new RelSpec(RelSpec.outer),joincol22);
    }
    else
    {

        expr[1].type2 = new AttrType(AttrType.attrInteger);
        expr[1].operand2.integer = joincol22;
    }

    expr[2] = null;
  }

  private void query2a_CondExpr(CondExpr[] expr) {

            expr[0].next  = null;
            if(condition_code == 1)
                expr[0].op    = new AttrOperator(AttrOperator.aopLT);
            else if(condition_code == 2)
                expr[0].op    = new AttrOperator(AttrOperator.aopLE);
            else if(condition_code == 3)
                expr[0].op    = new AttrOperator(AttrOperator.aopGE);
            else if(condition_code == 4)
                expr[0].op    = new AttrOperator(AttrOperator.aopGT);
            expr[0].type1 = new AttrType(AttrType.attrSymbol);
            expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),joincol1);

            if (relationFlag == 1)
            {
                    expr[0].type2 = new AttrType(AttrType.attrSymbol);
                    relationFlag = 0;
                    expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.outer),joincol2);
            }
            else
            {

                expr[0].type2 = new AttrType(AttrType.attrInteger);
                expr[0].operand2.integer = joincol2;
            }

            /*if(condition_code2 == 1)
                expr[1].op    = new AttrOperator(AttrOperator.aopLT);
            else if(condition_code2 == 2)
                expr[1].op    = new AttrOperator(AttrOperator.aopLE);
            else if(condition_code2 == 3)
                expr[1].op    = new AttrOperator(AttrOperator.aopGE);
            else if(condition_code2 == 4)
                expr[1].op    = new AttrOperator(AttrOperator.aopGT);
            expr[1].next  = null;
            expr[1].type1 = new AttrType(AttrType.attrSymbol);

            expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),joincol21);
            if (relationFlag1 == 1)
            {
                    expr[1].type2 = new AttrType(AttrType.attrSymbol);
                    relationFlag1 = 0;
                    expr[1].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),joincol22);
            }
            else
            {

                expr[1].type2 = new AttrType(AttrType.attrInteger);
                expr[1].operand2.integer = joincol22;
            }*/

            expr[1] = null;
          }

  private void query1a_CondExpr(CondExpr[] expr) {

    expr[0].next  = null;
    if(condition_code == 1)
        expr[0].op    = new AttrOperator(AttrOperator.aopLT);
    else if(condition_code == 2)
        expr[0].op    = new AttrOperator(AttrOperator.aopLE);
    else if(condition_code == 3)
        expr[0].op    = new AttrOperator(AttrOperator.aopGE);
    else if(condition_code == 4)
        expr[0].op    = new AttrOperator(AttrOperator.aopGT);
    expr[0].type1 = new AttrType(AttrType.attrSymbol);
    expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),joincol1);
    if (relationFlag == 1)
    {
            expr[0].type2 = new AttrType(AttrType.attrSymbol);
            relationFlag = 0;
            expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.outer),joincol2);
    }
    else
    {

        expr[0].type2 = new AttrType(AttrType.attrInteger);
        expr[0].operand2.integer = joincol2;
    }

    expr[1] = null;
  }

public void Query2a() throws JoinsException, IndexException, PageNotReadException, TupleUtilsException, SortException, LowMemException, UnknownKeyTypeException, Exception
{


        System.out.print("**********************Query1 starting *********************\n");
    boolean status = OK;

    String line,line2,line3,line4,line5;




                String filename = "/home/pratik/workspace3/Project/src/query_2a.txt";
//      ArrayList<Sailor> sailors  = new ArrayList<Sailor>();
                //boats    = new Vector();
                //reserves = new Vector();
                ArrayList<String> tablenames1 = new ArrayList<String>();
                try
                {
                        BufferedReader br = new BufferedReader(new FileReader(filename));

                                line = br.readLine();

                        String arr[]=line.split(" ");

                                projcol1=Integer.parseInt(arr[0].substring(arr[0].indexOf("_")+1));
                                projcol2=Integer.parseInt(arr[1].substring(arr[1].indexOf("_")+1));

                                line2=br.readLine();
                                String arr2=line2;
                                tablenames1.add(arr2);

                                table1 = arr2;


                                line3=br.readLine();
                                String arr3[]=line3.split(" ");

                                joincol1=Integer.parseInt(arr3[0].substring(arr3[0].indexOf("_")+1));
                                condition_code=Integer.parseInt(arr3[1]);
                                if(arr3[2].contains("_"))
                                {
                                        joincol2=Integer.parseInt(arr3[2].substring(arr3[2].indexOf("_")+1));
                                        relationFlag = 1;
                                }
                                else
                                                joincol2 = Integer.parseInt(arr3[2]);


                                line4=br.readLine();
                                condition=line4;

                                line5=br.readLine();
                                String arr5[]=line5.split(" ");
                                joincol21=Integer.parseInt(arr5[0].substring(arr5[0].indexOf("_")+1));
                                condition_code2=Integer.parseInt(arr3[1]);
                                if(arr3[2].contains("_"))
                                {
                                        joincol2=Integer.parseInt(arr3[2].substring(arr3[2].indexOf("_")+1));
                                        relationFlag = 1;
                                }
                                else
                                                joincol2 = Integer.parseInt(arr3[2]);
                                if(arr5[2].contains("_"))
                                {
                                        joincol22=Integer.parseInt(arr5[2].substring(arr5[2].indexOf("_")+1));
                                        relationFlag1 = 1;
                                }
                                else
                                                joincol22 = Integer.parseInt(arr5[2]);





                        br.close();
                        System.out.println(projcol1+" "+projcol2+" "+table1+" "+table2+" "+joincol1+" "+condition_code+" "+joincol2);
                        System.out.println(condition);
                        System.out.println(joincol21+" "+condition_code2+" "+joincol22);
                }
                catch(Exception e)
                {
                        e.printStackTrace();
                }

        openFiles(tablenames1);
    CondExpr[] outFilter = new CondExpr[2];
    outFilter[0] = new CondExpr();
    outFilter[1] = new CondExpr();


    query2a_CondExpr(outFilter);

    Tuple t = new Tuple();
    //CHANGES HERE
    AttrType [] Stypes = new AttrType[4];
    Stypes[0] = new AttrType (AttrType.attrInteger);
    Stypes[1] = new AttrType (AttrType.attrInteger);
    Stypes[2] = new AttrType (AttrType.attrInteger);
    Stypes[3] = new AttrType (AttrType.attrInteger);

    //SOS
    short [] Ssizes = new short[1];
    Ssizes[0] = 30; //first elt. is 30

    FldSpec [] Sprojection = new FldSpec[4];
    Sprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
    Sprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
    Sprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
    Sprojection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

    CondExpr [] selects = new CondExpr [1];
    selects = null;
    String filename1 = new String(table1+".in");
   // String filename2  = new String(table2+".in");

    FileScan am = null;
    FileScan am2 = null;
    try {
      am  = new FileScan(filename1, Stypes, Ssizes,
                                  (short)4, (short)4,
                                  Sprojection, null);
      am2 = new FileScan(filename1, Stypes, Ssizes,
                          (short)4, (short)4,
                          Sprojection, null);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }

    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for sailors");
      Runtime.getRuntime().exit(1);
    }
    //CHANGES HERE



    //proj_list means what we want to show in the output(This comes from first line of query file)
    FldSpec [] proj_list = new FldSpec[2];
    proj_list[0] = new FldSpec(new RelSpec(RelSpec.innerRel), projcol1);
    proj_list[1] = new FldSpec(new RelSpec(RelSpec.outer), projcol2);

    AttrType [] jtype = new AttrType[2];
    //CHANGES HERE
    //THIS IS THE TYPE OF proj_list values
    jtype[0] = new AttrType (AttrType.attrInteger);
    jtype[1] = new AttrType (AttrType.attrInteger);

    TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
    /*SortMerge sm = null;
    try {
      sm = new SortMerge(Stypes, 4, Ssizes,
                         Rtypes, 3, Rsizes,
                         1, 4,
                         1, 4,
                         10,
                         am, am2,
                         false, false, ascending,
                         outFilter, proj_list, 2);
    }*/
    TupleOrder LeftOrder = new TupleOrder(TupleOrder.Ascending);
    TupleOrder RightOrder = new TupleOrder(TupleOrder.Ascending);
    if(condition_code==1||condition_code==2)
    {
        LeftOrder = new TupleOrder(TupleOrder.Descending);

    }
    else if(condition_code==3||condition_code==4)
    {
        LeftOrder = new TupleOrder(TupleOrder.Ascending);
    }
    if(condition_code2==1||condition_code2==2)
    {
        RightOrder = new TupleOrder(TupleOrder.Ascending);

    }
    else if(condition_code2==3||condition_code2==4)
    {
        RightOrder = new TupleOrder(TupleOrder.Descending);
    }
   IESelfJoin sm = null;
    try {
      sm = new IESelfJoin(Stypes, 4, Ssizes, Stypes, 4, Ssizes, 10, joincol1, 10, joincol2, 10, LeftOrder, RightOrder, am, am2, outFilter, proj_list, 2);

    }
    catch (Exception e) {
      System.err.println("*** join error in SortMerge constructor ***");
      status = FAIL;
      System.err.println (""+e);
      e.printStackTrace();
    }

    if (status != OK) {
      //bail out
      System.err.println ("*** Error constructing SortMerge");
      Runtime.getRuntime().exit(1);
    }



    //QueryCheck qcheck1 = new QueryCheck(1);


    t = null;
    try
    {
    while ((t = sm.get_next()) != null) {

 System.out.println("Printing tuple");
 //t.print(jtype);
    }
    }
    catch(Exception e)
    {
        System.out.println("Done traversing all tuples");
    }

    sm.joinTuples(joincol1,joincol2,projcol1,projcol2,condition_code);
    /*try {
      while ((t = sm.get_next()) != null) {
        t.print(jtype);

        //qcheck1.Check(t);
      }
    }
    catch (Exception e) {
      System.err.println (""+e);
       e.printStackTrace();
       status = FAIL;
    }*/
    if (status != OK) {
      //bail out
      System.err.println ("*** Error in get next tuple ");
      Runtime.getRuntime().exit(1);
    }

    //qcheck1.report(1);
    try {
      sm.close();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    System.out.println ("\n");
    if (status != OK) {
      //bail out
      System.err.println ("*** Error in closing ");
      Runtime.getRuntime().exit(1);
    }
}

  public void Query1b() {

    System.out.print("**********************Query1 starting *********************\n");
    boolean status = OK;

    String line,line2,line3,line4,line5;




                String filename = "/home/pratik/workspace3/Project/src/query_1b.txt";
//      ArrayList<Sailor> sailors  = new ArrayList<Sailor>();
                //boats    = new Vector();
                //reserves = new Vector();
                ArrayList<String> tablenames1 = new ArrayList<String>();
                try
                {
                        BufferedReader br = new BufferedReader(new FileReader(filename));

                                line = br.readLine();

                        String arr[]=line.split(" ");

                                projcol1=Integer.parseInt(arr[0].substring(arr[0].indexOf("_")+1));
                                projcol2=Integer.parseInt(arr[1].substring(arr[1].indexOf("_")+1));

                                line2=br.readLine();
                                String arr2[]=line2.split(" ");
                                tablenames1.add(arr2[0]);
                                tablenames1.add(arr2[1]);
                                table1 = arr2[0];
                                table2 = arr2[1];

                                line3=br.readLine();
                                String arr3[]=line3.split(" ");

                                joincol1=Integer.parseInt(arr3[0].substring(arr3[0].indexOf("_")+1));
                                condition_code=Integer.parseInt(arr3[1]);
                                if(arr3[2].contains("_"))
                                {
                                        joincol2=Integer.parseInt(arr3[2].substring(arr3[2].indexOf("_")+1));
                                        relationFlag = 1;
                                }
                                else
                                                joincol2 = Integer.parseInt(arr3[2]);


                                line4=br.readLine();
                                condition=line4;

                                line5=br.readLine();
                                String arr5[]=line5.split(" ");
                                joincol21=Integer.parseInt(arr5[0].substring(arr5[0].indexOf("_")+1));
                                condition_code2=Integer.parseInt(arr3[1]);
                                if(arr3[2].contains("_"))
                                {
                                        joincol2=Integer.parseInt(arr3[2].substring(arr3[2].indexOf("_")+1));
                                        relationFlag = 1;
                                }
                                else
                                                joincol2 = Integer.parseInt(arr3[2]);
                                if(arr5[2].contains("_"))
                                {
                                        joincol22=Integer.parseInt(arr5[2].substring(arr5[2].indexOf("_")+1));
                                        relationFlag1 = 1;
                                }
                                else
                                                joincol22 = Integer.parseInt(arr5[2]);





                        br.close();
                        System.out.println(projcol1+" "+projcol2+" "+table1+" "+table2+" "+joincol1+" "+condition_code+" "+joincol2);
                        System.out.println(condition);
                        System.out.println(joincol21+" "+condition_code2+" "+joincol22);
                }
                catch(Exception e)
                {
                        e.printStackTrace();
                }

        openFiles(tablenames1);
    CondExpr[] outFilter = new CondExpr[3];
    outFilter[0] = new CondExpr();
    outFilter[1] = new CondExpr();
    outFilter[2] = new CondExpr();

    query1b_CondExpr(outFilter);

    Tuple t = new Tuple();
    //CHANGES HERE
    AttrType [] Stypes = new AttrType[4];
    Stypes[0] = new AttrType (AttrType.attrInteger);
    Stypes[1] = new AttrType (AttrType.attrInteger);
    Stypes[2] = new AttrType (AttrType.attrInteger);
    Stypes[3] = new AttrType (AttrType.attrInteger);

    //SOS
    short [] Ssizes = new short[1];
    Ssizes[0] = 30; //first elt. is 30

    FldSpec [] Sprojection = new FldSpec[4];
    Sprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
    Sprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
    Sprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
    Sprojection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

    CondExpr [] selects = new CondExpr [1];
    selects = null;
    String filename1 = new String(table1+".in");
    String filename2  = new String(table2+".in");

    FileScan am = null;
    try {
      am  = new FileScan(filename2, Stypes, Ssizes,
                                  (short)4, (short)4,
                                  Sprojection, null);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }

    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for sailors");
      Runtime.getRuntime().exit(1);
    }
    //CHANGES HERE
    AttrType [] Rtypes = new AttrType[4];
    Rtypes[0] = new AttrType (AttrType.attrInteger);
    Rtypes[1] = new AttrType (AttrType.attrInteger);
    Rtypes[2] = new AttrType (AttrType.attrInteger);
    Rtypes[3] = new AttrType (AttrType.attrInteger);

    short [] Rsizes = new short[1];
    Rsizes[0] = 15;
    FldSpec [] Rprojection = new FldSpec[4];
    Rprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
    Rprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
    Rprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
    Rprojection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

    FileScan am2 = null;
    try {
      am2 = new FileScan(filename2, Rtypes, Rsizes,
                                  (short)4, (short) 4,
                                  Rprojection, null);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }

    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for reserves");
      Runtime.getRuntime().exit(1);
    }

    //proj_list means what we want to show in the output(This comes from first line of query file)
    FldSpec [] proj_list = new FldSpec[2];
    proj_list[0] = new FldSpec(new RelSpec(RelSpec.innerRel), projcol1);
    proj_list[1] = new FldSpec(new RelSpec(RelSpec.outer), projcol2);

    AttrType [] jtype = new AttrType[2];
    //CHANGES HERE
    //THIS IS THE TYPE OF proj_list values
    jtype[0] = new AttrType (AttrType.attrInteger);
    jtype[1] = new AttrType (AttrType.attrInteger);

    TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
    /*SortMerge sm = null;
    try {
      sm = new SortMerge(Stypes, 4, Ssizes,
                         Rtypes, 3, Rsizes,
                         1, 4,
                         1, 4,
                         10,
                         am, am2,
                         false, false, ascending,
                         outFilter, proj_list, 2);
    }*/
    NestedLoopsJoins sm = null;
    try {
      sm = new NestedLoopsJoins (Stypes, 4, Ssizes,
                                  Rtypes, 4, Rsizes,
                                  10,
                                  am, filename1,
                                  outFilter, null, proj_list, 2);
    }
    catch (Exception e) {
      System.err.println("*** join error in SortMerge constructor ***");
      status = FAIL;
      System.err.println (""+e);
      e.printStackTrace();
    }

    if (status != OK) {
      //bail out
      System.err.println ("*** Error constructing SortMerge");
      Runtime.getRuntime().exit(1);
    }



    //QueryCheck qcheck1 = new QueryCheck(1);


    t = null;

    try {
      while ((t = sm.get_next()) != null) {
        t.print(jtype);

        //qcheck1.Check(t);
      }
    }
    catch (Exception e) {
      System.err.println (""+e);
       e.printStackTrace();
       status = FAIL;
    }
    if (status != OK) {
      //bail out
      System.err.println ("*** Error in get next tuple ");
      Runtime.getRuntime().exit(1);
    }

    //qcheck1.report(1);
    try {
      sm.close();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }
    System.out.println ("\n");
    if (status != OK) {
      //bail out
      System.err.println ("*** Error in closing ");
      Runtime.getRuntime().exit(1);
    }
  }



   public void Query1a() {

    System.out.print("**********************Query3 strating *********************\n");
    boolean status = OK;


    String line,line2,line3;
        String filename = "/home/pratik/workspace3/Project/src/query_1a.txt";
        ArrayList<String> tablenames1 = new ArrayList<String>();
        try
        {
                        BufferedReader br = new BufferedReader(new FileReader(filename));

                                line = br.readLine();

                        String arr[]=line.split(" ");

                        projcol1=Integer.parseInt(arr[0].substring(arr[0].indexOf("_")+1));
                        projcol2=Integer.parseInt(arr[1].substring(arr[1].indexOf("_")+1));

                        line2=br.readLine();
                        String arr2[]=line2.split(" ");
                        tablenames1.add(arr2[0]);
                        tablenames1.add(arr2[1]);
                        table1=arr2[0];
                        table2=arr2[1];
                        line3=br.readLine();
                        String arr3[]=line3.split(" ");

                        joincol1=Integer.parseInt(arr3[0].substring(arr3[0].indexOf("_")+1));
                        condition_code=Integer.parseInt(arr3[1]);
                        if(arr3[2].contains("_"))
                        {
                                joincol2=Integer.parseInt(arr3[2].substring(arr3[2].indexOf("_")+1));
                                relationFlag = 1;
                        }
                        else
                                        joincol2 = Integer.parseInt(arr3[2]);



                        br.close();
                        System.out.println(projcol1+" "+projcol2+" "+table1+" "+table2+" "+joincol1+" "+condition_code+" "+joincol2);
        }
        catch(Exception e)
        {
                        e.printStackTrace();
        }
        openFiles(tablenames1);
    CondExpr [] outFilter = new CondExpr[2];
    outFilter[0] = new CondExpr();
    outFilter[1] = new CondExpr();
    String filename1 = new String(table1+".in");
    String filename2  = new String(table2+".in");
    query1a_CondExpr(outFilter);

    Tuple t = new Tuple();
    t = null;

    AttrType Stypes[] = {
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrInteger)
    };
    short []   Ssizes = new short[1];
    Ssizes[0] = 30;

    AttrType [] Rtypes = {
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrInteger),
      new AttrType(AttrType.attrInteger)
    };
    short  []  Rsizes = new short[1];
    Rsizes[0] =15;

    FldSpec [] Sprojection = {
       new FldSpec(new RelSpec(RelSpec.outer), 1),
       new FldSpec(new RelSpec(RelSpec.outer), 2),
       new FldSpec(new RelSpec(RelSpec.outer), 3),
       new FldSpec(new RelSpec(RelSpec.outer), 4)
    };

    CondExpr[] selects = new CondExpr [1];
    selects = null;

    iterator.Iterator am = null;
    try {
      am  = new FileScan(filename2, Stypes, Ssizes,
                                  (short)4, (short) 4,
                                  Sprojection, null);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }

    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for sailors");
      Runtime.getRuntime().exit(1);
    }

    FldSpec [] Rprojection = {
       new FldSpec(new RelSpec(RelSpec.outer), 1),
       new FldSpec(new RelSpec(RelSpec.outer), 2),
       new FldSpec(new RelSpec(RelSpec.outer), 3),
       new FldSpec(new RelSpec(RelSpec.outer), 4)
    };

    iterator.Iterator am2 = null;
    try {
      am2 = new FileScan("reserves.in", Rtypes, Rsizes,
                                  (short)4, (short)4,
                                  Rprojection, null);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }

    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for reserves");
      Runtime.getRuntime().exit(1);
    }

    FldSpec [] proj_list = {
      new FldSpec(new RelSpec(RelSpec.innerRel), projcol1),
      new FldSpec(new RelSpec(RelSpec.outer), projcol2)
    };

    AttrType [] jtype     = { new AttrType(AttrType.attrInteger),
                new AttrType(AttrType.attrInteger)};

    TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
    /*SortMerge sm = null;
    try {
      sm = new SortMerge(Stypes, 4, Ssizes,
                         Rtypes, 4, Rsizes,
                         4, 4,
                         4, 4,
                         10,
                         am, am2,
                         false, false, ascending,
                         outFilter, proj_list, 1);
    }*/
    NestedLoopsJoins sm = null;
    try {
      sm = new NestedLoopsJoins (Stypes, 4, Ssizes,
                                  Rtypes, 4, Rsizes,
                                  10,
                                  am, filename1,
                                  outFilter, null, proj_list, 2);
    }
    catch (Exception e) {
      status = FAIL;
      System.err.println (""+e);
    }

    if (status != OK) {
      //bail out
      System.err.println ("*** Error constructing SortMerge");
      Runtime.getRuntime().exit(1);
    }

    //QueryCheck qcheck3 = new QueryCheck(3);


    t = null;

    try {
      while ((t = sm.get_next()) != null) {
        t.print(jtype);
       // qcheck3.Check(t);
      }
    }
    catch (Exception e) {
      System.err.println (""+e);
      e.printStackTrace();
       Runtime.getRuntime().exit(1);
    }


   // qcheck3.report(3);

    System.out.println ("\n");
    try {
      sm.close();
    }
    catch (Exception e) {
      status = FAIL;
      e.printStackTrace();
    }

    if (status != OK) {
      //bail out
      System.err.println ("*** Error setting up scan for sailors");
      Runtime.getRuntime().exit(1);
    }
  }



  private void Disclaimer() {
    System.out.print ("\n\nAny resemblance of persons in this database to"
         + " people living or dead\nis purely coincidental. The contents of "
         + "this database do not reflect\nthe views of the University,"
         + " the Computer  Sciences Department or the\n"
         + "developers...\n\n");
  }
}

public class JoinTest
{
  public static void main(String argv[]) throws JoinsException, IndexException, PageNotReadException, TupleUtilsException, SortException, LowMemException, UnknownKeyTypeException, Exception
  {
    boolean sortstatus;
    //SystemDefs global = new SystemDefs("bingjiedb", 100, 70, null);
    //JavabaseDB.openDB("/tmp/nwangdb", 5000);

    JoinsDriver jjoin = new JoinsDriver();

    sortstatus = jjoin.runTests();
    if (sortstatus != true) {
      System.out.println("Error ocurred during join tests");
    }
    else {
      System.out.println("join tests completed successfully");
    }
  }
}