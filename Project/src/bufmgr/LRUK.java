/* File LRUK.java */

package bufmgr;

import diskmgr.*;
import global.*;

  /**
   * class LRUK is a subclass of class Replacer using LRUK
   * algorithm for page replacement
   */
public class LRUK extends  Replacer {

  /**
   * private field
   * An array to hold number of frames in the buffer pool
   */

    private int  frames[];
    private int k;
    public long last_array[]; 
    public long hist[][];
 
  /**
   * private field
   * number of frames used
   */   
  private int  nframes; 

  /**
   * This pushes the given frame to the end of the list.
   * @param frameNo	the frame number
   */
  private void update(int frameNo)
  {

     int index;
     long c=System.currentTimeMillis();
     for ( index=0; index < nframes; ++index )
        if ( frames[index] == frameNo )
            break;
	if(hist[index][0]!=0)
	{
		for(int i=1;i<k;i++)
		{
			hist[index][i]=hist[index][i-1];	
		}	
	}
	
	else
	{
		for(int i=0;i<k;i++)
		{
			hist[index][i]=0;
		}
	}

	hist[index][0]=c;

    }

  /**
   * Calling super class the same method
   * Initializing the frames[] with number of buffer allocated
   * by buffer manager
   * set number of frame used to zero
   *
   * @param	mgr	a BufMgr object
   * @see	BufMgr
   * @see	Replacer
   */
    public void setBufferManager( BufMgr mgr )
     {
        super.setBufferManager(mgr);
	frames = new int [ mgr.getNumBuffers() ];
	nframes = 0;
     }


/* public methods */

  /**
   * Class constructor
   * Initializing frames[] pinter = null.
   */
    public LRUK(BufMgr mgrArg, int lastReference)
    {
      super(mgrArg);
      frames = null;
      k=lastReference;
      hist=new long[mgrArg.getNumBuffers()][k];
      last_array=new long[mgrArg.getNumBuffers()];
    }
  
  /**
   * calll super class the same method
   * pin the page in the given frame number 
   * move the page to the end of list  
   *
   * @param	 frameNo	 the frame number to pin
   * @exception  InvalidFrameNumberException
   */
 public void pin(int frameNo) throws InvalidFrameNumberException
 {
    super.pin(frameNo);
    update(frameNo);
    
 }

 
  /**
   * Finding a free frame in the buffer pool
   * or choosing a page to replace using LRUK policy
   *
   * @return 	return the frame number
   *		return -1 if failed
   */


 public int pick_victim() throws BufferPoolExceededException, 
	   PagePinnedException
 {
   
   int numBuffers = mgr.getNumBuffers();
   int frame=-1;
   long c;
	c= System.currentTimeMillis();
    if ( nframes < numBuffers ) {
	
        frame = nframes++;
        frames[frame] = frame;
        state_bit[frame].state = Pinned;
        (mgr.frameTable())[frame].pin();
	//update the frameNo information
	for(int i=0; i<k; i++)
	{
		hist[frame][i]=0;
	}
	hist[frame][0] = c;
	last_array[frame]=c;
		
		
        return frame;
    }
	else
    {
	long min=c;
	for(int i=0; i<nframes; i++)
	{
		if(hist[i][k-1]<min &&state_bit[i].state!=Pinned &&(c-last_array[i])>0)
		{
			frame=frames[i];
			min=hist[i][k-1];
		}
	}
     }
	

    for ( int i = 0; i < numBuffers; ++i ) {
         frame = frames[i];
        if ( state_bit[frame].state != Pinned ) {
            state_bit[frame].state = Pinned;
            (mgr.frameTable())[frame].pin();
            update(frame);
            return frame;
        }
  }

    throw new BufferPoolExceededException(null,"Buffer Exceeded");
 }

public long HIST(int frameNo, int k)
{
	return hist[frameNo][k-1];
}

public int[] getFrames()
{
	return frames;
}

public long back(int frameNo, int k)
{
	long c= System.currentTimeMillis();
	
	return (c-hist[frameNo][k-1]);
}
 
  /**
   * get the page replacement policy name
   *
   * @return	return the name of replacement policy used
   */  
    public String name() { return "LRUK"; }
 
  /**
   * print out the information of frame usage
   */ 
 public void info()
 {
    super.info();

    System.out.print( "LRUK REPLACEMENT");
    
    for (int i = 0; i < nframes; i++) {
        if (i % 5 == 0)
	System.out.println( );
	System.out.print( "\t" + frames[i]);
        
    }
    System.out.println();
 }
  
}



