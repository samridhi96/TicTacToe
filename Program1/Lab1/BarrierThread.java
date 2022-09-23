import java.util.*;

class BarrierThread implements Runnable {
    private int N = 0; // #threads
    private int X = 0; // iteration
    private Random r = null;
    private int[] sync = null; // threads share it and synchronize on it.
    // constructor
    public BarrierThread( int[] sync, int nThreads, int iteration ) {
	this.sync = sync;   
	N = nThreads;
	X = iteration;
	r = new Random( );
    }

    // the main body of this thread
    public void run( ) {
	for ( int i = 0; i < X; i++ ) {
	    try {
		Thread.sleep( r.nextInt( ) % 1000 );
	    } catch ( Exception e ) { }
	    barrier( );
	    System.out.println( i + " barriers completed by " +
				Thread.currentThread( ) );
	}
    }

    // this is what you implement
    private void barrier( ) {
	synchronized( sync ) {
	    r = new Random();	
	    sync[0] ++;// because I reached the barrier
	    //if sync[0] does not reach N, #threads, there must be someone else who has not called barrier( )
	    if( sync[0] < N) {
	    //   let's wait
	    	try {
		sync.wait(1000);
	    	 //Thread.sleep(r.nextInt( ) % 1000);
		} catch ( Exception e ) { }
            }
	    // else, all threads called barrier( ) and I am the last
	    else {
	         sync.notifyAll();
	    	
	    //   let's wake them all
	    //   zero-initialize sync[0] for the next barrier
	    sync[0] = 0;
	    }	
	}
    }

    
    public static void main( String args[] ) {
	// java BarrierThread #Threads iterations
	int[] sync = new int[1]; // used to count the number of threads that called barrier so far
	sync[0] = 0;
	int nThreads = Integer.parseInt( args[0] );
	int iteration = Integer.parseInt( args[1] );

	// spawn N - 1 child threads
	// this is what you implement
	for ( int i = 0; i < nThreads - 1; i++ ) {
	    new Thread( new BarrierThread( sync, nThreads, iteration ) ).start( );
	}

	// the main calls run( ), too!
	( new BarrierThread( sync, nThreads, iteration ) ).run( );
    }
}
