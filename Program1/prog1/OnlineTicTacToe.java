import java.io.*;
import java.net.*;
import java.util.*;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JOptionPane;

/**
 * @author Samridhi Agrawal
 */

public class OnlineTicTacToe implements ActionListener {
    private final int INTERVAL = 1000; // 1 second
    private final int NBUTTONS = 9; // #bottons
    private ObjectInputStream input = null; // input from my counterpart
    private ObjectOutputStream output = null; // output from my counterpart
    private JFrame window = null; // the tic-tac-toe window
    private JButton[] button = new JButton[NBUTTONS]; // button[0] - button[9]
    private boolean[] myTurn = new boolean[1]; // T: my turn, F: your turn
    private String myMark = null; // "O" or "X"
    private String yourMark = null; // "X" or "O"
    Set<Integer> trackBlockedNumberOnGameWindow
            = new HashSet<Integer>(); //set of integer values of already blocked cells

    /**
     * Prints out the usage.
     */
    private static void usage() {
        System.err.
                println("Usage: java OnlineTicTacToe ipAddr ipPort(>=5000) [auto]");
        System.exit(-1);
    }

    /**
     * Prints out the track trace upon a given error and quits the application.
     *
     * @param an exception
     */
    private static void error(Exception e) {
        e.printStackTrace();
        System.exit(-1);
    }

    /**
     * Starts the online tic-tac-toe game.
     *
     * @param args[0]: my counterpart's ip address, args[1]: his/her port, (arg[2]: "auto")
     *                 if args.length == 0, this Java program is remotely launched by JSCH.
     */
    public static void main(String[] args) {
        if (args.length == 0) {
            // if no arguments, this process was launched through JSCH
            try {
                OnlineTicTacToe game = new OnlineTicTacToe();
            } catch (IOException e) {
                error(e);
            }
        } else {
            // this process wa launched from the user console.
            // verify the number of arguments
            if (args.length != 2 && args.length != 3) {
                System.err.println("args.length = " + args.length);
                usage();
            }
            // verify the correctness of my counterpart address
            InetAddress addr = null;
            try {
                addr = InetAddress.getByName(args[0]);
            } catch (UnknownHostException e) {
                error(e);
            }
            // verify the correctness of my counterpart port
            int port = 0;
            try {
                port = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                error(e);
            }
            if (port < 5000) {
                usage();
            }
            // check args[2] == "auto"
            if (args.length == 3 && args[2].equals("auto")) {
                // auto play
                OnlineTicTacToe game = new OnlineTicTacToe(args[0]);
            } else {
                // interactive play
                OnlineTicTacToe game = new OnlineTicTacToe(addr, port);
            }
        }
    }

    /**
     * Is the constructor that is remote invoked by JSCH. It behaves as a server.
     * The constructor uses a Connection object for communication with the client.
     * It always assumes that the client plays first.
     */
    public OnlineTicTacToe() throws IOException {
        // receive an ssh2 connection from a user-local master server.
        Connection connection = new Connection();
        input = connection.in;
        output = connection.out;

        // for debugging, always good to write debugging messages to the local file
        // don't use System.out that is a connection back to the client.
        PrintWriter logs = new PrintWriter(new FileOutputStream("logs.txt"));
        logs.println("Autoplay: got started.");
        logs.flush();

        myMark = "X"; // auto player is always the 2nd.
        yourMark = "O";
        // the main body of auto play.
        // start my counterpart thread


        //Read the information from its counterpart
        while (true) {
            try {
                int buttonId = (int) (input.readObject());

                //add the number already used into set
                trackBlockedNumberOnGameWindow.add(buttonId);
                
                //set the myTurn to true
                myTurn[0] = true;

            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }

            //check if myTurn is "true"
            if (myTurn[0]) {

                Random rn = new Random();
                int counterButtonId = -1;

                //creates and assign a random number between
                // 0 and 8 inclusive to counterpart
                while (counterButtonId == -1
                        || trackBlockedNumberOnGameWindow.contains(counterButtonId))
                    counterButtonId = rn.nextInt(8 - 0 + 1) + 0;

                //write the button clicked to real player
                output.writeObject(counterButtonId);
                output.flush();

                //add the number already used into set
                trackBlockedNumberOnGameWindow.add(counterButtonId);

                //turn gets changed
                myTurn[0] = false;
            }

        }
    }



    /**
     * Is the constructor that, upon receiving the "auto" option,
     * launches a remote OnlineTicTacToe through JSCH. This
     * constructor always assumes that the local user should play
     * first. The constructor uses a Connection object for
     * communicating with the remote process.
     *
     * @param my auto counter part's ip address
     */
    public OnlineTicTacToe(String hostname) {
        final int JschPort = 22; // Jsch IP port

        // Read username, password, and a remote host from keyboard
        Scanner keyboard = new Scanner(System.in);
        String username = null;
        String password = null;
        // establish an ssh2 connection to ip and run
        // Server there.

        try {
            // read the user name from the console
            System.out.print( "User: " );
            username = keyboard.nextLine( );

            // read the password from the console
            Console console = System.console( );
            password = new String( console.readPassword( "Password: " ) );

        } catch( Exception e ) {
            e.printStackTrace( );
            System.exit( -1 );
        }

        // A command to launch remotely:
        //          java -cp ./jsch-0.1.54.jar:. JSpace.Server
        String cur_dir = System.getProperty("user.dir");
        String command
                = "java -cp " + cur_dir + "/jsch-0.1.54.jar:" + cur_dir +
                " OnlineTicTacToe";

        Connection connection = new Connection(username, password,
                hostname, command);

        // the main body of the master server
        input = connection.in;
        output = connection.out;


        // set up a window
        makeWindow(true); // I'm a former
        // start my counterpart thread
        Counterpart counterpart = new Counterpart();
        counterpart.start();

    }

    /**
     * Is the constructor that sets up a TCP connection with my counterpart,
     * brings up a game window, and starts a slave thread for listenning to
     * my counterpart.
     *
     * @param my counterpart's ip address
     * @param my counterpart's port
     */
    public OnlineTicTacToe(InetAddress addr, int port) {
        // set up a TCP connection with my counterpart
        // Prepare a server socket and make it non-blocking
        ServerSocket server = null;
        Socket client = null;
        try {

            //get localhost address
            InetAddress localHost=InetAddress.getLocalHost();
            server = new ServerSocket(port);

            //check if the server is not the localhost
            if(!(addr.isAnyLocalAddress()||
                    addr.isLoopbackAddress() || localHost==addr)) {

                //set the server non-blocking, (i.e. time out beyon 1000)
                server.setSoTimeout(INTERVAL);
            }

        } catch (BindException be) {
            System.out.println(be.getMessage());

            try {
                client = new Socket(addr, port);
                makeWindow(false);

            } catch (IOException ioe) {
                // Connection refused
            }

            if (client != null)
                // Exchange a message with my counter part.
                try {
                    System.out.println("TCP connection established...");
                    output = new ObjectOutputStream(client.getOutputStream());

                    input = new ObjectInputStream(client.getInputStream());

                } catch (Exception ie) {
                    error(ie);
                }
        }catch (Exception e){
            error(e);
        }

        if (client == null) {
            // While accepting a remote request, try to send my connection request
            while (true) {
                try {
                    client = server.accept();
                    makeWindow(true);

                } catch (SocketTimeoutException ste) {
                    // Couldn't receive a connection request withtin INTERVAL
                } catch (IOException ioe) {
                    error(ioe);
                }
                // Check if a connection was established. If so, leave the loop
                if (client != null) {
                    break;
                }

                try {
                    client = new Socket(addr, port);
                    makeWindow(false);

                } catch (IOException ioe) {
                    // Connection refused
                }

                // Check if a connection was established, If so, leave the loop
                if (client != null)
                    break;

            }

            // Check if a connection was established. If so, leave the loop
            if (client != null)
                // Exchange a message with my counter part.
                try {
                    System.out.println("TCP connection established...");
                    output = new ObjectOutputStream(client.getOutputStream());

                    input = new ObjectInputStream(client.getInputStream());

                } catch (Exception e) {
                    error(e);
                }
        }

        // start my counterpart thread
        Counterpart counterpart = new Counterpart();
        counterpart.start();
    }


    /**
     * Creates a 3x3 window for the tic-tac-toe game
     *
     * @param true if this window is created by the former, (i.e., the
     *             person who starts first. Otherwise false.
     */
    private void makeWindow(boolean amFormer) {
        myTurn[0] = amFormer;
        myMark = (amFormer) ? "O" : "X"; // 1st person uses "O"
        yourMark = (amFormer) ? "X" : "O"; // 2nd person uses "X"
        // create a window
        window = new JFrame("OnlineTicTacToe(" +
                ((amFormer) ? "former)" : "latter)") + myMark);
        window.setSize(300, 300);
        window.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        window.setLayout(new GridLayout(3, 3));
        // initialize all nine cells.
        for (int i = 0; i < NBUTTONS; i++) {
            button[i] = new JButton();
            window.add(button[i]);
            button[i].addActionListener(this);
        }
        // make it visible
        window.setVisible(true);
    }

    /**
     * Marks the i-th button with mark ("O" or "X")
     *
     * @param the  i-th button
     * @param a    mark ( "O" or "X" )
     * @param true if it has been marked in success
     */
    private boolean markButton(int i, String mark) {
        if (button[i].getText().equals("")) {
            button[i].setText(mark);
            button[i].setEnabled(false);
            return true;
        }
        return false;
    }

    /**
     * Checks which button has been clicked
     *
     * @param an event passed from AWT
     * @return an integer (0 through to 8) that shows which button has been
     * clicked. -1 upon an error.
     */

    private int whichButtonClicked(ActionEvent event) {
        for (int i = 0; i < NBUTTONS; i++) {
            if (event.getSource() == button[i])
                return i;
        }
        return -1;
    }

    /**
     * Checks if the i-th button has been marked with mark( "O" or "X" ).
     *
     * @param the i-th button
     * @param a   mark ( "O" or "X" )
     * @return true if the i-th button has been marked with mark.
     */
    private boolean buttonMarkedWith(int i, String mark) {
        return button[i].getText().equals(mark);
    }

    /**
     * checks the player mark win either ( "O" or "X" )
     *
     * @param a mark ( "O" or "X" )
     */
    private boolean checkForWin(String mark) {
        if(buttonMarkedWith(0, mark) && buttonMarkedWith(1, mark) && buttonMarkedWith(2, mark)) {
            return true;
        } else if(buttonMarkedWith(3, mark) && buttonMarkedWith(4, mark) && buttonMarkedWith(5, mark)){
            return true;
        } else if(buttonMarkedWith(6, mark) && buttonMarkedWith(7, mark) && buttonMarkedWith(8, mark)){
            return true;
        } else if(buttonMarkedWith(0, mark) && buttonMarkedWith(3, mark) && buttonMarkedWith(6, mark)){
            return true;
        } else if(buttonMarkedWith(1, mark) && buttonMarkedWith(4, mark) && buttonMarkedWith(7, mark)){
            return true;
        } else if(buttonMarkedWith(2, mark) && buttonMarkedWith(5, mark) && buttonMarkedWith(8, mark)){
            return true;
        } else if(buttonMarkedWith(0, mark) && buttonMarkedWith(4, mark) && buttonMarkedWith(8, mark)){
            return true;
        } else if(buttonMarkedWith(2, mark) && buttonMarkedWith(4, mark) && buttonMarkedWith(6, mark)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * checks the game is draw, nobody wins
     *
     */
    private boolean checkForGameDraw() {
        boolean flag = false;
        
        //check the length of the set to be full
        if ( trackBlockedNumberOnGameWindow.size() == 9) {
            if (! checkForWin("O") && ! checkForWin("X")) {
                
                //set the flag to true, game is draw
                flag = true;
            }
        }
        return flag;
    }

    /**
     * Pops out another small window indicating that mark("O" or "X") won!
     *
     * @param a mark ( "O" or "X" )
     */
    private void showWon(String mark) {
        JOptionPane.showMessageDialog(null, mark + " won!");
    }

    /**
     * Pops out another small window indicating that game is draw
     *
     */
    private void showDraw() {
        JOptionPane.showMessageDialog(null,  "Game Draw! Nobody won, try again!!");
    }

    /**
     * Is called by AWT whenever any button has been clicked. You have to:
     * <ol>
     * <li> check if it is my turn,
     * <li> check which button was clicked with whichButtonClicked( event ),
     * <li> mark the corresponding button with markButton( buttonId, mark ),
     * <li> send this informatioin to my counterpart,
     * <li> checks if the game was completed with
     * buttonMarkedWith( buttonId, mark )
     * <li> shows a winning message with showWon( )
     */
    public void actionPerformed(ActionEvent event) {

        //synchronize on myTurn object to make it non-blocking
        synchronized (myTurn) {

            //check if it myTurn
            if (myTurn[0]) {

                try {

                    //locally stores the button clicked
                    int buttonId = whichButtonClicked(event);

                    //marks the tick
                    markButton(buttonId, myMark);

                    //clicked button is stored in a tracking array
                    trackBlockedNumberOnGameWindow.add(buttonId);

                    //check if the game has won by "myMark"
                    if (checkForWin(myMark)) {
                        showWon(myMark);
                    }

                    //check if the game is draw
                    if (checkForGameDraw()) {
                        showDraw();
                    }

                    //sending clicked button to the remote server
                    output.writeObject(buttonId);
                    output.flush();

                    //change the turn
                    myTurn[0] = false;

                    //notifies that the turn is completed
                    myTurn.notifyAll();

                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            } else{
                try {
                    //wait for request from the counterpart
                    myTurn.wait();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
        }
    }

    /**
     * This is a reader thread that keeps reading fomr and behaving as my
     * counterpart.
     */
    private class Counterpart extends Thread {
        /**
         * Is the body of the Counterpart thread.
         */
        @Override
        public void run() {

            //thread is active with "true"
            while (true) {

                //synchronize on myTurn object to make it non-blocking
                synchronized (myTurn) {

                    //check if it is myTurn
                    if (!myTurn[0]) {

                        try {

                            //read and mark the buttonId from the first player
                            int buttonId = (int) (input.readObject());
                            markButton(buttonId, yourMark);

                            //clicked button is stored in a tracking array
                            trackBlockedNumberOnGameWindow.add(buttonId);

                            //check if the game has won by "yourMark"
                            if (checkForWin(yourMark))
                                showWon(yourMark);

                            //check if the game is draw
                            if (checkForGameDraw()) {
                                showDraw();
                            }

                            //change the turn
                            myTurn[0] = true;

                            //notifies that the turn is completed
                            myTurn.notifyAll();


                        } catch (Exception e) {
                            e.printStackTrace();
                            System.exit(-1);
                        }
                    } else {
                        try {
                            myTurn.wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                            System.exit(-1);
                        }

                    }
                }
            }
        }
    }
}