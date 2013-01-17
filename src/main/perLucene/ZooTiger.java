package perLucene;


/*
    Copyright contributors as noted in the AUTHORS file.
                
    This file is part of PLATANOS.

    PLATANOS is free software; you can redistribute it and/or modify it under
    the terms of the GNU Affero General Public License as published by
    the Free Software Foundation; either version 3 of the License, or
    (at your option) any later version.
            
    PLATANOS is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.
        
    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

import java.nio.ByteBuffer;
import java.util.Date;


import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;

import org.jeromq.ZMQ;
import org.jeromq.ZContext;
import org.jeromq.ZFrame;
import org.jeromq.ZMsg;
import org.jeromq.ZMQ.Msg;
import org.jeromq.ZMQ.PollItem;
import org.jeromq.ZMQ.Socket;


import java.io.FileNotFoundException;



//up        1 search
//          2 index + search 

//all replicas should hava 2 as value in case they become leader 
//only the leader can have 1 as value , only the leaders value is checked by the gdd, when it syncs a replica's state

//the existence of up is checked by the leader as well as the gd for all replicas


//the sbroker socket should only be used by the asynchronous thread of the zookeeper

class ZooTiger extends ZooAbstract
{

    protected int interval;
    protected int replica;

    protected int leader;
    protected Stat leaderStat = new Stat ();

    protected boolean isLeader;
    protected int maximum_size; // only the leader knows these
    protected int split_size;

    protected boolean isSynced = false;

    protected boolean leaderExists;
    protected long lastTimeWorking;

    protected Socket sbroker;

    protected ZContext ctx;


    //requires a string of 2 size
      ZooTiger ()
    {
        String[]line = Config.readLocalConfig ();

        super.initZookeeper (line[0], Integer.parseInt (line[1]), new Watcher ()
                             {
                             @Override public void process (WatchedEvent e)
                             {
                             if (!
                                 (e.getState ().
                                  equals (Watcher.Event.KeeperState.
                                          SyncConnected))) {
                             System.out.println ("Disconnected.. exiting");
                             System.exit (0);}
                             }
                             }
        );


        interval = Integer.parseInt (line[3]);
        replica = Integer.parseInt (line[4]);

        initBroker ();


    }

    public void leader (int leader)
    {
        this.leader = leader;

    }
    public int leader ()
    {
        return leader;

    }

    public int interval ()
    {
        return interval;

    }



    public boolean isLeader ()
    {

        return isLeader;

    }


    public boolean isSynced ()
    {

        return isSynced;

    }
    public void leaderExists (boolean leaderExits)
    {

        this.leaderExists = leaderExists;
    }

    public void lastTimeWorking (long lastTimeWorking)
    {

        this.lastTimeWorking = lastTimeWorking;
    }

//returns true if the leader is alive
    public boolean getLeader ()
    {
        try {
            String path = "/tiger/Servers/" + Integer.toString (interval);

            leader =
                ByteBuffer.wrap (zoo.getData (path + "/leader",
                                              new WatcherLeader (this),
                                              leaderStat)).getInt ();
//check the leader is alive
            if (zoo.exists
                (path + "/replicas/" + Integer.toString (leader) + "/up",
                 new WatcherLeaderAlive (this)) != null) {

                if (leader == replica) {
                    isLeader = true;
                }
                else {
                    isLeader = false;
                }

                leaderExists = true;
                return true;
            }
            else {
                this.stoppedWorking ();
                return false;
            }

        }
        catch (KeeperException e) {
            System.out.println
                ("zookeeper client exited with error code " +
                 e.code ().toString ());
            System.out.println (e.toString ());
            System.exit (-1);
        }
        catch (Exception e) {
            System.out.println ("zookeeper client interrupted");
            System.out.println (e.toString ());
            System.exit (-1);
        }

//default value, will never happen 
        return false;

    }

//only done at the beginning, always set to 2
    public void goOnline ()
    {

        String path =
            "/tiger/Servers/" + Integer.toString (interval) + "/replicas/" +
            Integer.toString (leader) + "/up";

        try {
            zoo.create (path,
                        ByteBuffer.allocate (4).putInt (2).array (), acl,
                        CreateMode.EPHEMERAL);


            //any new server is considered unsynched

            if (getLeader ()) {
                if (!isLeader) {
                    sync ();
                    connect ();
                }
                else {
                    bind ();
                }
            }

        }
        catch (KeeperException e) {
            if (e.code ().equals (KeeperException.Code.NODEEXISTS)) {

                System.out.println
                    ("This replica is already online, possible local configuration error");

            }
            else {

                System.out.println
                    ("zookeeper client exited with error code " +
                     e.code ().toString ());
                System.out.println (e.toString ());


            }
            System.exit (-1);
        }
        catch (Exception e) {
            System.out.println ("zookeeper client interrupted");
            System.out.println (e.toString ());
            System.exit (-1);
        }


    }



//if all replicas are offline, then wait for the last leader to go online
//this way we dont lose documents

//all till bind should only be used by the asynchronous thread of zookeeper


//0 sync location
//1 connect
//2 bind location

    protected void sync ()
    {


    }


    public void connect ()
    {


    }

    public void bind ()
    {


    }

    public void becomeLeader ()
    {

        isLeader = true;

        try {
            String path = "/tiger/Servers/" + Integer.toString (interval);

            zoo.setData (path + "/leader",
                         ByteBuffer.allocate (4).putInt (replica).array (),
                         leaderStat.getVersion ());

        }
        catch (KeeperException e) {
            if (e.code ().equals (KeeperException.Code.BADVERSION)) {

                isLeader = false;
//someone else became a leader
            }
            else {
//TODO maybe clarify the possible errors
                System.out.println
                    ("zookeeper client exited with error code " +
                     e.code ().toString ());
                System.out.println (e.toString ());
                System.exit (-1);
            }
        }
        catch (Exception e) {
            System.out.println ("zookeeper client interrupted");
            System.out.println (e.toString ());
            System.exit (-1);
        }




        if (isLeader) {


            try {
                String path = "/tiger/Servers/" + Integer.toString (interval);

                maximum_size =
                    ByteBuffer.wrap (zoo.getData (path + "/maximum_size",
                                                  new WatcherNotify (this),
                                                  null)).getInt ();

                split_size =
                    ByteBuffer.wrap (zoo.getData (path + "/split_size",
                                                  new WatcherNotify (this),
                                                  null)).getInt ();

            }
            catch (KeeperException e) {
                System.out.println
                    ("zookeeper client exited with error code " +
                     e.code ().toString ());
                System.out.println (e.toString ());
                System.exit (-1);
            }
            catch (Exception e) {
                System.out.println ("zookeeper client interrupted");
                System.out.println (e.toString ());
                System.exit (-1);
            }



//TODO get which other replicas are online

        }




    }


    public void stoppedWorking ()
    {

        if (leaderExists) {
            this.leaderExists (false);
            this.lastTimeWorking ((new Date ()).getTime ());
        }

    }



    private void initBroker ()
    {

        ctx = new ZContext ();

        sbroker = ctx.createSocket (ZMQ.ROUTER);

        String address = "tcp://127.0.0.1:49002";
        sbroker.bind (address);


    }


    public String getConfig ()
    {

        try {
            Stat stat = new Stat ();

            return (new
                    String (zoo.getData ("/tiger/Servers/" +
                                         Integer.toString (interval) +
                                         "/replicas/" +
                                         Integer.toString (leader)
                                         , null, stat), "UTF-8"));




        }
        catch (KeeperException e) {

            System.out.println
                ("This replica doesnt exists, or its configuration in zookeeper is corrupt");
            System.exit (0);


        }
        catch (Exception e) {
            System.out.println ("zookeeper client interrupted");
            System.out.println (e.toString ());
            System.exit (-1);
        }

        return null;


    }

//used only when this replica is the leader
    public void notifyZoo ()
    {



    }


}
