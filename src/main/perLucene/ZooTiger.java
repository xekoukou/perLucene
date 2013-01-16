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


import java.io.FileNotFoundException;



//up        1 search
//          2 index + search 

//all replicas should hava 2 as value in case they become leader 
//only the leader can have 1 as value , only the leaders value is checked by the gdd, when it syncs a replica's state

//the existence of up is checked by the leader as well as the gd for all replicas

//each server doesnt create the ephemeral node up unless it has been already synced and is ready for search

class ZooTiger extends ZooAbstract
{

    protected int interval;
    protected int replica;

    protected int leader;
    protected Stat leaderStat = new Stat ();

    protected boolean isLeader;
    protected boolean isSynced = false;

    protected boolean leaderExists;
    protected long lastTimeWorking;


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

        goOnline ();

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

    public void leader (int leader)
    {
        this.leader = leader;

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
    protected void goOnline ()
    {

        String path =
            "/tiger/Servers/" + Integer.toString (interval) + "/replicas/" +
            Integer.toString (leader) + "/up";

        try {
            zoo.create (path,
                        ByteBuffer.allocate (4).putInt (2).array (), acl,
                        CreateMode.EPHEMERAL);


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

    protected void sync ()
    {


    }


    public void connect ()
    {


    }

    public void becomeLeader ()
    {



    }

    public void bind ()
    {


    }

    public void stoppedWorking ()
    {

        if (leaderExists) {
            this.leaderExists (false);
            this.lastTimeWorking ((new Date ()).getTime ());
        }

    }



}
