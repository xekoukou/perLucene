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
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import java.util.ArrayList;
import org.apache.zookeeper.data.Stat;

public class ZooTigerRegister extends ZooAbstract
{

    protected ArrayList acl;

    public static void main ()
    {

        ZooTigerRegister zoo = new ZooTigerRegister ();

        GlobalWatcher watcher = new GlobalWatcher ();

          zoo.initZookeeper (Config.readLocalConfig (), watcher);

          zoo.initTigerIfNotThere ();







    }

    ZooTigerRegister ()
    {
        this.initACL ();
    }

    protected void initACL ()
    {

        acl = new ArrayList < ACL > ();
        acl.add (new ACL (ZooDefs.Perms.ALL, ZooDefs.Ids.ANYONE_ID_UNSAFE));

    }

//should not be executed concurrently many times
    protected void initTigerIfNotThere ()
    {

        try {
            if (zoo.exists ("/Tiger", false) == null) {

                zoo.create ("/Tiger", null, acl, CreateMode.PERSISTENT);
                zoo.create ("/Tiger/Servers",
                            ByteBuffer.allocate (4).putInt (0).array (), acl,
                            CreateMode.PERSISTENT);

                zoo.create ("/Tiger/last_upper_boundary",
                            ByteBuffer.allocate (8).putLong (0).array (), acl,
                            CreateMode.PERSISTENT);

            }
        }
        catch (KeeperException e) {
            if (!e.code ().equals (KeeperException.Code.OK)) {

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

    }


//registering only one server at a time, otherwise there might be problems
//location should be UTF-8 encoded

// the maximum size is in Gigabytes

//condition 0 only indexing
//          1 fully operational

    protected void registerTiger (long added, String location, int maximum_size,
                                  int split_size)
    {

        try {
            Stat stat = new Stat ();
            int nServers =
                ByteBuffer.wrap (zoo.getData ("/tiger/Servers", false,
                                              stat)).getInt ();

            String path = "/Tiger/Servers/" + Integer.toString (nServers + 1);
            zoo.create (path, null, acl, CreateMode.PERSISTENT);

            long upper_boundary =
                ByteBuffer.wrap (zoo.
                                 getData ("/tiger/last_upper_boundary", false,
                                          null)).getLong ();
            zoo.create (path + "/up",
                        ByteBuffer.allocate (8).putLong (added +
                                                         upper_boundary).array
                        (), acl, CreateMode.PERSISTENT);

            zoo.create (path + "/down",
                        ByteBuffer.allocate (8).putLong (upper_boundary).
                        array (), acl, CreateMode.PERSISTENT);

            zoo.create (path + "/replicas",
                        ByteBuffer.allocate (4).putInt (1).array
                        (), acl, CreateMode.PERSISTENT);


            zoo.create (path + "/replicas/1",
                        location.getBytes ("UTF-8"), acl,
                        CreateMode.PERSISTENT);

            zoo.create (path + "/replicas/1/notifications",
                        null, acl, CreateMode.PERSISTENT);

            zoo.create (path + "/replicas/1/condition",
                        ByteBuffer.allocate (4).putInt (0).array (), acl,
                        CreateMode.PERSISTENT);

//the minimum of all the max_sizes of the replicas

            zoo.create (path + "/maximum_size",
                        ByteBuffer.allocate (4).putInt (maximum_size).array (),
                        acl, CreateMode.PERSISTENT);

            zoo.create (path + "/split_size",
                        ByteBuffer.allocate (4).putInt (maximum_size).array (),
                        acl, CreateMode.PERSISTENT);


            // update the number of servers
            zoo.setData ("/tiger/Servers",
                         ByteBuffer.allocate (4).putInt (nServers).array (),
                         stat.getVersion ());


        } catch (KeeperException e) {
            if (!e.code ().equals (KeeperException.Code.OK)) {

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





    }


    protected void replicateTiger (int whichInterval, String location,
                                   int maximum_size)
    {

        try {
            Stat stat = new Stat ();
            String path = "/Tiger/Servers/" + Integer.toString (whichInterval);

            int nReplica =
                ByteBuffer.wrap (zoo.getData (path + "/replicas", false,
                                              stat)).getInt ();



            zoo.create (path + "/replicas/" + Integer.toString (nReplica + 1),
                        location.getBytes ("UTF-8"), acl,
                        CreateMode.PERSISTENT);

            zoo.create (path + "/replicas/" + Integer.toString (nReplica + 1) +
                        "/notifications", null, acl, CreateMode.PERSISTENT);

            zoo.create (path + "/replicas/" + Integer.toString (nReplica + 1) +
                        "/condition",
                        ByteBuffer.allocate (4).putInt (0).array (), acl,
                        CreateMode.PERSISTENT);

// update the replicas

            zoo.setData (path + "/replicas",
                         ByteBuffer.allocate (4).putInt (nReplica).array (),
                         stat.getVersion ());


            zoo.create (path + "/maximum_size",
                        ByteBuffer.allocate (4).putInt (maximum_size).array (),
                        acl, CreateMode.PERSISTENT);

            int max =
                ByteBuffer.wrap (zoo.getData (path + "/maximum_size", false,
                                              stat)).getInt ();
            if (maximum_size < max) {

                zoo.setData (path + "/maximum_size",
                             ByteBuffer.allocate (4).
                             putInt (maximum_size).array (),
                             stat.getVersion ());
            }




        }
        catch (KeeperException e) {
            if (!e.code ().equals (KeeperException.Code.OK)) {

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


    }

}
