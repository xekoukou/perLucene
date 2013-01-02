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

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;

class ZooAbstract
{

    protected ZooKeeper zoo;

//requires a string of 2 size
    protected void initZookeeper (String line[], Watcher watcher)
    {

        try {
            zoo = new ZooKeeper (line[0], Integer.parseInt (line[1]), watcher);
        } catch (Exception e)
        {
            System.out.println
                ("failed to initialize the zookeeper object or there was an error");
            System.out.println (e.toString ());
            System.exit (-1);
        }
    }
}
