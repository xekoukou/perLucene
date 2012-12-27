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


import java.io.FileReader;
import org.apache.zookeeper.ZooKeeper;

import java.io.FileNotFoundException;


class ZooLander{


ZooLander(){



}

public String readLocalConfig(){

FileReader config;

try{
config=new FileReader("./config");

char[] buffer=new char[1000];

while((-1)!=config.read(buffer)){};

return new String(buffer);



}catch(Exception e){
System.out.println("local configuration not found or there was an error");
System.exit(-1);
}

return "";
}



}

