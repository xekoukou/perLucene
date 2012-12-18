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




public class VarInt
{

    private byte[] buffer;

    private int dim;

    private int lastPosition = 0;

/* size of last read */
    public int size;

    public VarInt (byte[]buffer, int dim)
    {
     if(buffer.length==0){
throw new IllegalArgumentException("varint buffer is empty");
}
        this.buffer = buffer;
        this.dim=dim;

    }
/*returns -1 on error */
    public long read (int index)
    {
        int shift = 0;
        long result = 0;
        int size = index;
        while (shift < 64) {
            final byte b = buffer[index];
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                size = index - size + 1;
                return result;
            }
            shift += 7;
            index++;
        }
        return -1;
    }

    public int bsearch (long key)
    {
        int end = buffer.length;
        long result;
        int min_bracket=20 * 2 + 2 * dim + 1;

        while (end - lastPosition > min_bracket) {

            int middle = (lastPosition + buffer.length) / 2;
            result = read (middle);
            middle += size;
            result = read (middle);
            while (size == 1) {
                middle++;
                result = read (middle);
            }

            if (result > key) {
                end = middle;

            }
            else {
                if (result < key) {
                    lastPosition = middle + 1;

                }
                else {
                    lastPosition = middle + 1;
                    return middle;
                }

            }


        }

        while (lastPosition < end) {
            result = read (lastPosition);
            if (result == key) {
                lastPosition++;
                return lastPosition - 1;
            }
        }
        return -1;
    }



}
