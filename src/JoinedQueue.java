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



import org.apache.lucene.util.PriorityQueue;

final class JoinedQueue extends PriorityQueue < JoinedDoc >
{

  /**
   * Creates a new instance with <code>size</code> elements. If
   * <code>prePopulate</code> is set to true, the queue will pre-populate itself
   * with sentinel objects and set its {@link #size()} to <code>size</code>. In
   * that case, you should not rely on {@link #size()} to get the number of
   * actual elements that were added to the queue, but keep track yourself.<br>
   * <b>NOTE:</b> in case <code>prePopulate</code> is true, you should pop
   * elements from the queue using the following code example:
   * 
   * <pre class="prettyprint">
   * PriorityQueue&lt;ScoreDoc&gt; pq = new HitQueue(10, true); // pre-populate.
   * ScoreDoc top = pq.top();
   * 
   * // Add/Update one element.
   * top.score = 1.0f;
   * top.doc = 0;
   * top = (ScoreDoc) pq.updateTop();
   * int totalHits = 1;
   * 
   * // Now pop only the elements that were *truly* inserted.
   * // First, pop all the sentinel elements (there are pq.size() - totalHits).
   * for (int i = pq.size() - totalHits; i &gt; 0; i--) pq.pop();
   * 
   * // Now pop the truly added elements.
   * ScoreDoc[] results = new ScoreDoc[totalHits];
   * for (int i = totalHits - 1; i &gt;= 0; i--) {
   *   results[i] = (ScoreDoc) pq.pop();
   * }
   * </pre>
   * 
   * <p><b>NOTE</b>: This class pre-allocate a full array of
   * length <code>size</code>.
   * 
   * @param size
   *          the requested size of this queue.
   * @param prePopulate
   *          specifies whether to pre-populate the queue with sentinel values.
   * @see #getSentinelObject()
   */


  JoinedQueue (int size, boolean prePopulate)
  {
    super (size, prePopulate);
  }


   @Override protected JoinedDoc getSentinelObject ()
  {
    // Always set the doc Id to MAX_VALUE so that it won't be favored by
    // lessThan. This generally should not happen since if score is not NEG_INF,
    // TopScoreDocCollector will always add the object to the queue.
    return new JoinedDoc (Integer.MAX_VALUE, Float.NEGATIVE_INFINITY, -1);
  }

  @Override protected final boolean lessThan (JoinedDoc hitA, JoinedDoc hitB)
  {
    if (hitA.score == hitB.score)
      return hitA.doc > hitB.doc;
    else
      return hitA.score < hitB.score;
  }


}
