package perLucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;

import com.spatial4j.core.context.jts.JtsSpatialContext;

import com.spatial4j.core.io.JtsWKTShapeParser;
import com.spatial4j.core.shape.Shape;

//this is not thread-safe

class Spatial {

	protected JtsSpatialContext ctx;
	protected RecursivePrefixTreeStrategy strategy;
	protected JtsWKTShapeParser wktParser;

	Spatial() {
		this.ctx = new JtsSpatialContext(true);
		int maxLevels = 49; // sub-meter precision
		double distErrPct = 0.025;

		SpatialPrefixTree grid = new QuadPrefixTree(ctx, maxLevels);

		strategy = new RecursivePrefixTreeStrategy(grid, "location"); // only
		// one
		// field
		// required
		// //many
		// documents
		// for
		// diff
		// locations
		wktParser = new JtsWKTShapeParser(ctx);
		strategy.setDistErrPct(distErrPct);
	}

	public boolean addFields(Document doc, String wkt) {
		try {
			Shape shape = wktParser.parse(wkt);

			for (Field f : strategy.createIndexableFields(shape)) {
				doc.add(f);
				return true;
			}
		} catch (Exception e) {
			return false;
		}
		return false;
	}

	public Query query(String wkt) {
		try {
			Shape shape = wktParser.parse(wkt);

			return strategy.makeQuery(new SpatialArgs(
					SpatialOperation.Intersects, shape));

		} catch (Exception e) {
			return null;
		}
	}

}
