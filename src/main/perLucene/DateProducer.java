package perLucene;

import java.util.Calendar;
import java.util.TimeZone;

public class DateProducer {

	protected static Calendar calendar = Calendar.getInstance(TimeZone
			.getTimeZone("UTC"));

	public static long date() {
		return calendar.get(Calendar.YEAR) * 100000000
				+ calendar.get(Calendar.MONTH) * 1000000
				+ calendar.get(Calendar.DAY_OF_MONTH) * 10000
				+ calendar.get(Calendar.HOUR) * 100
				+ calendar.get(Calendar.MINUTE);
	}
}
