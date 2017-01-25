package edu.iastate.geol.meteor.swat.analytics.model;

public class AnalyticsFilter {
	
	private String dateTime;
	private boolean isExpert;
	public String getDateTime() {
		return dateTime;
	}
	public void setDateTime(String dateTime) {
		/*DateTimeZone.setDefault(DateTimeZone.UTC);
		DateTimeFormatter dtf = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm");
		DateTimeFormatter dtfOut = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
		DateTime dt = DateTime.parse(dateTime , dtf);*/
		this.dateTime = dateTime;//dtfOut.print(dt);
	}
	public boolean isExpert() {
		return isExpert;
	}
	public void setExpert(boolean isExpert) {
		this.isExpert = isExpert;
	}

	
	

	

}
