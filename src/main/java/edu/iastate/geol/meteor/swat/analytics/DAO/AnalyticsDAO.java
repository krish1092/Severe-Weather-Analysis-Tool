package edu.iastate.geol.meteor.swat.analytics.DAO;

import java.util.List;
import java.util.Map;

import edu.iastate.geol.meteor.swat.analytics.bean.AnalyticsClassificationBean;
import edu.iastate.geol.meteor.swat.analytics.bean.AnalyticsReportBean;
import edu.iastate.geol.meteor.swat.analytics.model.AnalyticsFilter;
import edu.iastate.geol.meteor.swat.analytics.model.ExpertClassification;

public interface AnalyticsDAO {
	
	public void setDataSource();
	
	public List<Map<String, Object>> getCountsOfDateTime(AnalyticsFilter analyticsFilter);
	
	public boolean insertExpertClassification(AnalyticsClassificationBean analyticsClassificationBean);

	public List<AnalyticsReportBean> getOverallAnalyticsReport();
	
	public List<AnalyticsClassificationBean> getClassificationAndUserForGivenDate(String date,String region);
	
	public List<ExpertClassification> getExpertClassificationForGivenDateAndRegion(String dateTime, String region);
	
	public boolean insertIntoExpertClassification(ExpertClassification expertClassification);
}
