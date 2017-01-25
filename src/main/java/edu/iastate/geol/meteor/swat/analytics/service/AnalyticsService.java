package edu.iastate.geol.meteor.swat.analytics.service;

import java.util.List;
import java.util.Map;

import edu.iastate.geol.meteor.swat.analytics.DAO.AnalyticsDAO;
import edu.iastate.geol.meteor.swat.analytics.DAOImpl.AnalyticsDAOImpl;
import edu.iastate.geol.meteor.swat.analytics.bean.AnalyticsClassificationBean;
import edu.iastate.geol.meteor.swat.analytics.bean.AnalyticsReportBean;
import edu.iastate.geol.meteor.swat.analytics.model.AnalyticsFilter;
import edu.iastate.geol.meteor.swat.analytics.model.ExpertClassification;

public class AnalyticsService {
	
	public List<Map<String,Object>> getAnalyticsReport(AnalyticsFilter analyticsFilter){
		
		AnalyticsDAO analyticsDAO = new AnalyticsDAOImpl();
		analyticsDAO.setDataSource();
		List<Map<String,Object>> list = analyticsDAO.getCountsOfDateTime(analyticsFilter);
		return list;
	}
	
	public boolean insertExpertClassification (ExpertClassification expertClassification){
		AnalyticsDAO analyticsDAO = new AnalyticsDAOImpl();
		analyticsDAO.setDataSource();
		boolean inserted = analyticsDAO.insertIntoExpertClassification(expertClassification);
		return inserted;
	}
	
	public List<AnalyticsReportBean> getOverallAnalyticsReport (){
		AnalyticsDAO analyticsDAO = new AnalyticsDAOImpl();
		analyticsDAO.setDataSource();
		List<AnalyticsReportBean> list = analyticsDAO.getOverallAnalyticsReport();
		return list;
	}
	
	public List<AnalyticsClassificationBean> getClassificationAndUserForGivenDate(String date,String region){
		AnalyticsDAO analyticsDAO = new AnalyticsDAOImpl();
		analyticsDAO.setDataSource();
		List<AnalyticsClassificationBean> list = analyticsDAO.getClassificationAndUserForGivenDate(date,region);
		return list;
	}
	
	public List<ExpertClassification> getExpertClassification(String date,String region){
		AnalyticsDAO analyticsDAO = new AnalyticsDAOImpl();
		analyticsDAO.setDataSource();
		List<ExpertClassification> list = analyticsDAO.getExpertClassificationForGivenDateAndRegion(date, region);
		return list;
	}
	
}
