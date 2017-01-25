package edu.iastate.geol.meteor.swat.analytics.service;


import java.math.BigInteger;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import edu.iastate.geol.meteor.swat.analytics.DAO.AnalyticsResultDAO;
import edu.iastate.geol.meteor.swat.analytics.DAOImpl.AnalyticsResultDAOImpl;
import edu.iastate.geol.meteor.swat.analytics.bean.OverallResult;
import edu.iastate.geol.meteor.swat.analytics.model.FilterForResult;

public class AnalyticsResultService {

	private FilterForResult filterForResult;
	public AnalyticsResultService(FilterForResult filterForResult){
		this.filterForResult = filterForResult;
	}
	
	public AnalyticsResultService(){
		
	}
	
	public boolean userHasResultAccess(String userEmail) throws SQLException{
		AnalyticsResultDAO resultDAO = new AnalyticsResultDAOImpl();
		resultDAO.setDataSource();
		
		boolean userHasAccess = resultDAO.userHasResultAccess(userEmail);
		return userHasAccess;
		
	}
	
	public List<OverallResult> getOverallResult() throws SQLException{
		AnalyticsResultDAO resultDAO = new AnalyticsResultDAOImpl();
		resultDAO.setDataSource();
		List<OverallResult> list = resultDAO.fetchAllResults();
		return list;
	}
	
	public List<OverallResult> fetchResultsWithState(String state)throws SQLException{
		AnalyticsResultDAO resultDAO = new AnalyticsResultDAOImpl();
		resultDAO.setDataSource();
		List<OverallResult> list = resultDAO.fetchResultsWithState(state);
		return list;
	}
	
	public List<OverallResult> fetchResultsWithDates(String from, String to)throws SQLException{
		AnalyticsResultDAO resultDAO = new AnalyticsResultDAOImpl();
		resultDAO.setDataSource();
		List<OverallResult> list = resultDAO.fetchResultsWithDates(from, to);
		return list;
	}
	
	public List<OverallResult> fetchResultsWithMonth(String month)throws SQLException{
		AnalyticsResultDAO resultDAO = new AnalyticsResultDAOImpl();
		resultDAO.setDataSource();
		List<OverallResult> list = resultDAO.fetchResultsWithMonth(month);
		return list;
	}
	
	
	
	public List<OverallResult> fetchResultsWithDateState(String from, String to, String state)throws SQLException{
		AnalyticsResultDAO resultDAO = new AnalyticsResultDAOImpl();
		resultDAO.setDataSource();
		List<OverallResult> list = resultDAO.fetchResultsWithDateState(from, to, state);
		return list;
	}
	

	public List<OverallResult> fetchResultsWithDateMonth(String from, String to, String month)throws SQLException{
		AnalyticsResultDAO resultDAO = new AnalyticsResultDAOImpl();
		resultDAO.setDataSource();
		List<OverallResult> list = resultDAO.fetchResultsWithDateMonth(from , to , month);
		return list;
	}
	
	
	public List<OverallResult> fetchResultsWithStateMonth(String state,String month)throws SQLException{
		AnalyticsResultDAO resultDAO = new AnalyticsResultDAOImpl();
		resultDAO.setDataSource();
		List<OverallResult> list = resultDAO.fetchResultsWithStateMonth(state,month);
		return list;
	}
	
	
	public List<OverallResult> fetchResultsWithDateMonthState(String from, String to,String month,String state)throws SQLException{
		AnalyticsResultDAO resultDAO = new AnalyticsResultDAOImpl();
		resultDAO.setDataSource();
		List<OverallResult> list = resultDAO.fetchResultsWithDateMonthState(from, to, month, state);
		return list;
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	@Deprecated
	public HashMap<String,BigInteger> fetchFilteredResult(){
		
		AnalyticsResultDAO resultDAO = new AnalyticsResultDAOImpl();
		HashMap<String, BigInteger> results = resultDAO.filteredSelect(filterForResult);
		return results;
		
	}
	
	@Deprecated
	public List<Map<String,Object>> fetchAllResultsForHail()throws SQLException{
		AnalyticsResultDAO resultDAO = new AnalyticsResultDAOImpl();
		resultDAO.setDataSource();
		List<Map<String,Object>> hailList = resultDAO.fetchAllResultsForHail();
		return hailList;
	}
	
	@Deprecated
	public List<Map<String,Object>> fetchAllResultsForThunderStormWind()throws SQLException{
		AnalyticsResultDAO resultDAO = new AnalyticsResultDAOImpl();
		resultDAO.setDataSource();
		List<Map<String,Object>> thunderstormWindList = resultDAO.fetchAllResultsForThunderStormWind();
		return thunderstormWindList;
		
	}
	
	@Deprecated
	public List<Map<String,Object>> fetchAllResultsForFlashflood()throws SQLException{
		AnalyticsResultDAO resultDAO = new AnalyticsResultDAOImpl();
		resultDAO.setDataSource();
		List<Map<String,Object>> flashfloodList = resultDAO.fetchAllResultsForFlashflood();
		return flashfloodList;
		
	}
	
	@Deprecated
	public List<Map<String,Object>> fetchAllResultsForTornado()throws SQLException{
		AnalyticsResultDAO resultDAO = new AnalyticsResultDAOImpl();
		resultDAO.setDataSource();
		List<Map<String,Object>> tornadoList = resultDAO.fetchAllResultsForTornado();
		return tornadoList;
		
	}
	
	@Deprecated
	public List<OverallResult> fetchResultsWithClassification(String morphology)throws SQLException{
		AnalyticsResultDAO resultDAO = new AnalyticsResultDAOImpl();
		resultDAO.setDataSource();
		List<OverallResult> list = resultDAO.fetchAllResultsWithClassification(morphology);
		return list;
	}
	
	@Deprecated
	public List<OverallResult> fetchResultsWithClassificationAndState(String morphology, String state)throws SQLException{
		AnalyticsResultDAO resultDAO = new AnalyticsResultDAOImpl();
		resultDAO.setDataSource();
		List<OverallResult> list = resultDAO.fetchResultsWithClassificationAndState(morphology,state);
		return list;
	}
	
	
}
