package edu.iastate.geol.meteor.swat.analytics.DAOImpl;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

import edu.iastate.geol.meteor.swat.analytics.DAO.AnalyticsDAO;
import edu.iastate.geol.meteor.swat.analytics.bean.AnalyticsClassificationBean;
import edu.iastate.geol.meteor.swat.analytics.bean.AnalyticsReportBean;
import edu.iastate.geol.meteor.swat.analytics.model.AnalyticsFilter;
import edu.iastate.geol.meteor.swat.analytics.model.ExpertClassification;
import edu.iastate.geol.meteor.swat.analytics.rowmapper.AnalyticsClassificationBeanResultSetExtractor;
import edu.iastate.geol.meteor.swat.analytics.rowmapper.AnalyticsReportResultSetExtractor;
import edu.iastate.geol.meteor.swat.analytics.rowmapper.ExpertClassificationBeanResultSetExtractor;

public class AnalyticsDAOImpl implements AnalyticsDAO {
	
	private JdbcTemplate jdbcTemplate;	
	
	
	@Override
	public List<AnalyticsReportBean> getOverallAnalyticsReport() {
	
		String sql = "select "
				+ " t1.date_time, count(t1.date_time) as count,"
				+ "t2.region"
				+ " from "
				+ " ("
				+ "  select  * from unmodified_user_classification"
				+ "  where "
				+ " email_address is not null"
				+ " ) as t1"
				+ " left outer join"
				+ " ("
				+ " select * from information"
				+ " where email_address is not null"
				+ " ) as t2"
				+ " on t1.info_id = t2.info_id"
				+ " group by t1.date_time"
				+ " order by count(t1.date_time) desc, date_time"
				+ " limit 20";
				
		try{
		List<AnalyticsReportBean> list = jdbcTemplate.query(sql,new AnalyticsReportResultSetExtractor());
		return list;
		}catch(DataAccessException | NullPointerException e){
			return null;
		}
		
	}
	
	@Override
	public List<Map<String, Object>> getCountsOfDateTime(AnalyticsFilter analyticsFilter) {
	
		String sql = "select date_time, count(date_time) as count "
				+ " from unmodified_user_classification "
				+ " group by date_time"
				+ " order by count(date_time) desc, date_time";
				
		try{
		List<Map<String, Object>> list = jdbcTemplate.queryForList(sql, new Object[] {});
		return list;
		}catch(DataAccessException | NullPointerException e){
			return null;
		}
		
	}
	

	
	@Override
	public List<AnalyticsClassificationBean> getClassificationAndUserForGivenDate(String date,String region) {
	
		String sql = "select"
				+ " t1.classification, t1.email_address"
				+ " from"
				+ " ("
				+ " select classification, email_address, info_id from unmodified_user_classification"
				+ " where date_time = ? and email_address is not null"
				+ " ) as t1"
				+ " left outer join"
				+ " ("
				+ " select info_id from information where email_address is not null and region = ?"
				+ " ) as t2"
				+ " on t1.info_id = t2.info_id"
				+ " order by t1.email_address";
				
		try{
			List<AnalyticsClassificationBean> list = jdbcTemplate.query(sql,new Object[] {date,region},new AnalyticsClassificationBeanResultSetExtractor());
		return list;
		}catch(DataAccessException | NullPointerException e){
			return null;
		}
		
	}
	
	@Override
	public List<ExpertClassification> getExpertClassificationForGivenDateAndRegion(String date, String region) {
		
		String sql = "select classification, email_address from expert_classification"
				+ " where date_time = ? and region = ?";
		
		try{
			List<ExpertClassification> list = 
					jdbcTemplate.query(sql,new Object[] {date,region},new ExpertClassificationBeanResultSetExtractor());
		return list;
		}catch(DataAccessException | NullPointerException e){
			return null;
		}
	}
	
	
	
	@Override
	public boolean insertExpertClassification(AnalyticsClassificationBean analyticsClassificationBean) {
		String sql = "insert into unmodified_user_classification (date_time, classification, email_address)"
				+ "values (?,?,?)";
		try{
			jdbcTemplate.update(sql, analyticsClassificationBean.getDateTime(), analyticsClassificationBean.getClassification(),analyticsClassificationBean.getEmailAddress());
			return true;
			
		}catch(DataAccessException | NullPointerException e){
			e.printStackTrace();
			return false;
		}
		
	}
	
	
	@Override
	public boolean insertIntoExpertClassification(ExpertClassification expertClassification) {
		String sql = "insert into expert_classification (date_time, classification, email_address, region)"
				+ "values (?,?,?,?)";
		try{
			jdbcTemplate.update(sql, expertClassification.getDateTime(), expertClassification.getClassification(),
					expertClassification.getEmailAddress(), expertClassification.getRegion());
			return true;
			
		}catch(DataAccessException | NullPointerException e){
			e.printStackTrace();
			return false;
		}
		
	}
	
	@Override
	public void setDataSource() {
        try{
        	SimpleDriverDataSource dataSource = new SimpleDriverDataSource();
        	dataSource.setDriver(new com.mysql.jdbc.Driver());
        	dataSource.setUrl("jdbc:mysql://localhost:3306/swat");
        	dataSource.setUsername("root");
        	dataSource.setPassword("SwatTool@2015");
        	//dataSource.setPassword("Swat@2016");
        	this.jdbcTemplate = new JdbcTemplate(dataSource);
        	
        }catch(SQLException e){
        	e.printStackTrace();
        	this.jdbcTemplate = null;
        }
	}

}
