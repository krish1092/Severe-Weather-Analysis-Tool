package edu.iastate.geol.meteor.swat.analytics.rowmapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

import edu.iastate.geol.meteor.swat.analytics.bean.AnalyticsReportBean;


public class AnalyticsReportResultSetExtractor implements ResultSetExtractor<List<AnalyticsReportBean>>{
	
	
	@Override
	public List<AnalyticsReportBean> extractData(ResultSet rs) throws SQLException, DataAccessException {
		
		List<AnalyticsReportBean> list = new ArrayList<AnalyticsReportBean>();
		while(rs.next()){
		AnalyticsReportBean aRB = new AnalyticsReportBean();
		aRB.setDateTime(rs.getTimestamp("date_time"));
		aRB.setCount(rs.getLong("count"));
		aRB.setRegion(rs.getString("region"));
		list.add(aRB);
		}
		return list;
	}
	

}
