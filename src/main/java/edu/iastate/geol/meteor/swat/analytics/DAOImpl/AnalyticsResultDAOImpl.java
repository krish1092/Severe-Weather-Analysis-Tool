package edu.iastate.geol.meteor.swat.analytics.DAOImpl;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

import edu.iastate.geol.meteor.swat.analytics.DAO.AnalyticsResultDAO;
import edu.iastate.geol.meteor.swat.analytics.bean.OverallResult;
import edu.iastate.geol.meteor.swat.analytics.model.FilterForResult;
import edu.iastate.geol.meteor.swat.analytics.rowmapper.OverallResultRowMapper;
import edu.iastate.geol.meteor.swat.analytics.rowmapper.OverallResultWithOutNullEventRowMapper;

public class AnalyticsResultDAOImpl implements AnalyticsResultDAO {

	private JdbcTemplate jdbcTemplate;
	
	@Override
	public void setDataSource() {

		SimpleDriverDataSource dataSource = new SimpleDriverDataSource();
        try{
        	dataSource.setDriver(new com.mysql.jdbc.Driver());
        }
        catch(Exception e){
        	e.printStackTrace();
        }
        dataSource.setUrl("jdbc:mysql://localhost:3306/swat");
        dataSource.setUsername("root");
        dataSource.setPassword("SwatTool@2015");
        //dataSource.setPassword("Swat@2016");
        this.jdbcTemplate = new JdbcTemplate(dataSource);
	

	}

	
	//Restricitng User access to results
	@Override
	public boolean userHasResultAccess(String userEmail) {
		
		String sql = "select exists(select 1 from result_access where email_address = ? limit 1)";
		int userExistsInDB = jdbcTemplate.queryForObject(sql,Integer.class, userEmail);
		if(userExistsInDB == 1) return true;
		return false;
	}
	
	
	//All results
	
	@Override
	public List<OverallResult> fetchAllResults() {
		
		String sql = "select t1_morphology.classification, t1_morphology.classification_count,"
				+ "coalesce(t2_hail.hail_below_one,0) as 'hail < 1',"
				+ "coalesce(t2_hail.hail_above_one,0) as '1 <= hail < 2', "
				+ "coalesce(t2_hail.hail_above_two,0) as 'hail >= 2',"
				+ "coalesce(t3_thunderstorm_wind.thunderstorm_wind_below_65,0) as 'thunderstorm_wind < 65',"
				+ "coalesce(t3_thunderstorm_wind.thunderstorm_wind_above_65,0) as 'thunderstorm_wind >= 65',"
				+ "coalesce(t4_flashflood.flashflood_count, 0) as flashflood_count,"
				+ "coalesce(t5_tornado.EF0, 0) as EF0,"
				+ "coalesce(t5_tornado.EF1, 0) as EF1,"
				+ "coalesce(t5_tornado.EF2, 0) as EF2,"
				+ "coalesce(t5_tornado.EF3, 0) as EF3,"
				+ "coalesce(t5_tornado.EF4, 0) as EF4,"
				+ "coalesce(t5_tornado.EF5, 0) as EF5,"
				+ "coalesce(t6_null_events.null_classification_count ,0) as null_classification_count"
				+ " from "
				+ "	("
				+ " select count(classification.classification) as classification_count,classification.classification from classification group by classification) t1_morphology"
				+ " left outer join"
				+ "("
				+ "select "
				+ " classification.classification,"
				+ "count(case when hail.magnitude < 1.0 then hail.magnitude end) as hail_below_one,"
				+ "count(case when (hail.magnitude >= 1.0 AND hail.magnitude < 2.0) then hail.magnitude end) as hail_above_one,"
				+ "count(case when hail.magnitude >= 2 then hail.magnitude end) as hail_above_two"
				+ " from classification,hail"
				+ " where hail.classification_id = classification.classification_id"
				+ " group by classification.classification"
				+ ") t2_hail"
				+ " on t2_hail.classification = t1_morphology.classification"
				+ " left outer join"
				+ " ("
				+ " select"
				+ " classification.classification,"
				+ "count(case when thunderstorm.magnitude < 65 then thunderstorm.magnitude end) as thunderstorm_wind_below_65,"
				+ "count(case when thunderstorm.magnitude >= 65 then thunderstorm.magnitude end) as thunderstorm_wind_above_65"
				+ " from thunderstorm,classification where thunderstorm.classification_id = classification. classification_id"
				+ " group by classification.classification"
				+ ") t3_thunderstorm_wind"
				+ " on t3_thunderstorm_wind.classification = t1_morphology.classification left outer join"
				+ "("
				+ "select "
				+ " classification.classification,"
				+ " count(flashflood_id) as flashflood_count"
				+ " from flashflood,classification where flashflood.classification_id = classification.classification_id"
				+ " group by classification.classification ) t4_flashflood"
				+ " on t4_flashflood.classification = t1_morphology.classification left outer join"
				+ "(select "
				+ "classification.classification,"
				+ "count(case when magnitude = 'EF0' then magnitude end) as EF0,"
				+ "count(case when magnitude = 'EF1' then magnitude end) as EF1,"
				+ "count(case when magnitude = 'EF2' then magnitude end) as EF2,"
				+ "count(case when magnitude = 'EF3' then magnitude end) as EF3,"
				+ "count(case when magnitude = 'EF4' then magnitude end) as EF4,"
				+ "count(case when magnitude = 'EF5' then magnitude end) as EF5"
				+ " from tornado,classification"
				+ " where tornado.classification_id = classification.classification_id"
				+ " group by classification.classification"
				+ " ) t5_tornado"
				+ " on t5_tornado.classification = t1_morphology.classification"
				+ " left outer join"
				+ " ("
				+ " select"
				+ " classification.classification,"
				+ " count(classification.classification) as null_classification_count"
				+ " from classification "
				+ " where "
				+ " classification.classification_id not in (select classification_id from hail) "
				+ " and "
				+ " classification.classification_id not in (select classification_id from tornado) "
				+ " and"
				+ " classification.classification_id not in (select classification_id from thunderstorm) "
				+ " and "
				+ " classification.classification_id not in (select classification_id from flashflood) "
				+ " group by classification.classification"
				+ ") t6_null_events"
				+ " on t6_null_events.classification = t1_morphology.classification";
		
		
		List<OverallResult> list = jdbcTemplate.query(sql,new OverallResultRowMapper());
		
		return list;
		
	}
	
	
	//State Filter
	@Override
	public List<OverallResult> fetchResultsWithState(String state) {
		
		String sql = "select t1_morphology.classification, t1_morphology.classification_count,"
				+ "coalesce(t2_hail.hail_below_one,0) as 'hail < 1',"
				+ "coalesce(t2_hail.hail_above_one,0) as '1 <= hail < 2', "
				+ "coalesce(t2_hail.hail_above_two,0) as 'hail >= 2',"
				+ "coalesce(t3_thunderstorm_wind.thunderstorm_wind_below_65,0) as 'thunderstorm_wind < 65',"
				+ "coalesce(t3_thunderstorm_wind.thunderstorm_wind_above_65,0) as 'thunderstorm_wind >= 65',"
				+ "coalesce(t4_flashflood.flashflood_count, 0) as flashflood_count,"
				+ "coalesce(t5_tornado.EF0, 0) as EF0,"
				+ "coalesce(t5_tornado.EF1, 0) as EF1,"
				+ "coalesce(t5_tornado.EF2, 0) as EF2,"
				+ "coalesce(t5_tornado.EF3, 0) as EF3,"
				+ "coalesce(t5_tornado.EF4, 0) as EF4,"
				+ "coalesce(t5_tornado.EF5, 0) as EF5 "
				+ " from "
				+ " ("
				+ " select count(classification.classification) as classification_count, classification.classification from classification"
				+ " where (classification.classification_id in (select hail.classification_id from hail where hail.state = ? )"
				+ " or"
				+ " classification.classification_id in (select thunderstorm.classification_id from thunderstorm where thunderstorm.state = ?)"
				+ " or"
				+ " classification.classification_id in (select flashflood.classification_id from flashflood where flashflood.state = ?)"
				+ " or"
				+ " classification.classification_id in (select tornado.classification_id from tornado where tornado.state = ?) "
				+ " ) group by classification.classification"
				+ " ) t1_morphology"
				+ " left outer join"
				+ "("
				+ "select "
				+ " classification.classification,"
				+ "count(case when hail.magnitude < 1.0 then hail.magnitude end) as hail_below_one,"
				+ "count(case when (hail.magnitude >= 1.0 AND hail.magnitude < 2.0) then hail.magnitude end) as hail_above_one,"
				+ "count(case when hail.magnitude >= 2 then hail.magnitude end) as hail_above_two"
				+ " from classification,hail"
				+ " where hail.classification_id = classification.classification_id and hail.state = ? "
				+ " group by classification.classification"
				+ ") t2_hail"
				+ " on t2_hail.classification = t1_morphology.classification"
				+ " left outer join"
				+ " ("
				+ " select"
				+ " classification.classification,"
				+ "count(case when thunderstorm.magnitude < 65 then thunderstorm.magnitude end) as thunderstorm_wind_below_65,"
				+ "count(case when thunderstorm.magnitude >= 65 then thunderstorm.magnitude end) as thunderstorm_wind_above_65"
				+ " from thunderstorm,classification where thunderstorm.classification_id = classification. classification_id and thunderstorm.state = ? "
				+ " group by classification.classification"
				+ ") t3_thunderstorm_wind"
				+ " on t3_thunderstorm_wind.classification = t1_morphology.classification left outer join"
				+ "("
				+ "select "
				+ " classification.classification,"
				+ " count(flashflood_id) as flashflood_count"
				+ " from flashflood,classification where flashflood.classification_id = classification.classification_id and flashflood.state = ? "
				+ " group by classification.classification ) t4_flashflood"
				+ " on t4_flashflood.classification = t1_morphology.classification left outer join"
				+ "(select "
				+ "classification.classification,"
				+ "count(case when magnitude = 'EF0' then magnitude end) as EF0,"
				+ "count(case when magnitude = 'EF1' then magnitude end) as EF1,"
				+ "count(case when magnitude = 'EF2' then magnitude end) as EF2,"
				+ "count(case when magnitude = 'EF3' then magnitude end) as EF3,"
				+ "count(case when magnitude = 'EF4' then magnitude end) as EF4,"
				+ "count(case when magnitude = 'EF5' then magnitude end) as EF5"
				+ " from tornado,classification"
				+ " where tornado.classification_id = classification.classification_id and tornado.state = ? "
				+ " group by classification.classification"
				+ " ) t5_tornado"
				+ " on t5_tornado.classification = t1_morphology.classification";
		
		List<OverallResult> list = jdbcTemplate.query(sql, new Object[] {state,state,state,state,state,state,state,state} ,new OverallResultWithOutNullEventRowMapper());
		
		return list;
		
	}
	
	//Date and State filter
	@Override
	public List<OverallResult> fetchResultsWithDateState(String from, String to, String state) {
		
		String sql = "select t1_morphology.classification, t1_morphology.classification_count,"
				+ "coalesce(t2_hail.hail_below_one,0) as 'hail < 1',"
				+ "coalesce(t2_hail.hail_above_one,0) as '1 <= hail < 2', "
				+ "coalesce(t2_hail.hail_above_two,0) as 'hail >= 2',"
				+ "coalesce(t3_thunderstorm_wind.thunderstorm_wind_below_65,0) as 'thunderstorm_wind < 65',"
				+ "coalesce(t3_thunderstorm_wind.thunderstorm_wind_above_65,0) as 'thunderstorm_wind >= 65',"
				+ "coalesce(t4_flashflood.flashflood_count, 0) as flashflood_count,"
				+ "coalesce(t5_tornado.EF0, 0) as EF0,"
				+ "coalesce(t5_tornado.EF1, 0) as EF1,"
				+ "coalesce(t5_tornado.EF2, 0) as EF2,"
				+ "coalesce(t5_tornado.EF3, 0) as EF3,"
				+ "coalesce(t5_tornado.EF4, 0) as EF4,"
				+ "coalesce(t5_tornado.EF5, 0) as EF5 "
				+ " from "
				+ " ("
				+ " select count(classification.classification) as classification_count, classification.classification from classification"
				+ " where (classification.classification_id in (select hail.classification_id from hail where hail.state = ? )"
				+ " or"
				+ " classification.classification_id in (select thunderstorm.classification_id from thunderstorm where thunderstorm.state = ?)"
				+ " or"
				+ " classification.classification_id in (select flashflood.classification_id from flashflood where flashflood.state = ?)"
				+ " or"
				+ " classification.classification_id in (select tornado.classification_id from tornado where tornado.state = ?) "
				+ " ) "
				+ " and "
				+ " classification.info_id in"
				+ " ("
				+ "  select information.info_id from information where date(information.start_time) >= ? and date(information.end_time) <= ? "
				+ " ) group by classification.classification"
				+ " ) t1_morphology"
				+ " left outer join"
				+ "("
				+ "select "
				+ " classification.classification,"
				+ "count(case when hail.magnitude < 1.0 then hail.magnitude end) as hail_below_one,"
				+ "count(case when (hail.magnitude >= 1.0 AND hail.magnitude < 2.0) then hail.magnitude end) as hail_above_one,"
				+ "count(case when hail.magnitude >= 2 then hail.magnitude end) as hail_above_two"
				+ " from classification,hail"
				+ " where hail.classification_id = classification.classification_id and hail.state = ? "
				+ " group by classification.classification"
				+ ") t2_hail"
				+ " on t2_hail.classification = t1_morphology.classification"
				+ " left outer join"
				+ " ("
				+ " select"
				+ " classification.classification,"
				+ "count(case when thunderstorm.magnitude < 65 then thunderstorm.magnitude end) as thunderstorm_wind_below_65,"
				+ "count(case when thunderstorm.magnitude >= 65 then thunderstorm.magnitude end) as thunderstorm_wind_above_65"
				+ " from thunderstorm,classification where thunderstorm.classification_id = classification. classification_id and thunderstorm.state = ? "
				+ " group by classification.classification"
				+ ") t3_thunderstorm_wind"
				+ " on t3_thunderstorm_wind.classification = t1_morphology.classification left outer join"
				+ "("
				+ "select "
				+ " classification.classification,"
				+ " count(flashflood_id) as flashflood_count"
				+ " from flashflood,classification where flashflood.classification_id = classification.classification_id and flashflood.state = ? "
				+ " group by classification.classification ) t4_flashflood"
				+ " on t4_flashflood.classification = t1_morphology.classification left outer join"
				+ "(select "
				+ "classification.classification,"
				+ "count(case when magnitude = 'EF0' then magnitude end) as EF0,"
				+ "count(case when magnitude = 'EF1' then magnitude end) as EF1,"
				+ "count(case when magnitude = 'EF2' then magnitude end) as EF2,"
				+ "count(case when magnitude = 'EF3' then magnitude end) as EF3,"
				+ "count(case when magnitude = 'EF4' then magnitude end) as EF4,"
				+ "count(case when magnitude = 'EF5' then magnitude end) as EF5"
				+ " from tornado,classification"
				+ " where tornado.classification_id = classification.classification_id and tornado.state = ? "
				+ " group by classification.classification"
				+ " ) t5_tornado"
				+ " on t5_tornado.classification = t1_morphology.classification";
		
		List<OverallResult> list = jdbcTemplate.query(sql, new Object[] {state,state,state,state, from, to, state,state,state,state} ,new OverallResultWithOutNullEventRowMapper());
		
		return list;
		
	}
	
	//Month and State filters
	
	@Override
	public List<OverallResult> fetchResultsWithStateMonth(String state, String month) {
		
		String sql = "select t1_morphology.classification, t1_morphology.classification_count,"
				+ "coalesce(t2_hail.hail_below_one,0) as 'hail < 1',"
				+ "coalesce(t2_hail.hail_above_one,0) as '1 <= hail < 2', "
				+ "coalesce(t2_hail.hail_above_two,0) as 'hail >= 2',"
				+ "coalesce(t3_thunderstorm_wind.thunderstorm_wind_below_65,0) as 'thunderstorm_wind < 65',"
				+ "coalesce(t3_thunderstorm_wind.thunderstorm_wind_above_65,0) as 'thunderstorm_wind >= 65',"
				+ "coalesce(t4_flashflood.flashflood_count, 0) as flashflood_count,"
				+ "coalesce(t5_tornado.EF0, 0) as EF0,"
				+ "coalesce(t5_tornado.EF1, 0) as EF1,"
				+ "coalesce(t5_tornado.EF2, 0) as EF2,"
				+ "coalesce(t5_tornado.EF3, 0) as EF3,"
				+ "coalesce(t5_tornado.EF4, 0) as EF4,"
				+ "coalesce(t5_tornado.EF5, 0) as EF5 "
				+ " from "
				+ " ("
				+ " select count(classification.classification) as classification_count, classification.classification from classification"
				+ " where (classification.classification_id in (select hail.classification_id from hail where hail.state = ? )"
				+ " or"
				+ " classification.classification_id in (select thunderstorm.classification_id from thunderstorm where thunderstorm.state = ?)"
				+ " or"
				+ " classification.classification_id in (select flashflood.classification_id from flashflood where flashflood.state = ?)"
				+ " or"
				+ " classification.classification_id in (select tornado.classification_id from tornado where tornado.state = ?) "
				+ " ) "
				+ " and "
				+ " classification.info_id in"
				+ " ("
				+ "  select information.info_id from information where monthname(information.start_time) = ? "
				+ " ) group by classification.classification"
				+ " ) t1_morphology"
				+ " left outer join"
				+ "("
				+ "select "
				+ " classification.classification,"
				+ "count(case when hail.magnitude < 1.0 then hail.magnitude end) as hail_below_one,"
				+ "count(case when (hail.magnitude >= 1.0 AND hail.magnitude < 2.0) then hail.magnitude end) as hail_above_one,"
				+ "count(case when hail.magnitude >= 2 then hail.magnitude end) as hail_above_two"
				+ " from classification,hail"
				+ " where hail.classification_id = classification.classification_id and hail.state = ? "
				+ " group by classification.classification"
				+ ") t2_hail"
				+ " on t2_hail.classification = t1_morphology.classification"
				+ " left outer join"
				+ " ("
				+ " select"
				+ " classification.classification,"
				+ "count(case when thunderstorm.magnitude < 65 then thunderstorm.magnitude end) as thunderstorm_wind_below_65,"
				+ "count(case when thunderstorm.magnitude >= 65 then thunderstorm.magnitude end) as thunderstorm_wind_above_65"
				+ " from thunderstorm,classification where thunderstorm.classification_id = classification. classification_id and thunderstorm.state = ? "
				+ " group by classification.classification"
				+ ") t3_thunderstorm_wind"
				+ " on t3_thunderstorm_wind.classification = t1_morphology.classification left outer join"
				+ "("
				+ "select "
				+ " classification.classification,"
				+ " count(flashflood_id) as flashflood_count"
				+ " from flashflood,classification where flashflood.classification_id = classification.classification_id and flashflood.state = ? "
				+ " group by classification.classification ) t4_flashflood"
				+ " on t4_flashflood.classification = t1_morphology.classification left outer join"
				+ "(select "
				+ "classification.classification,"
				+ "count(case when magnitude = 'EF0' then magnitude end) as EF0,"
				+ "count(case when magnitude = 'EF1' then magnitude end) as EF1,"
				+ "count(case when magnitude = 'EF2' then magnitude end) as EF2,"
				+ "count(case when magnitude = 'EF3' then magnitude end) as EF3,"
				+ "count(case when magnitude = 'EF4' then magnitude end) as EF4,"
				+ "count(case when magnitude = 'EF5' then magnitude end) as EF5"
				+ " from tornado,classification"
				+ " where tornado.classification_id = classification.classification_id and tornado.state = ? "
				+ " group by classification.classification"
				+ " ) t5_tornado"
				+ " on t5_tornado.classification = t1_morphology.classification";
		
		List<OverallResult> list = jdbcTemplate.query(sql, new Object[] {state,state,state,state,month,state,state,state,state} ,new OverallResultWithOutNullEventRowMapper());
		
		return list;
		
	}
	
	
	//Date, Month and State filters
	
		@Override
		public List<OverallResult> fetchResultsWithDateMonthState(String from, String to, String month, String state) {
			
			String sql = "select t1_morphology.classification, t1_morphology.classification_count,"
					+ "coalesce(t2_hail.hail_below_one,0) as 'hail < 1',"
					+ "coalesce(t2_hail.hail_above_one,0) as '1 <= hail < 2', "
					+ "coalesce(t2_hail.hail_above_two,0) as 'hail >= 2',"
					+ "coalesce(t3_thunderstorm_wind.thunderstorm_wind_below_65,0) as 'thunderstorm_wind < 65',"
					+ "coalesce(t3_thunderstorm_wind.thunderstorm_wind_above_65,0) as 'thunderstorm_wind >= 65',"
					+ "coalesce(t4_flashflood.flashflood_count, 0) as flashflood_count,"
					+ "coalesce(t5_tornado.EF0, 0) as EF0,"
					+ "coalesce(t5_tornado.EF1, 0) as EF1,"
					+ "coalesce(t5_tornado.EF2, 0) as EF2,"
					+ "coalesce(t5_tornado.EF3, 0) as EF3,"
					+ "coalesce(t5_tornado.EF4, 0) as EF4,"
					+ "coalesce(t5_tornado.EF5, 0) as EF5 "
					+ " from "
					+ " ("
					+ " select count(classification.classification) as classification_count, classification.classification from classification"
					+ " where (classification.classification_id in (select hail.classification_id from hail where hail.state = ? )"
					+ " or"
					+ " classification.classification_id in (select thunderstorm.classification_id from thunderstorm where thunderstorm.state = ?)"
					+ " or"
					+ " classification.classification_id in (select flashflood.classification_id from flashflood where flashflood.state = ?)"
					+ " or"
					+ " classification.classification_id in (select tornado.classification_id from tornado where tornado.state = ?) "
					+ " ) "
					+ " and "
					+ " classification.info_id in"
					+ " ("
					+ "  select information.info_id from information where date(information.start_time) >= ? and date(information.end_time) <= ? and monthname(information.start_time) = ?"
					+ " ) group by classification.classification"
					+ " ) t1_morphology"
					+ " left outer join"
					+ "("
					+ "select "
					+ " classification.classification,"
					+ "count(case when hail.magnitude < 1.0 then hail.magnitude end) as hail_below_one,"
					+ "count(case when (hail.magnitude >= 1.0 AND hail.magnitude < 2.0) then hail.magnitude end) as hail_above_one,"
					+ "count(case when hail.magnitude >= 2 then hail.magnitude end) as hail_above_two"
					+ " from classification,hail"
					+ " where hail.classification_id = classification.classification_id and hail.state = ? "
					+ " group by classification.classification"
					+ ") t2_hail"
					+ " on t2_hail.classification = t1_morphology.classification"
					+ " left outer join"
					+ " ("
					+ " select"
					+ " classification.classification,"
					+ "count(case when thunderstorm.magnitude < 65 then thunderstorm.magnitude end) as thunderstorm_wind_below_65,"
					+ "count(case when thunderstorm.magnitude >= 65 then thunderstorm.magnitude end) as thunderstorm_wind_above_65"
					+ " from thunderstorm,classification where thunderstorm.classification_id = classification. classification_id and thunderstorm.state = ? "
					+ " group by classification.classification"
					+ ") t3_thunderstorm_wind"
					+ " on t3_thunderstorm_wind.classification = t1_morphology.classification left outer join"
					+ "("
					+ "select "
					+ " classification.classification,"
					+ " count(flashflood_id) as flashflood_count"
					+ " from flashflood,classification where flashflood.classification_id = classification.classification_id and flashflood.state = ? "
					+ " group by classification.classification ) t4_flashflood"
					+ " on t4_flashflood.classification = t1_morphology.classification left outer join"
					+ "(select "
					+ "classification.classification,"
					+ "count(case when magnitude = 'EF0' then magnitude end) as EF0,"
					+ "count(case when magnitude = 'EF1' then magnitude end) as EF1,"
					+ "count(case when magnitude = 'EF2' then magnitude end) as EF2,"
					+ "count(case when magnitude = 'EF3' then magnitude end) as EF3,"
					+ "count(case when magnitude = 'EF4' then magnitude end) as EF4,"
					+ "count(case when magnitude = 'EF5' then magnitude end) as EF5"
					+ " from tornado,classification"
					+ " where tornado.classification_id = classification.classification_id and tornado.state = ? "
					+ " group by classification.classification"
					+ " ) t5_tornado"
					+ " on t5_tornado.classification = t1_morphology.classification";
			
			List<OverallResult> list = jdbcTemplate.query(sql, new Object[] {state,state,state,state, from, to, month,state,state,state,state} ,new OverallResultWithOutNullEventRowMapper());
			
			return list;
			
		}
		
	//Date filters
	
	@Override
	public List<OverallResult> fetchResultsWithDates(String from, String to) {
		
		String sql = "select t1_morphology.classification, t1_morphology.classification_count,"
				+ "coalesce(t2_hail.hail_below_one,0) as 'hail < 1',"
				+ "coalesce(t2_hail.hail_above_one,0) as '1 <= hail < 2', "
				+ "coalesce(t2_hail.hail_above_two,0) as 'hail >= 2',"
				+ "coalesce(t3_thunderstorm_wind.thunderstorm_wind_below_65,0) as 'thunderstorm_wind < 65',"
				+ "coalesce(t3_thunderstorm_wind.thunderstorm_wind_above_65,0) as 'thunderstorm_wind >= 65',"
				+ "coalesce(t4_flashflood.flashflood_count, 0) as flashflood_count,"
				+ "coalesce(t5_tornado.EF0, 0) as EF0,"
				+ "coalesce(t5_tornado.EF1, 0) as EF1,"
				+ "coalesce(t5_tornado.EF2, 0) as EF2,"
				+ "coalesce(t5_tornado.EF3, 0) as EF3,"
				+ "coalesce(t5_tornado.EF4, 0) as EF4,"
				+ "coalesce(t5_tornado.EF5, 0) as EF5,"
				+ "coalesce(t6_null_events.null_classification_count ,0) as null_classification_count"
				+ " from "
				+ " ("
				+ " select count(classification.classification) as classification_count, classification.classification from classification"
				+ " where "
				+ " classification.info_id in"
				+ " ("
				+ "  select information.info_id from information where date(information.start_time) >= ? and date(information.end_time) <= ? "
				+ " ) group by classification.classification"
				+ " ) t1_morphology"
				+ " left outer join"
				+ "("
				+ "select "
				+ " classification.classification,"
				+ "count(case when hail.magnitude < 1.0 then hail.magnitude end) as hail_below_one,"
				+ "count(case when (hail.magnitude >= 1.0 AND hail.magnitude < 2.0) then hail.magnitude end) as hail_above_one,"
				+ "count(case when hail.magnitude >= 2 then hail.magnitude end) as hail_above_two"
				+ " from classification,hail"
				+ " where hail.classification_id = classification.classification_id  "
				+ " group by classification.classification"
				+ ") t2_hail"
				+ " on t2_hail.classification = t1_morphology.classification"
				+ " left outer join"
				+ " ("
				+ " select"
				+ " classification.classification,"
				+ "count(case when thunderstorm.magnitude < 65 then thunderstorm.magnitude end) as thunderstorm_wind_below_65,"
				+ "count(case when thunderstorm.magnitude >= 65 then thunderstorm.magnitude end) as thunderstorm_wind_above_65"
				+ " from thunderstorm,classification where thunderstorm.classification_id = classification. classification_id "
				+ " group by classification.classification"
				+ ") t3_thunderstorm_wind"
				+ " on t3_thunderstorm_wind.classification = t1_morphology.classification left outer join"
				+ "("
				+ "select "
				+ " classification.classification,"
				+ " count(flashflood_id) as flashflood_count"
				+ " from flashflood,classification where flashflood.classification_id = classification.classification_id "
				+ " group by classification.classification ) t4_flashflood"
				+ " on t4_flashflood.classification = t1_morphology.classification left outer join"
				+ "(select "
				+ "classification.classification,"
				+ "count(case when magnitude = 'EF0' then magnitude end) as EF0,"
				+ "count(case when magnitude = 'EF1' then magnitude end) as EF1,"
				+ "count(case when magnitude = 'EF2' then magnitude end) as EF2,"
				+ "count(case when magnitude = 'EF3' then magnitude end) as EF3,"
				+ "count(case when magnitude = 'EF4' then magnitude end) as EF4,"
				+ "count(case when magnitude = 'EF5' then magnitude end) as EF5"
				+ " from tornado,classification"
				+ " where tornado.classification_id = classification.classification_id  "
				+ " group by classification.classification"
				+ " ) t5_tornado"
				+ " on t5_tornado.classification = t1_morphology.classification"
				+ " left outer join"
				+ " ("
				+ " select"
				+ " classification.classification,"
				+ " count(classification.classification) as null_classification_count"
				+ " from classification "
				+ " where "
				+ " classification.classification_id not in (select classification_id from hail) "
				+ " and "
				+ " classification.classification_id not in (select classification_id from tornado) "
				+ " and"
				+ " classification.classification_id not in (select classification_id from thunderstorm) "
				+ " and "
				+ " classification.classification_id not in (select classification_id from flashflood) "
				+ " and"
				+ " classification.info_id in "
				+ " ("
				+ "  select information.info_id from information where date(information.start_time) >= ? and date(information.end_time) <= ?"
				+ " )"
				
				+ " group by classification.classification"
				+ ") t6_null_events"
				+ " on t6_null_events.classification = t1_morphology.classification";
		
		List<OverallResult> list = jdbcTemplate.query(sql, new Object[] {from, to, from, to} ,new OverallResultRowMapper());
		
		return list;
		
	}
	
	//Date and month filter 
	
	@Override
	public List<OverallResult> fetchResultsWithDateMonth(String from, String to, String month) {
		
		String sql = "select t1_morphology.classification, t1_morphology.classification_count,"
				+ "coalesce(t2_hail.hail_below_one,0) as 'hail < 1',"
				+ "coalesce(t2_hail.hail_above_one,0) as '1 <= hail < 2', "
				+ "coalesce(t2_hail.hail_above_two,0) as 'hail >= 2',"
				+ "coalesce(t3_thunderstorm_wind.thunderstorm_wind_below_65,0) as 'thunderstorm_wind < 65',"
				+ "coalesce(t3_thunderstorm_wind.thunderstorm_wind_above_65,0) as 'thunderstorm_wind >= 65',"
				+ "coalesce(t4_flashflood.flashflood_count, 0) as flashflood_count,"
				+ "coalesce(t5_tornado.EF0, 0) as EF0,"
				+ "coalesce(t5_tornado.EF1, 0) as EF1,"
				+ "coalesce(t5_tornado.EF2, 0) as EF2,"
				+ "coalesce(t5_tornado.EF3, 0) as EF3,"
				+ "coalesce(t5_tornado.EF4, 0) as EF4,"
				+ "coalesce(t5_tornado.EF5, 0) as EF5,"
				+ "coalesce(t6_null_events.null_classification_count ,0) as null_classification_count"
				+ " from "
				+ " ("
				+ " select count(classification.classification) as classification_count, classification.classification from classification"
				+ " where "
				+ " classification.info_id in"
				+ " ("
				+ "  select information.info_id from information where date(information.start_time) >= ? and date(information.end_time) <= ? and monthname(information.start_time) = ? "
				+ " ) group by classification.classification"
				+ " ) t1_morphology"
				+ " left outer join"
				+ "("
				+ "select "
				+ " classification.classification,"
				+ "count(case when hail.magnitude < 1.0 then hail.magnitude end) as hail_below_one,"
				+ "count(case when (hail.magnitude >= 1.0 AND hail.magnitude < 2.0) then hail.magnitude end) as hail_above_one,"
				+ "count(case when hail.magnitude >= 2 then hail.magnitude end) as hail_above_two"
				+ " from classification,hail"
				+ " where hail.classification_id = classification.classification_id  "
				+ " group by classification.classification"
				+ ") t2_hail"
				+ " on t2_hail.classification = t1_morphology.classification"
				+ " left outer join"
				+ " ("
				+ " select"
				+ " classification.classification,"
				+ "count(case when thunderstorm.magnitude < 65 then thunderstorm.magnitude end) as thunderstorm_wind_below_65,"
				+ "count(case when thunderstorm.magnitude >= 65 then thunderstorm.magnitude end) as thunderstorm_wind_above_65"
				+ " from thunderstorm,classification where thunderstorm.classification_id = classification. classification_id "
				+ " group by classification.classification"
				+ ") t3_thunderstorm_wind"
				+ " on t3_thunderstorm_wind.classification = t1_morphology.classification left outer join"
				+ "("
				+ "select "
				+ " classification.classification,"
				+ " count(flashflood_id) as flashflood_count"
				+ " from flashflood,classification where flashflood.classification_id = classification.classification_id "
				+ " group by classification.classification ) t4_flashflood"
				+ " on t4_flashflood.classification = t1_morphology.classification left outer join"
				+ "(select "
				+ "classification.classification,"
				+ "count(case when magnitude = 'EF0' then magnitude end) as EF0,"
				+ "count(case when magnitude = 'EF1' then magnitude end) as EF1,"
				+ "count(case when magnitude = 'EF2' then magnitude end) as EF2,"
				+ "count(case when magnitude = 'EF3' then magnitude end) as EF3,"
				+ "count(case when magnitude = 'EF4' then magnitude end) as EF4,"
				+ "count(case when magnitude = 'EF5' then magnitude end) as EF5"
				+ " from tornado,classification"
				+ " where tornado.classification_id = classification.classification_id  "
				+ " group by classification.classification"
				+ " ) t5_tornado"
				+ " on t5_tornado.classification = t1_morphology.classification"
				+ " left outer join"
				+ " ("
				+ " select"
				+ " classification.classification,"
				+ " count(classification.classification) as null_classification_count"
				+ " from classification "
				+ " where "
				+ " classification.classification_id not in (select classification_id from hail) "
				+ " and "
				+ " classification.classification_id not in (select classification_id from tornado) "
				+ " and"
				+ " classification.classification_id not in (select classification_id from thunderstorm) "
				+ " and "
				+ " classification.classification_id not in (select classification_id from flashflood) "
				+ " and"
				+ " classification.info_id in "
				+ " ("
				+ "  select information.info_id from information where date(information.start_time) >= ? and date(information.end_time) <= ? and monthname(information.start_time) = ? "
				+ " )"
				
				+ " group by classification.classification"
				+ ") t6_null_events"
				+ " on t6_null_events.classification = t1_morphology.classification";
		
		List<OverallResult> list = jdbcTemplate.query(sql, new Object[] {from, to, month, from, to, month} ,new OverallResultRowMapper());
		
		return list;
		
	}
	
	
	//Month filter
	
	@Override
	public List<OverallResult> fetchResultsWithMonth(String month) {
		
		String sql = "select t1_morphology.classification, t1_morphology.classification_count,"
				+ "coalesce(t2_hail.hail_below_one,0) as 'hail < 1',"
				+ "coalesce(t2_hail.hail_above_one,0) as '1 <= hail < 2', "
				+ "coalesce(t2_hail.hail_above_two,0) as 'hail >= 2',"
				+ "coalesce(t3_thunderstorm_wind.thunderstorm_wind_below_65,0) as 'thunderstorm_wind < 65',"
				+ "coalesce(t3_thunderstorm_wind.thunderstorm_wind_above_65,0) as 'thunderstorm_wind >= 65',"
				+ "coalesce(t4_flashflood.flashflood_count, 0) as flashflood_count,"
				+ "coalesce(t5_tornado.EF0, 0) as EF0,"
				+ "coalesce(t5_tornado.EF1, 0) as EF1,"
				+ "coalesce(t5_tornado.EF2, 0) as EF2,"
				+ "coalesce(t5_tornado.EF3, 0) as EF3,"
				+ "coalesce(t5_tornado.EF4, 0) as EF4,"
				+ "coalesce(t5_tornado.EF5, 0) as EF5,"
				+ "coalesce(t6_null_events.null_classification_count ,0) as null_classification_count"
				+ " from "
				+ " ("
				+ " select count(classification.classification) as classification_count, classification.classification from classification"
				+ " where "
				+ " classification.info_id in"
				+ " ("
				+ "  select information.info_id from information where monthname(information.start_time) = ? "
				+ " ) group by classification.classification"
				+ " ) t1_morphology"
				+ " left outer join"
				+ "("
				+ "select "
				+ " classification.classification,"
				+ "count(case when hail.magnitude < 1.0 then hail.magnitude end) as hail_below_one,"
				+ "count(case when (hail.magnitude >= 1.0 AND hail.magnitude < 2.0) then hail.magnitude end) as hail_above_one,"
				+ "count(case when hail.magnitude >= 2 then hail.magnitude end) as hail_above_two"
				+ " from classification,hail"
				+ " where hail.classification_id = classification.classification_id  "
				+ " group by classification.classification"
				+ ") t2_hail"
				+ " on t2_hail.classification = t1_morphology.classification"
				+ " left outer join"
				+ " ("
				+ " select"
				+ " classification.classification,"
				+ "count(case when thunderstorm.magnitude < 65 then thunderstorm.magnitude end) as thunderstorm_wind_below_65,"
				+ "count(case when thunderstorm.magnitude >= 65 then thunderstorm.magnitude end) as thunderstorm_wind_above_65"
				+ " from thunderstorm,classification where thunderstorm.classification_id = classification. classification_id "
				+ " group by classification.classification"
				+ ") t3_thunderstorm_wind"
				+ " on t3_thunderstorm_wind.classification = t1_morphology.classification left outer join"
				+ "("
				+ "select "
				+ " classification.classification,"
				+ " count(flashflood_id) as flashflood_count"
				+ " from flashflood,classification where flashflood.classification_id = classification.classification_id "
				+ " group by classification.classification ) t4_flashflood"
				+ " on t4_flashflood.classification = t1_morphology.classification left outer join"
				+ "(select "
				+ "classification.classification,"
				+ "count(case when magnitude = 'EF0' then magnitude end) as EF0,"
				+ "count(case when magnitude = 'EF1' then magnitude end) as EF1,"
				+ "count(case when magnitude = 'EF2' then magnitude end) as EF2,"
				+ "count(case when magnitude = 'EF3' then magnitude end) as EF3,"
				+ "count(case when magnitude = 'EF4' then magnitude end) as EF4,"
				+ "count(case when magnitude = 'EF5' then magnitude end) as EF5"
				+ " from tornado,classification"
				+ " where tornado.classification_id = classification.classification_id  "
				+ " group by classification.classification"
				+ " ) t5_tornado"
				+ " on t5_tornado.classification = t1_morphology.classification"
				+ " left outer join"
				+ " ("
				+ " select"
				+ " classification.classification,"
				+ " count(classification.classification) as null_classification_count"
				+ " from classification "
				+ " where "
				+ " classification.classification_id not in (select classification_id from hail) "
				+ " and "
				+ " classification.classification_id not in (select classification_id from tornado) "
				+ " and"
				+ " classification.classification_id not in (select classification_id from thunderstorm) "
				+ " and "
				+ " classification.classification_id not in (select classification_id from flashflood) "
				+ " and"
				+ " classification.info_id in "
				+ " ("
				+ "  select information.info_id from information where monthname(information.start_time) = ? "
				+ " )"
				
				+ " group by classification.classification"
				+ ") t6_null_events"
				+ " on t6_null_events.classification = t1_morphology.classification";
		
		List<OverallResult> list = jdbcTemplate.query(sql, new Object[] {month, month} ,new OverallResultRowMapper());
		
		return list;
		
	}
	
	
	
	
	
	
	@Override 
	public List<Map<String,Object>> fetchAllResultsForHail() {
		
		String sql = "select t2_morphology.classification, t2_morphology.classification_count, t1_hail.hail_below_one, t1_hail.hail_above_one, t1_hail.hail_above_two from"
				+ " ("
				+ "select"
				+ "		classification.classification,"
				+ "		count(case when hail.magnitude < 1.0 then hail.magnitude end) as hail_below_one,"
				+ "		count(case when (hail.magnitude >= 1.0 AND hail.magnitude < 2.0) then hail.magnitude end) as hail_above_one,"
				+ "		count(case when hail.magnitude >= 2 then hail.magnitude end) as hail_above_two"
				+ "		from classification,hail"
				+ "		where hail.classification_id = classification.classification_id"
				+ "		group by classification.classification"
				+ "	) t1_hail"
				+ " left join"
				+ "	(select count(classification.classification) as classification_count,classification.classification from classification group by classification) t2_morphology"
				+ " on t1_hail.classification = t2_morphology.classification ";
		
		List<Map<String,Object>> list = jdbcTemplate.queryForList(sql);
		
		return list;
		
	};
	
	@Override
	public List<Map<String, Object>> fetchAllResultsForFlashflood() {
		String sql = "select t2_morphology.classification, t2_morphology.classification_count, t1_hail.hail_below_one, t1_hail.hail_above_one, t1_hail.hail_above_two from"
				+ " ("
				+ "select"
				+ "		classification.classification,"
				+ "		count(case when hail.magnitude < 1.0 then hail.magnitude end) as hail_below_one,"
				+ "		count(case when (hail.magnitude >= 1.0 AND hail.magnitude < 2.0) then hail.magnitude end) as hail_above_one,"
				+ "		count(case when hail.magnitude >= 2 then hail.magnitude end) as hail_above_two"
				+ "		from classification,hail"
				+ "		where hail.classification_id = classification.classification_id"
				+ "		group by classification.classification"
				+ "	) t1_hail"
				+ "left join"
				+ "	(select count(classification.classification) as classification_count,classification.classification from classification group by classification) t2_morphology"
				+ "on t1_hail.classification = t2_morphology.classification ";
		
		List<Map<String,Object>> list = jdbcTemplate.queryForList(sql);
		
		return list;
	}
	
	@Override
	public List<Map<String, Object>> fetchAllResultsForThunderStormWind() {
		String sql = "select"
				+ " t2_morphology.classification, t2_morphology.classification_count,"
				+ "coalesce(t3_thunderstorm_wind.thunderstorm_wind_below_65,0) as thunderstorm_wind_below_65,"
				+ "coalesce(t3_thunderstorm_wind.thunderstorm_wind_above_65,0) as thunderstorm_wind_above_65"
				+ " from"
				+ " ("
				+ "	  select"
				+ " 	classification.classification,"
				+ "		count(case when thunderstorm.magnitude < 65 then thunderstorm.magnitude end) as thunderstorm_wind_below_65,"
				+ "		count(case when thunderstorm.magnitude >= 65 then thunderstorm.magnitude end) as thunderstorm_wind_above_65"
				+ "		from thunderstorm,classification"
				+ "		where thunderstorm.classification_id = classification. classification_id"
				+ "		group by classification.classification"
				+ " ) t3_thunderstorm_wind"
				+ " left join"
				+ "  (select count(classification.classification) as classification_count,classification.classification from classification group by classification) t2_morphology"
				+ " on t3_thunderstorm_wind.classification = t2_morphology.classification ";
		
		List<Map<String,Object>> list = jdbcTemplate.queryForList(sql);
		
		return list;
	}
	
	@Override
	public List<Map<String, Object>> fetchAllResultsForTornado() {
		String sql = "select t2_morphology.classification, t2_morphology.classification_count, t1_hail.hail_below_one, t1_hail.hail_above_one, t1_hail.hail_above_two from"
				+ "("
				+ "select"
				+ "		classification.classification,"
				+ "		count(case when hail.magnitude < 1.0 then hail.magnitude end) as hail_below_one,"
				+ "		count(case when (hail.magnitude >= 1.0 AND hail.magnitude < 2.0) then hail.magnitude end) as hail_above_one,"
				+ "		count(case when hail.magnitude >= 2 then hail.magnitude end) as hail_above_two"
				+ "		from classification,hail"
				+ "		where hail.classification_id = classification.classification_id"
				+ "		group by classification.classification"
				+ "	) t1_hail"
				+ "left join"
				+ "	(select count(classification.classification) as classification_count,classification.classification from classification group by classification) t2_morphology"
				+ "on t1_hail.classification = t2_morphology.classification ";
		
		List<Map<String,Object>> list = jdbcTemplate.queryForList(sql);
		
		return list;
	}

	@Override
	public HashMap<String, BigInteger> filteredSelect(FilterForResult filterForResult) {
		
		String sql = "select "
				+ "t2_morphology.classification, t2_morphology.classification_count, t1_hail.hail_below_one, t1_hail.hail_above_one, t1_hail.hail_above_two "
				+ "from"
				+ "("
				+ "select	"
				+ "classification.classification,"
				+ "count(case when hail.magnitude < 1.0 then hail.magnitude end) as hail_below_one,"
				+ "count(case when (hail.magnitude >= 1.0 AND hail.magnitude < 2.0) then hail.magnitude end) as hail_above_one,"
				+ "count(case when hail.magnitude >= 2 then hail.magnitude end) as hail_above_two"
				+ "from classification,hail"
				+ "where hail.classification_id = classification.classification_id and (hail.state = ? )"
				+ "and"
				+ "classification.info_id in ("
				+ "select information.info_id from information where information.region = 'cent_plains'"
				+ ")"
				+ "group by classification.classification) t1_hail"
				+ "left join"
				+ " (select count(classification.classification) as classification_count,classification.classification from classification group by classification) "
				+ "t2_morphology"
				+ "	on t1_hail.classification = t2_morphology.classification";
		
		 jdbcTemplate.queryForMap(sql);
		
		
		
		return null;
	}
	

	
	
	
	@Override
	public List<OverallResult> fetchAllResultsWithClassification(String morphology) {
		
		String sql = "select t1_morphology.classification, t1_morphology.classification_count,"
				+ "coalesce(t2_hail.hail_below_one,0) as 'hail < 1',"
				+ "coalesce(t2_hail.hail_above_one,0) as '1 <= hail < 2', "
				+ "coalesce(t2_hail.hail_above_two,0) as 'hail >= 2',"
				+ "coalesce(t3_thunderstorm_wind.thunderstorm_wind_below_65,0) as 'thunderstorm_wind < 65',"
				+ "coalesce(t3_thunderstorm_wind.thunderstorm_wind_above_65,0) as 'thunderstorm_wind >= 65',"
				+ "coalesce(t4_flashflood.flashflood_count, 0) as flashflood_count,"
				+ "coalesce(t5_tornado.EF0, 0) as EF0,"
				+ "coalesce(t5_tornado.EF1, 0) as EF1,"
				+ "coalesce(t5_tornado.EF2, 0) as EF2,"
				+ "coalesce(t5_tornado.EF3, 0) as EF3,"
				+ "coalesce(t5_tornado.EF4, 0) as EF4,"
				+ "coalesce(t5_tornado.EF5, 0) as EF5,"
				+ "coalesce(t6_null_events.null_classification_count ,0) as null_classification_count"
				+ " from "
				+ "	(select count(classification.classification) as classification_count,classification.classification from classification  where classification = ? group by classification) t1_morphology"
				+ " left outer join"
				+ "("
				+ "select "
				+ " classification.classification,"
				+ "count(case when hail.magnitude < 1.0 then hail.magnitude end) as hail_below_one,"
				+ "count(case when (hail.magnitude >= 1.0 AND hail.magnitude < 2.0) then hail.magnitude end) as hail_above_one,"
				+ "count(case when hail.magnitude >= 2 then hail.magnitude end) as hail_above_two"
				+ " from classification,hail"
				+ " where hail.classification_id = classification.classification_id"
				+ " group by classification.classification"
				+ ") t2_hail"
				+ " on t2_hail.classification = t1_morphology.classification"
				+ " left outer join"
				+ " ("
				+ " select"
				+ " classification.classification,"
				+ "count(case when thunderstorm.magnitude < 65 then thunderstorm.magnitude end) as thunderstorm_wind_below_65,"
				+ "count(case when thunderstorm.magnitude >= 65 then thunderstorm.magnitude end) as thunderstorm_wind_above_65"
				+ " from thunderstorm,classification where thunderstorm.classification_id = classification. classification_id"
				+ " group by classification.classification"
				+ ") t3_thunderstorm_wind"
				+ " on t3_thunderstorm_wind.classification = t1_morphology.classification left outer join"
				+ "("
				+ "select "
				+ " classification.classification,"
				+ " count(flashflood_id) as flashflood_count"
				+ " from flashflood,classification where flashflood.classification_id = classification.classification_id"
				+ " group by classification.classification ) t4_flashflood"
				+ " on t4_flashflood.classification = t1_morphology.classification left outer join"
				+ "(select "
				+ "classification.classification,"
				+ "count(case when magnitude = 'EF0' then magnitude end) as EF0,"
				+ "count(case when magnitude = 'EF1' then magnitude end) as EF1,"
				+ "count(case when magnitude = 'EF2' then magnitude end) as EF2,"
				+ "count(case when magnitude = 'EF3' then magnitude end) as EF3,"
				+ "count(case when magnitude = 'EF4' then magnitude end) as EF4,"
				+ "count(case when magnitude = 'EF5' then magnitude end) as EF5"
				+ " from tornado,classification"
				+ " where tornado.classification_id = classification.classification_id"
				+ " group by classification.classification"
				+ " ) t5_tornado"
				+ " on t5_tornado.classification = t1_morphology.classification"
				+ " left outer join"
				+ " ("
				+ " select"
				+ " classification.classification,"
				+ "count(classification.classification) as null_classification_count"
				+ " from classification,null_events"
				+ " where null_events.classification_id = classification.classification_id group by classification.classification"
				+ ") t6_null_events"
				+ " on t6_null_events.classification = t1_morphology.classification";
		
		List<OverallResult> list = jdbcTemplate.query(sql, new Object[] {morphology} ,new OverallResultRowMapper());
		
		return list;
		
	}
	
	
	@Override
	public List<OverallResult> fetchResultsWithClassificationAndState(String morphology, String state) {
		
		String sql = "select t1_morphology.classification, t1_morphology.classification_count,"
				+ "coalesce(t2_hail.hail_below_one,0) as 'hail < 1',"
				+ "coalesce(t2_hail.hail_above_one,0) as '1 <= hail < 2', "
				+ "coalesce(t2_hail.hail_above_two,0) as 'hail >= 2',"
				+ "coalesce(t3_thunderstorm_wind.thunderstorm_wind_below_65,0) as 'thunderstorm_wind < 65',"
				+ "coalesce(t3_thunderstorm_wind.thunderstorm_wind_above_65,0) as 'thunderstorm_wind >= 65',"
				+ "coalesce(t4_flashflood.flashflood_count, 0) as flashflood_count,"
				+ "coalesce(t5_tornado.EF0, 0) as EF0,"
				+ "coalesce(t5_tornado.EF1, 0) as EF1,"
				+ "coalesce(t5_tornado.EF2, 0) as EF2,"
				+ "coalesce(t5_tornado.EF3, 0) as EF3,"
				+ "coalesce(t5_tornado.EF4, 0) as EF4,"
				+ "coalesce(t5_tornado.EF5, 0) as EF5,"
				+ "coalesce(t6_null_events.null_classification_count ,0) as null_classification_count"
				+ " from "
				+ "	(select t1_morphology.classification, t1_morphology.classification_count,coalesce(t2_hail.hail_below_one,0) as 'hail &lt; 1',coalesce(t2_hail.hail_above_one,0) as '1 &lt;= hail &lt; 2', coalesce(t2_hail.hail_above_two,0) as 'hail &gt;= 2',coalesce(t3_thunderstorm_wind.thunderstorm_wind_below_65,0) as 'thunderstorm_wind &lt; 65',coalesce(t3_thunderstorm_wind.thunderstorm_wind_above_65,0) as 'thunderstorm_wind &gt;= 65',coalesce(t4_flashflood.flashflood_count, 0) as flashflood_count,coalesce(t5_tornado.EF0, 0) as EF0,coalesce(t5_tornado.EF1, 0) as EF1,coalesce(t5_tornado.EF2, 0) as EF2,coalesce(t5_tornado.EF3, 0) as EF3,coalesce(t5_tornado.EF4, 0) as EF4,coalesce(t5_tornado.EF5, 0) as EF5,coalesce(t6_null_events.null_classification_count ,0) as null_classification_count from 	select count(classification.classification) as classification_count, classification.classification from classification where (classification.classification_id in (select hail.classification_id from hail where hail.state = ? )  or classification.classification_id in (select thunderstorm.classification_id from thunderstorm where thunderstorm.state = ?) or classification.classification_id in (select flashflood.classification_id from flashflood where flashflood.state = ?) or classification.classification_id in (select tornado.classification_id from tornado where tornado.state = ?) )  group by classification.classification left outer join(select  classification.classification,count(case when hail.magnitude &lt; 1.0 then hail.magnitude end) as hail_below_one,count(case when (hail.magnitude &gt;= 1.0 AND hail.magnitude &lt; 2.0) then hail.magnitude end) as hail_above_one,count(case when hail.magnitude &gt;= 2 then hail.magnitude end) as hail_above_two from classification,hail where hail.classification_id = classification.classification_id and hail.state = ?  group by classification.classification) t2_hail on t2_hail.classification = t1_morphology.classification left outer join ( select classification.classification,count(case when thunderstorm.magnitude &lt; 65 then thunderstorm.magnitude end) as thunderstorm_wind_below_65,count(case when thunderstorm.magnitude &gt;= 65 then thunderstorm.magnitude end) as thunderstorm_wind_above_65 from thunderstorm,classification where thunderstorm.classification_id = classification. classification_id and thunderstorm.state = ?  group by classification.classification) t3_thunderstorm_wind on t3_thunderstorm_wind.classification = t1_morphology.classification left outer join(select  classification.classification, count(flashflood_id) as flashflood_count from flashflood,classification where flashflood.classification_id = classification.classification_id and flashflood.state = ?  group by classification.classification ) t4_flashflood on t4_flashflood.classification = t1_morphology.classification left outer join(select classification.classification,count(case when magnitude = 'EF0' then magnitude end) as EF0,count(case when magnitude = 'EF1' then magnitude end) as EF1,count(case when magnitude = 'EF2' then magnitude end) as EF2,count(case when magnitude = 'EF3' then magnitude end) as EF3,count(case when magnitude = 'EF4' then magnitude end) as EF4,count(case when magnitude = 'EF5' then magnitude end) as EF5 from tornado,classification where tornado.classification_id = classification.classification_id and tornado.state = ?  group by classification.classification ) t5_tornado on t5_tornado.classification = t1_morphology.classification left outer join ( select classification.classification,count(classification.classification) as null_classification_count from classification,null_events where null_events.classification_id = classification.classification_id and null_events.state = ?  group by classification.classification) t6_null_events on t6_null_events.classification = t1_morphology.classification) t1_morphology"
				+ " left outer join"
				+ "("
				+ "select "
				+ " classification.classification,"
				+ "count(case when hail.magnitude < 1.0 then hail.magnitude end) as hail_below_one,"
				+ "count(case when (hail.magnitude >= 1.0 AND hail.magnitude < 2.0) then hail.magnitude end) as hail_above_one,"
				+ "count(case when hail.magnitude >= 2 then hail.magnitude end) as hail_above_two"
				+ " from classification,hail"
				+ " where hail.classification_id = classification.classification_id and hail.state = ? "
				+ " group by classification.classification"
				+ ") t2_hail"
				+ " on t2_hail.classification = t1_morphology.classification"
				+ " left outer join"
				+ " ("
				+ " select"
				+ " classification.classification,"
				+ "count(case when thunderstorm.magnitude < 65 then thunderstorm.magnitude end) as thunderstorm_wind_below_65,"
				+ "count(case when thunderstorm.magnitude >= 65 then thunderstorm.magnitude end) as thunderstorm_wind_above_65"
				+ " from thunderstorm,classification where thunderstorm.classification_id = classification. classification_id and thunderstorm.state = ? "
				+ " group by classification.classification"
				+ ") t3_thunderstorm_wind"
				+ " on t3_thunderstorm_wind.classification = t1_morphology.classification left outer join"
				+ "("
				+ "select "
				+ " classification.classification,"
				+ " count(flashflood_id) as flashflood_count"
				+ " from flashflood,classification where flashflood.classification_id = classification.classification_id and flashflood.state = ? "
				+ " group by classification.classification ) t4_flashflood"
				+ " on t4_flashflood.classification = t1_morphology.classification left outer join"
				+ "(select "
				+ "classification.classification,"
				+ "count(case when magnitude = 'EF0' then magnitude end) as EF0,"
				+ "count(case when magnitude = 'EF1' then magnitude end) as EF1,"
				+ "count(case when magnitude = 'EF2' then magnitude end) as EF2,"
				+ "count(case when magnitude = 'EF3' then magnitude end) as EF3,"
				+ "count(case when magnitude = 'EF4' then magnitude end) as EF4,"
				+ "count(case when magnitude = 'EF5' then magnitude end) as EF5"
				+ " from tornado,classification"
				+ " where tornado.classification_id = classification.classification_id and tornado.state = ? "
				+ " group by classification.classification"
				+ " ) t5_tornado"
				+ " on t5_tornado.classification = t1_morphology.classification"
				+ " left outer join"
				+ " ("
				+ " select"
				+ " classification.classification,"
				+ "count(classification.classification) as null_classification_count"
				+ " from classification,null_events"
				+ " where null_events.classification_id = classification.classification_id and null_events.state = ?  group by classification.classification"
				+ ") t6_null_events"
				+ " on t6_null_events.classification = t1_morphology.classification";
		
		List<OverallResult> list = jdbcTemplate.query(sql, new Object[] {state,state,state,state,morphology,state,state,state,state,state} ,new OverallResultRowMapper());
		
		return list;
		
	}


	/* (non-Javadoc)
	 * @see edu.iastate.geol.meteor.swat.analytics.DAO.AnalyticsResultDAO#filteredSelect(edu.iastate.geol.meteor.swat.analytics.model.FilterForResult)
	 */
/*	@Override
	public HashMap<String, BigInteger> filteredSelect(FilterForResult filterForResult) {
		// TODO Auto-generated method stub
		return null;
	}
	*/

}
