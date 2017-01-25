/**
 * 
 */
package edu.iastate.geol.meteor.swat.analytics.controller;

import java.sql.SQLException;
import java.util.List;

import javax.servlet.http.HttpSession;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.SessionAttributes;

import edu.iastate.geol.meteor.swat.analytics.bean.AnalyticsReportBean;
import edu.iastate.geol.meteor.swat.analytics.service.AnalyticsResultService;
import edu.iastate.geol.meteor.swat.analytics.service.AnalyticsService;


/**
 * @author Krishnan Subramanian
 *
 */

@Controller
@SessionAttributes("userEmail")
public class AnalyticsController {
	
	
	@RequestMapping(value = "/result/analytics" , method = RequestMethod.GET)
	public String analytics(HttpSession session, Model model) throws SQLException
	{
		if(session.getAttribute("userEmail") == null 
				|| (session.getAttribute("userHasResultAccess") != null &&  !(boolean)(session.getAttribute("userHasResultAccess") ))){
			model.addAttribute("error", "You do not have access to this page");
			return "error";
		}
		else
		{
			AnalyticsResultService  rs = new AnalyticsResultService();
			boolean userHasResultAccess = rs.userHasResultAccess((String)session.getAttribute("userEmail"));
			session.setAttribute("userHasResultAccess", userHasResultAccess);
			if(userHasResultAccess)
			{
				AnalyticsService analyticsService = new AnalyticsService();
				List<AnalyticsReportBean> analyticsWithDateAndCount = analyticsService.getOverallAnalyticsReport();
				model.addAttribute("analyticsWithDateAndCount",analyticsWithDateAndCount);
				
				for(AnalyticsReportBean aRB: analyticsWithDateAndCount){
						System.out.println("Key:"+ aRB.getDateTime() + ", Value:" + aRB.getCount());
					
				}
				
				return "analytics";
				
			}
			else
			{
				model.addAttribute("error", "You do not have access to this page");
				return "error";
			}
			
		}
	}

}
