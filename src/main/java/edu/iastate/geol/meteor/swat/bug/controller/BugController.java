package edu.iastate.geol.meteor.swat.bug.controller;



import java.io.UnsupportedEncodingException;


import javax.servlet.http.HttpSession;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

import edu.iastate.geol.meteor.swat.bug.bean.Bug;
import edu.iastate.geol.meteor.swat.bug.service.BugService;

@Controller
public class BugController {

	private Logger logger = LoggerFactory.getLogger(BugController.class);

	@RequestMapping(value = "/bugSubmit", method = RequestMethod.GET)
	public ModelAndView bugForm(HttpSession session) throws UnsupportedEncodingException {
		
		ModelAndView model = new ModelAndView("bug");
		model.addObject("bugForm", new Bug());
		return model;
	}
	
	
	@RequestMapping(value = "/bugSubmit", method = RequestMethod.POST)
	
	public @ResponseBody String bugSubmit(Bug bugForm,HttpSession session, Model model) throws UnsupportedEncodingException {
		
		logger.info(bugForm.getDescription());
		BugService bugService = new BugService();
		boolean bugInserted = bugService.insert(bugForm);
		bugService.notifyByEmail(bugForm.getDescription());
		String str = null;
		if(bugInserted)
			str = "Your bug has been inserted into the system. We will work to rectify it!";
		else
			str = "Your bug was not inserted. Retry!";
		JSONObject jobj = new JSONObject();
		try{
			jobj.put("bugInserted", str);
		}catch(JSONException e){
			
		}
		return jobj.toString();
	}
}
