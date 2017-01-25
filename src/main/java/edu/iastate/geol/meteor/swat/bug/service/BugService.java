package edu.iastate.geol.meteor.swat.bug.service;

import java.io.File;
import java.util.Properties;

import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.springframework.core.io.FileSystemResource;
import org.springframework.mail.MailException;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.springframework.mail.javamail.MimeMessageHelper;

import edu.iastate.geol.meteor.swat.bug.DAO.BugDAO;
import edu.iastate.geol.meteor.swat.bug.DAOImpl.BugDAOImpl;
import edu.iastate.geol.meteor.swat.bug.bean.Bug;

public class BugService {

	private Properties properties;
	
	public boolean insert(Bug bug)
	{
		BugDAO bugDAO = new BugDAOImpl();
		bugDAO.setDataSource();
		return bugDAO.insertBug(bug);
	}
	
	protected void setProperties()
	{
		Properties properties = System.getProperties();
		properties.put("mail.smtp.host", "mailhub.iastate.edu");
		properties.put("mail.smtp.port","25");
		//properties.put("mail.smtp.starttls.enable","true");
		properties.put("mail.smtp.auth", "false");
		this.properties = properties;
	}
	
	
	public void notifyByEmail(String description){
		setProperties();
		Session session = Session.getDefaultInstance(properties);
		try{
			MimeMessage  m = new MimeMessage(session);
			
			
			JavaMailSenderImpl sender = new JavaMailSenderImpl(); 
			sender.setJavaMailProperties(properties);
			
			MimeMessage message = sender.createMimeMessage();
			MimeMessageHelper helper = new MimeMessageHelper(message, true);
			
			helper.setFrom(new InternetAddress("noreply@cyclone.agron.iastate.edu"));
			
			helper.setSubject("Bug in Severe Weather Analysis Tool");
			
			helper.setTo(InternetAddress.parse("krish@iastate.edu"));
			helper.setCc(InternetAddress.parse("krish1610@gmail.com"));
			
			StringBuffer stringBuffer = new StringBuffer();
			stringBuffer.append("Hello,");
			stringBuffer.append("<br>");
			stringBuffer.append("<p style = \" padding-left:5em;\"	>"
					+ "A bug has been found in the tool with the following description:"
					+ " <br> \"  " + description + " \" ");
			stringBuffer.append("<br>");
			stringBuffer.append("</p>");
			
			
			helper.setText(stringBuffer.toString(),true);
			
			FileSystemResource file = new FileSystemResource(new File("/var/home/tomcat/webapps/swat/resources/images/BE_example.png"));
			helper.addAttachment("flashflood.jpg", file);
			
			sender.send(message);
			
			/*//From address
			m.setFrom(new InternetAddress("noreply@cyclone.agron.iastate.edu"));
			
			//To address
			//flory@iastate.edu
			//wgallus@iastate.edu
			m.setRecipients(Message.RecipientType.TO, InternetAddress.parse("krish@iastate.edu"));
			m.setRecipients(Message.RecipientType.CC, InternetAddress.parse("krish1610@gmail.com"));
			
			
			
			
			
			//Email Contents
			m.setSubject("Bug in Severe Weather Analysis Tool");
			StringBuffer s = new StringBuffer();
			s.append("Hello,");
			s.append("<br>");
			s.append("<p style = \" padding-left:5em;\"	>"
					+ "A bug has been found in the tool with the following description:"
					+ " <br> \"  " + description + " \" ");
			s.append("<br>");
			s.append("</p>");
			
			String charset = "UTF-8";
			m.setText(s.toString(),charset , "html");
			Transport.send(m);*/
			
		}catch(MessagingException | MailException m){
			m.printStackTrace();
		}
		
	}
	
}
