package cs4347.jdbcProject.ecomm.dao.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

import cs4347.jdbcProject.ecomm.dao.CreditCardDAO;
import cs4347.jdbcProject.ecomm.entity.CreditCard;
import cs4347.jdbcProject.ecomm.util.DAOException;


public class CreditCardDaoImpl implements CreditCardDAO
{

	@Override
	public CreditCard create(Connection connection, CreditCard creditCard, Long customerID)
			throws SQLException, DAOException {
		// TODO Auto-generated method stub
		Statement db = connection.createStatement();
		String s1 = String.format("INSERT INTO creditcard (name, ccNumber, expDate, securityCode, customerID) values ('%s', '%s', '%s', '%s', %d);", creditCard.getName(), creditCard.getCcNumber(), creditCard.getExpDate(), creditCard.getSecurityCode(), customerID);
		db.executeUpdate(s1);
		return creditCard;
	}

	@Override
	public CreditCard retrieveForCustomerID(Connection connection, Long customerID) throws SQLException, DAOException {
		Statement db = connection.createStatement();
		//retrieve creditcard with given id
		String s1 = "SELECT * FROM creditcard where customerID = " + customerID;
		ResultSet rs = db.executeQuery(s1);		
		if (!rs.next()){
			return null;
		} 
		//create a new instance of credit card
		CreditCard myCreditCard = new CreditCard();
		
		myCreditCard.setName(rs.getString("name"));
		myCreditCard.setCcNumber(rs.getString("ccNumber"));
		myCreditCard.setExpDate(rs.getString("expDate"));
		myCreditCard.setSecurityCode(rs.getString("securityCode"));
		return myCreditCard;
	}

	@Override
	public void deleteForCustomerID(Connection connection, Long customerID) throws SQLException, DAOException {
		// TODO Auto-generated method stub
		Statement db = connection.createStatement();
		//if customerID is null, throw exception
		if (customerID == null) 
		{
			throw new DAOException("Invalid ID, Delete failed");
		}
		String s1 = "DELETE FROM creditcard where customerID = " +customerID;
		db.executeUpdate(s1);
	}

}
