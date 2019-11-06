/*
	This part was done by AZEEM ALI (aaa096020)
*/
package cs4347.jdbcProject.ecomm.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import cs4347.jdbcProject.ecomm.dao.PurchaseDAO;
import cs4347.jdbcProject.ecomm.entity.Purchase;
import cs4347.jdbcProject.ecomm.services.PurchaseSummary;
import cs4347.jdbcProject.ecomm.util.DAOException;

public class PurchaseDaoImpl implements PurchaseDAO
{
	
	@Override	
		public Purchase create(Connection connection, Purchase purchase) throws SQLException, DAOException 
	{
		//SQL statement for insert
		final String insertQuery = "INSERT INTO purchase (customerID, productID, purchaseDate, purchaseAmount)VALUES (?, ?, ?, ?);";	
		
		//check to see if the purchase already exists (not null ID)
		if (purchase.getId() != null) 
		{
			throw new DAOException("Trying to create purchase that already exists (Not-Null ID)");
		}
		PreparedStatement SQLStatement = null;
	
		SQLStatement = connection.prepareStatement(insertQuery, Statement.RETURN_GENERATED_KEYS);
		SQLStatement.setLong(1, purchase.getCustomerID());
		SQLStatement.setLong(2, purchase.getProductID());
		SQLStatement.setDate(3, purchase.getPurchaseDate());
		SQLStatement.setDouble(4, purchase.getPurchaseAmount());

		//check to see if expected number of rows were updated
			
		int res = SQLStatement.executeUpdate();
		if(res != 1) 
		{
			throw new DAOException("Create Did Not Update Expected Number Of Rows");
		}

		//Copy the generated auto-increment primary key to the ID.
		ResultSet keys = SQLStatement.getGeneratedKeys();
		keys.next();
		int lastKey = keys.getInt(1);
		purchase.setId((long) lastKey);

		return purchase;
	}

	@Override
	public Purchase retrieve(Connection connection, Long id) throws SQLException, DAOException 
	{
		//SQL statement to select purchases
		final String retrieveQuery = "SELECT id, customerID, productID, purchaseDate, purchaseAmount FROM purchase where id = ?";
		
		//if id = null throw error
		if (id == null) 
		{
			throw new DAOException("Trying to retrieve a purchase that does not exists (NULL ID)");
		}

		PreparedStatement SQLStatement = null;
		
		SQLStatement = connection.prepareStatement(retrieveQuery);
		SQLStatement.setLong(1, id);
		ResultSet rs = SQLStatement.executeQuery();
		if (!rs.next()) 
		{
			return null;
		}

		//create new instance of purchase
		Purchase purch = new Purchase();
		purch.setId(rs.getLong("id"));
		purch.setCustomerID(rs.getLong("customerID"));
		purch.setProductID(rs.getLong("productID"));
		purch.setPurchaseDate(rs.getDate("purchaseDate"));
		purch.setPurchaseAmount(rs.getDouble("purchaseAmount"));
		return purch;		
	}

	@Override
	public int update(Connection connection, Purchase purchase) throws SQLException, DAOException 
	{
		//SQL statement for update
		final String updateQuery = "UPDATE purchase SET productID = ?, customerID = ?, purchaseDate = ?, purchaseAmount = ? WHERE id = ?;";

		//if ID is null, throw error
		if (purchase.getId() == null) 
		{
			throw new DAOException("Trying to update a purchase that does not exists (NULL ID)");
		}
		
		PreparedStatement SQLStatement = null;
		
		SQLStatement = connection.prepareStatement(updateQuery);
			
		SQLStatement.setLong(1, purchase.getProductID());
		SQLStatement.setLong(2, purchase.getCustomerID());
		SQLStatement.setDate(3, purchase.getPurchaseDate());
		SQLStatement.setDouble(4, purchase.getPurchaseAmount());
		SQLStatement.setLong(5, purchase.getId());

		int rows = SQLStatement.executeUpdate();
		return rows;
	}

	@Override
	public int delete(Connection connection, Long id) throws SQLException, DAOException 
	{
		//SQL statement for delete
		final String deleteQuery = "DELETE FROM purchase WHERE id = ?;";

		//if ID is null throw exception
		if (id == null) 
		{
			throw new DAOException("Trying to delete a purchase that does not exist (NULL ID)");
		}

		PreparedStatement SQLStatement = null;
			
		SQLStatement = connection.prepareStatement(deleteQuery);
		SQLStatement.setLong(1, id);

		int rows = SQLStatement.executeUpdate();
		return rows;
	}

	@Override
	public List<Purchase> retrieveForCustomerID(Connection connection, Long customerID) throws SQLException, DAOException 
	{
		//SQL statement for select
		final String retrieveQuery = "SELECT id, customerID, productID, purchaseDate, purchaseAmount FROM purchase where customerID = ?";
		
		PreparedStatement SQLStatement = null;
		
		//create new instance of Array list
		List<Purchase> p = new ArrayList<Purchase>();

		SQLStatement = connection.prepareStatement(retrieveQuery);
		SQLStatement.setLong(1, customerID);
		ResultSet rs = SQLStatement.executeQuery();

		while (rs.next()) 
		{
			//create new instance of purchase
			Purchase purch = new Purchase();
			purch.setId(rs.getLong("id"));
			purch.setCustomerID(rs.getLong("customerID"));
			purch.setProductID(rs.getLong("productID"));
			purch.setPurchaseDate(rs.getDate("purchaseDate"));
			purch.setPurchaseAmount(rs.getDouble("purchaseAmount"));
			p.add(purch);
		}
		return p;
	}

	@Override
	public List<Purchase> retrieveForProductID(Connection connection, Long productID) throws SQLException, DAOException 
	{
		//SQL select statement
		final String retrieveQuery =  "SELECT id, customerID, productID, purchaseDate, purchaseAmount FROM purchase where productID = ?";

		PreparedStatement SQLStatement = null;
		
		//new instance of Array list
		List<Purchase> p = new ArrayList<Purchase>();

		SQLStatement = connection.prepareStatement(retrieveQuery);
		SQLStatement.setLong(1, productID);
		ResultSet rs = SQLStatement.executeQuery();

		while (rs.next()) 
		{
			//new instance of purchase
			Purchase purch = new Purchase();
			purch.setId(rs.getLong("id"));
			purch.setCustomerID(rs.getLong("customerID"));
			purch.setProductID(rs.getLong("productID"));
			purch.setPurchaseDate(rs.getDate("purchaseDate"));
			purch.setPurchaseAmount(rs.getDouble("purchaseAmount"));
			p.add(purch);
		}
		return p;
	}

	@Override
	public PurchaseSummary retrievePurchaseSummary(Connection connection, Long customerID) throws SQLException, DAOException 
	{
		//SQL statement for select
		final String storeQuery = "SELECT id, customerID, productID, purchaseDate, purchaseAmount FROM purchase where customerID = ?";
		
		PreparedStatement SQLStatement = null;
		
		//new instance of purchaseSumary
		PurchaseSummary p = new PurchaseSummary();

		SQLStatement = connection.prepareStatement(storeQuery);
		SQLStatement.setLong(1, customerID);
		ResultSet rs = SQLStatement.executeQuery();

		//default values
		float minPurchase = Float.MAX_VALUE;
		float maxPurchase = 0;
		float count = 0;
		float totalPurchase = 0;
		float thisPurchase;
			
		while (rs.next()) 
		{
			thisPurchase = rs.getFloat("purchaseAmount");
				
			if(thisPurchase>maxPurchase)
			{
				//set this purchase as max
				maxPurchase = thisPurchase;
			}
			else if (thisPurchase<minPurchase)
			{
				//set this purchase as min
				minPurchase = thisPurchase;
			}
				else
			{
				//do nothing
			}
				
			//add this purchase to the total purchase
			totalPurchase = totalPurchase + thisPurchase;
				
			//increment the item count
			count++;
		}
			
		p.minPurchase = minPurchase;
		p.maxPurchase = maxPurchase;
		p.avgPurchase = totalPurchase/count;
		return p;
		
	}
}

