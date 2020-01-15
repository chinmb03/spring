/**
 *
 */
package com.prismmicro.resultset.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.prismmicro.resultset.exception.CustomException;
import com.prismmicro.resultset.exception.ResourceAlreadyExistsException;
import com.prismmicro.resultset.exception.ResourcePersistanceException;
import com.prismmicro.resultset.exception.ResultsetResourceNotFoundException;
import com.prismmicro.resultset.model.App;
import com.prismmicro.resultset.model.ChartDataRequest;
import com.prismmicro.resultset.model.DMLRequest;
import com.prismmicro.resultset.model.DataSourceTableDataPage;
import com.prismmicro.resultset.model.Neo4jRequest;
import com.prismmicro.resultset.model.Resultset;
import com.prismmicro.resultset.model.ResultsetJoin;
import com.prismmicro.resultset.model.SuccessResponse;
import com.prismmicro.resultset.model.TableColumns;
import com.prismmicro.resultset.model.TableRequest;
import com.prismmicro.resultset.repository.AppRepository;
import com.prismmicro.resultset.repository.Neo4jRepository;
import com.prismmicro.resultset.repository.ResultsetRepository;
import com.prismmicro.resultset.repository.UtilityRepository;
import com.prismmicro.resultset.utility.ConnectionFactory;
import com.prismmicro.resultset.utility.Constants;
import com.prismmicro.resultset.utility.QueryUtility;

/**
 * @author Geethasree
 *
 */
@Component("resultsetTableService")
public class ResultsetService {

	private static final Logger logger = LogManager.getLogger(ResultsetService.class);

	@Autowired
	UtilityRepository utilityRepository;

	@Autowired
	ResultsetRepository resultsetRepository;

	@Autowired
	AppRepository appRepository;
	
	@Autowired
	Neo4jRepository neo4jRepository;

	@Autowired
	ConnectionFactory connectionFactory;

	@Autowired
	QueryUtility queryUtility;

	public Map<String, Object> getTableData(TableRequest request) {
		return utilityRepository.getTableData(request);
	}

	public List<TableColumns> getColumns(TableRequest request) {
		return utilityRepository.getTableColumns(request);
	}

	public Map<String, Object> getChartData(ChartDataRequest chartDataRequest) {
		return utilityRepository.getChartData(chartDataRequest);
	}

	@SuppressWarnings("unchecked")
	public List<Map<String, Object>> getExcelData(TableRequest request) {
		List<Map<String, Object>> tableDataMapList = null;
		try {
			final Map<String, Object> rawData = utilityRepository.getTableData(request);
			final DataSourceTableDataPage tableData = (DataSourceTableDataPage) rawData.get("data");
			tableDataMapList = (List<Map<String, Object>>) tableData.getContent();
		} catch (final Exception e) {
			logger.error(e);
			throw e;
		}
		return tableDataMapList;
	}

	public SuccessResponse registerTable(Resultset resultset) {
		try {
			final Optional<Resultset> oldResultset = resultsetRepository.findById(resultset.getId());
			if (oldResultset.isPresent()) {
				throw new ResourceAlreadyExistsException(
						"Table already exist with given table_id: " + resultset.getId() + " , please try another id");
			}
			final Optional<App> app = appRepository.findById(resultset.getId().getAppId());
			if (!app.isPresent()) {
				throw new ResultsetResourceNotFoundException(
						"No record found with appid: " + resultset.getId().getAppId());
			}
			resultsetRepository.save(resultset);
			return new SuccessResponse("Table Registered successfully");
		} catch (ResultsetResourceNotFoundException | ResourceAlreadyExistsException e) {
			throw e;
		} catch (final Exception e) {
			logger.error(e);
			throw new ResourcePersistanceException("problem in registering table  " + e);
		}
	}
	
	
	public SuccessResponse registerNeo4j(Resultset resultset) {
		try {
			final Optional<Resultset> oldResultset = resultsetRepository.findById(resultset.getId());
			if (oldResultset.isPresent()) {
				throw new ResourceAlreadyExistsException(
						"ResultsetId already exist with given Resultset_id: " + resultset.getId() + " , please try another id");
			}
			final Optional<App> app = appRepository.findById(resultset.getId().getAppId());
			if (!app.isPresent()) {
				throw new ResultsetResourceNotFoundException(
						"No record found with appid: " + resultset.getId().getAppId());
			}
			resultsetRepository.save(resultset);
			return new SuccessResponse("Neo4jTable Registered successfully");
		} catch (ResultsetResourceNotFoundException | ResourceAlreadyExistsException e) {
			throw e;
		} catch (final Exception e) {
			logger.error(e);
			throw new ResourcePersistanceException("problem in registering Neo4jtable  " + e);
		}
	}

	public SuccessResponse saveTable(Resultset resultset) {
		try {
			final Optional<App> app = appRepository.findById(resultset.getId().getAppId());
			if (!app.isPresent()) {
				throw new ResultsetResourceNotFoundException(
						"No record found with appid: " + resultset.getId().getAppId());
			}
			final Optional<Resultset> oldResultset = resultsetRepository.findById(resultset.getId());
			if (!oldResultset.isPresent()) {
				throw new ResultsetResourceNotFoundException("No record found with resultsetId : " + resultset.getId()
						+ " , to update, please verify it once again");
			}

			updateTableColumnsAndTableJoins(resultset);
			resultsetRepository.save(resultset);
			return new SuccessResponse("Table saved successfully");
		} catch (final ResultsetResourceNotFoundException e) {
			throw e;
		} catch (final Exception e) {
			logger.error(e);
			throw new ResourcePersistanceException("problem in saving table  " + e);
		}
	}

	private void updateTableColumnsAndTableJoins(Resultset resultset) {
		ArrayList<TableColumns> tableColumns = new ArrayList<>();
		ArrayList<ResultsetJoin> tableJoins = new ArrayList<>();
		if (resultset.getTableColumns() != null && !resultset.getTableColumns().isEmpty()) {
			tableColumns.addAll(resultset.getTableColumns());
		}
		if (resultset.getTableJoins() != null && !resultset.getTableJoins().isEmpty()) {
			tableJoins.addAll(resultset.getTableJoins());
		}
		resultset.setTableColumns(tableColumns);
		resultset.setTableJoins(tableJoins);
	}

	public Resultset getTable(String tableId, String appId) {
		try {
			final Resultset resultset = resultsetRepository.findByIdIdAndIdAppId(tableId, appId);
			if (resultset == null) {
				throw new ResultsetResourceNotFoundException(
						"No record found with resultsetId: " + tableId + ", appId:  " + appId);
			}
			return resultset;
		} catch (final Exception e) {
			logger.error(e);
			throw e;
		}
	}

	/**
	 * check if the insert/update operation is for impala, if so throw custom
	 * exception. insert/update record(s) based on resultset id and app id by
	 * building insert query using requested(DML Request) data
	 */
	public SuccessResponse saveRecord(DMLRequest request) {
		try {
			String query = null;
			/** check if operation is for impala */
			checkIfOperationForImpala(request);

			/** get resultset object based on resultset id and app id */
			Resultset resultset = getTable(request.getResultsetId(), request.getAppId());

			/** checking operation type */
			if ("insert".equalsIgnoreCase(request.getOperation())) {

				/** build query to insert record */
				query = queryUtility.buildQueryToInsertRecord(request, resultset);

			} else {
				/** build query to update record */
				query = queryUtility.buildQueryToUpdateRecord(request, resultset);

			}
			/** execute the query */
			int noOfReordsInserted = utilityRepository.executeUpdate(query, resultset.getId().getAppId());

			/** prepare the success response */
			return new SuccessResponse(noOfReordsInserted + " record(s) saved succesfully");

		} catch (CustomException | ResultsetResourceNotFoundException e) {
			logger.error(e);
			throw e;
		} catch (Exception e) {
			logger.error(e);
			throw new CustomException("Unable to save the record" + e.getMessage(), HttpStatus.SC_FAILED_DEPENDENCY);
		}
	}

	/** check if operation is for impala, if so throw exception */
	private void checkIfOperationForImpala(DMLRequest insertRecRequest) {
		if (Constants.DB_TYPE_IMPALA.equals(utilityRepository.getDbType(insertRecRequest.getAppId()))) {
			throw new CustomException("Operation does not allowed for Impala", HttpStatus.SC_NOT_ACCEPTABLE);
		}
	}

	public SuccessResponse registerQuery(Neo4jRequest neo4jRequest) {
		try {
		Optional<Neo4jRequest> optNeo4jRequest=neo4jRepository.findById(neo4jRequest.getQueryId());
		if (optNeo4jRequest.isPresent()) {
			throw new ResourceAlreadyExistsException(
					"Record already exists with queryId: " +neo4jRequest.getQueryId());
		}
		neo4jRepository.save(neo4jRequest);
		return new SuccessResponse("Query saved successfully");
		}
		catch (ResourceAlreadyExistsException e) {
			throw e;
		} catch (final Exception e) {
			logger.error(e);
			throw new ResourcePersistanceException("problem in registering query  " + e);
		}
		
	}

	public SuccessResponse saveQuery(Neo4jRequest neo4jRequest) {
		
		try {
		Optional<Neo4jRequest> optNeo4jRequest=neo4jRepository.findById(neo4jRequest.getQueryId());
		if(!optNeo4jRequest.isPresent())
		{
			throw new ResultsetResourceNotFoundException(
					"No Record found with queryId: " +neo4jRequest.getQueryId());
		}
		if(neo4jRequest.getQuery()!=null && !neo4jRequest.getQuery().isEmpty())
		{
			optNeo4jRequest.get().setQuery(neo4jRequest.getQuery());
		}
		neo4jRepository.save(neo4jRequest);
		return new SuccessResponse("Query updated successfully");
	}
		catch (final ResultsetResourceNotFoundException e) {
			throw e;
		} catch (final Exception e) {
			logger.error(e);
			throw new ResourcePersistanceException("problem in saving query  " + e);
		}
}
}
