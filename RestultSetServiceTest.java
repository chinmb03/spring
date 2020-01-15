package com.prismmicro.resultset.service;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.poi.ss.usermodel.CellStyle;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.context.junit4.SpringRunner;

import com.prismmicro.resultset.exception.ResourceAlreadyExistsException;
import com.prismmicro.resultset.exception.ResourcePersistanceException;
import com.prismmicro.resultset.exception.ResultsetResourceNotFoundException;
import com.prismmicro.resultset.model.App;
import com.prismmicro.resultset.model.ChartDataRequest;
import com.prismmicro.resultset.model.DMLRequest;
import com.prismmicro.resultset.model.DataSourceTableDataPage;
import com.prismmicro.resultset.model.Measure;
import com.prismmicro.resultset.model.Neo4jRequest;
import com.prismmicro.resultset.model.Resultset;
import com.prismmicro.resultset.model.ResultsetId;
import com.prismmicro.resultset.model.ResultsetJoin;
import com.prismmicro.resultset.model.SuccessResponse;
import com.prismmicro.resultset.model.TableColumns;
import com.prismmicro.resultset.model.TableRequest;
import com.prismmicro.resultset.repository.AppRepository;
import com.prismmicro.resultset.repository.Neo4jRepository;
import com.prismmicro.resultset.repository.ResultsetRepository;
import com.prismmicro.resultset.repository.UtilityRepository;
import com.prismmicro.resultset.utility.QueryUtility;

/**
 * @author chinmaya
 * @CreatedOn June 29, 2018
 */
@RunWith(SpringRunner.class)
public class RestultSetServiceTest {
	@InjectMocks
	ResultsetService resultsetserviceTest;
	@Mock
	UtilityRepository utilityRepository;
	@Mock
	ResultsetRepository resultsetRepository;
	@Mock
	AppRepository appRepository;
	@Mock
	CellStyle cellStyle;
	@Mock
	QueryUtility queryUtility;
	@Mock
	Neo4jRepository neo4jRepository;

	@Before
	public void initMocks() {
		MockitoAnnotations.initMocks(this);
	}

	// ============================== GetTableData ==========================
	@Test
	public void getTableDataTest() throws Exception {
		final Map<String, Object> response = new HashMap<String, Object>();
		response.put("config", "TableColumn");
		TableRequest request = new TableRequest();
		when(utilityRepository.getTableData(request)).thenReturn(response);
		Map<String, Object> expected = resultsetserviceTest.getTableData(request);
		Map<String, Object> actual = response;
		// verify
		assertEquals(actual, expected);
	}
	
	// ============================== getColumns ==========================
		@Test
		public void getColumnsTest() throws Exception {
			TableRequest request=new TableRequest();
			request.setResultsetId("resultsetId");
			request.setAppId("appId");
			Resultset resultsetTable=new Resultset();
			TableColumns tableColumns=new TableColumns();
			List<TableColumns> tableColumnslist=new ArrayList<>();
			tableColumnslist.add(tableColumns);
			resultsetTable.setTableColumns(tableColumnslist);
			when( utilityRepository.getTableColumns(request)).thenReturn(tableColumnslist);
			List<TableColumns> actual=resultsetserviceTest. getColumns(request);
			List<TableColumns> expected=tableColumnslist;
			assertEquals(expected, actual);
		}

	// ============================== GetChartData ==========================
	@Test
	public void getChartDataTest() throws Exception {
		final Map<String, Object> response = new HashMap<String, Object>();
		response.put("config", "TableColumn");
		List<String> dimensions = new ArrayList<String>();
		List<Measure> measures = new ArrayList<Measure>();
		ChartDataRequest chartDataRequest = new ChartDataRequest();
		chartDataRequest.setResultsetId("resultsetId");
		chartDataRequest.setAppId("appId");
		chartDataRequest.setChartType("chartType");
		chartDataRequest.setDimensions(dimensions);
		chartDataRequest.setMeasures(measures);
		chartDataRequest.setFilters("filters");
		chartDataRequest.setNlpFilters("nlpFilters");
		chartDataRequest.setSortColumn("sortColumn");
		chartDataRequest.setSortOrder("sortOrder");
		when(utilityRepository.getChartData(chartDataRequest)).thenReturn(response);
		Map<String, Object> expected = resultsetserviceTest.getChartData(chartDataRequest);
		Map<String, Object> actual = response;
		// verify
		assertEquals(actual, expected);

	}

	// ============================== GetExcelData ==========================
	@Test
	public void getExcelDataTest1() throws Exception {
		TableRequest request = new TableRequest();
		Map<String, Object> rawData = new HashMap<String, Object>();
		when(utilityRepository.getTableData(request)).thenReturn(rawData);
		DataSourceTableDataPage tableData = new DataSourceTableDataPage();
		rawData.put("data", tableData);
		List<Map<String, Object>> actual = resultsetserviceTest.getExcelData(request);
		// verify
		assertEquals(null, actual);
	}


	// ============================== RegisterTable ==========================

	@Test(expected = ResourceAlreadyExistsException.class)
	public void registerTableTest1() throws Exception {
		Resultset resultset = new Resultset();
		when(resultsetRepository.findById(resultset.getId())).thenReturn(Optional.of(resultset));
		SuccessResponse expected = resultsetserviceTest.registerTable(resultset);

	}

	@Test(expected = ResourcePersistanceException.class)
	public void registerTableTest2() throws Exception {
		Resultset resultset = new Resultset();
		when(resultsetRepository.findById(resultset.getId())).thenReturn(Optional.empty());
		SuccessResponse expected = resultsetserviceTest.registerTable(resultset);

	}

	@Test
	public void registerTableTest3() throws Exception {
		Resultset resultset = new Resultset();
		ResultsetId ResultsetId = new ResultsetId();
		resultset.setId(ResultsetId);
		App app = new App();
		Optional<App> dummyapp = Optional.of(app);
		when(resultsetRepository.findById(resultset.getId())).thenReturn(Optional.ofNullable(null));
		when(appRepository.findById(resultset.getId().getAppId())).thenReturn(Optional.of(app));
		SuccessResponse expected = resultsetserviceTest.registerTable(resultset);
		SuccessResponse actual = new SuccessResponse("Table Registered successfully");
		// verify
		assertEquals(expected, actual);
	}

	@Test(expected = ResultsetResourceNotFoundException.class)
	public void registerTableTest4() throws Exception {
		Resultset resultset = new Resultset();
		ResultsetId ResultsetId = new ResultsetId();
		resultset.setId(ResultsetId);
		App app = new App();
		Optional<App> dummyapp = Optional.of(app);
		when(resultsetRepository.findById(resultset.getId())).thenReturn(Optional.ofNullable(null));
		when(appRepository.findById(resultset.getId().getAppId())).thenReturn(Optional.empty());
		SuccessResponse expected = resultsetserviceTest.registerTable(resultset);
	}

	// ============================== SaveTable ==========================

	@Test
	public void saveTableTest1() throws Exception {
		Resultset resultset = new Resultset();
		ResultsetId resultsetId = new ResultsetId();
		resultset.setId(resultsetId);
		ResultsetJoin resultsetJoin=new ResultsetJoin();
		List<ResultsetJoin> tableJoins=new ArrayList<>();
		tableJoins.add(resultsetJoin);
		TableColumns tableColumns=new TableColumns();
		List<TableColumns> listTableColumns=new ArrayList<>();
		listTableColumns.add(tableColumns);
		resultset.setTableColumns(listTableColumns);
		resultset.setTableJoins(tableJoins);
		App app = new App();
		when(appRepository.findById(resultset.getId().getAppId())).thenReturn(Optional.of(app));
		when(resultsetRepository.findById(resultset.getId())).thenReturn(Optional.of(resultset));
		SuccessResponse expected = resultsetserviceTest.saveTable(resultset);

		SuccessResponse actual = new SuccessResponse("Table saved successfully");
		// verify
		assertEquals(actual, expected);
	}

	@Test(expected = ResultsetResourceNotFoundException.class)
	public void saveTableTest2() throws Exception {
		Resultset resultset = new Resultset();
		ResultsetId resultsetId = new ResultsetId();
		resultset.setId(resultsetId);
		when(appRepository.findById(resultset.getId().getAppId())).thenReturn(Optional.empty());
		when(resultsetRepository.findById(resultset.getId())).thenReturn(Optional.of(resultset));
		SuccessResponse expected = resultsetserviceTest.saveTable(resultset);

	}

	@Test(expected = ResultsetResourceNotFoundException.class)
	public void saveTableTest3() throws Exception {
		Resultset resultset = new Resultset();
		ResultsetId resultsetId = new ResultsetId();
		resultset.setId(resultsetId);
		App app = new App();
		when(appRepository.findById(resultset.getId().getAppId())).thenReturn(Optional.of(app));
		when(resultsetRepository.findById(resultset.getId())).thenReturn(Optional.empty());
		SuccessResponse expected = resultsetserviceTest.saveTable(resultset);

	}
	// ============================== getTable ==========================
	@Test
	public void getTableTest1() throws Exception {
		String tableId = "tableId";
		String appId = "appId";
		Resultset resultset = new Resultset();
		when(resultsetRepository.findByIdIdAndIdAppId(tableId, appId)).thenReturn(resultset);
		Resultset actual = resultsetserviceTest.getTable(tableId, appId);
		Resultset expected = resultset;
		assertEquals(expected, actual);
	}
	@Test(expected=ResultsetResourceNotFoundException.class)
	public void getTableTest2() throws Exception {
		String tableId = "tableId";
		String appId = "appId";
		Resultset resultset = null;
		when(resultsetRepository.findByIdIdAndIdAppId(tableId, appId)).thenReturn(resultset);
		Resultset actual = resultsetserviceTest.getTable(tableId, appId);
	}
	// ============================== saveRecord ==========================
	@Test
	public void saveRecord1() throws Exception {
		DMLRequest request = new DMLRequest();
		request.setAppId("postGres");
		request.setResultsetId("task_details_data3");
		request.setAppId("ENT");
		String tableId = "task_details_data3";
		String appId = "ENT";
		request.setOperation("insert");
		Resultset result = new Resultset();
		result.setName("taskDetails");
		ResultsetId id = new ResultsetId();
		result.setId(id);
		when(resultsetRepository.findByIdIdAndIdAppId(tableId, appId)).thenReturn(result);
		when(queryUtility.buildQueryToInsertRecord(request, result)).thenReturn("insertQuery");
		when(utilityRepository.executeUpdate(any(), any())).thenReturn(1);
		SuccessResponse actual = resultsetserviceTest.saveRecord(request);
		SuccessResponse expected=new SuccessResponse(1+" record(s) saved succesfully");
		assertEquals(expected, actual);
	}

	@Test
	public void saveRecord2() throws Exception {
		DMLRequest request = new DMLRequest();
		request.setAppId("postGres");
		request.setResultsetId("task_details_data3");
		request.setAppId("ENT");
		String tableId = "task_details_data3";
		String appId = "ENT";
		request.setOperation("update");
		Resultset result = new Resultset();
		result.setName("taskDetails");
		ResultsetId id = new ResultsetId();
		result.setId(id);
		when(resultsetRepository.findByIdIdAndIdAppId(tableId, appId)).thenReturn(result);
		when(queryUtility.buildQueryToInsertRecord(request, result)).thenReturn("insertQuery");
		when(utilityRepository.executeUpdate(any(), any())).thenReturn(1);
		SuccessResponse actual = resultsetserviceTest.saveRecord(request);
		SuccessResponse expected=new SuccessResponse(1+" record(s) saved succesfully");
		assertEquals(expected, actual);
	}
	// ============================== registerQuery ==========================
	
	@Test(expected=ResourceAlreadyExistsException.class)
	public void registerQuery1() throws Exception {
		Neo4jRequest neo4jRequest=new Neo4jRequest();
		neo4jRequest.setQueryId("queryId");
		Optional<Neo4jRequest> optNeo4jRequest=Optional.of(neo4jRequest);
		when(neo4jRepository.findById(neo4jRequest.getQueryId())).thenReturn(optNeo4jRequest);
		SuccessResponse actual=resultsetserviceTest.registerQuery(neo4jRequest);
		}
		
	@Test
    public void registerQuery2() throws Exception {
		Neo4jRequest neo4jRequest=new Neo4jRequest();
		//neo4jRequest.setQueryId("queryId");
		Optional<Neo4jRequest> optNeo4jRequest=Optional.empty();
		when(neo4jRepository.findById(neo4jRequest.getQueryId())).thenReturn(optNeo4jRequest);
		SuccessResponse actual=resultsetserviceTest.registerQuery(neo4jRequest);
		SuccessResponse expected=new SuccessResponse("Query saved successfully");
		assertEquals(expected, actual);
			
		}
	// ============================== saveQuery ==========================
	@Test
	public void saveQuery1() throws Exception {
		Neo4jRequest neo4jRequest=new Neo4jRequest();
		neo4jRequest.setQuery("query");
		Optional<Neo4jRequest> optNeo4jRequest=Optional.of(neo4jRequest);
		when(neo4jRepository.findById(neo4jRequest.getQueryId())).thenReturn(optNeo4jRequest);
		SuccessResponse actual=resultsetserviceTest.saveQuery(neo4jRequest);
		SuccessResponse expected=new SuccessResponse("Query updated successfully");
		assertEquals(expected, actual);
	}
	@Test(expected=ResultsetResourceNotFoundException.class)
	public void saveQuery2() throws Exception {
		Neo4jRequest neo4jRequest=new Neo4jRequest();
		Optional<Neo4jRequest> optNeo4jRequest=Optional.empty();
		when(neo4jRepository.findById(neo4jRequest.getQueryId())).thenReturn(optNeo4jRequest);
		SuccessResponse actual=resultsetserviceTest.saveQuery(neo4jRequest);
	}

}
