package com.prismmicro.resultset.controller;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;

import com.prismmicro.resultset.model.App;
import com.prismmicro.resultset.model.ChartDataRequest;
import com.prismmicro.resultset.model.DMLRequest;
import com.prismmicro.resultset.model.LoadFilterRequest;
import com.prismmicro.resultset.model.Measure;
import com.prismmicro.resultset.model.Neo4jRequest;
import com.prismmicro.resultset.model.RegisterFilterRequest;
import com.prismmicro.resultset.model.Resultset;
import com.prismmicro.resultset.model.ReturnFilterListRequest;
import com.prismmicro.resultset.model.ReturnFilterRequest;
import com.prismmicro.resultset.model.SuccessResponse;
import com.prismmicro.resultset.model.TableColumns;
import com.prismmicro.resultset.model.TableRequest;
import com.prismmicro.resultset.repository.UtilityRepository;
import com.prismmicro.resultset.service.AppService;
import com.prismmicro.resultset.service.FilterService;
import com.prismmicro.resultset.service.ResultsetService;

/**
 * @author chinmaya
 * @CreatedOn June 25, 2018
 */

public class ResultsetTableControllerTest {
	@InjectMocks
	ResultsetTableController resultsetTableController;
	@Mock
	ResultsetService resultsetService;
	@Mock
	UtilityRepository utilityRepository;
	@Mock
	FilterService filterService;
	@Mock
	AppService appService;
	@Mock
	HttpServletRequest request;
	@Mock
	HttpServletResponse response;
	@Mock
	JSONObject jsonObject;
	@Mock
	SuccessResponse successResponse;

	@Before
	public void initMocks() {
		MockitoAnnotations.initMocks(this);
	}

	// ============================== ReturnData ==========================

	@Test
	public void returnDataTest() throws Exception {

		final Map<String, Object> response = new HashMap<String, Object>();
		response.put("data", "tableDataPage");
		TableRequest request = new TableRequest();
		request.setAppId("appid");
		request.setCustomQuery("customQuery");
		// setup
		when(resultsetService.getTableData(request)).thenReturn(response);
		ResponseEntity<Map<String, Object>> actual = resultsetTableController.returnData(request);
		ResponseEntity<Map<String, Object>> Re = new ResponseEntity<>(response, HttpStatus.OK);
		// verify
		assertEquals(actual, Re);

	}
	
	//====================ReturnDataBhakta===============================
	@Test
	public void returnData10() {
		
		TableRequest request = new 	TableRequest();
		request.setAppId("app");
		request.setResultsetId("rest");
		Map<String, Object> mp = new HashMap<String, Object>();
		when(resultsetService.getTableData(request)).thenReturn(mp);
		ResponseEntity<Map<String, Object>> actual =resultsetTableController.returnData(request);
		ResponseEntity<Map<String, Object>> expected = new ResponseEntity<>(mp,HttpStatus.OK);
		assertEquals(actual, expected);
		
	}
	
	// ============================== ReturnData ==========================

	@Test
	public void returnChartDataTest() throws Exception {
		final Map<String, Object> response = new HashMap<String, Object>();
		response.put("data", "mapList");
		List<String> strList = new ArrayList<>();
		strList.add("sup1");
		Measure measure = new Measure("measureField", "measureType");
		List<Measure> listMeasure = new ArrayList<>();
		listMeasure.add(measure);
		ChartDataRequest chartDataRequest = new ChartDataRequest();
		chartDataRequest.setResultsetId("resultsetId");
		chartDataRequest.setAppId("appId");
		chartDataRequest.setChartType("chartType");
		chartDataRequest.setDimensions(strList);
		chartDataRequest.setMeasures(listMeasure);
		chartDataRequest.setFilters("filters");
		chartDataRequest.setNlpFilters("nlpFilters");
		chartDataRequest.setSortColumn("sortColumn");
		chartDataRequest.setSortOrder("sortOrder");
		// setup
		when(resultsetService.getChartData(chartDataRequest)).thenReturn(response);
		ResponseEntity<Map<String, Object>> actual = resultsetTableController.returnChartData(chartDataRequest);
		ResponseEntity<Map<String, Object>> Re = new ResponseEntity<>(response, HttpStatus.OK);
		// verify
		assertEquals(actual, Re);

	}

	// ============================== ReturnFilter ==========================

	@Test
	public void returnFilterTest() throws Exception {
		ReturnFilterRequest filterRequest = new ReturnFilterRequest();
		filterRequest.setAppId("appId");
		filterRequest.setFilterId("filterId");
		final JSONObject jsonObject = new JSONObject();
		jsonObject.put("description", "description");

		when(filterService.returnFilter(filterRequest.getAppId(), filterRequest.getFilterId())).thenReturn(jsonObject);
		ResponseEntity<String> actual = resultsetTableController.returnFilter(filterRequest, request);

		ResponseEntity<String> Re = new ResponseEntity<>(jsonObject.toString(), HttpStatus.OK);
		// verify
		System.out.println(jsonObject.toString());
		System.out.println(Re);
		assertEquals(actual, Re);
	}

	// ============================== LoadFilterData ==========================

	@Test
	public void loadFilterTest() throws Exception {
		LoadFilterRequest request = new LoadFilterRequest();
		final Map<String, Object> mapTest = new HashMap<String, Object>();
		mapTest.put("config", "TableColumn");
		List<Map<String, Object>> response = new ArrayList();
		response.add(mapTest);
		// setup
		when(filterService.loadFilterData(request.getAppId(), request.getFilterId(), request.getFilters(),
				request.getCustomQuery())).thenReturn(response);
		ResponseEntity<List<Map<String, Object>>> expected = resultsetTableController.loadFilter(request);
		ResponseEntity<List<Map<String, Object>>> actual = new ResponseEntity<>(response, HttpStatus.OK);
		// verify
		assertEquals(actual, expected);
	}

	// ============================== ReturnFilterList ==========================

	@Test
	public void returnFilterListTest() throws Exception {
		ReturnFilterListRequest filterRequest = new ReturnFilterListRequest();
		// setup
		when(filterService.getFilterList(filterRequest)).thenReturn("filterListTest");
		ResponseEntity<String> actual = resultsetTableController.returnFilterList(filterRequest);
		ResponseEntity<String> expected = new ResponseEntity<>("filterListTest", HttpStatus.OK);
		// verify
		assertEquals(actual, expected);
	}

	// ============================== RegisterFilter ==========================

	@Test
	public void registerFilterTest() throws Exception {
		RegisterFilterRequest filter = new RegisterFilterRequest();
		SuccessResponse successResponse = new SuccessResponse("Filter registered successfully");
		// setup
		when(filterService.registerFilter(filter)).thenReturn(successResponse);
		ResponseEntity<SuccessResponse> actual = resultsetTableController.registerFilter(filter);
		ResponseEntity<SuccessResponse> expected = new ResponseEntity<SuccessResponse>(successResponse,
				HttpStatus.CREATED);
		// verify
		assertEquals(actual, expected);
	}
	// ============================== SaveFilter ==========================

	@Test
	public void saveFilterTest() throws Exception {
		RegisterFilterRequest filter = new RegisterFilterRequest();
		SuccessResponse successResponse = new SuccessResponse("Filter saved successfully");
		// setup
		when(filterService.saveFilter(filter)).thenReturn(successResponse);
		ResponseEntity<SuccessResponse> actual = resultsetTableController.saveFilter(filter);
		ResponseEntity<SuccessResponse> expected = new ResponseEntity<SuccessResponse>(successResponse,
				HttpStatus.CREATED);
		// verify
		assertEquals(actual, expected);
	}

	// ============================== ReturnExcel ==========================

	@Test
	public void returnExcelTest() throws Exception {
		TableRequest request = new TableRequest();
		final Map<String, Object> mapTest = new HashMap<String, Object>();
		mapTest.put("config", "TableColumn");
		List<Map<String, Object>> response = new ArrayList();
		response.add(mapTest);
		// setup
		when(resultsetService.getExcelData(request)).thenReturn(response);
		ResponseEntity<List<Map<String, Object>>> actual = resultsetTableController.returnExcel(request);
		ResponseEntity<List<Map<String, Object>>> expected = new ResponseEntity<>(response, HttpStatus.OK);
		// verify
		assertEquals(actual, expected);

	}

	// ============================== GetTable =============================

	@Test
	public void getTableTest() throws Exception {
		Resultset resultset = new Resultset();
		// setup
		String tableId = "tableId";
		String appId = "appid";

		when(resultsetService.getTable(tableId, appId)).thenReturn(resultset);
		ResponseEntity<Resultset> actual = resultsetTableController.getTable(tableId, appId);
		ResponseEntity<Resultset> expected = new ResponseEntity<Resultset>(resultset, HttpStatus.OK);
		// verify
		assertEquals(actual, expected);

	}

	// ============================== RegisterTable ==========================

	@Test
	public void registerTableTest() throws Exception {
		Resultset resultset = new Resultset();
		// setup
		when((resultsetService.registerTable(resultset))).thenReturn(successResponse);
		ResponseEntity<SuccessResponse> actual = resultsetTableController.registerTable(resultset);
		ResponseEntity<SuccessResponse> expected = new ResponseEntity<SuccessResponse>(successResponse,
				HttpStatus.CREATED);
		// verify
		assertEquals(actual, expected);
	}

	// ============================== SaveTable ==========================
	@Test
	public void saveTableTest() throws Exception {
		Resultset resultset = new Resultset();
		// setup
		when(resultsetService.saveTable(resultset)).thenReturn(successResponse);
		ResponseEntity<SuccessResponse> actual = resultsetTableController.saveTable(resultset);
		ResponseEntity<SuccessResponse> expected = new ResponseEntity<SuccessResponse>(successResponse, HttpStatus.OK);
		// verify
		assertEquals(actual, expected);

	}

	// ============================== RegisterApp ==========================

	@Test
	public void registerAppTest() throws Exception {
		App app = new App();
		// setup
		when(appService.registerApp(app)).thenReturn(successResponse);
		ResponseEntity<SuccessResponse> actual = resultsetTableController.registerApp(app);
		ResponseEntity<SuccessResponse> expected = new ResponseEntity<SuccessResponse>(successResponse,
				HttpStatus.CREATED);
		// verify
		assertEquals(actual, expected);
	}

	// ============================== SaveApp ==========================

	@Test
	public void saveAppTest() throws Exception {
		App app = new App();
		// setup
		when(appService.saveApp(app)).thenReturn(successResponse);
		ResponseEntity<SuccessResponse> actual = resultsetTableController.saveApp(app);
		ResponseEntity<SuccessResponse> expected = new ResponseEntity<SuccessResponse>(successResponse, HttpStatus.OK);
		// verify
		assertEquals(actual, expected);
	}

	// ============================== GetApp ==========================
	@Test
	public void getAppTest() throws Exception {
		App app = new App();
		String appId = "appId";
		// setup
		when(appService.getApp(appId)).thenReturn(app);
		ResponseEntity<App> actual = resultsetTableController.getApp(appId);
		ResponseEntity<App> expected = new ResponseEntity<App>(app, HttpStatus.OK);
		// verify
		assertEquals(actual, expected);
	}
	
	// ============================== GetColumns ==========================
		@Test
		public void getColumnsTest() throws Exception {
			TableRequest request=new TableRequest();
			TableColumns tableColumns=new TableColumns();
			List<TableColumns> listTableColumns=new ArrayList<>();
			listTableColumns.add(tableColumns);
			when(resultsetService.getColumns(request)).thenReturn(listTableColumns);
			ResponseEntity<List<TableColumns>> actual=resultsetTableController. getColumns(request);
			ResponseEntity<List<TableColumns>> expected=new ResponseEntity<List<TableColumns>>(listTableColumns,HttpStatus.OK);
			assertEquals(expected, actual);
			
		}
	// ============================== saveRecord ==========================
	
	@Test
	public void saveRecord() throws Exception {
		DMLRequest dmlRequest = new DMLRequest();
		SuccessResponse successResponse =new SuccessResponse();
		when(resultsetService.saveRecord(dmlRequest)).thenReturn(successResponse);
		ResponseEntity<SuccessResponse> actual= resultsetTableController.saveRecord(dmlRequest);
		ResponseEntity<SuccessResponse> expected = new ResponseEntity<>(successResponse,HttpStatus.OK);
		assertEquals(expected, actual);
		
	}
	// ============================== registerQuery ==========================
	
		@Test
		public void registerQuery() throws Exception {
			Neo4jRequest neo4jRequest=new Neo4jRequest();
			SuccessResponse successResponse =new SuccessResponse();
			when(resultsetService.registerQuery(neo4jRequest)).thenReturn(successResponse);
			ResponseEntity<SuccessResponse> actual=resultsetTableController.registerQuery(neo4jRequest );
			ResponseEntity<SuccessResponse> expected = new ResponseEntity<>(successResponse,HttpStatus.OK);
			assertEquals(expected, actual);
		}
	// ============================== saveQuery ==========================
		
		@Test
		public void saveQuery() throws Exception {
			Neo4jRequest neo4jRequest=new Neo4jRequest();
			SuccessResponse successResponse =new SuccessResponse();
			when(resultsetService.saveQuery(neo4jRequest)).thenReturn(successResponse);
			ResponseEntity<SuccessResponse> actual=resultsetTableController.saveQuery(neo4jRequest );
			ResponseEntity<SuccessResponse> expected = new ResponseEntity<>(successResponse,HttpStatus.OK);
			assertEquals(expected, actual);
		}
}
