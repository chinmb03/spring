package com.prismmicro.resultset.controller;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.netflix.ribbon.RibbonClient;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.prismmicro.resultset.model.App;
import com.prismmicro.resultset.model.ChartDataRequest;
import com.prismmicro.resultset.model.DMLRequest;
import com.prismmicro.resultset.model.LoadFilterRequest;
import com.prismmicro.resultset.model.Neo4jRequest;
import com.prismmicro.resultset.model.RegisterFilterRequest;
import com.prismmicro.resultset.model.Resultset;
import com.prismmicro.resultset.model.ReturnFilterListRequest;
import com.prismmicro.resultset.model.ReturnFilterRequest;
import com.prismmicro.resultset.model.SuccessResponse;
import com.prismmicro.resultset.model.TableColumns;
import com.prismmicro.resultset.model.TableRequest;
import com.prismmicro.resultset.service.AppService;
import com.prismmicro.resultset.service.FilterService;
import com.prismmicro.resultset.service.ResultsetService;

import io.swagger.annotations.ApiOperation;

@RestController
@FeignClient(name = "netflix-zuul-api-gateway-server")
@RibbonClient(name = "prismmicro-resultset")
public class ResultsetTableController {

	@Autowired
	ResultsetService resultsetService;

	@Autowired
	FilterService filterService;

	@Autowired
	AppService appService;

	@ApiOperation(value = "Load Table Grid", response = Iterable.class)
	@PostMapping("/returnData")
	public ResponseEntity<Map<String, Object>> returnData(@Valid @RequestBody TableRequest request) {
		return new ResponseEntity<>(resultsetService.getTableData(request), HttpStatus.OK);
	}

	@PostMapping("/returnChartData")
	public ResponseEntity<Map<String, Object>> returnChartData(@Valid @RequestBody ChartDataRequest chartDataRequest) {
		return new ResponseEntity<>(resultsetService.getChartData(chartDataRequest), HttpStatus.OK);
	}

	@PostMapping("/returnFilter")
	public ResponseEntity<String> returnFilter(@Valid @RequestBody ReturnFilterRequest filterRequest,
			HttpServletRequest request) {
		return new ResponseEntity<>(
				filterService.returnFilter(filterRequest.getAppId(), filterRequest.getFilterId()).toString(),
				HttpStatus.OK);
	}

	@PostMapping("/loadFilter")
	public ResponseEntity<List<Map<String, Object>>> loadFilter(@Valid @RequestBody LoadFilterRequest request) {
		return new ResponseEntity<>(filterService.loadFilterData(request.getAppId(), request.getFilterId(),
				request.getFilters(), request.getCustomQuery()), HttpStatus.OK);
	}

	@PostMapping("/returnFilterList")
	public ResponseEntity<String> returnFilterList(@Valid @RequestBody ReturnFilterListRequest filterRequest) {
		return new ResponseEntity<>(filterService.getFilterList(filterRequest), HttpStatus.OK);
	}

	@PostMapping("/registerFilter")
	public ResponseEntity<SuccessResponse> registerFilter(@Valid @RequestBody RegisterFilterRequest filter) {
		return new ResponseEntity<>(filterService.registerFilter(filter), HttpStatus.CREATED);

	}

	@PostMapping("/saveFilter")
	public ResponseEntity<SuccessResponse> saveFilter(@Valid @RequestBody RegisterFilterRequest filter) {
		return new ResponseEntity<>(filterService.saveFilter(filter), HttpStatus.CREATED);

	}

	@PostMapping(value = "/returnExcel")
	public ResponseEntity<List<Map<String, Object>>> returnExcel(@Valid @RequestBody TableRequest request) {

		return new ResponseEntity<>(resultsetService.getExcelData(request), HttpStatus.OK);
	}

	@PostMapping("/registerTable")
	public ResponseEntity<SuccessResponse> registerTable(@Valid @RequestBody Resultset resultset) {
		return new ResponseEntity<>(resultsetService.registerTable(resultset), HttpStatus.CREATED);
	}

	@GetMapping("/getTable")
	public ResponseEntity<Resultset> getTable(String tableId, String appId) {
		return new ResponseEntity<>(resultsetService.getTable(tableId, appId), HttpStatus.OK);
	}

	@PostMapping("/saveTable")
	public ResponseEntity<SuccessResponse> saveTable(@Valid @RequestBody Resultset resultset) {
		return new ResponseEntity<>(resultsetService.saveTable(resultset), HttpStatus.OK);
	}

	@PostMapping("/registerApp")
	public ResponseEntity<SuccessResponse> registerApp(@Valid @RequestBody App app) {
		return new ResponseEntity<>(appService.registerApp(app), HttpStatus.CREATED);
	}

	@PostMapping("/saveApp")
	public ResponseEntity<SuccessResponse> saveApp(@Valid @RequestBody App app) {
		return new ResponseEntity<>(appService.saveApp(app), HttpStatus.OK);
	}

	@GetMapping("/getApp")
	public ResponseEntity<App> getApp(String appId) {
		return new ResponseEntity<>(appService.getApp(appId), HttpStatus.OK);
	}

	@PostMapping("/getColumns")
	public ResponseEntity<List<TableColumns>> getColumns(@Valid @RequestBody TableRequest request) {
		return new ResponseEntity<>(resultsetService.getColumns(request), HttpStatus.OK);
	}

	@PostMapping("/saveRecord")
	public ResponseEntity<SuccessResponse> saveRecord(@RequestBody DMLRequest request) {
		return new ResponseEntity<>(resultsetService.saveRecord(request), HttpStatus.OK);
	}
	//register
	@PostMapping("/registerQuery")
	public ResponseEntity<SuccessResponse> registerQuery(@RequestBody Neo4jRequest neo4jRequest ) {
		return new ResponseEntity<>(resultsetService.registerQuery(neo4jRequest), HttpStatus.OK);
	}
	//save
		@PostMapping("/saveQuery")
		public ResponseEntity<SuccessResponse> saveQuery(@RequestBody Neo4jRequest neo4jRequest ) {
			return new ResponseEntity<>(resultsetService.saveQuery(neo4jRequest), HttpStatus.OK);
		}
}
