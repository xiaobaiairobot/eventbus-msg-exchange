package com.yunli.bigdata.example.controller;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.yunli.bigdata.common.CommonMessageCode;
import com.yunli.bigdata.common.EventResponseCode;
import com.yunli.bigdata.example.dto.CreateCredentialMsgRequest;
import com.yunli.bigdata.example.dto.CreateMessageRequest;
import com.yunli.bigdata.example.service.EventService;
import com.yunli.bigdata.util.Result;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

/**
 * @author david
 * @date 2020/7/28 7:37 下午
 */
@Api(tags = "event-eb-sample", value = "消息测试")
@ApiResponses(value = {
    @ApiResponse(code = EventResponseCode.SUCCESS_CODE, message = EventResponseCode.SUCCESS_MSG),
    @ApiResponse(code = EventResponseCode.BAD_REQUEST_CODE, message = EventResponseCode.BAD_REQUEST_MSG),
    @ApiResponse(code = EventResponseCode.UNAUTHORIZED_CODE, message = EventResponseCode.UNAUTHORIZED_MSG),
    @ApiResponse(code = EventResponseCode.FORBIDDEN_CODE, message = EventResponseCode.FORBIDDEN_MSG),
    @ApiResponse(code = EventResponseCode.NOT_FOUND_CODE, message = EventResponseCode.NOT_FOUND_MSG),
    @ApiResponse(code = EventResponseCode.EXISTS_CODE, message = EventResponseCode.EXISTS_MSG),
    @ApiResponse(code = EventResponseCode.INTERNAL_SERVER_ERROR_CODE, message = EventResponseCode.INTERNAL_SERVER_ERROR_MSG),
})
@RequestMapping(value = "/eb/test")
@RestController
@CrossOrigin
public class EventController {

  private final EventService eventService;

  @Autowired
  public EventController(EventService eventService) {
    this.eventService = eventService;
  }

  @ApiOperation(value = "按照参数配置发送消息，需要证书信息", notes = "按照参数配置发送消息，需要证书信息", nickname = "sendMessageByCredential")
  @PostMapping(value = "credential")
  public ResponseEntity<Result<String>> sendMessageByCredential(
      @ApiParam(value = "消息参数请求体", required = true) @Valid @RequestBody CreateCredentialMsgRequest createCredentialMsgRequest
  ) {
    try {
      eventService.createMessageService(createCredentialMsgRequest);
      return ResponseEntity.status(HttpStatus.CREATED).body(Result.success("作业提交成功"));
    } catch (Exception e) {
      e.printStackTrace();
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
          .body(Result.error(CommonMessageCode.ERROR_500.getCode(), CommonMessageCode.ERROR_500.getMessage()));
    }
  }
}
