package com.mario.transformer.controllers;

import com.mario.transformer.exception.ResourceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import java.util.LinkedHashMap;
import java.util.Map;

@ControllerAdvice
public class ControllerExceptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(ControllerExceptionHandler.class);

    @ResponseBody
    @ResponseStatus(HttpStatus.NOT_FOUND)
    @ExceptionHandler({ResourceNotFoundException.class})
    public Map<String, Object> handleNotFound(ResourceNotFoundException ex) {
        logger.error("Element is not found: ", ex);
        return createResponse(ex, HttpStatus.NOT_FOUND);
    }

    private Map<String, Object> createResponse(RuntimeException ex, HttpStatus status) {
        var body = new LinkedHashMap<String, Object>();
        body.put("message", ex.getMessage());
        body.put("error", ex.getClass().getSimpleName());
        body.put("status", status.value());
        return body;
    }

}
