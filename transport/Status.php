<?php

namespace riiak\transport;

use \CJSON,
    \Exception,
    \Yii,
    \CLogger;

/**
 * The Status object allows you to validate Riak response
 * @package riiak.transport
 *
 * @abstract
 */
abstract class Status {
    
    /**
     * List of normal codes for Riak operations
     * 
     * @var array 
     */
    protected $normalCodes;
    
    /**
     * List of Error codes for Riak operations
     * 
     * @var array 
     */
    protected $errorCodes;
    
    /**
     * List of Expected status codes for Riak operations
     * 
     * @var array 
     */
    protected $expectedStatus;

    /**
     * Validate Riak response
     * 
     * @param string $response
     * @param string $action
     * @return bool 
     */
    abstract public function validateStatus($response, $action);
    
    /**
     * Handle Riak response
     * 
     * @param string $status
     * @param string $index
     * @return bool 
     */
    abstract public function handleResponse($status, $index);
    
    /**
     * Get Riak response status code
     * 
     * @param string $response
     * @return string 
     */
    abstract public function getResponseStatus($response);
    
    /**
     * Get expected status codes for Riak operation
     * 
     * @param string $action
     * @return array 
     */
    abstract public function getExpectedStatus($action = '');
}