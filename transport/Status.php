<?php

namespace riiak\transport;

use \CJSON,
    \Exception,
    \Yii,
    \CLogger;

abstract class Status {
    /**
     * List of normal codes for riak operations
     * 
     * @var array 
     */
    protected $normalCodes;
    
    /**
     * List of Error codes for riak operations
     * 
     * @var array 
     */
    protected $errorCodes;
    
    /**
     * List of Expected status codes from riak
     * @var array 
     */
    protected $expectedStatus;

    /**
     *
     * @param string $response
     * @param string $action
     * @return bool 
     */
    abstract public function validateStatus($response, $action);
    
    /**
     *
     * @param string $status
     * @param string $index
     * @return bool 
     */
    abstract public function handleResponse($status, $index);
    
    /**
     *
     * @param string $response
     * @return string 
     */
    abstract public function getResponseStatus($response);
    
    /**
     *
     * @param string $action
     * @return array 
     */
    abstract public function getExpecetedStatus($action = '');
}