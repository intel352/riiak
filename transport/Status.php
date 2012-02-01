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
     * List of Expected status codes for Riak operations
     *
     * @var array
     */
    protected $expectedStatus;

    /**
     * List of Error codes for Riak operations
     * 
     * @var array 
     */
    protected $errorCodes;

    /**
     * Handle Riak response
     *
     * @param array $response
     * @param string $action
     * @return bool 
     */
    abstract public function validateStatus(array $response, $action);

    /**
     * Get expected status codes for Riak operation
     * 
     * @param string $action
     * @return array 
     */
    abstract public function getExpectedStatus($action);
}