<?php

namespace riiak\transport\http;

use \CJSON,
    \Exception,
    \Yii,
    \CLogger;

/**
 * Status Object allows you to validate all Riak operation
 * responses.
 * @package riiak.transport.http
 *
 * @todo Streamline custom error definitions
 */
class Status extends \riiak\transport\Status {

    /**
     * List of Expected status codes for Riak operations.
     *
     * @var array
     */
    protected $expectedStatus = array(
        'listBuckets' => array('200'),
        'getBucketProperties' => array('200'),
        'setBucketProperties' => array('204'),
        'fetchObject' => array('200', '300', '304'),
        'storeObject' => array('200', '201', '204', '300'),
        'deleteObject' => array('204', '404'),
        'linkWalking' => array('200'),
        'mapReduce' => array('200'),
        'secondaryIndex' => array('200'),
        'ping' => array('200'),
        'status' => array('200'),
        'listResource' => array('200')
    );

    /**
     * List of known error codes for Riak operations
     *
     * @var array
     */
    protected $errorCodes = array(
        'setBucketProperties' => array(
            '400' => 'Submitted JSON is invalid',
            '415' => 'The Content-Type was not set to application/json in the request'
        ),
        'fetchObject' => array(
            '400' => 'Bad request, e.g. r parameter is invalid (> N)',
            '404' => 'The object could not be found on enough partitions',
            '503' => 'The request timed out'
        ),
        'storeObject' => array(
            '400' => 'Bad request, e.g. r, w, or dw parameters are invalid (> N)',
            '412' => 'Precondition Failed (r, w, or dw parameters are invalid (> N))'
        ),
        'deleteObject' => array(
            '400' => 'Bad request, e.g. rw parameter is invalid (> N)'
        ),
        'linkWalking' => array(
            '400' => 'Format of the query in the URL is invalid',
            '404' => 'Origin object of the walk was missing'
        ),
        'mapReduce' => array(
            '400' => 'Invalid job is submitted.',
            '500' => 'There was an error in processing a map or reduce function',
            '503' => 'The job timed out before it could complete'
        ),
        'secondaryIndex' => array(
            '400' => 'The index name or index value is invalid.',
            '500' => 'There was an error in processing a map or reduce function, or indexing is not supported by the system.',
            '503' => 'The job timed out before it could complete'
        ),
        'status' => array(
            '404' => 'The setting "riak_kv_stat" may be disabled',
        )
    );

    /**
     * Handle Riak response
     *
     * @param array $response
     * @param string $action
     * @return bool
     */
    public function validateStatus(array $response, $action) {
        $httpCode = $response['headers']['http_code'];
        $httpStatus = $response['headers']['http_status'];

        /**
         * Check for normal status codes
         */
        if (!in_array($httpCode, $this->getExpectedStatus($action))) {
            $errorMsg = (is_array($httpStatus) ? implode(', ', $httpStatus) : $httpStatus) . ' - ';
            /**
             * Check for error definitions
             */
            if (array_key_exists($httpCode, $this->errorCodes[$action]))
                $errorMsg .= $this->errorCodes[$action][$httpCode];
            else
                $errorMsg .= 'An undefined error has occurred!';

            Yii::log($errorMsg, CLogger::LEVEL_ERROR, 'ext.riiak.transport.http.status.validateStatus');
            return false;
        }
        return true;
    }

    /**
     * Get expected status codes for Riak operation
     *
     * @param string $action
     * @return array
     */
    public function getExpectedStatus($action) {
        /**
         * Check for action is exists in expectedStatus array or not.
         */
        if (!array_key_exists($action, $this->expectedStatus))
            return array('200');

        return $this->expectedStatus[$action];
    }

}