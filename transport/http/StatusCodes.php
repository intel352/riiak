<?php

namespace riiak\transport\http;

use \CJSON,
    \Exception,
    \Yii,
    \CLogger;

class StatusCodes extends \riiak\transport\Status {
    /**
     * List of Expected status codes from riak operations.
     * 
     * @var array 
     */
    protected $expectedStatus   =  array(
        'setBucketProperties'   => array('200'),
        'fetchObject'           => array('200', '300', '304'),
        'storeObject'           => array('201', '204', '300'),
        'deleteObject'          => array('204', '404'),
        'linkWalking'           => array('200'),
        'mapReduce'             => array('200'),
        'secondaryIndexes'      => array('200'),
        'listBucket'            => array('200'),
        'listKeys'              => array('200'),
        'getBucketProperties'   => array('200'),
        'ping'                  => array('200'),
        'status'                => array('200'),
        'listResource'          => array('200')
        );
    /**
     * List of normal codes for riak operations
     * 
     * @var array 
     */
    protected $normalCodes = array(
        'setBucketProperties' => array(
            '200' => 'OK'
        ),
        'fetchObject' => array(
            '200' => 'OK',
            '300' => 'Multiple Choices',
            '304' => 'Not Modified'
        ),
        'storeObject' => array(
            '201' => 'Created',
            '204' => 'No Content',
            '300' => 'Multiple Choices'
        ),
        'deleteObject' => array(
            '204' => 'No Content',
            '404' => 'Not Found'
        ),
        'linkWalking' => array(
            '200' => 'Ok'
        ),
        'mapReduce' => array(
            '200' => 'Ok'
        ),
        'secondaryIndexes' => array(
            '200' => 'Ok'
        )
    );
    
    /**
     * List of Error codes for riak operations
     * 
     * @var array 
     */
    protected $errorCodes = array(
        'setBucketProperties' => array(
            '204' => 'Set Bucket Properties - No Content',
            '400' => 'Set Bucket Properties - Bad Request - Submitted JSON is invalid',
            '415' => 'Set Bucket Properties - Unsupported Media Type - The Content-Type was not set to application/json in the request'
        ),
        'fetchObject' => array(
            '400' => 'Fetch Bucket Properties - Bad Request - e.g. r parameter is invalid (> N)',
            '404' => 'Fetch Bucket Properties - The object could not be found on enough partitions',
            '503' => 'Fetch Bucket Properties - Service Unavailable - The request timed out'
        ),
        'storeObject' => array(
            '400' => 'Store Object - Bad Request - e.g. r, w, or dw parameters are invalid (> N)',
            '412' => 'Store Object - Precondition Failed (r, w, or dw parameters are invalid (> N))'
        ),
        'deleteObject' => array(
            '400' => 'Delete Object - Bad Request - e.g. rw parameter is invalid (> N)'
        ),
        'linkWalking' => array(
            '400' => 'Link Walking - Bad Request - Format of the query in the URL is invalid',
            '404' => 'Link Walking - Not Found - Origin object of the walk was missing'
        ),
        'mapReduce' => array(
            '400' => 'MapReduce - Bad Request – Invalid job is submitted.',
            '500' => 'MapReduce - Internal Server Error – There was an error in processing a map or reduce function',
            '503' => 'MapReduce - Service Unavailable – The job timed out before it could complete'
        ),
        'secondaryIndexes' => array(
            '400' => 'Secondary Indexes - Bad Request - The index name or index value is invalid.',
            '500' => 'Secondary Indexes - Internal Server Error - There was an error in processing a map or reduce function, or indexing is not supported by the system.',
            '503' => 'Secondary Indexes - Service Unavailable – The job timed out before it could complete'
        )
    );
    
    /**
     *
     * @param string $response
     * @param string $action
     * @return bool 
     */
    public function validateStatus($response, $action) {
        $status = $this->getResponseStatus($response); 
        switch($action) {
            case 'setBucketProperties':
                return $this->handleResponse($status, 'setBucketProperties');
            break;
            case 'fetchObject':
                return $this->handleResponse($status, 'fetchObject');
            break;
            case 'storeObject':
                return $this->handleResponse($status, 'storeObject');
            break;
            case 'deleteObject':
                return $this->handleResponse($status, 'deleteObject');
            break;
            case 'linkWalking':
                return $this->handleResponse($status, 'linkWalking');
            break;
            case 'mapReduce':
                return $this->handleResponse($status, 'mapReduce');
            break;
            case 'secondaryIndexes':
                return $this->handleResponse($status, 'secondaryIndexes');
            break;
            case 'listBucket':
            case 'listKeys':
            case 'getSibling':
            case 'getBucketProperties':
            case 'ping':
            case 'status':
            case 'listResource':
            default:
                if($status != 200) {
                    return false;   
                }
            return true;
            break;
        }
    }
    
    public function validateStatus($response, $action) {
        $status = $this->getResponseStatus($response); 
        if($status == 200) {
            return true;   
        }
        return $this->handleResponse($status, $action);
    }
    
    /**
     *
     * @param string $status
     * @param string $index
     * @return bool 
     */
    public function handleResponse($status, $index) {
        /**
         * Check for OK status(200)
         */
        if($status != 200) {
            /**
             * Check for normal status codes
             */
            if(!in_array($status, $this->normalCodes[$index])) {
                /**
                 * Check for error codes
                 */
                if(in_array($status, $this->errorCodes[$index])) {
                    Yii::log($this->errorCodes[$index][$status], CLogger::LEVEL_ERROR, 'ext.riiak.Transport.Http.Status');
                    return false;
                }
            }
        }
        return true;
    }
    
    /**
     *
     * @param string $response
     * @return string 
     */
    public function getResponseStatus($response) {
        return $response['headers']['http_code'];
    }
    
    /**
     * Get expected results form riak operation
     * 
     * @param string $action
     * @return array 
     */
    public function getExpecetedStatus($action = ''){
        /**
         * Check for action is exists in expectedStaus array or not.
         */
        if(!array_key_exists($action, $this->expectedStatus))
            return array('200');
        
        return $this->expectedStatus[$action];
    }
}