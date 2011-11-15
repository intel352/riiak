<?php

namespace riiak\transport\http;

use \CJSON,
    \Exception,
    \Yii,
    \CLogger;

class Status {
    /**
     * List of normal codes for riak operations
     * 
     * @var array 
     */
    private $normalCodes = array(
        "setBucketProperties" => array(
            '200' => 'OK'
        ),
        "fetchObject" => array(
            '200' => 'OK',
            '300' => 'Multiple Choices',
            '304' => 'Not Modified'
        )
    );
    
    /**
     * List of Error codes for riak operations
     * 
     * @var array 
     */
    private $errorCodes = array(
        "setBucketProperties" => array(
            '204' => 'Set Bucket Properties - No Content',
            '400' => 'Set Bucket Properties - Bad Request - submitted JSON is invalid',
            '415' => 'Set Bucket Properties - Unsupported Media Type - The Content-Type was not set to application/json in the request'
        ),
        "fetchObject" => array(
            '400' => 'Fetch Bucket Properties - Bad Request - e.g. when r parameter is invalid (> N)',
            '404' => 'Fetch Bucket Properties - The object could not be found on enough partitions',
            '503' => 'Fetch Bucket Properties - Service Unavailable - the request timed out'
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
            case 'listBucket':
            case 'listKeys':
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
    
    /**
     *
     * @param string $status
     * @param string $index
     * @return bool 
     */
    public function handleResponse($status, $index){
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
}