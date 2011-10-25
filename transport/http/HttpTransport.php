<?php

namespace riiak\transport\http;

use \CComponent,
    \CJSON,
    \Exception,
    \Yii;

/**
 * Contains transport layer actions of Riak
 * @package http
 */
class HttpTransport extends \riiak\transport\Transport {
    
   /**
     * Builds URL to connect to Riak server
     *
     * @param object $objClient
     * @return string
     */
    public function buildUrl(\riiak\Riiak $objClient) {
        return 'http' . ($objClient->ssl ? 's' : '') . '://' . $objClient->host . ':' . $objClient->port;
    }
    
    /**
     * Return processing method object either CURL, PHP stream or fopen.
     * 
     * @param string $strMethod
     * @return object  
     */
    protected function getProcessingObject($strMethod = NULL){
        switch($strMethod){
            case 'Curl':
                /**
                 * Return CURL as processing method object.
                 */
                return new Curl();
                break;
            case 'fopen':
                break;
            default:
                /**
                 * Default: return CURL as request processing method.
                 */
                return new Curl();
                break;
        }
    }
    /**
     * Method to fetch bucket properties.
     * 
     * @param \riiak\Riiak $objClient
     * @param array $params
     * @param string $key
     * @param string $spec
     * @return array 
     */
    public function get(\riiak\Riiak $objClient, \riiak\Bucket $objBucket = NULL, array $params = array(), $key = null, $spec = null){
        /**
         * Construct the URL
         */
        $url = $this->buildRestPath($objClient, $objBucket, $key, $spec, $params);
        
        Yii::trace('Fetching transport layer Bucket properties for bucket "' . $objBucket->name . '"', 'ext.transport.httpRequest');
        
        /**
         * Process request.
         */
        $response = $this->processRequest($objClient, 'GET', $url);
        
        /**
         * Remove bulk of empty keys.
         */
        $response['body'] = $this->_objProcessMethod->getStreamedBucketKeys($response, $params);
        
        /**
         * Return response
         */
        return $response;
     }
     
    /**
     * Method to set multiple bucket properties in one call.
     * 
     * @param \riiak\Riiak $objClient
     * @param \riiak\Bucket $objBucket
     * @param string $content
     * @param string $headers
     * @return array $response
     */
    public function put(\riiak\Riiak $objClient, \riiak\Bucket $objBucket = NULL, $headers = NULL, $contents = '', $url = ''){
        /**
         * Construct the request URL.
         */
        if($url == '')
        $url = $this->buildRestPath($objClient, $objBucket);
        
        /**
         * Prepare response header
         */
        
        Yii::trace('Setting transport layer Bucket properties for bucket "' . $objBucket->name . '"', 'ext.transport.httpRequest');
        /**
         * Process request.
         */
        $response = $this->processRequest($objClient, 'PUT', $url, $headers, $contents);
        /**
         * Set status code
         */
        $response['statusCode'] = $response['headers']['http_code'];
        /**
         * Return response
         */
        return $response;
    }
    
    /**
     * Builds a REST URL to access Riak API
     *
     * @param object $objClient
     * @param object $objBucket
     * @param string $key
     * @param string $spec
     * @param array $params
     * @return string
     */
    public function buildRestPath(\riiak\Riiak $objClient, \riiak\Bucket $objBucket = NULL, $key = NULL, $spec = NULL, array $params = NULL) {
        /**
         * Build http[s]://hostname:port/prefix[/bucket]
         */
        $streamKey = '';
        /**
         * Check for get bucket keys using keys=stream.
         */
        if(!is_null($params) && 0 < count($params) && array_key_exists('keys', $params) && $params['keys'] != 'false'){
            $path = $this->buildUrl($objClient) . '/' . $objClient->bucketPrefix;
            $streamKey = '/' . $objClient->keyPrefix;
        }else{
            $path = $this->buildUrl($objClient) . '/' . $objClient->prefix;
        }
        
        /**
         * Add bucket
         */
        if (!is_null($objBucket) && $objBucket instanceof \riiak\Bucket)
            $path .= '/' . urlencode($objBucket->name) . $streamKey;

        /**
         * Add key
         */
        if (!is_null($key))
            $path .= '/' . urlencode($key);

        /**
         * Add params for link walking
         * bucket, tag, keep
         */
        if (!is_null($spec))
            foreach ($spec as $el)
                $path .= '/' . urlencode($el[0]) . ',' . urlencode($el[1]) . ',' . $el[2];

        /**
         * Add query parameters
         */
        if (!is_null($params))
            $path .= '?' . http_build_query($params, '', '&');

        return $path;
    }
    
    /**
     * Executes HTTP request, returns named array(headers, body) of request, or null on error
     *
     * @param object $client
     * @param string $method GET|POST|PUT|DELETE
     * @param string $url
     * @param array $requestHeaders
     * @param string $obj
     * @return array|null
     */
    public function processRequest(\riiak\Riiak $client, $method, $url, array $requestHeaders = array(), $obj = '') {
        try{
            /**
             * Process http request using processing method (Curl,fopen etc).
             */
            $responseData = $this->_objProcessMethod->processRequest($client, $method, $url, $requestHeaders, $obj);
            
            /**
             * Get headers
             */
            $parsedHeaders = self::parseHttpHeaders($responseData['headerData']);
            $responseHeaders = array_merge(array('http_code' => $responseData['http_code']), array_change_key_case($parsedHeaders, CASE_LOWER));

            /**
             * Return headers/body array
             */
            return array('headers' => $responseHeaders, 'body' => $responseData['body']);
        }catch (Exception $e) {
            error_log('Error: ' . $e->getMessage());
            return NULL;
        }
    }
    
    /**
     * Parse HTTP header string into an assoc array
     *
     * @param string $headers
     * @return array
     */
    public static function parseHttpHeaders($headers) {
        $retVal = array();
        $fields = array_filter(explode("\r\n", preg_replace('/\x0D\x0A[\x09\x20]+/', ' ', $headers)));
        foreach ($fields as $field) {
            if (preg_match('/([^:]+): (.+)/m', $field, $match)) {
                $match[1] = preg_replace('/(?<=^|[\x09\x20\x2D])./e', 'strtoupper("\0")', strtolower(trim($match[1])));
                if (isset($retVal[$match[1]])) {
                    $retVal[$match[1]] = array($retVal[$match[1]], $match[2]);
                } else {
                    $retVal[$match[1]] = trim($match[2]);
                }
            }
        }
        return $retVal;
    }
    
    /**
     * Check if Riak server is alive
     *
     * @return bool
     */
    public function getIsAlive(\riiak\Riiak $objClient) {
        Yii::trace('Pinging Riak server', 'ext.transport.http');
        $response = $this->processRequest('GET', $this->buildUrl($objClient) . '/ping');
        return ($response != NULL) && ($response['body'] == 'OK');
    }
    
    /**
     * Return array of Bucket objects
     *
     * @return array
     */
    public function getBuckets(\riiak\Riiak $objClient) {
        Yii::trace('Fetching list of buckets', 'ext.transport.http');
        /**
         * Construct URL
         */
        $url = $this->buildRestPath($objClient) . '?buckets=true';
        /**
         * Send request to fetch buckets.
         */
        $response = $this->processRequest($this->_client, 'GET', $url);
        $responseObj = (array)CJSON::decode($response['body']);
        $buckets = array();
        /**
         * Prepare loop to process bucket list.
         */
        foreach ($responseObj['buckets'] as $name)
            $buckets[] = $this->_client->bucket($name);
        /**
         * Return bucket array.
         */
        return $buckets; 
    }
    
    /**
     * Method to set multiple bucket properties in one call.
     * 
     * @param \riiak\Riiak $objClient
     * @param \riiak\Bucket $objBucket
     * @param array $params
     * @return array $response
     */
    public function post(\riiak\Riiak $objClient, $url = NULL, array $params = array(), $headers = ''){
        /**
         * Prepare response header
         */
        Yii::trace('Store the object in Riak ', 'ext.transport.http');
        
        /**
         * Process request.
         */
        $response = $this->processRequest($objClient, 'POST', $url, $params, $headers);
        /**
         * Set status code
         */
        $response['statusCode'] = $response['headers']['http_code'];
        /**
         * Return response
         */
        return $response;
    }
}
