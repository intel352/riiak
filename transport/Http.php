<?php

namespace riiak\transport;

use \CJSON,
    \Exception,
    \Yii,
    \CLogger;

/**
 * Contains transport layer actions of Riak
 * @package riiak.transport
 */
abstract class Http extends \riiak\Transport {

    /**
     *
     * @var object http\Status 
     */
    public $status;

    /**
     * Get staus handling class object
     * 
     * @return object http\Status
     */
    public function getStatusObject() {
        /**
         * Check for status handling class object
         */
        if (!is_object($this->status)) {
            $this->status = new http\StatusCodes();
        }
        /*
         * Return status class object
         */
        return $this->status;
    }

    /**
     * Method to validate riak response
     * 
     * @param string $response
     * @param string $action
     * @return bool
     */
    public function validateResponse($response, $action) {
        return $this->getStatusObject()->validateStatus($response, $action);
    }

    /**
     * Builds URL to connect to Riak server
     *
     * @return string
     */
    public function buildUrl() {
        return 'http' . ($this->client->ssl ? 's' : '') . '://' . $this->client->host . ':' . $this->client->port;
    }

    /**
     * Get (fetch) an object
     * 
     * @param \riiak\Bucket $objBucket
     * @param array $params
     * @param string $key
     * @param string $spec
     * @return array 
     */
    public function get(\riiak\Bucket $objBucket = NULL, array $params = array(), $key = null, $spec = null) {
        /**
         * Construct the URL
         */
        $url = $this->buildRestPath($objBucket, $key, $spec, $params);
        Yii::trace('Fetching transport layer Bucket properties for bucket "' . $objBucket->name . '"', 'ext.transport.httpRequest');

        /**
         * Process request.
         */
        $response = $this->processRequest('GET', $url);

        /**
         * Remove bulk of empty keys.
         */
        $response['body'] = $this->getStreamedBucketKeys($response, $params);

        /**
         * Return response
         */
        return $response;
    }

    /**
     * Put (save) an object
     * 
     * @param \riiak\Bucket $objBucket
     * @param string $contents
     * @param string $headers
     * @return array $response
     */
    public function put(\riiak\Bucket $objBucket = NULL, $headers = NULL, $contents = '', $url = '') {
        /**
         * Construct the request URL.
         */
        if ($url == '')
            $url = $this->buildRestPath($objBucket);

        /**
         * Prepare response header
         */
        /**
         * Process request.
         */
        $response = $this->processRequest('PUT', $url, $headers, $contents);

        /**
         * Set status code
         */
        $response['statusCode'] = $response['headers']['http_code'];

        /**
         * Return response
         */
        return $response;
    }

    public function buildSIRestPath($objBucket = NULL, $key = NULL, array $params = NULL) {
        /**
         * Build http[s]://hostname:port/prefix[/bucket]
         */
        $path = $this->buildUrl() . '/' . $this->client->bucketPrefix;

        /**
         * Add bucket
         */
        if (!is_null($objBucket))
            $path .= '/' . urlencode($objBucket);

        if (!is_null($this->client->secIndexPrefix))
            $path .= '/' . $this->client->secIndexPrefix;

        /**
         * Add query parameters
         */
        if (!empty($params)) {
            foreach ($params as $key => $value) {
                $path .= '/' . $value['column'] . $value['type'];

                if (!empty($value['operator']))
                    $path .= '/' . $value['operator'];

                $path .= '/' . $value['keyword'];
            }
        }

        return $path;
    }

    /**
     * Builds a REST URL to access Riak API
     *
     * @param \riiak\Bucket $objBucket
     * @param string $key
     * @param string $spec
     * @param array $params
     * @return string
     */
    public function buildRestPath(\riiak\Bucket $objBucket = NULL, $key = NULL, $spec = NULL, array $params = NULL) {
        if ($this->client->_useSecondaryIndex && $this->client->getIsSecondaryIndexSupport() && $params['r'] == 1) {
            //return $this->buildSIRestPath($objBucket, $key, $spec, $params);
        }
        /**
         * Build http[s]://hostname:port/prefix[/bucket]
         */
        $streamKey = '';
        /**
         * Check for get bucket keys using keys=stream.
         */
        if (!is_null($params) && 0 < count($params) && array_key_exists('keys', $params) && $params['keys'] != 'false') {
            $path = $this->buildUrl() . '/' . $this->client->bucketPrefix;
            $streamKey = '/' . $this->client->keyPrefix;
        } else {
            $path = $this->buildUrl() . '/' . $this->client->prefix;
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
     * @param string $method GET|POST|PUT|DELETE
     * @param string $url
     * @param array $requestHeaders
     * @param string $obj
     * @return array|null
     */
    public function processRequest($method, $url, array $requestHeaders = array(), $obj = '') {
        try {
            /**
             * Process http request using processing method (Curl,fopen etc).
             */
            $responseData = $this->sendRequest($method, $url, $requestHeaders, $obj);

            /**
             * Get headers
             */
            $parsedHeaders = $this->processHeaders($responseData['headerData']);
            $responseHeaders = array_merge(array('http_code' => $responseData['http_code']), array_change_key_case($parsedHeaders, CASE_LOWER));

            /**
             * Return headers/body array
             */
            return array('headers' => $responseHeaders, 'body' => $responseData['body']);
        } catch (Exception $e) {
            Yii::log($e->getMessage(), CLogger::LEVEL_ERROR, 'ext.riiak.Transport.Http');
            throw new Exception(Yii::t('Riiak', 'Failed to process request.'), (int) $e->getCode(), $e->errorInfo);
        }
    }

    /**
     * Parse HTTP header string into an assoc array
     *
     * @param string $headers
     * @return array
     */
    public function processHeaders($headers) {
        $retVal = array();
        $retVal = $this->processHeaders($headers);
        return $retVal;
    }

    /**
     * Check if Riak server is alive
     *
     * @return bool
     */
    public function getIsAlive() {
        Yii::trace('Pinging Riak server', 'ext.transport.http');
        $response = $this->processRequest('GET', $this->buildUrl() . '/ping');
        return ($response != NULL) && ($response['body'] == 'OK');
    }

    /**
     * Return array of Bucket objects
     *
     * @return array
     */
    public function getBuckets() {
        Yii::trace('Fetching list of buckets', 'ext.transport.http');
        /**
         * Construct URL
         */
        $url = $this->buildRestPath() . '?buckets=true';

        /**
         * Send request to fetch buckets.
         */
        $response = $this->processRequest('GET', $url);
        $responseObj = (array) CJSON::decode($response['body']);
        $buckets = array();

        /**
         * Prepare loop to process bucket list.
         */
        foreach ($responseObj['buckets'] as $name)
            $buckets[] = $this->client->bucket($name);

        /**
         * Return bucket array.
         */
        return $buckets;
    }

    /**
     * @param array $params
     * @return array $response
     */
    public function post($url = NULL, array $params = array(), $headers = '') {
        /**
         * Process request.
         */
        $response = $this->processRequest('POST', $url, $params, $headers);

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
     *
     * @param string $url
     * @param array $params
     * @param string $headers 
     */
    public function delete(\riiak\Bucket $objBucket = NULL, $key = '', array $params = array(), $headers = '') {
        /**
         * Construct URL
         */
        $url = $this->buildRestPath($objBucket, $key, null, $params);

        /**
         * Prepare response header
         */
        Yii::trace('Delete the object in Riak ', 'ext.transport.http');

        /**
         * Process request.
         */
        $response = $this->processRequest('DELETE', $url);

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
     * Get (fetch) multiple objects
     * 
     * @param array $urls
     * @param array $requestHeaders
     * @param string $obj
     * @return array 
     */
    public function multiGet(array $urls, array $requestHeaders = array(), $obj = '') {
        try {
            /**
             * Process http request using processing method (Curl,fopen etc).
             */
            $responseData = $this->multiGet($urls, $requestHeaders, $obj);

            /**
             * Return headers/body array
             */
            return $responseData;
        } catch (Exception $e) {
            Yii::log($e->getMessage(), CLogger::LEVEL_ERROR, 'ext.riiak.Transport.Http');
            throw new Exception(Yii::t('Riiak', 'Failed to process multi-request.'), (int) $e->getCode(), $e->errorInfo);
        }
    }

    /**
     * Populates the object. Only for internal use
     *
     * @param object $objObject
     * @param object $objBucket
     * @param array $response Output of transport layer processing
     * @param array $expectedStatuses List of statuses
     * @return \riiak\Object
     */
    public function populate(\riiak\Object &$objObject, \riiak\Bucket $objBucket, $response = array(), $action = '') {
        /**
         * Check for allowed response status list.
         */
        $expectedStatuses = $this->getStatusObject()->getExpectedStatus($action);

        if (0 >= count($expectedStatuses))
            $expectedStatuses = array(200, 201, 300);
        /**
         * Check for riiak\Object class object
         */
        if (!is_object($objObject))
            $objObject = new \riiak\Object($this->client, $objBucket);

        $objObject->clear();

        /**
         * If no response given, then return
         */
        if ($response == null)
            return $this;

        /**
         * Update the object
         */
        $objObject->headers = $response['headers'];
        $objObject->_data = $response['body'];

        /**
         * Check if the server is down (status==0)
         */
        if ($objObject->status == 0)
            throw new Exception('Could not contact Riak Server: ' . $this->buildUrl($this->client) . '!');

        /**
         * Verify that we got one of the expected statuses. Otherwise, throw an exception
         */
        if (!$this->validateResponse($response, $action)) {
            throw new Exception('Expected status ' . implode(' or ', $expectedStatuses) . ', received ' . $objObject->status);
        }

        /**
         * If 404 (Not Found), then clear the object
         */
        if ($objObject->status == 404) {
            $objObject->clear();
            return $objObject;
        }

        /**
         * If we are here, then the object exists
         */
        $objObject->_exists = true;

        /**
         * Parse the link header
         */
        if (array_key_exists('link', $objObject->headers))
            $objObject->populateLinks($objObject->headers['link']);

        /**
         * If 300 (siblings), load first sibling, store the rest
         */
        if ($objObject->status == 300) {
            $siblings = explode("\n", trim($objObject->_data));
            array_shift($siblings); # Get rid of 'Siblings:' string.
            $objObject->siblings = $siblings;
            $objObject->_exists = true;
            return $objObject;
        }

        if ($objObject->status == 201) {
            $pathParts = explode('/', $objObject->headers['location']);
            $objObject->key = array_pop($pathParts);
        }

        /**
         * Possibly JSON decode
         */
        if (($objObject->status == 200 || $objObject->status == 201) && $objObject->jsonize)
            $objObject->_data = CJSON::decode($objObject->_data, true);

        return $objObject;
    }

    /**
     * Get riak configuration details.
     * 
     * @param \riiak\Riiak $objClient
     * @return array Riak configuration details 
     */
    public function getRiakConfiguration() {
        Yii::trace('Get riak configuration', 'ext.transport.http');
        if (0 < count($this->client->_riakConfiguration)) {
            return $this->client->_riakConfiguration;
        }
        /**
         * Get riak configuration
         */
        $response = $this->processRequest('GET', $this->buildUrl() . '/stats');
        $this->client->_riakConfiguration = $response['body'];

        /**
         * Return riak configuration
         */
        return $this->client->_riakConfiguration;
    }

    /**
     * Check riak supports multi-backend functionality or not.
     * 
     * @return bool
     */
    public function getIsMultiBackendSupport() {
        Yii::trace('Checking Riak multibackend support', 'ext.transport.http');
        /**
         * Get riak configuration
         */
        $arrConfiguration = CJSON::decode($this->getRiakConfiguration());

        /**
         * Check riak supports multibackend or not
         */
        if ($arrConfiguration['storage_backend'] == 'riak_kv_multi_backend') {
            return true;
        }
        return false;
    }

    /**
     * Check riak supports secondary index or not.
     * 
     * @return bool
     * @todo Need to add check for leveldb installtion with multi-backend support. 
     */
    public function getIsSecondaryIndexSupport() {
        Yii::trace('Checking Secondary Indexes support', 'ext.transport.http');
        /**
         * Get riak configuration
         */
        $arrConfiguration = CJSON::decode($this->getRiakConfiguration());

        /**
         * Check riak supports leveldb or not
         */
        if ($arrConfiguration['storage_backend'] == 'riak_kv_eleveldb_backend') {
            return true;
        }
        return false;
    }

}