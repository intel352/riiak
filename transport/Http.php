<?php

namespace riiak\transport;

use \CJSON,
    \Exception,
    \Yii,
    \CLogger;

/**
 * The Http object allows you to perform all Riak operation
 * through HTTP protocol
 * @package riiak.transport
 *
 * @abstract
 */
abstract class Http extends \riiak\Transport {

    /**
     * @var http\Status
     */
    public $status;

    /**
     * Get status handling class object
     *
     * @return object http\StatusCodes
     */
    public function getStatusObject() {
        /**
         * Check for existing status handling class object
         */
        if (!is_object($this->status))
            $this->status = new http\StatusCodes();

        /*
         * Return status class object
         */
        return $this->status;
    }

    /**
     * Validate Riak response using http\StatusCodes class
     *
     * @param string $response
     * @param string $action
     * @return bool
     */
    public function validateResponse($response, $action) {
        return $this->getStatusObject()->validateStatus($response, $action);
    }

    /**
     * Builds URL for Riak server communication
     *
     * @return string
     */
    public function baseUrl() {
        return 'http' . ($this->client->ssl ? 's' : '') . '://' . $this->client->host . ':' . $this->client->port;
    }

    /**
     * Get (fetch) an object
     *
     * @param \riiak\Bucket $bucket
     * @param array $params
     * @param string $key
     * @param string $spec
     * @return array
     */
    public function get(\riiak\Bucket $bucket = NULL, array $params = array(), $key = null, $spec = null) {
        /**
         * Construct the URL
         */
        $url = $this->buildBucketPath($bucket, $key, $spec, $params);
        Yii::trace('Fetching transport layer Bucket properties for bucket "' . $bucket->name . '"', 'ext.riiak.transport.httpRequest');

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
     * @param \riiak\Bucket $bucket
     * @param string $headers
     * @param string $contents
     * @param string $url
     * @return array $response
     */
    public function put(\riiak\Bucket $bucket = NULL, $headers = NULL, $contents = '', $url = '') {
        /**
         * Construct the request URL.
         */
        if ($url == '')
            $url = $this->buildBucketPath($bucket);

        /**
         * Process request.
         */
        $response = $this->processRequest('PUT', $url, $headers, $contents);

        /**
         * Set status code
         */
        $response['statusCode'] = $response['headers']['http_code'];

        /**
         * Return Riak response
         */
        return $response;
    }

    /**
     * Builds a REST URL to access Riak API
     *
     * @param string $key
     * @param array $params
     * @return string
     */
    public function buildSIRestPath($objBucket = NULL, $key = NULL, array $params = NULL) {
        /**
         * Build http[s]://hostname:port/prefix[/bucket]
         */
        $path = $this->baseUrl() . '/' . $this->client->bucketPrefix;

        /**
         * Add bucket information
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

        /**
         * Return constructed URL (Riak API Path)
         */
        return $path;
    }

    public function buildMapReducePath() {
        ;
    }

    public function buildBucketIndexPath(\riiak\Bucket $bucket, array $indexes, array $params = NULL) {
        ;
    }

    public function buildBucketKeyPath(\riiak\Bucket $bucket, $key = NULL, array $links = NULL, array $params = NULL) {
        ;
    }

    /**
     * Builds URL to access bucket information via Riak API
     *
     * @param \riiak\Bucket $bucket
     * @param string $key
     * @param array $links
     * @param array $params
     * @return string
     */
    public function buildBucketPath(\riiak\Bucket $bucket = NULL, $key = NULL, array $links = NULL, array $params = NULL) {
        /**
         * Build http[s]://hostname:port/buckets[/bucket[/keys[/key]]]
         */
        $path = '/' . $this->client->bucketPrefix;

        /**
         * Add bucket
         */
        if (!is_null($bucket) && $bucket instanceof \riiak\Bucket) {
            $path .= '/' . urlencode($bucket->name);

            /**
             * Add key
             */
            if (!is_null($key)) {
                $path .= '/' . $this->client->keyPrefix . '/' . urlencode($key);

                /**
                 * Add params for link walking
                 * bucket, tag, keep
                 */
                if (!is_null($links))
                    foreach ($links as $el)
                        $path .= '/' . urlencode($el[0]) . ',' . urlencode($el[1]) . ',' . $el[2];
            }
        }

        /**
         * Return constructed URL (Riak API Path)
         */
        return $this->buildUri($path, $params);
    }

    public function buildUri($path, array $params = NULL) {
        $path = $this->baseUrl() . $path;

        /**
         * Add query parameters
         */
        if (!is_null($params))
            $path .= '?' . http_build_query($params, '', '&');

        return $path;
    }

    /**
     * Executes HTTP request, returns named array(headers, body) of request.
     * Throws exception upon error
     *
     * @param string $method GET|POST|PUT|DELETE
     * @param string $url
     * @param array $requestHeaders
     * @param string $obj
     * @return array
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
        Yii::trace('Pinging Riak server', 'ext.riiak.transport.http');
        $response = $this->processRequest('GET', $this->baseUrl() . '/ping');
        return ($response != NULL) && ($response['body'] == 'OK');
    }

    /**
     * Return array of Bucket objects
     *
     * @return array
     */
    public function getBuckets() {
        Yii::trace('Fetching list of buckets', 'ext.riiak.transport.http');
        /**
         * Construct URL
         */
        $url = $this->buildBucketPath(null, null, null, array('buckets' => 'true'));

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
     * @param string $url
     * @param array $params
     * @param string $headers
     * @return array
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
     * @param \riiak\Bucket $bucket
     * @param string $key
     * @param array $params
     * @param string $headers
     * @return array
     */
    public function delete(\riiak\Bucket $bucket = NULL, $key = '', array $params = array(), $headers = '') {
        /**
         * Construct URL
         */
        $url = $this->buildBucketPath($bucket, $key, null, $params);

        /**
         * Prepare response header
         */
        Yii::trace('Delete the object in Riak ', 'ext.riiak.transport.http');

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
     * @return array|null
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
            Yii::log($e->getMessage(), CLogger::LEVEL_ERROR, 'ext.riiak.transport.http');
            throw new Exception(Yii::t('Riiak', 'Failed to process multi-request.'), (int) $e->getCode(), $e->errorInfo);
        }
    }

    /**
     * Populates the object. Only for internal use
     *
     * @param object $object
     * @param object $bucket
     * @param array $response Output of transport layer processing
     * @param array $expectedStatuses List of statuses
     * @return \riiak\Object
     */
    public function populate(\riiak\Object $object, \riiak\Bucket $bucket, $response = array(), $action = '') {
        /**
         * Check for allowed response status list.
         */
        $expectedStatuses = $this->getStatusObject()->getExpectedStatus($action);

        if (0 >= count($expectedStatuses))
            $expectedStatuses = array(200, 201, 300);

        /**
         * Check for riiak\Object class object
         */
        if (!is_object($object))
            $object = new \riiak\Object($this->client, $bucket);

        $object->clear();

        /**
         * If no response given, then return
         */
        if (is_null($response))
            return $this;

        /**
         * Update the object
         */
        $object->headers = $response['headers'];
        $object->data = $response['body'];

        /**
         * Check if the server is down (status==0)
         */
        if ($object->status == 0)
            throw new Exception('Could not contact Riak Server: ' . $this->baseUrl($this->client) . '!');

        /**
         * Verify that we got one of the expected statuses. Otherwise, throw an exception
         */
        if (!$this->validateResponse($response, $action))
            throw new Exception('Expected status ' . implode(' or ', $expectedStatuses) . ', received ' . $object->status);

        /**
         * If 404 (Not Found), then clear the object
         */
        if ($object->status == 404) {
            $object->clear();
            return $object;
        }

        /**
         * If we are here, then the object exists
         */
        $object->exists = true;

        /**
         * Parse the link header
         */
        if (array_key_exists('link', $object->headers))
            $object->populateLinks($object->headers['link']);

        /**
         * If 300 (siblings), load first sibling, store the rest
         */
        if ($object->status == 300) {
            $siblings = explode("\n", trim($object->data));
            array_shift($siblings); # Get rid of 'Siblings:' string.
            $object->siblings = $siblings;
            $object->exists = true;
            return $object;
        }

        if ($object->status == 201) {
            $pathParts = explode('/', $object->headers['location']);
            $object->key = array_pop($pathParts);
        }

        /**
         * Possibly JSON decode
         */
        if (($object->status == 200 || $object->status == 201) && $object->jsonize)
            $object->data = CJSON::decode($object->data, true);

        return $object;
    }

    /**
     * Get riak configuration details.
     *
     * @return array Riak configuration details
     */
    public function getRiakConfiguration() {
        Yii::trace('Get riak configuration', 'ext.riiak.transport.http');

        /**
         * Get riak configuration
         */
        $response = $this->processRequest('GET', $this->baseUrl() . '/stats');
        return CJSON::decode($response['body']);
    }

    /**
     * Check riak supports multi-backend functionality or not.
     *
     * @return bool
     */
    public function getIsMultiBackendSupport() {
        Yii::trace('Checking Riak multibackend support', 'ext.riiak.transport.http');

        /**
         * Get riak configuration
         */
        $arrConfiguration = $this->client->serverConfig;

        /**
         * Check riak supports multibackend or not
         */
        if ($arrConfiguration['storage_backend'] == 'riak_kv_multi_backend')
            return true;

        return false;
    }

    /**
     * Check riak supports secondary index or not.
     *
     * @return bool
     * @todo Need to add check for leveldb installtion with multi-backend support.
     */
    public function getIsSecondaryIndexSupport() {
        Yii::trace('Checking Secondary Indexes support', 'ext.riiak.transport.http');

        /**
         * Get riak configuration
         */
        $arrConfiguration = $this->client->serverConfig;

        /**
         * Check riak supports leveldb or not
         */
        if ($arrConfiguration['storage_backend'] == 'riak_kv_eleveldb_backend')
            return true;

        return false;
    }

}