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
 *
 * @method array get() get(string $url, array $requestHeaders = array(), string $content = '') Alias for processRequest('GET', ...)
 * @method array post() post(string $url, array $requestHeaders = array(), string $content = '') Alias for processRequest('POST', ...)
 * @method array put() put(string $url, array $requestHeaders = array(), string $content = '') Alias for processRequest('PUT', ...)
 * @method array delete() delete(string $url, array $requestHeaders = array(), string $content = '') Alias for processRequest('DELETE', ...)
 */
abstract class Http extends \riiak\Transport {

    /**
     * @var http\Status
     */
    public $status;

    public function __call($name, $parameters) {
        /**
         * Adding magic support for transport->get|post|put|delete
         */
        switch ($name) {
            case 'get':
            case 'post':
            case 'put':
            case 'delete':
                /**
                 * Process request.
                 */
                array_unshift($parameters, strtoupper($name));
                return call_user_func_array(array($this, 'processRequest'), $parameters);
                break;
        }

        return parent::__call($name, $parameters);
    }

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
     * This is renamed from "get", as "get" should be a generic function...
     * @todo Function is in dire need of refactoring
     *
     * @param \riiak\Bucket $bucket
     * @param array $params
     * @param string $key
     * @param string $spec
     * @return array
     */
    public function getObject(\riiak\Bucket $bucket = NULL, array $params = array(), $key = null, $spec = null) {
        /**
         * Construct the URL
         */
        $url = $this->buildBucketKeyPath($bucket, $key, $spec, $params);
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
     * This is renamed from "put", as "put" should be a generic function...
     *
     * @param \riiak\Bucket $bucket
     * @param string $headers
     * @param string $content
     * @param string $url
     * @return array $response
     */
    public function putObject(\riiak\Bucket $bucket = NULL, $headers = NULL, $content = '', $url = '') {
        /**
         * Construct the request URL.
         */
        if ($url == '')
            $url = $this->buildBucketKeyPath($bucket);

        /**
         * Process request.
         */
        $response = $this->processRequest('PUT', $url, $headers, $content);

        /**
         * Return Riak response
         */
        return $response;
    }

    /**
     * @param string $appendPath optional
     * @param array $params optional
     * @return string
     */
    public function buildMapReducePath($appendPath = NULL, array $params = NULL) {
        $path = '/' . $this->client->mapredPrefix;

        /**
         * Return constructed URL (Riak API Path)
         */
        return $this->buildUri($path . $appendPath, $params);
    }

    /**
     * Given bucket, key, linkspec, params, construct and return url for searching
     * secondary indices
     *
     * @author Eric Stevens <estevens@taglabsinc.com>
     *
     * @param \riiak\Bucket $bucket
     * @param string $index Index name and type (e.g. - 'indexName_bin')
     * @param string|int $start Starting value or exact match if no end value
     * @param string|int $end optional Ending value for range search
     * @param array $params optional Any extra query parameters
     * @return string
     */
    public function buildBucketIndexPath(\riiak\Bucket $bucket, $index, $start, $end = NULL, array $params = NULL) {
        $path = '/' . $this->client->indexPrefix . '/' . urlencode($index) . '/' . urlencode($start);

        if (!is_null($end))
            $path .= '/' . urlencode($end);

        return $this->buildBucketPath($bucket, $path, $params);
    }

    /**
     * Builds URL for Riak bucket/keys query
     *
     * @param \riiak\Bucket $bucket
     * @param string $key optional
     * @param array $links optional
     * @param array $params optional
     * @return string
     */
    public function buildBucketKeyPath(\riiak\Bucket $bucket, $key = NULL, array $links = NULL, array $params = NULL) {
        $path = '/' . $this->client->keyPrefix;

        /**
         * Add key
         */
        if (!is_null($key)) {
            $path .= '/' . urlencode($key);

            /**
             * Add params for link walking
             * bucket, tag, keep
             */
            if (!is_null($links))
                foreach ($links as $el)
                    $path .= '/' . urlencode($el[0]) . ',' . urlencode($el[1]) . ',' . $el[2];
        }

        return $this->buildBucketPath($bucket, $path, $params);
    }

    /**
     * Builds URL for Riak bucket query
     *
     * @param \riiak\Bucket $bucket optional
     * @param string $appendPath optional
     * @param array $params optional
     * @return string
     */
    public function buildBucketPath(\riiak\Bucket $bucket = NULL, $appendPath = NULL, array $params = NULL) {
        /**
         * Build http[s]://hostname:port/buckets[/bucket[/keys[/key]]]
         */
        $path = '/' . $this->client->bucketPrefix;

        /**
         * Add bucket
         */
        if (!is_null($bucket) && $bucket instanceof \riiak\Bucket)
            $path .= '/' . urlencode($bucket->name);

        /**
         * Return constructed URL (Riak API Path)
         */
        return $this->buildUri($path . $appendPath, $params);
    }

    /**
     * Generic method for building uri
     *
     * @param string $path
     * @param array $params optional
     * @return string
     */
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
     * @param string $content
     * @return array
     */
    public function processRequest($method, $url, array $requestHeaders = array(), $content = '') {
        try {
            /**
             * Process http request using processing method (Curl,fopen etc).
             */
            $responseData = $this->sendRequest($method, $url, $requestHeaders, $content);

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
        $response = $this->processRequest('GET', $this->buildUri('/' . $this->client->pingPrefix));
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
        $url = $this->buildBucketPath(null, null, array('buckets' => 'true'));

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
     * Get (fetch) multiple objects
     *
     * @param array $urls
     * @param array $requestHeaders
     * @param string $content
     * @return array
     */
    abstract public function multiGet(array $urls, array $requestHeaders = array(), $content = '');

    /**
     * Populates the object. Only for internal use
     *
     * @param \riiak\Object $object
     * @param \riiak\Bucket $bucket
     * @param array $response Output of transport layer processing
     * @param string $action Action label (used to fetch expected statuses)
     * @return \riiak\Object
     */
    public function populate(\riiak\Object $object, \riiak\Bucket $bucket, array $response = array(), $action = '') {
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
            throw new Exception('Could not contact Riak Server: ' . $this->baseUrl() . '!');

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
        $response = $this->processRequest('GET', $this->buildUri('/' . $this->client->statsPrefix));
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