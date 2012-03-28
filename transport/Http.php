<?php

namespace riiak\transport;

use \CJSON,
\Exception,
\Yii,
\CLogger;

/**
 * The Http object allows you to perform all Riak operation
 * through HTTP protocol
 *
 * @package            riiak.transport
 *
 * @abstract
 *
 * @method array get   () get(string $url, array $requestHeaders = array(), string $content = '') Alias for processRequest('GET', ...)
 * @method array post  () post(string $url, array $requestHeaders = array(), string $content = '') Alias for processRequest('POST', ...)
 * @method array put   () put(string $url, array $requestHeaders = array(), string $content = '') Alias for processRequest('PUT', ...)
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

    public function listBuckets() {
        /**
         * Construct URL
         */
        $url = $this->buildBucketPath(null, null, array('buckets' => 'true'));
        Yii::trace('Running listBuckets request', 'ext.riiak.transport.http.listBuckets');

        /**
         * Run the request
         */
        $response = $this->processRequest('GET', $url);
        $this->validateResponse($response, 'listBuckets');

        /**
         * Return array of bucket names
         */
        $body = (array)CJSON::decode($response['body']);
        if (isset($body['buckets']) && is_array($body['buckets']))
            return array_map('urldecode', array_unique($body['buckets']));
        return array();
    }

    public function listBucketKeys(\riiak\Bucket $bucket) {
        /**
         * Fetch the bucket
         */
        Yii::trace('Running listKeys request for bucket "' . $bucket->name . '"', 'ext.riiak.transport.http.listKeys');
        $bucketArr = $this->getBucket($bucket, array('props' => 'false', 'keys' => 'stream'));

        if (isset($bucketArr['keys']) && is_array($bucketArr['keys']))
            return array_map('urldecode', array_unique($bucketArr['keys']));
        return array();
    }

    public function listBucketProps(\riiak\Bucket $bucket) {
        /**
         * Fetch the bucket
         */
        Yii::trace('Running listProps request for bucket "' . $bucket->name . '"', 'ext.riiak.transport.http.listProps');
        $bucketArr = $this->getBucket($bucket, array('props' => 'true', 'keys' => 'false'));

        if (isset($bucketArr['props']) && is_array($bucketArr['props']))
            return array_map('urldecode', array_unique($bucketArr['props']));
        return array();
    }

    public function getBucket(\riiak\Bucket $bucket, array $params = array()) {
        /**
         * Construct the URL
         */
        $url = $this->buildBucketKeyPath($bucket, null, null, $params);
        Yii::trace('Running getBucket request for bucket "' . $bucket->name . '"', 'ext.riiak.transport.http.getBucket');

        /**
         * Run the request.
         */
        $response = $this->processRequest('GET', $url);
        $this->validateResponse($response, 'getBucket');

        /**
         * Return decoded bucket array
         */
        return (array)CJSON::decode($response['body']);
    }

    public function setBucket(\riiak\Bucket $bucket, array $properties) {
        /**
         * Construct the contents
         */
        $headers = array('Content-Type: application/json');
        $content = CJSON::encode(array('props' => $properties));

        /**
         * Construct the request URL.
         */
        $url = $this->buildBucketKeyPath($bucket);
        Yii::trace('Running setBucket request for bucket "' . $bucket->name . '"', 'ext.riiak.transport.http.setBucket');

        /**
         * Process request & return response
         */
        $response = $this->processRequest('PUT', $url, $headers, $content);
        $this->validateResponse($response, 'setBucket');

        return true;
    }

    public function fetchObject(\riiak\Bucket $bucket, $key, array $params = null) {
        /**
         * Construct the URL
         */
        $url = $this->buildBucketKeyPath($bucket, $key, null, $params);
        Yii::trace('Running fetchObject request for bucket "' . $bucket->name . '"', 'ext.riiak.transport.fetchObject');

        /**
         * Process request.
         */
        $response = $this->processRequest('GET', $url);
        try {
            $this->validateResponse($response, 'fetchObject');
        }catch(\Exception $e) {
            /**
             * Allow 404 for missing objects
             * @todo Perhaps 404 should still toss exception?
             */
            if($e->getCode()!='404')
                throw $e;
        }

        /**
         * Return response
         */
        return $response;
    }

    public function storeObject(\riiak\Object $object, array $params = array()) {
        /**
         * Construct the URL
         */
        $url    = $this->buildBucketKeyPath($object->bucket, $object->key, null, $params);
        Yii::trace('Running storeObject request for bucket "'. $object->bucket->name .'", object with key "' . $object->key . '"', 'ext.riiak.transport.storeObject');

        /**
         * Construct the headers
         */
        $headers = array('Accept: text/plain, */*; q=0.5',
            'Content-Type: ' . $object->getContentType(),
            'X-Riak-ClientId: ' . $object->client->clientId);

        /**
         * Add the vclock if it exists
         */
        if (!empty($object->vclock))
            $headers[] = 'X-Riak-Vclock: ' . $object->vclock;

        /**
         * Add the Links
         */
        foreach ($object->links as $link)
            $headers[] = 'Link: ' . $link->toLinkHeader($object->client);

        /**
         * Add the auto indexes
         */
        if (is_array($object->autoIndexes) && !empty($object->autoIndexes)) {
            if (!is_array($object->data))
                throw new Exception('Auto index feature requires that "$object->data" be an array.');

            $collisions = array();
            foreach ($object->autoIndexes as $index => $fieldName) {
                $value = null;
                // look up the value
                if (isset($object->data[$fieldName])) {
                    $value     = $object->data[$fieldName];
                    $headers[] = 'X-Riak-Index-' . $index . ': ' . urlencode($value);

                    // look for value collisions with normal indexes
                    if (isset($object->indexes[$index]))
                        if (false !== array_search($value, $object->indexes[$index]))
                            $collisions[$index] = $value;
                }
            }

            $object->meta['client-autoindex']          = count($object->autoIndexes) > 0 ? CJSON::encode($object->autoIndexes) : null;
            $object->meta['client-autoindexcollision'] = count($collisions) > 0 ? CJSON::encode($collisions) : null;
        }

        /**
         * Add the indexes
         */
        foreach ($object->indexes as $index => $values)
            if (is_array($values))
                $headers[] = 'X-Riak-Index-' . $index . ': ' . implode(', ', array_map('urlencode', $values));

        /**
         * Add the metadata
         */
        foreach ($object->meta as $metaName => $metaValue)
            if ($metaValue !== null)
                $headers[] = 'X-Riak-Meta-' . $metaName . ': ' . $metaValue;

        if ($object->jsonize)
            $content = CJSON::encode($object->data);
        else
            $content = $object->data;

        /**
         * Run the operation
         */
        if ($object->key !== null) {
            Yii::trace('Storing object with key "' . $object->key . '" in bucket "' . $object->bucket->name . '"', 'ext.riiak.Object');
            $response = $this->put($url, $headers, $content);
        } else {
            Yii::trace('Storing new object in bucket "' . $object->bucket->name . '"', 'ext.riiak.Object');
            $response = $this->post($url, $headers, $content);
        }

        $action = array('storeObject');
        if (isset($params['returnbody']) && $params['returnbody'])
            array_push($action, 'fetchObject');
        $this->validateResponse($response, $action);

        return $response;
    }

    public function deleteObject(\riiak\Object $object, array $params = array()) {
        /**
         * Construct the URL
         */
        $url = $this->buildBucketKeyPath($object->bucket, $object->key, null, $params);
        Yii::trace('Running deleteObject request for object "' . $object->key . '"', 'ext.riiak.transport.deleteObject');

        /**
         * Process request.
         */
        $response = $this->processRequest('DELETE', $url);
        $this->validateResponse($response, 'deleteObject');

        /**
         * Return response
         */
        return $response;
    }

    /**
     * @todo Handle multipart/mixed response from linkwalk
     */
    public function linkWalk(\riiak\Bucket $bucket, $key, array $links, array $params = null) {
        /**
         * Construct the URL
         */
        $url = $this->buildBucketKeyPath($bucket, $key, $links, $params);
        Yii::trace('Running linkWalk request for object "' . $key . '"', 'ext.riiak.transport.linkWalk');

        /**
         * Process request.
         */
        $response = $this->processRequest('GET', $url);
        $this->validateResponse($response, 'linkWalk');

        /**
         * Return response
         */
        return $response;
    }

    public function mapReduce() {
        /**
         * @todo Build out this function
         */
    }

    public function secondaryIndex() {
        /**
         * @todo Build out this function
         */
    }

    public function ping() {
        Yii::trace('Pinging Riak server', 'ext.riiak.transport.http.ping');
        $response = $this->processRequest('GET', $this->buildUri('/' . $this->client->pingPrefix));
        $this->validateResponse($response, 'ping');

        return ($response !== NULL) && ($response['body'] == 'OK');
    }

    public function status() {
        /**
         * @todo implement
         */
    }

    public function listResources() {
        /**
         * @todo implement
         */
    }

    /**
     * Get status handling class object
     *
     * @return object http\Status
     */
    public function getStatusObject() {
        /**
         * Check for existing status handling class object
         */
        if (!is_object($this->status))
            $this->status = new http\Status();

        /*
         * Return status class object
         */
        return $this->status;
    }

    /**
     * Validate Riak response using http\Status class
     *
     * @param array  $response
     * @param string|array $action Action or array of possible actions for which any related status is valid
     * @throws \Exception
     */
    public function validateResponse($response, $action) {
        $action = (array) $action;
        $statusObject = $this->getStatusObject();
        $validated = array_filter($action, function($action)use($response, $statusObject){
            return $statusObject->validateStatus($response, $action);
        });

        /**
         * If $validated is empty, no status was valid...
         */
        if($validated == array()) {
            $httpCode = $response['headers']['http_code'];
            $httpStatus = $response['headers']['http_status'];

            $errorMsg = (is_array($httpStatus) ? implode(', ', $httpStatus) : $httpStatus) . ' - ';
            /**
             * Check for error definitions
             */
            $actionErrors = array_map(function($action)use($statusObject, $httpCode){
                $errorMsg = $action.' failed with reason: ';
                if (array_key_exists($httpCode, $statusObject->errorCodes[$action]))
                    $errorMsg .= $statusObject->errorCodes[$action][$httpCode];
                else
                    $errorMsg .= 'An undefined error has occurred!';
                return $errorMsg;
            }, $action);

            $errorMsg .= implode(' -OR- ', $actionErrors);

            throw new Exception($errorMsg, $httpCode);
        }
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
     * @param string $appendPath optional
     * @param array  $params     optional
     *
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
     * @param string        $index  Index name and type (e.g. - 'indexName_bin')
     * @param string|int    $start  Starting value or exact match if no end value
     * @param string|int    $end    optional Ending value for range search
     * @param array         $params optional Any extra query parameters
     *
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
     * @param string        $key    optional
     * @param array         $links  optional
     * @param array         $params optional
     *
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
     * @param \riiak\Bucket $bucket     optional
     * @param string        $appendPath optional
     * @param array         $params     optional
     *
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
     * @param array  $params optional
     *
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
     * Returns process headers along with status code & body
     *
     * @param int    $httpCode
     * @param string $headers
     * @param string $body
     *
     * @return array[headers,body]
     */
    public function processResponse($httpCode, $headers, $body) {
        $headers = $this->processHeaders($headers);
        $headers = array_merge(array('http_code' => $httpCode), array_change_key_case($headers, CASE_LOWER));
        return array('headers' => $headers, 'body' => $body);
    }

    /**
     * Parse HTTP header string into an assoc array
     *
     * @param string $headers
     *
     * @return array
     */
    public function processHeaders($headers) {
        $retVal = array();
        $fields = array_filter(explode("\r\n", preg_replace('/\x0D\x0A[\x09\x20]+/', ' ', $headers)));
        foreach ($fields as $field) {
            if (preg_match('@^HTTP/1\.1\s+(\d+\s+[\w\s]+)@', $field, $match)) {
                if (isset($retVal['http_status']))
                    $retVal['http_status'] = array_merge((array)$retVal['http_status'], (array)trim($match[1]));
                else
                    $retVal['http_status'] = trim($match[1]);
            } elseif (preg_match('/([^:]+): (.+)/m', $field, $match)) {
                $match[1] = preg_replace('/(?<=^|[\x09\x20\x2D])./e', 'strtoupper("\0")', strtolower(trim($match[1])));
                if (isset($retVal[$match[1]]))
                    $retVal[$match[1]] = array_merge((array)$retVal[$match[1]], (array)trim($match[2]));
                else
                    $retVal[$match[1]] = trim($match[2]);
            }
        }
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
     * Get (fetch) multiple objects
     *
     * @param array  $urls
     * @param array  $requestHeaders
     * @param string $content
     *
     * @return array
     */
    abstract public function multiGet(array $urls, array $requestHeaders = array(), $content = '');

    /**
     * Populates the object. Only for internal use
     *
     * @param \riiak\Object $object
     * @param array         $response Output of transport layer processing
     *
     * @return \riiak\Object
     */
    public function populate(\riiak\Object $object, array $response = array()) {
        $object->clear();

        /**
         * Update the object
         */
        $object->headers = $response['headers'];
        $object->data    = $response['body'];

        /**
         * If 404 (Not Found), then clear the object
         */
        if ($object->httpCode == 404) {
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
        if ($object->httpCode == 300) {
            $siblings = explode("\n", trim($object->data));
            array_shift($siblings); # Get rid of 'Siblings:' string.
            $object->siblings = $siblings;
            $object->exists   = true;
            return $object;
        }elseif ($object->httpCode == 201) {
            $pathParts   = explode('/', $object->headers['location']);
            $object->key = array_pop($pathParts);
        }

        /**
         * Possibly JSON decode
         */
        if (($object->httpCode == 200 || $object->httpCode == 201) && $object->jsonize)
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
        $arrConfiguration = $this->client->getConfiguration();

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
        $arrConfiguration = $this->client->getConfiguration();

        /**
         * Check riak supports leveldb or not
         */
        if ($arrConfiguration['storage_backend'] == 'riak_kv_eleveldb_backend')
            return true;

        return false;
    }

}