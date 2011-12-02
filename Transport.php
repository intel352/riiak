<?php

namespace riiak;

use \CComponent,
    \CJSON,
    \Exception,
    \Yii;

/**
 * Contains transport layer actions of Riak
 * @package riiak
 */
abstract class Transport extends CComponent {

    /**
     * Riiak client
     *
     * @var \riiak\Riiak
     */
    public $client;

    /**
     * Initialise processing method object.
     * @param \riiak\Riiak $client
     */
    public function __construct(\riiak\Riiak $client) {
        $this->client = $client;
    }

    /**
     * Builds URL to connect to Riak server
     *
     * @return string
     */
    #abstract public function buildUrl();

    /**
     * Builds a REST URL to access Riak API
     *
     * @param object $bucket
     * @param string $key
     * @param string $spec
     * @param array $params
     * @return string
     */
    #abstract public function buildRestPath(Bucket $bucket = NULL, $key = NULL, $spec = NULL, array $params = NULL);

    /**
     * Return array of Bucket objects
     *
     * @return array
     */
    abstract public function getBuckets();

    /**
     * Check if Riak server is alive
     *
     * @return bool
     */
    abstract public function getIsAlive();

    /**
     * Get (fetch) an object
     *
     * @param \riiak\Bucket $bucket
     * @param array $params
     * @param string $key
     * @param string $spec
     * @return array
     */
    #abstract public function get(Bucket $bucket = NULL, array $params = array(), $key = null, $spec = null);

    /**
     * Put (save) an object
     *
     * @param \riiak\Bucket $bucket
     * @param array $params
     * @return array $response
     */
    #abstract public function put(Bucket $bucket = NULL, $headers = NULL, $contents = '');

    /**
     * Method to store object in Riak.
     *
     * @param string $url
     * @param array $params
     * @param string $headers
     * @return array
     */
    #abstract public function post($url = NULL, array $params = array(), $headers = '');

    /**
     * Method to delete object in Riak.
     *
     * @param string $url
     * @param array $params
     * @param string $headers
     * @return array
     */
    #abstract public function delete(Bucket $bucket = NULL, $key = '', array $params = array(), $headers = '');

    /**
     * Executes request, returns named array(headers, body) of request, or null on error
     *
     * @param 'GET'|'POST'|'PUT'|'DELETE' $method
     * @param string $url
     * @param array $requestHeaders
     * @param string $obj
     * @return array|null
     */
    abstract public function processRequest($method, $url, array $requestHeaders = array(), $obj = '');

    /**
     * Parse header string into an assoc array
     *
     * @param string $headers
     * @return array
     */
    abstract public function processHeaders($headers);

    /**
     * Get riak configuration
     *
     * @return array
     */
    abstract public function getRiakConfiguration();

    /**
     * Check riak supports multi-backend or not.
     *
     * @return bool
     */
    abstract public function getIsMultiBackendSupport();

    /**
     * Check riak supports secondary index or not.
     *
     * @return bool
     */
    abstract public function getIsSecondaryIndexSupport();

    /**
     * Method to validate riak response
     *
     * @return bool
     */
    abstract public function validateResponse($response, $action);

    /**
     * Get staus handling class object
     *
     * @return object http\Status
     */
    abstract public function getStatusObject();
}