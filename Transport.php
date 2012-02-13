<?php

namespace riiak;

use \CComponent,
    \CJSON,
    \Exception,
    \Yii;

/**
 * Contains transport layer actions of Riak
 * @package riiak
 *
 * @property-read bool $isAlive
 * @property-read array $riakConfiguration
 * @property-read \riiak\transport\Status $statusObject
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
     * Return array of Bucket names
     *
     * @return array
     */
    abstract public function listBuckets();

    /**
     * Return array of Bucket's object keys
     *
     * @return array
     */
    abstract public function listBucketKeys(\riiak\Bucket $bucket);
    abstract public function listBucketProps(\riiak\Bucket $bucket);

    /**
     * Fetches Bucket object
     *
     * @return array
     */
    abstract public function getBucket(\riiak\Bucket $bucket, array $params = array());

    /**
     * Updates (sets) Bucket object
     *
     * @return array
     */
    abstract public function setBucket(\riiak\Bucket $bucket, array $properties);

    abstract public function fetchObject(\riiak\Bucket $bucket, $key, array $params = null);

    abstract public function storeObject(\riiak\Object $object, array $params = array());

    abstract public function deleteObject(\riiak\Object $object, array $params = array());

    abstract public function linkWalk(\riiak\Bucket $bucket, $key, array $links, array $params = null);

    abstract public function mapReduce();

    abstract public function secondaryIndex();

    abstract public function ping();

    abstract public function status();

    abstract public function listResources();

    /**
     * Check if Riak server is alive
     *
     * @return bool
     */
    abstract public function getIsAlive();

    /**
     * Executes request, returns named array(headers, body) of request, or null on error
     *
     * @param 'GET'|'POST'|'PUT'|'DELETE' $method
     * @param string $url
     * @param array $requestHeaders optional
     * @param string $content optional
     * @return array|null
     */
    abstract public function processRequest($method, $url, array $requestHeaders = array(), $content = '');

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
     * @param string $response
     * @param string|array $action
     * @throws \Exception
     */
    abstract public function validateResponse($response, $action);

    /**
     * Get status handling class object
     *
     * @return object http\Status
     */
    abstract public function getStatusObject();
}