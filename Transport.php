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
     * Request processing method
     * 
     * @var string Default:Curl
     */
    private $_processMethod = 'Curl';

    /**
     * Riiak client
     * 
     * @var \riiak\Riiak
     */
    public $client;

    /**
     * Object of processing method
     * 
     * @var object 
     */
    public $_objProcessMethod;

    /**
     * Initialise processing method object.
     */
    public function __construct(Riiak $client) {
        $this->client = $client;
        /**
         * Check whether processing method object exists or not.
         */
        if (!is_object($this->_objProcessMethod)) {
            $this->_objProcessMethod = $this->getProcessingObject();
        }
    }

    /**
     * Builds URL to connect to Riak server
     *
     * @return string
     */
    abstract public function buildUrl();
    
    /**
     * Builds a REST URL to access Riak API
     *
     * @param object $objBucket
     * @param string $key
     * @param string $spec
     * @param array $params
     * @return string
     */
    abstract public function buildRestPath(Bucket $objBucket = NULL, $key = NULL, $spec = NULL, array $params = NULL);

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
     * Return processing method object either CURL, PHP stream or fopen.
     * 
     * @param string $strMethod
     * @return object  
     */
    protected function getProcessingObject($strMethod = NULL) {
        switch ($strMethod) {
            default:
            case 'Curl':
                /**
                 * Return CURL as processing method object.
                 */
                return new http\Curl($this->client);
                break;
            case 'fopen':
                break;
        }
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
    abstract public function get(Bucket $objBucket = NULL, array $params = array(), $key = null, $spec = null);
    
    /**
     * Get (fetch) multiple objects
     * @param array $urls
     * @param array $requestHeaders
     * @param string $obj 
     */
    abstract public function multiGet(array $urls, array $requestHeaders = array(), $obj = '');

    /**
     * Put (save) an object
     * 
     * @param \riiak\Bucket $objBucket
     * @param array $params
     * @return array $response
     */
    abstract public function put(Bucket $objBucket = NULL, $headers = NULL, $contents = '');

    /**
     * Method to store object in Riak.
     * 
     * @param string $url
     * @param array $params
     * @param string $headers
     * @return array 
     */
    abstract public function post($url = NULL, array $params = array(), $headers = '');

    /**
     * Method to delete object in Riak.
     * 
     * @param string $url
     * @param array $params
     * @param string $headers
     * @return array 
     */
    abstract public function delete(Bucket $objBucket = NULL, $key = '', array $params = array(), $headers = '');

    /**
     * Executes request, returns named array(headers, body) of request, or null on error
     *
     * @param string $method GET|POST|PUT|DELETE
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
    
}