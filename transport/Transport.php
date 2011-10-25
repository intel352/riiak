<?php

namespace riiak\transport;

use \CComponent,
    \CJSON,
    \Exception,
    \Yii;

/**
 * Contains transport layer actions of Riak
 * @package http
 */
abstract class Transport extends CComponent {
    /**
     * Request processing method
     * 
     * @var string Default:Curl
     */
    private $_processMethod = 'Curl';
    
    /**
     * Riiak object
     * 
     * @var object
     */
    public $_client;
    
    /**
     * Object of processing method
     * 
     * @var object 
     */
    public $_objProcessMethod;
    
    /**
     * Initialise processing method object.
     */
    public function __construct(){
        /**
         * Check whether processing method object is exits or not.
         */
        if(!is_object($this->_objProcessMethod)){
            $this->_objProcessMethod = $this->getProcessingObject();
        }
    }
    
    /**
     * Method to set Riiak object
     * @param \riiak\Riiak $objRiiak 
     */
    public function setClient(\riiak\Riiak $objRiiak){
        $this->_client = $objRiiak;
    }
    /**
     * Builds URL to connect to Riak server
     *
     * @param object $objClient
     * @return string
     */
    abstract public function buildUrl(\riiak\Riiak $objClient);
    
    /**
     * Return array of Bucket objects
     *
     * @return array
     */
    abstract public function getBuckets(\riiak\Riiak $objClient);
    
    /**
     * Check if Riak server is alive
     *
     * @return bool
     */
    abstract public function getIsAlive(\riiak\Riiak $objClient);
    
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
                return new http\Curl();
                break;
            case 'fopen':
                break;
            default:
                /**
                 * Default: return CURL as request processing method.
                 */
                return new http\Curl();
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
    abstract public function get(\riiak\Riiak $objClient, \riiak\Bucket $objBucket = NULL, array $params = array(), $key = null, $spec = null);
    
    /**
     * Method to set multiple bucket properties in one call.
     * 
     * @param \riiak\Riiak $objClient
     * @param \riiak\Bucket $objBucket
     * @param array $params
     * @return array $response
     */
    abstract public function put(\riiak\Riiak $objClient, \riiak\Bucket $objBucket = NULL, $headers = NULL, $contents = '' );
    
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
    abstract public function buildRestPath(\riiak\Riiak $objClient, \riiak\Bucket $objBucket = NULL, $key = NULL, $spec = NULL, array $params = NULL);
    
    /**
     * Executes request, returns named array(headers, body) of request, or null on error
     *
     * @param object $client
     * @param string $method GET|POST|PUT|DELETE
     * @param string $url
     * @param array $requestHeaders
     * @param string $obj
     * @return array|null
     */
    abstract public function processRequest(\riiak\Riiak $client, $method, $url, array $requestHeaders = array(), $obj = '');
    
    /**
     * Parse HTTP header string into an assoc array
     *
     * @param string $headers
     * @return array
     */
    abstract public static function parseHttpHeaders($headers);
    
    /**
     * Method to store object in Riak.
     * 
     * @param \riiak\Riiak $objClient
     * @param string $url
     * @param array $params
     * @param string $headers
     * @return array 
     */
    abstract public function post(\riiak\Riiak $objClient, $url = NULL, array $params = array(), $headers = '');
}
