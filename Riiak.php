<?php

namespace riiak;

use \CApplicationComponent,
    \CJSON,
    \Yii;

/**
 * The Riiak object holds information necessary to connect to
 * Riak. The Riak API uses HTTP, so there is no persistent
 * connection, and the Riiak object is extremely lightweight.
 * @package riiak
 */
class Riiak extends CApplicationComponent {

    /**
     * Hostname or IP address
     *
     * @var string Default: '127.0.0.1'
     */
    public $host = '127.0.0.1';

    /**
     * Port number
     *
     * @var int Default: 8098
     */
    public $port = 8098;

    /**
     * Whether SSL is enabled
     *
     * @var bool Default: false
     */
    public $ssl = false;

    /**
     * Interface prefix
     *
     * @var string Default: 'riak'
     */
    public $prefix = 'riak';
    
    /**
     * Riak key stream url prefix
     * 
     * @var string Default: 'buckets' 
     */
    public $bucketPrefix = 'buckets';
    
    /**
     * Riak key for secondary indexes implementation
     * 
     * @var string Default: 'index' 
     */
    public $SIPrefix = 'index';
    
    /**
     * Riak key stream prefix
     * 
     * @var string 
     */
    public $keyPrefix = 'keys';
    
    /**
     * Secondary Index equal operator
     */
    public $SIEqual = 'eq';

    /**
     * MapReduce prefix
     *
     * @var string Default: 'mapred'
     */
    public $mapredPrefix = 'mapred';

    /**
     * The clientID for this Riak client instance.
     * Only specify if you know what you're doing.
     *
     * @var string
     */
    public $clientId;

    /**
     * R-Value setting for client.
     * Used by other Riiak class components as fallback value.
     *
     * @var int Default: 2
     */
    public $r = 2;

    /**
     * W-Value setting for client.
     * Used by other Riiak class components as fallback value.
     *
     * @var int Default: 2
     */
    public $w = 2;

    /**
     * DW-Value setting for client.
     * Used by other Riiak class components as fallback value.
     *
     * @var int Default: 2
     */
    public $dw = 2;

    /**
     * When enabled, profiles requests using Yii::beginProfile && Yii::endProfile
     *
     * @var bool
     */
    public $enableProfiling = false;

    /**
     * @var \riiak\MapReduce
     */
    protected $_mr;
    
    /**
     *
     * @var \riiak\SecondaryIndexes 
     */
    protected $_sIndex;
    
    /**
     * Defines whether to use secondary index or not.
     *  
     * @var bool Default:false
     */
    public $_useSecondaryIndex = true;
    
    /**
     * Riak configuration details
     * 
     * @var array 
     */
    public $_riakConfiguration;

    /**
     *  Create transport layer object
     * 
     * @var object 
     */
    public $_transport;
    
    /**
     * Define transport layer protocol
     * 
     * @var string Default: http
     */
    public $_TLProtocol = 'HTTP';
    
    /**
     * Define transport layer processing method.
     * 
     * @var string Default:Curl
     */
    public $_TLProcessingMethod = 'CURL';
    
    /**
     * Initialise Riiak
     */
    public function init() {
        parent::init();
        /**
         * Default the value of clientId if not already specified
         */
        if (empty($this->clientId))
            $this->clientId = 'php_' . base64_encode(rand(1, 1073741824));
        /**
         * Check if transport layer object is present or not.
         */
        if(!is_object($this->_transport)){
            /**
             * Create transport layer object.
             */
            $this->_transport = $this->createTLObject($this);
        }
    }
    
    /**
     * Method to create transport layer object as per the protocol.
     * 
     * @param object $objClient
     * @return Object Transport Layer object
     */
    public static function createTLObject(Riiak $objClient){
        switch($objClient->_TLProtocol){
            default:
            case 'HTTP':
                /**
                 * Get processing method object for HTTP protocol.
                 */
                switch($objClient->_TLProcessingMethod){
                    default:
                    case 'CURL':
                         return new \riiak\transport\http\Curl($objClient);
                        break;
                    case 'FOPEN':
                        break;
                    case 'PHPSTREAM':
                        break;
                }
                break;
            case 'PBC':
                /**
                 * Protocol Buffer Transport layer class object.
                 */
                break;
        }
    }

    /**
     * Get bucket by name
     *
     * @param string $name
     * @return \riiak\Bucket
     */
    public function bucket($name) {
        return new Bucket($this, $name);
    }

    /**
     * Return array of Bucket objects
     *
     * @return array
     */
    public function buckets() {
        Yii::trace('Fetching list of buckets', 'ext.riiak.Riiak');
        $buckets = array();
        $buckets = $this->_transport->getBuckets($this);
        return $buckets; 
    }

    /**
     * Check if Riak server is alive
     *
     * @return bool
     */
    public function getIsAlive() {
        Yii::trace('Pinging Riak server', 'ext.riiak.Riiak');
        return $this->_transport->getBuckets($this);
    }

    /**
     * Returns the MapReduce instance (created if not exists)
     *
     * @param bool $reset Whether to create a new MapReduce instance
     * @return \riiak\MapReduce
     */
    public function getMapReduce($reset = false) {
        if ($reset || !($this->_mr instanceof MapReduce))
            $this->_mr = new MapReduce($this);
        return $this->_mr;
    }
    
    /**
     * Returns the SecondaryIndex instance (created if not exists)
     *
     * @param bool $reset Whether to create a new SecondaryIndex instance
     * @return \riiak\SecondaryIndex
     */
    public function getSecondaryIndexObject($reset = false) {
        if ($reset || !($this->_sIndex instanceof MapReduce))
            $this->_sIndex = new SecondaryIndexes ($this);
        return $this->_sIndex;
    }
    
    /**
     * Check whether riak supports multi-backend or not.
     * 
     * @return bool
     */
    public function getIsMultiBackendSupport(){
        Yii::trace('Checking multi-backend support', 'ext.riiak.Riiak');
        return $this->_transport->getIsMultiBackendSupport();
    }
    
    /**
     *  Check whether riak supports secondary index or not.
     * 
     * @return bool 
     */
    public function getIsSecondaryIndexSupport(){
        Yii::trace('Checking Secondary Index support', 'ext.riiak.Riiak');
        return $this->_transport->getIsSecondaryIndexSupport();
    }
}