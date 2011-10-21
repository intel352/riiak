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
     * Riak key stream prefix
     * 
     * @var string 
     */
    public $keyPrefix = 'keys';

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
    
    public function init() {
        parent::init();
        /**
         * Default the value of clientId if not already specified
         */
        if (empty($this->clientId))
            $this->clientId = 'php_' . base64_encode(rand(1, 1073741824));
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
        return Transport::buckets($this);        
    }

    /**
     * Check if Riak server is alive
     *
     * @return bool
     */
    public function getIsAlive() {
        return Transport::getIsAlive($this);
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

}