<?php

namespace riiak;

use \CComponent,
    \CJSON,
    \Exception,
    \Yii;

/**
 * The Backend object holds information about necessary inputs for Secondary index(2i) 
 * and map/reduce operations on Riak.
 * @package riiak
 *
 * @abstract
 */
abstract class Backend extends CComponent{
    
    /**
     * @var \riiak\Riiak A Riak client object
     */
    public $client;

    /**
     * Phases to performs map/reduce operations
     * 
     * @var array
     */
    public $phases = array();

    /**
     * Bucket name (string) or array of inputs
     * If bucket name, then all keys of bucket will be used as inputs (expensive)
     *
     * @var string|array
     */
    public $inputs = array();

    /**
     * Input mode specifies operating mode (e.g Bucket)
     * 
     * @var string
     */
    public $inputMode;

    /**
     * List of key filters used for map/reduce and 
     * Secondary index(2i) implementation operations on Riak
     * 
     * @var array
     */
    public $keyFilters = array();
    
    /**
     * Construct a new Object
     *
     * @param \riiak\Riiak $client A Riiak object
     */
    public function __construct(Riiak $client) {
        $this->client = $client;
    }
    
    /**
     * Set key filters for map/reduce and secondary index(2i)
     * operations
     * 
     * @abstract
     * @param array $filter List of key filters
     */
    abstract public function keyFilterAnd(array $filter);
    
    /**
     * Set key filters for map/reduce operations
     * 
     * @abstract
     * @param array $filter List of key filters
     */
    abstract public function keyFilterOr(array $filter);
    
    /**
     * Run map/reduce or Secondary Index operation,
     * Call transport layer methods for riak connection, handle response and
     * return result-set.
     * 
     * @abstract
     * @param string $timeout
     */
    abstract public function run($timeout = null);
}
?>
