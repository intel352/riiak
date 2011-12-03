<?php

namespace riiak;

use \CComponent,
    \CJSON,
    \Exception,
    \Yii;

/**
 * The Bucket object allows you to access and change information
 * about a Riak bucket, and provides methods to create or retrieve
 * objects within the bucket.
 * @package riiak
 *
 * @property array $properties
 * @property bool $allowMultiples
 * @property int $dw
 * @property int $nVal
 * @property int $r
 * @property int $w
 *
 * @property-read array $keys
 */
class Bucket extends CComponent {

    /**
     * Client instance
     *
     * @var \riiak\Riiak
     */
    public $client;

    /**
     * Bucket name
     *
     * @var string
     */
    public $name;

    /**
     * R-Value
     *
     * @var int
     */
    protected $_r;

    /**
     * W-Value
     *
     * @var int
     */
    protected $_w;

    /**
     * DW-Value
     *
     * @var int
     */
    protected $_dw;

    /**
     * @var array
     */
    protected $_properties;

    /**
     * @var array
     */
    protected $_keys;

    public function __construct(Riiak $client, $name) {
        $this->client = $client;
        $this->name = $name;
    }

    /**
     * Get the R-Value for this bucket, if set. Falls back to client value.
     *
     * @param int $r Optional: The R-Value to be returned if not null
     * @return int
     */
    public function getR($r = null) {
        if ($r != null)
            return $r;
        if ($this->_r != null)
            return $this->_r;
        return $this->client->r;
    }

    /**
     * Set the R-value for this bucket.
     * get/getBinary operations may use this value
     *
     * @param int $r The new R-Value
     * @return \riiak\Bucket
     */
    public function setR($r) {
        $this->_r = $r;
        return $this;
    }

    /**
     * Get the W-Value for this bucket, if set. Falls back to client value.
     *
     * @param int $w Optional: The W-Value to be returned if not null
     * @return int
     */
    public function getW($w) {
        if ($w != null)
            return $w;
        if ($this->_w != null)
            return $this->_w;
        return $this->client->w;
    }

    /**
     * Set the W-Value for this bucket
     * get/getBinary operations may use this value
     *
     * @param int $w The new W-Value
     * @return \riiak\Bucket
     */
    public function setW($w) {
        $this->_w = $w;
        return $this;
    }

    /**
     * Get the DW-Value for this bucket, if set. Falls back to client value.
     *
     * @param int $dw Optional: The DW-Value to be returned if not null
     * @return int
     */
    public function getDW($dw) {
        if ($dw != null)
            return $dw;
        if ($this->_dw != null)
            return $this->_dw;
        return $this->client->dw;
    }

    /**
     * Set the DW-Value for this bucket
     * get/getBinary operations may use this value
     *
     * @param int $dw The new DW-Value
     * @return \riiak\Bucket
     */
    public function setDW($dw) {
        $this->_dw = $dw;
        return $this;
    }

    /**
     * Create a new Riak object that will be stored as JSON
     *
     * @param string $key optional Key value
     * @param mixed $data optional Data to store (Default: null)
     * @return \riiak\Object
     */
    public function newObject($key = null, $data = null) {
        return $this->newBinary($key, $data, 'application/json', true);
    }

    /**
     * Create a new Riak object that will be stored as Binary
     *
     * @param string $key optional Key value
     * @param mixed $data optional Data to store
     * @param string $contentType optional Content type of the object (Default: application/json)
     * @param bool $jsonize optional Whether to treat the object as JSON (Default: false)
     * @return \riiak\Object
     */
    public function newBinary($key = null, $data = null, $contentType = 'application/json', $jsonize = false) {
        $obj = new Object($this->client, $this, $key);
        $obj->data = $data;
        $obj->contentType = $contentType;
        $obj->jsonize = $jsonize;
        return $obj;
    }

    /**
     * Retrieve a JSON-encoded object from Riak
     *
     * @param string $key Name of the key
     * @param int $r optional R-Value of the request (Default: bucket's R)
     * @return \riiak\Object
     */
    public function get($key, $r = null) {
        return $this->getBinary($key, $r, true);
    }

    /**
     * Retrieves binary/string object from Riak
     *
     * @param string $key Name of the key
     * @param int $r R-Value of the request (Default: bucket's R)
     * @param bool $jsonize Whether to treat the object as JSON (Default: false)
     * @return \riiak\Object
     */
    public function getBinary($key, $r = null, $jsonize = false) {
        $obj = new Object($this->client, $this, $key);
        $obj->jsonize = $jsonize;
        $r = $this->getR($r);
        return $obj->reload($r);
    }

    public function getMulti(array $keys, $r = null) {
        return $this->getMultiBinary($keys, $r, true);
    }

    public function getMultiBinary(array $keys, $r = null, $jsonize = false) {
        $bucket = $this;
        $client = $this->client;
        
        $objects = array_map(function($key)use($jsonize, $client, $bucket) {
                    $obj = new Object($client, $bucket, $key);
                    $obj->jsonize = $jsonize;
                    return $obj;
                }, $keys);
        $r = $this->getR($r);
        return Object::reloadMulti($client, $objects, $r);
    }

    /**
     * Set N-value for this bucket. Controls number replicas of each object
     * that will be written. Set once before writing data to the bucket.
     * Should never change this value from the initially used N-Val, otherwise
     * unexpected results may occur. Only use if you know what you're doing
     *
     * @param int $nval The new N-Val
     */
    public function setNVal($nval) {
        $this->setProperty('n_val', $nval);
    }

    /**
     * Retrieve the N-value for this bucket
     *
     * @return int
     */
    public function getNVal() {
        return $this->getProperty('n_val');
    }

    /**
     * Whether writes can have conflicting data. Detect by calling hasSiblings()
     * and getSiblings(). Only use if you know what you are doing
     *
     * @param bool $bool True to store & return conflicting writes
     */
    public function setAllowMultiples($bool) {
        $this->setProperty('allow_mult', $bool);
    }

    /**
     * Retrieve the 'allow multiples' setting
     *
     * @return bool
     */
    public function getAllowMultiples() {
        return 'true' == $this->getProperty('allow_mult');
    }

    /**
     * Set a bucket property. Only use if you know what you're doing
     *
     * @param string $key
     * @param mixed $value
     */
    public function setProperty($key, $value) {
        $this->setProperties(array($key => $value));
    }

    /**
     * Retrieve a bucket property
     *
     * @param string $key The property to retrieve
     * @return mixed|null
     */
    public function getProperty($key) {
        $props = $this->getProperties();
        if (array_key_exists($key, $props))
            return $props[$key];
        else
            return null;
    }

    /**
     * Set multiple bucket properties in one call. Only use if you know
     * what you're doing
     *
     * @param array $props An associative array of $key=>$value
     * @return Bucket
     */
    public function setProperties(array $props) {
        /**
         * Construct the Contents
         */
        $content = CJSON::encode(array('props' => $props));

        /**
         * Run the request
         */
        Yii::trace('Setting Bucket properties for bucket "' . $this->name . '"', 'ext.riiak.Bucket');
        $headers = array('Content-Type: application/json');
        $response = $this->client->transport->putObject($this, $headers, $content);

        /**
         * Use a Object to interpret the response, we are just interested in the value
         */
        $obj = new Object($this->client, $this);
        $this->client->transport->populate($obj, $this, $response, 'setBucketProperties');

        if (!$obj->exists)
            throw new Exception('Error setting bucket properties.');

        $this->_properties = null;

        return $this;
    }

    /**
     * Retrieve an associative array of all bucket properties
     *
     * @param bool $refresh
     * @return array
     */
    public function getProperties($refresh = false) {
        if ($refresh || !is_array($this->_properties)) {
            Yii::trace('Fetching Bucket properties for bucket "' . $this->name . '"', 'ext.riiak.Bucket');
            $obj = $this->fetchBucketProperties(array('props' => 'true', 'keys' => 'false'));
            if(empty($obj->data['props']))
                return array();
            $this->_properties = $obj->data['props'];
        }
        return $this->_properties;
    }

    /**
     * Retrieve an array of all keys in this bucket
     * Note: this operation is pretty slow
     *
     * @param bool $refresh
     * @return array
     */
    public function getKeys($refresh = false) {
        if ($refresh || !is_array($this->_keys)) {
            Yii::log('Bucket key listing is a very intensive operation, and should never occur in production!', \CLogger::LEVEL_WARNING);
            Yii::trace('Fetching Bucket keys for bucket "' . $this->name . '"', 'ext.riiak.Bucket');
            /**
             * Non-null key param will prompt format of /buckets/BUCKET/keys/
             */
            $obj = $this->fetchBucketProperties(array('props' => 'false', 'keys' => 'stream'), '');
            if (empty($obj->data['keys']))
                return array();
            $this->_keys = array_map('urldecode', array_unique($obj->data['keys']));
        }
        return $this->_keys;
    }

    /**
     * Fetches bucket properties
     *
     * @param array $params
     * @param string $key
     * @return \riiak\Object
     */
    protected function fetchBucketProperties(array $params = array(), $key = null) {
        /**
         * Run the request
         */
        $response = $this->client->transport->getObject($this, $params, $key);

        /**
         * Use a Object to interpret the response, we are just interested in the value
         */
        $obj = new Object($this->client, $this);
        $this->client->transport->populate($obj, $this, $response, 'getBucketProperties');

        if (!$obj->exists)
            throw new Exception('Error getting bucket properties.');

        return $obj;
    }

    /**
     * Search a secondary index
     * @author Eric Stevens <estevens@taglabsinc.com>
     * @param string $name - The name of the index to search
     * @param string $type - The type of index ('int' or 'bin')
     * @param string|int $startOrExact
     * @param string|int $end optional
     * @param bool $dedupe - whether to eliminate duplicate entries if any
     * @return array of Links
     */
    public function indexSearch($name, $type, $startOrExact, $end = NULL, $dedupe = false) {
        $url = $this->client->transport->buildBucketIndexPath($this, $name . '_' . $type, $startOrExact, $end);
        $response = $this->client->transport->get($url);

        $obj = Object::populateResponse(new Object($this->client, $this), $response);
        if (!$obj->exists)
            throw new Exception('Error searching index.');

        $data = $obj->data;
        $keys = array_map('urldecode', $data['keys']);

        /**
         * Combo of array_keys+array_flip is faster than array_unique
         */
        if ($dedupe)
            $keys = array_keys(array_flip($keys));

        array_walk($keys, array($this, 'inflateLinkCallback'));
        return $keys;
    }

    protected function inflateLinkCallback(&$key, $k) {
        $key = new Link($this->name, $key);
        $key->client = $this->client;
    }

}
