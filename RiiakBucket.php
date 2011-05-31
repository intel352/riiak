<?php

/**
 * The RiiakBucket object allows you to access and change information
 * about a Riak bucket, and provides methods to create or retrieve
 * objects within the bucket.
 * @package RiiakBucket
 */
class RiiakBucket extends CComponent {
    
    /**
     * Client instance
     *
     * @var Riiak
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
    protected $r;
    /**
     * W-Value
     *
     * @var int
     */
    protected $w;
    /**
     * DW-Value
     *
     * @var int
     */
    protected $dw;

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
    public function getR($r=null) {
        if ($r != null)
            return $r;
        if ($this->r != null)
            return $this->r;
        return $this->client->r;
    }

    /**
     * Set the R-value for this bucket.
     * get/getBinary operations may use this value
     *
     * @param int $r The new R-Value
     * @return RiiakBucket
     */
    public function setR($r) {
        $this->r = $r;
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
        if ($this->w != null)
            return $this->w;
        return $this->client->w;
    }

    /**
     * Set the W-Value for this bucket
     * get/getBinary operations may use this value
     *
     * @param int $w The new W-Value
     * @return RiiakBucket 
     */
    public function setW($w) {
        $this->w = $w;
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
        if ($this->dw != null)
            return $this->dw;
        return $this->client->dw;
    }

    /**
     * Set the DW-Value for this bucket
     * get/getBinary operations may use this value
     *
     * @param int $dw The new DW-Value
     * @return RiiakBucket
     */
    public function setDW($dw) {
        $this->dw = $dw;
        return $this;
    }

    /**
     * Create a new Riak object that will be stored as JSON
     *
     * @param string $key Name of the key
     * @param object $data Data to store (Default: null)
     * @return RiiakObject 
     */
    public function newObject($key, $data=null) {
        return $this->newBinary($key, $data, 'text/json', true);
    }

    /**
     * Create a new Riak object that will be stored as Binary
     *
     * @param string $key Name of the key
     * @param object $data Data to store
     * @param string $contentType Content type of the object (Default: text/json)
     * @param bool $jsonize Whether to treat the object as JSON (Default: false)
     * @return RiiakObject
     */
    public function newBinary($key, $data, $contentType='text/json', $jsonize=false) {
        $obj = new RiiakObject($this->client, $this, $key);
        $obj->data = $data;
        $obj->contentType = $contentType;
        $obj->jsonize = $jsonize;
        return $obj;
    }

    /**
     * Retrieve a JSON-encoded object from Riak
     *
     * @param string $key Name of the key
     * @param int $r R-Value of the request (Default: bucket's R)
     * @return RiiakObject
     */
    public function get($key, $r=null) {
        return $this->getBinary($key, $r, true);
    }

    /**
     * Retrieves binary/string object from Riak
     *
     * @param string $key Name of the key
     * @param int $r R-Value of the request (Default: bucket's R)
     * @param bool $jsonize Whether to treat the object as JSON (Default: false)
     * @return RiiakObject
     */
    public function getBinary($key, $r=null, $jsonize=false) {
        $obj = new RiiakObject($this->client, $this, $key);
        $obj->jsonize = $jsonize;
        $r = $this->getR($r);
        return $obj->reload($r);
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
     */
    public function setProperties(array $props) {
        /**
         * Construct the URL, Headers, and Content
         */
        $url = RiiakUtils::buildRestPath($this->client, $this);
        $headers = array('Content-Type: application/json');
        $content = CJSON::encode(array('props' => $props));

        /**
         * Run the request
         */
        $response = RiiakUtils::httpRequest('PUT', $url, $headers, $content);

        /**
         * Handle the response
         */
        if ($response == null)
            throw Exception('Error setting bucket properties.');

        /**
         * Check the response value
         */
        $status = $response['headers']['http_code'];
        if ($status != 204)
            throw Exception('Error setting bucket properties.');
    }

    /**
     * Retrieve an associative array of all bucket properties
     *
     * @return array
     */
    public function getProperties() {
        $obj=$this->fetchBucketProperties(array('props' => 'true', 'keys' => 'false'));
        return $obj->data['props'];
    }

    /**
     * Retrieve an array of all keys in this bucket
     * Note: this operation is pretty slow
     *
     * @return array
     */
    public function getKeys() {
        $obj=$this->fetchBucketProperties(array('props' => 'false', 'keys' => 'true'));
        return array_map('urldecode', $obj->data['keys']);
    }

    /**
     * Fetches bucket
     *
     * @param array $params
     * @param string $key
     * @param string $spec
     * @return RiiakObject
     */
    protected function fetchBucketProperties(array $params=array(), $key=null, $spec=null) {
        /**
         * Run the request
         */
        $response = RiiakUtils::httpRequest('GET',
            RiiakUtils::buildRestPath($this->client, $this, $key, $spec, $params)
        );

        /**
         * Use a RiiakObject to interpret the response, we are just interested in the value
         */
        $obj = new RiiakObject($this->client, $this);
        $obj->populate($response, array(200));
        if (!$obj->exists)
            throw Exception('Error getting bucket properties.');
        
        return $obj;
    }

}