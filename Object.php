<?php

namespace riiak;
use \CComponent, \CJSON, \Exception;

/**
 * The Object holds meta information about a Riak object, plus the
 * object's data.
 * @package riiak
 * 
 * Magic properties
 * 
 * Private
 * @property string vclock
 */
class Object extends CComponent {
    
    /**
     * Client instance
     *
     * @var \riiak\Riiak
     */
    public $client;
    /**
     * Bucket
     *
     * @var \riiak\Bucket
     */
    public $bucket;
    /**
     * Key
     *
     * @var string
     */
    public $key;
    /**
     * Whether or not to treat object as json
     *
     * @var bool
     */
    public $jsonize=true;
    public $headers=array();
    /**
     * Array of Links
     *
     * @var array
     */
    protected $_links=array();
    public $siblings=null;
    /**
     * Whether the object exists
     *
     * @var bool
     */
    protected $_exists=false;
    /**
     * If constructed by newBinary|getBinary, returns string.
     * If not a string, will be JSON encoded when stored
     *
     * @var mixed
     */
    protected $_data;

    /**
     * Construct a new Object
     *
     * @param \riiak\Riiak $client A Riiak object
     * @param \riiak\Bucket $bucket A Bucket object
     * @param string $key Optional - If empty, generated upon store()
     */
    public function __construct(Riiak $client,Bucket $bucket, $key=null) {
        $this->client = $client;
        $this->bucket = $bucket;
        $this->key = $key;
    }

    /**
     * Returns HTTP status of last operation
     *
     * @return int
     */
    public function getStatus() {
        return $this->headers['http_code'];
    }

    /**
     * Returns the object's content type
     *
     * @return string
     */
    public function getContentType() {
        return $this->headers['content-type'];
    }

    /**
     * Set the object's content type
     *
     * @param string $contentType The new content type
     * @return \riiak\Object
     */
    public function setContentType($contentType) {
        $this->headers['content-type'] = $contentType;
        return $this;
    }

    /**
     * Returns the object's data
     *
     * @return mixed
     */
    public function getData() {
        return $this->_data;
    }

    /**
     * Set the object's data
     *
     * @param mixed $data The new data value
     * @return \riiak\Object
     */
    public function setData($data) {
        $this->_data = $data;
        return $this;
    }
    
    /**
     * Whether the object exists
     *
     * @return bool 
     */
    public function getExists() {
        return $this->_exists;
    }

    /**
     * Add a link to a Object
     *
     * @param \riiak\Link|\riiak\Object $obj Either Object or Link
     * @param string $tag Optional: link tag. Default: bucket name. Ignored for Link
     * @return \riiak\Object 
     */
    public function addLink($obj, $tag=null) {
        if ($obj instanceof Link)
            $newlink = $obj;
        else
            $newlink = new Link($obj->bucket->name, $obj->key, $tag);

        $this->removeLink($newlink);
        $this->_links[] = $newlink;

        return $this;
    }

    /**
     * Remove a link to a Object
     *
     * @param \riiak\Link|\riiak\Object $obj Either Object or Link
     * @param string $tag Optional: link tag. Default: bucket name. Ignored for Link
     * @return \riiak\Object 
     */
    public function removeLink($obj, $tag=null) {
        if ($obj instanceof Link)
            $oldlink = $obj;
        else
            $oldlink = new Link($obj->bucket->name, $obj->key, $tag);

        foreach ($this->_links as $k=>$link)
            if (!$link->isEqual($oldlink))
                unset($this->_links[$k]);

        return $this;
    }

    /**
     * Return an array of Link objects
     *
     * @return array
     */
    public function getLinks() {
        /**
         * Set the clients before returning
         */
        foreach ($this->_links as $link)
            $link->client = $this->client;
        return $this->_links;
    }

    /**
     * Store the object in Riak. Upon completion, object could contain new
     * metadata, and possibly new data if Riak contains a newer version of
     * the object according to the object's vector clock.
     *
     * @param int $w W-Value: X paritions must respond before returning
     * @param int $dw DW-Value: X partitions must confirm write before returning
     * @return \riiak\Object 
     */
    public function store($w=null, $dw=null) {
        /**
         * Use defaults if not specified
         */
        $w = $this->bucket->getW($w);
        $dw = $this->bucket->getDW($w);

        /**
         * Construct the URL
         */
        $params = array('returnbody' => 'true', 'w' => $w, 'dw' => $dw);
        $url = Utils::buildRestPath($this->client, $this->bucket, $this->key, null, $params);

        /**
         * Construct the headers
         */
        $headers = array('Accept: text/plain, */*; q=0.5',
            'Content-Type: ' . $this->getContentType(),
            'X-Riak-ClientId: ' . $this->client->clientId);

        /**
         * Add the vclock if it exists
         */
        if (!empty($this->vclock))
            $headers[] = 'X-Riak-Vclock: ' . $this->vclock;

        /**
         * Add the Links
         */
        foreach ($this->_links as $link)
            $headers[] = 'Link: ' . $link->toLinkHeader($this->client);

        if ($this->jsonize)
            $content = CJSON::encode($this->_data);
        else
            $content = $this->_data;

        $method = $this->key ? 'PUT' : 'POST';

        /**
         * Run the operation
         */
        $response = Utils::httpRequest($method, $url, $headers, $content);
        $this->populate($response, array(200, 201, 300));
        return $this;
    }

    /**
     * Reload the object from Riak. When this operation completes, the object
     * could contain new metadata and a new value, if the object was updated
     * in Riak since it was last retrieved.
     *
     * @param int $r R-Value: X partitions must respond before returning
     * @return \riiak\Object 
     */
    public function reload($r=null) {
        /**
         * Do the request
         */
        $params = array('r' => $this->bucket->getR($r));
        $url = Utils::buildRestPath($this->client, $this->bucket, $this->key, null, $params);
        $response = Utils::httpRequest('GET', $url);
        $this->populate($response, array(200, 300, 404));

        /**
         * If there are siblings, load the data for the first one by default
         */
        if ($this->getHasSiblings()) {
            $obj = $this->getSibling(0);
            $this->_data=$obj->data;
        }

        return $this;
    }

    /**
     * Delete this object from Riak
     *
     * @param int $dw DW-Value: X partitions must delete object before returning
     * @return \riiak\Object 
     */
    public function delete($dw=null) {
        /**
         * Use defaults if not specified
         */
        $dw = $this->bucket->getDW($dw);

        /**
         * Construct the URL
         */
        $params = array('dw' => $dw);
        $url = Utils::buildRestPath($this->client, $this->bucket, $this->key, null, $params);

        /**
         * Run the operation
         */
        $response = Utils::httpRequest('DELETE', $url);
        $this->populate($response, array(204, 404));

        return $this;
    }

    /**
     * Reset this object
     *
     * @return \riiak\Object 
     */
    private function clear() {
        $this->headers = array();
        $this->_links = array();
        $this->_data = null;
        $this->_exists = false;
        $this->siblings = null;
        return $this;
    }

    /**
     * Get the vclock of this object
     *
     * @return string|null
     */
    protected function getVclock() {
        if (array_key_exists('x-riak-vclock', $this->headers))
            return $this->headers['x-riak-vclock'];
        else
            return null;
    }

    /**
     * Populates the object. Only for internal use
     *
     * @param array $response Output of Utils::httpRequest
     * @param array $expectedStatuses List of statuses
     * @return \riiak\Object 
     */
    public function populate($response, $expectedStatuses) {
        $this->clear();

        /**
         * If no response given, then return
         */
        if ($response == null)
            return $this;

        /**
         * Update the object
         */
        $this->headers = $response['headers'];
        $this->_data = $response['body'];

        /**
         * Check if the server is down (status==0)
         */
        if ($this->status == 0)
            throw new Exception('Could not contact Riak Server: ' . Utils::buildUrl($this->client) . '!');

        /**
         * Verify that we got one of the expected statuses. Otherwise, throw an exception
         */
        if (!in_array($this->status, $expectedStatuses))
            throw new Exception('Expected status ' . implode(' or ', $expectedStatuses) . ', received ' . $this->status);

        /**
         * If 404 (Not Found), then clear the object
         */
        if ($this->status == 404) {
            $this->clear();
            return $this;
        }

        /**
         * If we are here, then the object exists
         */
        $this->_exists = true;

        /**
         * Parse the link header
         */
        if (array_key_exists('link', $this->headers))
            $this->populateLinks($this->headers['link']);

        /**
         * If 300 (siblings), load first sibling, store the rest
         */
        if ($this->status == 300) {
            $siblings = explode("\n", trim($this->_data));
            array_shift($siblings); # Get rid of 'Siblings:' string.
            $this->siblings = $siblings;
            $this->_exists = true;
            return $this;
        }

        if ($this->status == 201) {
            $pathParts = explode('/', $this->headers['location']);
            $this->key = array_pop($pathParts);
        }

        /**
         * Possibly JSON decode
         */
        if (($this->status == 200 || $this->status == 201) && $this->jsonize)
            $this->_data = CJSON::decode($this->_data, true);

        return $this;
    }

    /**
     * Populate object links
     * 
     * @return \riiak\Object
     */
    private function populateLinks($linkHeaders) {
        $linkHeaders = explode(',', trim($linkHeaders));
        foreach ($linkHeaders as $linkHeader)
            if (preg_match('/\<\/([^\/]+)\/([^\/]+)\/([^\/]+)\>; ?riaktag="([^"]+)"/', trim($linkHeader), $matches))
                $this->_links[] = new Link($matches[2], $matches[3], $matches[4]);

        return $this;
    }

    /**
     * Return true if this object has siblings
     *
     * @return bool
     */
    public function getHasSiblings() {
        return ($this->getSiblingCount() > 0);
    }

    /**
     * Get the number of siblings that this object contains
     *
     * @return int
     */
    public function getSiblingCount() {
        return count($this->siblings);
    }

    /**
     * Retrieve a sibling by sibling number
     *
     * @param int $i Sibling number
     * @param int $r R-Value: X partitions must respond before returning
     * @return \riiak\Object
     */
    public function getSibling($i, $r=null) {
        /**
         * Use defaults if not specified
         */
        $r = $this->bucket->getR($r);

        /**
         * Run the request
         */
        $vtag = $this->siblings[$i];
        $params = array('r' => $r, 'vtag' => $vtag);
        $url = Utils::buildRestPath($this->client, $this->bucket, $this->key, null, $params);
        $response = Utils::httpRequest('GET', $url);

        /**
         * Respond with a new object
         */
        $obj = new Object($this->client, $this->bucket, $this->key);
        $obj->jsonize = $this->jsonize;
        $obj->populate($response, array(200));
        return $obj;
    }

    /**
     * Retrieve an array of siblings
     *
     * @param int $r R-Value: X partitions must respond before returning
     * @return array Array of Objects
     */
    public function getSiblings($r=null) {
        $a = array();
        for ($i = 0; $i < $this->getSiblingCount(); $i++) {
            $a[] = $this->getSibling($i, $r);
        }
        return $a;
    }

    /**
     * Returns a MapReduce instance
     *
     * @param bool $reset Whether to create a new MapReduce instance
     * @return \riiak\MapReduce
     */
    public function getMapReduce($reset=false) {
        return $this->client->getMapReduce($reset);
    }

}