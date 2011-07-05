<?php

namespace riiak;
use \CComponent;

/**
 * The Link object represents a link from one Riak object to
 * another.
 * @package riiak
 */
class Link extends CComponent {
    
    /**
     * Bucket name
     *
     * @var string
     */
    public $bucket;
    /**
     * Key
     *
     * @var string
     */
    public $key;
    /**
     * Tag
     *
     * @var string
     */
    protected $_tag=null;
    /**
     * Client instance
     *
     * @var \riiak\Riiak
     */
    public $client;

    /**
     * Construct a Link object
     *
     * @param string $bucket The bucket name
     * @param string $key The key
     * @param string $tag The tag
     */
    public function __construct($bucket, $key, $tag=null) {
        $this->bucket = $bucket;
        $this->key = $key;
        $this->_tag = $tag;
    }

    /**
     * Retrieve the Object to which this link points
     *
     * @param int $r The R-value to use
     * @return \riiak\Object
     */
    public function get($r=NULL) {
        return $this->client->bucket($this->bucket)->get($this->key, $r);
    }

    /**
     * Retrieve the Object to which this link points, as a binary
     *
     * @param int $r The R-value to use
     * @return \riiak\Object
     */
    public function getBinary($r=NULL) {
        return $this->client->bucket($this->bucket)->getBinary($this->key, $r);
    }

    /**
     * Get the tag of this link
     *
     * @return string
     */
    public function getTag() {
        return $this->_tag?:$this->bucket;
    }

    /**
     * Set the tag of this link
     *
     * @param string $tag The tag
     * @return \riiak\Link
     */
    public function setTag($tag) {
        $this->_tag = $tag;
        return $this;
    }

    /**
     * Convert this Link object to a link header string. Used internally.
     *
     * @param string $client
     * @return string
     */
    public function toLinkHeader($client) {
        $link = '</' .
            $client->prefix . '/' .
            urlencode($this->bucket) . '/' .
            urlencode($this->key) . '>; riaktag=\'' .
            urlencode($this->getTag()) . '\'';
        return $link;
    }

    /**
     * Return true if the links are equal
     *
     * @param Link $link
     * @return bool
     */
    public function isEqual(Link $link) {
        $is_equal =
            ($this->bucket == $link->bucket) &&
            ($this->key == $link->key) &&
            ($this->getTag() == $link->getTag());
        return $is_equal;
    }

}