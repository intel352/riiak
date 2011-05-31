<?php

/**
 * The RiiakLink object represents a link from one Riak object to
 * another.
 * @package RiiakLink
 */
class RiiakLink extends CComponent {
    
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
    protected $tag=null;
    /**
     * Client instance
     *
     * @var Riiak
     */
    public $client;

    /**
     * Construct a RiiakLink object
     *
     * @param string $bucket The bucket name
     * @param string $key The key
     * @param string $tag The tag
     */
    public function __construct($bucket, $key, $tag=null) {
        $this->bucket = $bucket;
        $this->key = $key;
        $this->tag = $tag;
    }

    /**
     * Retrieve the RiiakObject to which this link points
     *
     * @param int $r The R-value to use
     * @return RiiakObject
     */
    public function get($r=NULL) {
        return $this->client->bucket($this->bucket)->get($this->key, $r);
    }

    /**
     * Retrieve the RiiakObject to which this link points, as a binary
     *
     * @param int $r The R-value to use
     * @return RiiakObject
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
        return $this->tag?:$this->bucket;
    }

    /**
     * Set the tag of this link
     *
     * @param string $tag The tag
     * @return RiiakLink
     */
    public function setTag($tag) {
        $this->tag = $tag;
        return $this;
    }

    /**
     * Convert this RiiakLink object to a link header string. Used internally.
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
     * @param RiiakLink $link
     * @return bool
     */
    public function isEqual(RiiakLink $link) {
        $is_equal =
            ($this->bucket == $link->bucket) &&
            ($this->key == $link->key) &&
            ($this->getTag() == $link->getTag());
        return $is_equal;
    }

}