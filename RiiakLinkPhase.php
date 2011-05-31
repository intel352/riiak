<?php

/**
 * The RiiakLinkPhase object holds information about a Link phase in a
 * map/reduce operation.
 * @package RiiakLinkPhase
 */
class RiiakLinkPhase extends CComponent {
    
    public $bucket;
    public $tag;
    public $keep;

    /**
     * Construct a RiiakLinkPhase object
     *
     * @param string $bucket The bucket name
     * @param string $tag The tag
     * @param bool $keep True to return results of this phase
     */
    public function __construct($bucket, $tag, $keep) {
        $this->bucket = $bucket;
        $this->tag = $tag;
        $this->keep = $keep;
    }

    /**
     * Convert the RiiakLinkPhase to an associative array. Used internally.
     *
     * @return array
     */
    public function toArray() {
        $stepdef = array('bucket' => $this->bucket,
            'tag' => $this->tag,
            'keep' => $this->keep);
        return array('link' => $stepdef);
    }

}