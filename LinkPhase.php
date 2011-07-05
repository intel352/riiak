<?php

namespace riiak;
use \CComponent;

/**
 * The LinkPhase object holds information about a Link phase in a
 * map/reduce operation.
 * @package riiak
 */
class LinkPhase extends CComponent {
    
    public $bucket;
    public $tag;
    public $keep;

    /**
     * Construct a LinkPhase object
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
     * Convert the LinkPhase to an associative array. Used internally.
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