<?php

namespace riiak;
use \CComponent;

/**
 * The MapReducePhase holds information about a Map or Reduce phase
 * in a MapReduce operation.
 * @package riiak
 */
class MapReducePhase extends CComponent {
    
    /**
     * @var string map|reduce
     */
    public $type;
    /**
     * @var mixed String or array
     */
    public $language;
    /**
     * @var string javascript|erlang
     */
    public $function;
    /**
     * @var bool Whether to return phase output in results
     */
    public $keep;
    /**
     * @var mixed Additional value for map/reduce function
     */
    public $arg;

    /**
     * Construct a MapReducePhase object.
     *
     * @param string $type map|reduce
     * @param mixed $function String or array
     * @param string $language javascript|erlang
     * @param bool $keep Whether to return phase output in results
     * @param mixed $arg Additional value for map/reduce function
     */
    public function __construct($type, $function, $language, $keep, $arg) {
        $this->type = $type;
        $this->language = $language;
        $this->function = $function;
        $this->keep = $keep;
        $this->arg = $arg;
    }

    /**
     * Convert the MapReducePhase to an associative array. Used internally.
     *
     * @return array
     */
    public function toArray() {
        $stepdef = array('keep' => $this->keep,
            'language' => $this->language,
            'arg' => $this->arg);

        if ($this->language == 'javascript' && is_array($this->function)) {
            $stepdef['bucket'] = $this->function[0];
            $stepdef['key'] = $this->function[1];
        } else if ($this->language == 'javascript' && is_string($this->function)) {
            if (strpos($this->function, '{') == FALSE)
                $stepdef['name'] = $this->function;
            else
                $stepdef['source'] = $this->function;
        } else if ($this->language == 'erlang' && is_array($this->function)) {
            $stepdef['module'] = $this->function[0];
            $stepdef['function'] = $this->function[1];
        }

        return array(($this->type) => $stepdef);
    }

}