<?php

namespace riiak;

use \CComponent,
    \CJSON,
    \Exception,
    \Yii;
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
abstract class Backend extends CComponent{
    
    /**
     * @var \riiak\Riiak A Riak client object
     */
    public $client;

    /**
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
     * @var string
     */
    public $inputMode;

    /**
     * @var array
     */
    public $keyFilters = array();
    
    public function __construct(Riiak $client) {
        $this->client = $client;
    }
    
    abstract public function keyFilterAnd(array $filter);
    
    abstract public function keyFilterOr(array $filter);
    
    abstract public function run($timeout = null);
}
?>
