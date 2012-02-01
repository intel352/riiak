<?php

namespace riiak;

use \CComponent,
    \CJSON,
    \Exception,
    \Yii;

/**
 * The MapReduce object allows you to build up and run a
 * map/reduce operation on Riak.
 * @package riiak
 */
class MapReduce extends CComponent {

    /**
     * A Riak client object
     *
     * @var Riiak
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
     * List of key filters
     *
     * @var array
     */
    public $keyFilters = array();

    /**
     * @param Riiak $client A Riiak object
     */
    public function __construct(Riiak $client) {
        $this->client = $client;
    }

    /**
     * Adds object's bucket name and key will be added to m/r inputs
     *
     * @param \riiak\Object $obj
     * @return \riiak\MapReduce
     */
    public function addObject(Object $obj) {
        return $this->addBucketKeyData($obj->bucket->name, $obj->key, null);
    }

    /**
     * Adds bucket, key, and optional data to m/r inputs
     *
     * @param string $bucket Bucket name
     * @param string $key Key name
     * @param string $data
     * @return \riiak\MapReduce
     */
    public function addBucketKeyData($bucket, $key, $data = null) {
        if ($this->inputMode == 'bucket')
            throw new Exception('Already added a bucket, can\'t add an object.');
        $this->inputs[] = array((string) $bucket, (string) $key, (string) $data);
        return $this;
    }

    /**
     * Adds bucket to m/r inputs
     * Means all of the bucket's keys will be used as inputs (expensive)
     *
     * @param string $bucket Bucket name
     * @return \riiak\MapReduce
     */
    public function addBucket($bucket) {
        $this->inputMode = 'bucket';
        $this->inputs = (string) $bucket;
        return $this;
    }

    /**
     * Begin a map/reduce operation using a Search. This command will
     * return an error unless executed against a Riak Search cluster.
     *
     * @param string $bucket The Bucket to search
     * @param string $query The Query to execute. (Lucene syntax.)
     * @return \riiak\MapReduce
     */
    public function search($bucket, $query) {
        $this->inputs = array('module' => 'riak_search', 'function' => 'mapred_search', 'arg' => array((string) $bucket, (string) $query));
        return $this;
    }

    /**
     * Add a link phase to the map/reduce operation.
     *
     * @param string $bucket Default: '_' - all buckets
     * @param string $tag Default: '_' - all buckets
     * @param bool $keep Whether to keep results from this stage in map/reduce
     * @return \riiak\MapReduce
     */
    public function link($bucket = '_', $tag = '_', $keep = false) {
        $this->phases[] = new LinkPhase((string) $bucket, (string) $tag, $keep);
        return $this;
    }

    /**
     * Add a map phase to the map/reduce operation.
     *
     * @param mixed $function Erlang (array) or Javascript function call (string)
     * @param array $options Optional assoc array containing language|keep|arg
     * @return \riiak\MapReduce
     */
    public function map($function, array $options = array()) {
        return $this->addPhase('map', $function, $options);
    }

    /**
     * Add a reduce phase to the map/reduce operation.
     *
     * @param mixed $function Erlang (array) or Javascript function call (string)
     * @param array $options Optional assoc array containing language|keep|arg
     * @return \riiak\MapReduce
     */
    public function reduce($function, array $options = array()) {
        return $this->addPhase('reduce', $function, $options);
    }

    /**
     * Add a map/reduce phase
     *
     * @param string $phase Name of phase-type to add (e.g.: map, reduce)
     * @param mixed $function Erlang (array) or Javascript function call (string)
     * @param array $options Optional assoc array containing language|keep|arg
     * @return \riiak\MapReduce
     */
    public function addPhase($phase, $function, array $options = array()) {
        $language = is_array($function) ? 'erlang' : 'javascript';
        $options = array_merge(
                array('language' => $language, 'keep' => false, 'arg' => null), $options);
        $this->phases[] = new MapReducePhase((string) $phase,
                        $function,
                        $options['language'],
                        $options['keep'],
                        $options['arg']
        );
        return $this;
    }

    /**
     * Add a key filter to the map/reduce operation.
     * @see function keyFilterAnd
     *
     * @param array $filter
     * @return \riiak\MapReduce
     */
    public function keyFilter(array $filter) {
        return call_user_func_array(array($this, 'keyFilterAnd'), func_get_args());
    }

    /**
     * Add a key filter to the map/reduce operation.
     * If there are already existing filters, an "and" condition will be used
     * to combine them.
     *
     * @param array $filter
     * @return \riiak\MapReduce
     */
    public function keyFilterAnd(array $filter) {
        $args = func_get_args();
        array_unshift($args, 'and');
        return call_user_func_array(array($this, 'keyFilterOperator'), $args);
    }

    /**
     * Add a key filter to the map/reduce operation.
     * If there are already existing filters, an "or" condition will be used
     * to combine them.
     *
     * @param array $filter
     * @return \riiak\MapReduce
     */
    public function keyFilterOr(array $filter) {
        $args = func_get_args();
        array_unshift($args, 'or');
        return call_user_func_array(array($this, 'keyFilterOperator'), $args);
    }

    /**
     * Add a key filter to the map/reduce operation.
     * If there are already existing filters, the conditional operator will be
     * used to combine them.
     *
     * @param string $operator Typically "and" or "or"
     * @param array $filter
     * @return \riiak\MapReduce
     */
    public function keyFilterOperator($operator, $filter) {
        $filters = func_get_args();
        array_shift($filters);
        if ($this->input_mode != 'bucket')
            throw new Exception('Key filters can only be used in bucket mode');

        if (count($this->keyFilters) > 0)
            $this->keyFilters = array(array(
                    $operator,
                    $this->keyFilters,
                    $filters
                    ));
        else
            $this->keyFilters = $filters;

        return $this;
    }

    /**
     * Run the map/reduce operation. Returns array of results
     * or Link objects if last phase is link phase
     *
     * @param integer $timeout optional Timeout in milliseconds. Riak default is 60000 (60s).
     * @return array
     */
    public function run($timeout = null) {
        $numPhases = count($this->phases);
        $linkResultsFlag = false;

        /**
         * If there are no phases, then just echo the inputs back to the user.
         */
        if ($numPhases == 0) {
            $this->reduce(array('riak_kv_mapreduce', 'reduce_identity'));
            $numPhases = 1;
            $linkResultsFlag = true;
        }

        /**
         * Convert all phases to associative arrays. Also, if none of the
         * phases are accumulating, then set the last one to accumulate.
         */
        $keepFlag = false;
        $query = array();
        for ($i = 0; $i < $numPhases; $i++) {
            $phase = $this->phases[$i];
            if ($i == ($numPhases - 1) && !$keepFlag)
                $phase->keep = true;
            if ($phase->keep)
                $keepFlag = true;
            $query[] = $phase->toArray();
        }

        /**
         * Add key filters if applicable
         */
        if ($this->inputMode == 'bucket' && count($this->keyFilters) > 0) {
            $this->inputs = array(
                'bucket' => $this->inputs,
                'key_filters' => $this->keyFilters
            );
        }

        /**
         * Construct the job, optionally set the timeout
         */
        $job = array('inputs' => $this->inputs, 'query' => $query);
        if ($timeout != null)
            $job['timeout'] = $timeout;
        $content = CJSON::encode($job);

        /**
         * Execute the request
         */
        Yii::trace('Running Map/Reduce query', 'ext.riiak.MapReduce');

        $transport = $this->client->transport;
        $response = $transport->post($transport->buildMapReducePath(), array(), $content);

        /**
         * Verify that we got one of the expected statuses. Otherwise, throw an exception
         */
        try {
            $transport->validateResponse($response, 'mapReduce');
        }catch(\Exception $e) {
            throw new \Exception($e . PHP_EOL . PHP_EOL . 'Job Request: '. $content . PHP_EOL . PHP_EOL
                . 'Response: '. \CVarDumper::dumpAsString($response), $e->getCode(), $e);
        }

        $result = CJSON::decode($response['body']);

        /**
         * If the last phase is NOT a link phase, then return the result.
         */
        $linkResultsFlag |= ( end($this->phases) instanceof LinkPhase);

        /**
         * If we don't need to link results, then just return.
         */
        if (!$linkResultsFlag)
            return $result;

        /**
         * Otherwise, if the last phase IS a link phase, then convert the
         * results to Link objects.
         */
        $a = array();
        foreach ($result as $r) {
            $tag = isset($r[2]) ? $r[2] : null;
            $link = new Link($r[0], $r[1], $tag);
            $link->client = $this->client;
            $a[] = $link;
        }
        return $a;
    }

}