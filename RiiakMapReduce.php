<?php

/**
 * The RiiakMapReduce object allows you to build up and run a
 * map/reduce operation on Riak.
 * @package RiiakMapReduce
 */
class RiiakMapReduce extends CComponent {

    /**
     * @var Riiak A Riak client object
     */
    public $client;
    /**
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
     *
     * @var type 
     */
    public $inputMode;

    public function __construct(Riiak $client) {
        $this->client = $client;
    }

    /**
     * Adds object's bucket name and key will be added to m/r inputs
     *
     * @param RiiakObject $obj
     * @return RiiakMapReduce
     */
    public function addObject(RiiakObject $obj) {
        return $this->addBucketKeyData($obj->bucket->name, $obj->key, NULL);
    }

    /**
     * Adds bucket, key, and optional data to m/r inputs
     *
     * @param string $bucket Bucket name
     * @param string $key Key name
     * @param string $data
     * @return RiiakMapReduce
     */
    public function addBucketKeyData($bucket, $key, $data=null) {
        if ($this->inputMode == 'bucket')
            throw new Exception('Already added a bucket, can\'t add an object.');
        $this->inputs[] = array($bucket, $key, $data);
        return $this;
    }

    /**
     * Adds bucket to m/r inputs
     * Means all of the bucket's keys will be used as inputs (expensive)
     *
     * @param string $bucket Bucket name
     * @return RiiakMapReduce 
     */
    public function addBucket($bucket) {
        $this->inputMode = 'bucket';
        $this->inputs = $bucket;
        return $this;
    }

    /**
     * Begin a map/reduce operation using a Search. This command will
     * return an error unless executed against a Riak Search cluster.
     *
     * @param string $bucket The Bucket to search
     * @param string $query The Query to execute. (Lucene syntax.)
     * @return RiiakMapReduce
     */
    public function search($bucket, $query) {
        $this->inputs = array('module' => 'riak_search', 'function' => 'mapred_search', 'arg' => array($bucket, $query));
        return $this;
    }

    /**
     * Add a link phase to the map/reduce operation.
     *
     * @param string $bucket Default: '_' - all buckets
     * @param string $tag Default: '_' - all buckets
     * @param bool $keep Whether to keep results from this stage in map/reduce
     * @return RiiakMapReduce
     */
    public function link($bucket='_', $tag='_', $keep=FALSE) {
        $this->phases[] = new RiiakLinkPhase($bucket, $tag, $keep);
        return $this;
    }

    /**
     * Add a map phase to the map/reduce operation.
     *
     * @param mixed $function Erlang (array) or Javascript function call (string)
     * @param array $options Optional assoc array containing language|keep|arg
     * @return RiiakMapReduce
     */
    public function map($function, array $options=array()) {
        return $this->addPhase('map', $function, $options);
    }

    /**
     * Add a reduce phase to the map/reduce operation.
     *
     * @param mixed $function Erlang (array) or Javascript function call (string)
     * @param array $options Optional assoc array containing language|keep|arg
     * @return RiiakMapReduce
     */
    public function reduce($function, $options=array()) {
        return $this->addPhase('reduce', $function, $options);
    }
    
    /**
     * Add a map/reduce phase
     *
     * @param string $phase Name of phase-type to add (e.g.: map, reduce)
     * @param mixed $function Erlang (array) or Javascript function call (string)
     * @param array $options Optional assoc array containing language|keep|arg
     * @return RiiakMapReduce
     */
    public function addPhase($phase, $function,array $options=array()) {
        $language = is_array($function) ? 'erlang' : 'javascript';
        $options=array_merge(
            array('language'=>$language,'keep'=>false,'arg'=>null),
            $options);
        $this->phases[] = new RiiakMapReducePhase($phase,
            $function,
            $options['language'],
            $options['keep'],
            $options['arg']
        );
        return $this;
    }
    
    /**
     * Run the map/reduce operation. Returns array of results
     * or RiiakLink objects if last phase is link phase
     *
     * @param integer $timeout Timeout in seconds. Default: null
     * @return array
     */
    public function run($timeout=null) {
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
         * Construct the job, optionally set the timeout
         */
        $job = array('inputs' => $this->inputs, 'query' => $query);
        if ($timeout != null)
            $job['timeout'] = $timeout;
        $content = CJSON::encode($job);

        /**
         * Execute the request
         */
        $url = 'http://' . $this->client->host . ':' . $this->client->port . '/' . $this->client->mapred_prefix;
        $response = RiiakUtils::httpRequest('POST', $url, array(), $content);
        $result = CJSON::decode($response[1]);

        /**
         * If the last phase is NOT a link phase, then return the result.
         */
        $linkResultsFlag |= ( end($this->phases) instanceof RiiakLinkPhase);

        /**
         * If we don't need to link results, then just return.
         */
        if (!$linkResultsFlag)
            return $result;

        /**
         * Otherwise, if the last phase IS a link phase, then convert the
         * results to RiiakLink objects.
         */
        $a = array();
        foreach ($result as $r) {
            $tag = isset($r[2]) ? $r[2] : null;
            $link = new RiiakLink($r[0], $r[1], $tag);
            $link->client = $this->client;
            $a[] = $link;
        }
        return $a;
    }

}