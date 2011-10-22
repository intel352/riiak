<?php

namespace riiak;

use \CComponent,
    \CJSON,
    \Exception,
    \Yii;

/**
 * Contains transport layer actions of Riak
 * @package riiak
 */
class Transport extends CComponent {
    
    /**
     * Set multiple bucket properties in one call. Only use if you know
     * what you're doing
     *
     * @param array $props An associative array of $key=>$value
     * @param object $objBucket An Bucket class object.
     */
    public static function setBucketProperties(array $props, Bucket $objBucket) {
        /**
         * Construct the URL, Headers, and Content
         */
        $url = self::buildRestPath($objBucket->client, $objBucket);
        $headers = array('Content-Type: application/json');
        $content = CJSON::encode(array('props' => $props));

        /**
         * Run the request
         */
        Yii::trace('Setting transport layer properties for bucket "' . $objBucket->name . '"', 'ext.riiak.Transport');
        $response = self::httpRequest($objBucket->client, 'PUT', $url, $headers, $content);

        /**
         * Handle the response
         */
        if ($response == null)
            throw new Exception('Error setting bucket properties.');

        /**
         * Check the response value
         */
        $status = $response['headers']['http_code'];
        if ($status != 204)
            throw new Exception('Error setting bucket properties.');
    }
    
    /**
     * Fetches bucket
     *
     * @param array $params
     * @param string $key
     * @param string $spec
     * @param Object $objBucket
     * @return \riiak\Object
     */
    public static function fetchBucketProperties(array $params = array(), $key = null, $spec = null, $objBucket) {
        /**
         * Construct the URL
         */
        $url = self::buildRestPath($objBucket->client, $objBucket, $key, $spec, $params);
        
        /**
         * Run the request
         */
        Yii::trace('Fetching transport layer properties for bucket "' . $objBucket->name . '"', 'ext.riiak.Transport');
        $response = self::httpRequest($objBucket->client, 'GET', $url);
        
        /**
         * Remove bulk of empty keys.
         */
        $response['body'] = $objBucket->getStreamedBucketKeys($response, $params);
        
        /**
         * Use a Object to interpret the response, we are just interested in the value
         */
        $obj = new Object($objBucket->client, $objBucket);
        $obj->populate($response, array(200));
        if (!$obj->exists)
            throw new Exception('Error getting bucket properties.');
        
        return $obj;
    }
    
    /**
     * Return array of Bucket objects
     *
     * @param \riiak\Riiak $objRiiak
     * @return array
     */
    public static function buckets(Riiak $objRiiak) {
        Yii::trace('Fetching list of buckets', 'ext.riiak.Transport');
        $response = self::httpRequest($objRiiak->client, 'GET', self::buildRestPath($objRiiak) . '?buckets=true');
        $responseObj = CJSON::decode($response['body']);
        $buckets = array();
        foreach ($responseObj->buckets as $name)
            $buckets[] = $objRiiak->bucket($name);
        
        return $buckets;
    }
    
    /**
     * Check if Riak server is alive
     *
     * @return bool
     */
    public static function getIsAlive(Riiak $objRiiak) {
        Yii::trace('Pinging Riak server', 'ext.riiak.Transport');
        $response = self::httpRequest('GET', self::buildUrl($objRiiak) . '/ping');
        return ($response != NULL) && ($response['body'] == 'OK');
    }

    /**
     * Builds URL to connect to Riak server
     *
     * @param \riiak\Riiak $objClient
     * @return string
     */
    public static function buildUrl(Riiak $objClient) {
        return 'http' . ($objClient->ssl ? 's' : '') . '://' . $objClient->host . ':' . $objClient->port;
    }

    /**
     * Builds a REST URL to access Riak API
     *
     * @param \riiak\Riiak $objClient
     * @param \riiak\Bucket $objBucket
     * @param string $key
     * @param string $spec
     * @param array $params
     * @return string
     */
    public static function buildRestPath(Riiak $objClient, Bucket $objBucket = NULL, $key = NULL, $spec = NULL, array $params = NULL) {
        /**
         * Build http[s]://hostname:port/prefix[/bucket]
         */
        $streamKey = '';
        /**
         * Check for get bucket keys using keys=stream. 
         */
        if(!is_null($params) && 0 < count($params) && array_key_exists('keys', $params) && ($params['keys'] == 'stream' || $params['keys'] == 'true')){
            $path = self::buildUrl($objClient) . '/' . $objClient->bucketPrefix;
            $streamKey = '/' . $objClient->keyPrefix;
        }else{
            $path = self::buildUrl($objClient) . '/' . $objClient->prefix;
        }
        
        /**
         * Add bucket
         */
        if (!is_null($objBucket) && $objBucket instanceof Bucket)
            $path .= '/' . urlencode($objBucket->name) . $streamKey;

        /**
         * Add key
         */
        if (!is_null($key))
            $path .= '/' . urlencode($key);

        /**
         * Add params for link walking
         * bucket, tag, keep
         */
        if (!is_null($spec))
            foreach ($spec as $el)
                $path .= '/' . urlencode($el[0]) . ',' . urlencode($el[1]) . ',' . $el[2];

        /**
         * Add query parameters
         */
        if (!is_null($params))
            $path .= '?' . http_build_query($params, '', '&');

        return $path;
    }
    
    /**
     * Builds a CURL URL to access Riak API
     *
     * @param string $method
     * @param string $url
     * @param array $requestHeaders
     * @param string $obj
     * @return string
     */
    protected static function buildCurlOpts($method, $url, array $requestHeaders = array(), $obj = '') {
        $curlOptions = array(
            CURLOPT_URL => $url,
            CURLOPT_HTTPHEADER => $requestHeaders,
        );

        switch ($method) {
            case 'GET':
                $curlOptions[CURLOPT_HTTPGET] = 1;
                break;
            case 'POST':
                $curlOptions[CURLOPT_POST] = 1;
                $curlOptions[CURLOPT_POSTFIELDS] = $obj;
                break;
            /**
             * PUT/DELETE both declare CUSTOMREQUEST, thus no break after PUT
             */
            case 'PUT':
                $curlOptions[CURLOPT_POSTFIELDS] = $obj;
            case 'DELETE':
                $curlOptions[CURLOPT_CUSTOMREQUEST] = $method;
                break;
        }

        return $curlOptions;
    }
    
    /**
     * Builds curl options array
     *
     * @param array $curlOpts
     * @return array
     */
    protected static function readableCurlOpts(array $curlOpts) {
        $constants = get_defined_constants(true);
        $curlConstants = array_flip(array_intersect_key(array_flip($constants['curl']), $curlOpts));
        return array_map(function($const)use($curlOpts) {
                            return $curlOpts[$const];
                        }, $curlConstants);
    }

    /**
     * Executes HTTP request, returns named array(headers, body) of request, or null on error
     *
     * @param \riiak\Riiak $client
     * @param string $method GET|POST|PUT|DELETE
     * @param string $url
     * @param array $requestHeaders
     * @param string $obj
     * @return array|null
     */
    public static function httpRequest(Riiak $client, $method, $url, array $requestHeaders = array(), $obj = '') {
        /**
         * Init curl
         */
        $ch = curl_init();
        $curlOpts = self::buildCurlOpts($method, $url, $requestHeaders, $obj);

        if ($client->enableProfiling)
            $profileToken = 'ext.riiak.Transport.httpRequest(' . \CVarDumper::dumpAsString(self::readableCurlOpts($curlOpts)) . ')';

        /**
         * Capture response headers
         */
        $curlOpts[CURLOPT_HEADERFUNCTION] =
                function($ch, $data) use(&$responseHeadersIO) {
                    $responseHeadersIO.=$data;
                    return strlen($data);
                };

        /**
         * Capture response body
         */
        $curlOpts[CURLOPT_WRITEFUNCTION] =
                function($ch, $data) use(&$responseBodyIO) {
                    $responseBodyIO.=$data;
                    return strlen($data);
                };

        curl_setopt_array($ch, $curlOpts);

        Yii::trace('Executing HTTP ' . $method . ': ' . $url . ($obj ? ' with content "' . $obj . '"' : ''), 'ext.riiak.Transport');
        try {
            if ($client->enableProfiling)
                Yii::beginProfile($profileToken, 'ext.riiak.Transport.httpRequest');

            /**
             * Run the request
             */
            curl_exec($ch);
            $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
            curl_close($ch);

            if ($client->enableProfiling)
                Yii::endProfile($profileToken, 'ext.riiak.Transport.httpRequest');

            /**
             * Get headers
             */
            $parsedHeaders = self::parseHttpHeaders($responseHeadersIO);
            $responseHeaders = array_merge(array('http_code' => $httpCode), array_change_key_case($parsedHeaders, CASE_LOWER));

            /**
             * Return headers/body array
             */
            return array('headers' => $responseHeaders, 'body' => $responseBodyIO);
        } catch (Exception $e) {
            curl_close($ch);
            error_log('Error: ' . $e->getMessage());
            return NULL;
        }
    }

    /**
     * Executes HTTP requests, returns named array(headers, body) of request, or null on error
     *
     * @param \riiak\Riiak $client
     * @param string $method GET|POST|PUT|DELETE
     * @param array $urls
     * @param array $requestHeaders
     * @param string $obj
     * @return array|null
     */
    public static function httpMultiRequest(Riiak $client, $method, array $urls, array $requestHeaders = array(), $obj = '') {
        /**
         * Init multi-curl
         */
        $mh = curl_multi_init();
        $curlOpts = self::buildCurlOpts($method, '', $requestHeaders, $obj);

        Yii::trace('Executing HTTP Multi ' . $method . ': ' . \CVarDumper::dumpAsString($urls) . ($obj ? ' with content "' . $obj . '"' : ''), 'ext.riiak.Transport');
        if ($client->enableProfiling)
            $profileToken = 'ext.riiak.Transport.httpMultiRequest(' . \CVarDumper::dumpAsString(self::readableCurlOpts($curlOpts)) . ')';

        $instanceMap = array();
        $responses = array_map(function($url)use(&$mh, $curlOpts, &$instanceMap) {
                    $instanceMap[$url] = (int) $ch = curl_init();

                    /**
                     * Override the URL specified in the options array
                     */
                    $curlOpts[CURLOPT_URL] = $url;

                    /**
                     * Capture response headers
                     */
                    $curlOpts[CURLOPT_HEADERFUNCTION] =
                            function($ch, $data) use(&$responseHeadersIO) {
                                $responseHeadersIO.=$data;
                                return strlen($data);
                            };

                    /**
                     * Capture response body
                     */
                    $curlOpts[CURLOPT_WRITEFUNCTION] =
                            function($ch, $data) use(&$responseBodyIO) {
                                $responseBodyIO.=$data;
                                return strlen($data);
                            };

                    curl_setopt_array($ch, $curlOpts);
                    curl_multi_add_handle($mh, $ch);

                    return array('instanceId' => (int) $ch, 'responseHeadersIO' => &$responseHeadersIO, 'responseBodyIO' => &$responseBodyIO);
                }, array_combine($urls, $urls));

        if ($client->enableProfiling)
            Yii::beginProfile($profileToken, 'ext.riiak.Transport.httpMultiRequest');

        do {
            $status = curl_multi_exec($mh, $active);
        } while ($status === CURLM_CALL_MULTI_PERFORM);

        $results = array();
        while ($active && $status === CURLM_OK) {
            if (curl_multi_select($mh) != -1) {
                do {
                    $status = curl_multi_exec($mh, $active);
                } while ($status === CURLM_CALL_MULTI_PERFORM);

                /**
                 * If a request finished
                 */
                if (($mhinfo = curl_multi_info_read($mh))) {
                    $ch = $mhinfo['handle'];
                    /**
                     * Find which URL this response belongs to
                     */
                    $url = array_search((int) $ch, $instanceMap);

                    try {
                        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);

                        /**
                         * Get headers
                         */
                        $parsedHeaders = self::parseHttpHeaders($responses[$url]['responseHeadersIO']);
                        $responseHeaders = array_merge(array('http_code' => $httpCode), array_change_key_case($parsedHeaders, CASE_LOWER));

                        /**
                         * Return headers/body array
                         */
                        $results[$url] = array('headers' => $responseHeaders, 'body' => $responses[$url]['responseBodyIO']);
                    } catch (Exception $e) {
                        error_log('Error: ' . $e->getMessage());
                        $results[$url] = null;
                    }
                    curl_multi_remove_handle($mh, $ch);
                    curl_close($ch);
                }
            }
        }

        if ($client->enableProfiling)
            Yii::endProfile($profileToken, 'ext.riiak.Transport.httpMultiRequest');

        curl_multi_close($mh);
        return $results;
    }

    /**
     * Parse HTTP header string into an assoc array
     *
     * @param string $headers
     * @return array
     */
    public static function parseHttpHeaders($headers) {
        $retVal = array();
        $fields = array_filter(explode("\r\n", preg_replace('/\x0D\x0A[\x09\x20]+/', ' ', $headers)));
        foreach ($fields as $field) {
            if (preg_match('/([^:]+): (.+)/m', $field, $match)) {
                $match[1] = preg_replace('/(?<=^|[\x09\x20\x2D])./e', 'strtoupper("\0")', strtolower(trim($match[1])));
                if (isset($retVal[$match[1]])) {
                    $retVal[$match[1]] = array($retVal[$match[1]], $match[2]);
                } else {
                    $retVal[$match[1]] = trim($match[2]);
                }
            }
        }
        return $retVal;
    }
    
    
    /**
     * Run the map/reduce operation. Returns array of results
     * or Link objects if last phase is link phase
     *
     * @param integer $timeout Timeout in seconds. Default: null
     * @param object $objMapReduce
     * @return array
     */
    public function run($timeout = null, $objMapReduce) {
        $numPhases = count($objMapReduce->phases);

        $linkResultsFlag = false;

        /**
         * If there are no phases, then just echo the inputs back to the user.
         */
        if ($numPhases == 0) {
            $objMapReduce->reduce(array('riak_kv_mapreduce', 'reduce_identity'));
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
            $phase = $objMapReduce->phases[$i];
            if ($i == ($numPhases - 1) && !$keepFlag)
                $phase->keep = true;
            if ($phase->keep)
                $keepFlag = true;
            $query[] = $phase->toArray();
        }

        /**
         * Add key filters if applicable
         */
        if ($objMapReduce->inputMode == 'bucket' && count($objMapReduce->keyFilters) > 0) {
            $objMapReduce->inputs = array(
                'bucket' => $objMapReduce->inputs,
                'key_filters' => $objMapReduce->keyFilters
            );
        }

        /**
         * Construct the job, optionally set the timeout
         */
        $job = array('inputs' => $objMapReduce->inputs, 'query' => $query);
        if ($timeout != null)
            $job['timeout'] = $timeout;
        $content = CJSON::encode($job);
        $bucket = $objMapReduce->inputs;


        /**
         * Execute the request
         */
        Yii::trace('Running Map/Reduce query', 'ext.riiak.Transport');
        $url = self::buildUrl($objMapReduce->client) . '/' . $objMapReduce->client->mapredPrefix;
        $response = self::httpRequest($objMapReduce->client, 'POST', $url, array(), $content);
        $result = CJSON::decode($response['body']);

        /**
         * If the last phase is NOT a link phase, then return the result.
         */
        $linkResultsFlag |= ( end($objMapReduce->phases) instanceof LinkPhase);

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
            $link->client = $objMapReduce->client;
            $a[] = $link;
        }
        return $a;
    }
    
    
    /**
     * Store the object in Riak. Upon completion, object could contain new
     * metadata, and possibly new data if Riak contains a newer version of
     * the object according to the object's vector clock.
     *
     * @param int $w W-Value: X paritions must respond before returning
     * @param int $dw DW-Value: X partitions must confirm write before returning
     * @param Object $objObject An object of \riiak\Object
     * @return \riiak\Object
     */
    public function store($w = null, $dw = null, $objObject) {
        /**
         * Use defaults if not specified
         */
        $w = $objObject->bucket->getW($w);
        $dw = $objObject->bucket->getDW($w);

        /**
         * Construct the URL
         */
        $params = array('returnbody' => 'true', 'w' => $w, 'dw' => $dw);
        $url = self::buildRestPath($objObject->client, $objObject->bucket, $objObject->key, null, $params);

        /**
         * Construct the headers
         */
        $headers = array('Accept: text/plain, */*; q=0.5',
            'Content-Type: ' . $objObject->getContentType(),
            'X-Riak-ClientId: ' . $objObject->client->clientId);

        /**
         * Add the vclock if it exists
         */
        if (!empty($objObject->vclock))
            $headers[] = 'X-Riak-Vclock: ' . $objObject->vclock;

        /**
         * Add the Links
         */
        foreach ($objObject->_links as $link)
            $headers[] = 'Link: ' . $link->toLinkHeader($objObject->client);

        if ($objObject->jsonize)
            $content = CJSON::encode($objObject->_data);
        else
            $content = $objObject->_data;

        $method = $objObject->key ? 'PUT' : 'POST';

        /**
         * Run the operation
         */
        Yii::trace('Storing object "' . $objObject->key . '" in bucket "' . $objObject->bucket->name . '"', 'ext.riiak.Transport');
        $response = self::httpRequest($objObject->client, $method, $url, $headers, $content);

        $objObject->populate($response, array(200, 201, 300));
        return $objObject;
    }
    
    /**
     * Reload the object from Riak. When this operation completes, the object
     * could contain new metadata and a new value, if the object was updated
     * in Riak since it was last retrieved.
     *
     * @param int $r R-Value: X partitions must respond before returning
     * @param Object $objObject An object of \riiak\Object
     * @return \riiak\Object
     */
    public function reload($r = null, $objObject) {
        /**
         * Do the request
         */
        $url = self::buildReloadUrl($objObject, $r);

        Yii::trace('Reloading object "' . $objObject->key . '" from bucket "' . $objObject->bucket->name . '"', 'ext.riiak.Transport');
        $response = self::httpRequest($objObject->client, 'GET', $url);

        return self::populateResponse($objObject, $response);
    }
    
    /**
     * Build reload URL.
     * 
     * @param Object $objObject An object of \riiak\Object
     * @param String $r
     * @return String  
     */
    public static function buildReloadUrl($objObject, $r = null) {
        $params = array('r' => $objObject->bucket->getR($r));
        return self::buildRestPath($objObject->client, $objObject->bucket, $objObject->key, null, $params);
    }
    
    /**
     * Load data using siblings
     * 
     * @param Object $object An object of \riiak\Object
     * @param Array $response
     * @return Object \riiak\Object
     */
    public static function populateResponse(Object &$object, $response) {
        $object->populate($response, array(200, 300, 404));

        /**
         * If there are siblings, load the data for the first one by default
         */
        if ($object->getHasSiblings()) {
            $obj = $object->getSibling(0);
            $object->_data = $obj->data;
        }
        
        return $object;
    }
    
    /**
     * Delete this object from Riak
     *
     * @param int $dw DW-Value: X partitions must delete object before returning
     * @param Object $objObject An object of \riiak\Object
     * @return \riiak\Object
     */
    public function delete($dw = null, $objObject) {
        /**
         * Use defaults if not specified
         */
        $dw = $objObject->bucket->getDW($dw);

        /**
         * Construct the URL
         */
        $params = array('dw' => $dw);
        $url = self::buildRestPath($objObject->client, $objObject->bucket, $objObject->key, null, $params);

        /**
         * Run the operation
         */
        Yii::trace('Deleting object "' . $objObject->key . '" from bucket "' . $objObject->bucket->name . '"', 'ext.riiak.Transport');
        $response = self::httpRequest($objObject->client, 'DELETE', $url);

        $objObject->populate($response, array(204, 404));

        return $objObject;
    }
    
    /**
     * Retrieve a sibling by sibling number
     *
     * @param int $i Sibling number
     * @param int $r R-Value: X partitions must respond before returning
     * @param Object $objObject An object of \riiak\Object
     * @return \riiak\Object
     */
    public function getSibling($i, $r = null, $objObject) {
        /**
         * Use defaults if not specified
         */
        $r = $objObject->bucket->getR($r);

        /**
         * Run the request
         */
        $vtag = $objObject->siblings[$i];
        $params = array('r' => $r, 'vtag' => $vtag);
        $url = self::buildRestPath($objObject->client, $objObject->bucket, $objObject->key, null, $params);

        Yii::trace('Fetching sibling "' . $i . '" of object "' . $objObject->key . '" from bucket "' . $objObject->bucket->name . '"', 'ext.riiak.Transport');
        $response = self::httpRequest($objObject->client, 'GET', $url);

        /**
         * Respond with a new object
         */
        $obj = new Object($objObject->client, $objObject->bucket, $objObject->key);
        $obj->jsonize = $objObject->jsonize;
        $obj->populate($response, array(200));
        return $obj;
    }
    /**
     *
     * @param Riiak $client
     * @param array $objects
     * @param String $r
     * @param Object $objObject An object of \riiak\Object
     * @return Object \riiak\Object
     */
     public static function reloadMulti(Riiak $client, array $objects, $r = null) {
        Yii::trace('Reloading multiple objects', 'ext.riiak.Transport');
        $objects = array_combine(array_map(array('self', 'buildReloadUrl'), $objects, array_fill(0, count($objects), $r)), $objects);
        $responses = self::httpMultiRequest($client, 'GET', array_keys($objects));
        array_walk($objects, function(&$object, $url)use(&$responses) {
                    Object::populateResponse($object, $responses[$url]);
                });
        return $objects;
    }
}

