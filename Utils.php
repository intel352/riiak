<?php

namespace riiak;

use \CComponent,
    \Exception,
    \Yii;

/**
 * Utility functions used by Riiak library.
 * @package riiak
 */
class Utils extends CComponent {
    /**
     * Builds URL to connect to Riak server
     *
     * @param object $objClient
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
            $profileToken = 'ext.riiak.Utils.httpRequest(' . \CVarDumper::dumpAsString(self::readableCurlOpts($curlOpts)) . ')';

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

        Yii::trace('Executing HTTP ' . $method . ': ' . $url . ($obj ? ' with content "' . $obj . '"' : ''), 'ext.riiak.Utils');
        try {
            if ($client->enableProfiling)
                Yii::beginProfile($profileToken, 'ext.riiak.Utils.httpRequest');

            /**
             * Run the request
             */
            curl_exec($ch);
            $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
            curl_close($ch);

            if ($client->enableProfiling)
                Yii::endProfile($profileToken, 'ext.riiak.Utils.httpRequest');

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

        Yii::trace('Executing HTTP Multi ' . $method . ': ' . \CVarDumper::dumpAsString($urls) . ($obj ? ' with content "' . $obj . '"' : ''), 'ext.riiak.Utils');
        if ($client->enableProfiling)
            $profileToken = 'ext.riiak.Utils.httpMultiRequest(' . \CVarDumper::dumpAsString(self::readableCurlOpts($curlOpts)) . ')';

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
            Yii::beginProfile($profileToken, 'ext.riiak.Utils.httpMultiRequest');

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
            Yii::endProfile($profileToken, 'ext.riiak.Utils.httpMultiRequest');

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
}