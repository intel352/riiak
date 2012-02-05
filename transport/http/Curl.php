<?php

namespace riiak\transport\http;

use \CComponent,
    \CJSON,
    \Exception,
    \Yii,
    \CLogger;

/**
 * Curl Object handles all Transport layer operations using CURL
 * Performs set curl options, read curl options, process request using CURL
 * and process HTTP response headers.
 * @package riiak.transport.http
 */
class Curl extends \riiak\transport\Http {

    /**
     * Builds CURL URL to access Riak API
     *
     * @param string $method
     * @param string $url
     * @param array $requestHeaders
     * @param string $content
     * @return string
     */
    public function buildCurlOpts($method, $url, array $requestHeaders = array(), $content = '') {
        $curlOptions = array(
            CURLOPT_URL => $url,
            CURLOPT_HTTPHEADER => $requestHeaders,
            CURLOPT_FOLLOWLOCATION => true,
            CURLOPT_MAXREDIRS => -1, // follow all redirects
        );
        switch ($method) {
            case 'GET':
                $curlOptions[CURLOPT_HTTPGET] = 1;
                break;
            case 'POST':
                $curlOptions[CURLOPT_POST] = 1;
                $curlOptions[CURLOPT_POSTFIELDS] = $content;
                break;
            /**
             * PUT/DELETE both declare CUSTOMREQUEST, thus no break after PUT
             */
            case 'PUT':
                $curlOptions[CURLOPT_POSTFIELDS] = $content;
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
    public function readableCurlOpts(array $curlOpts) {
        $constants = get_defined_constants(true);
        /**
         * Ensure that we're only comparing against CURLOPT constants, due to overlaps in numeric codes
         */
        $curlOptConstants = array_intersect_key($constants['curl'], array_flip(preg_filter('/^CURLOPT\_/','$0',array_keys($constants['curl']))));
        $curlConstants = array_intersect($curlOptConstants,array_keys($curlOpts));
        return array_map(function($const)use($curlOpts) {
                            return $curlOpts[$const];
                        }, $curlConstants);
    }

    /**
     * Process HTTP request
     *
     * @param 'GET'|'POST'|'PUT'|'DELETE' $method
     * @param string $url
     * @param array $requestHeaders optional
     * @param string $content optional
     * @return array|null
     */
    public function processRequest($method, $url, array $requestHeaders = array(), $content = '') {
        /**
         * Init curl
         */
        $ch = curl_init();
        $curlOpts = $this->buildCurlOpts($method, $url, $requestHeaders, $content);

        Yii::trace('Processing HTTP "'.strtoupper($method).'" request to "'.$url.'"', 'ext.riiak.transport.http.curl.processRequest');
        \CVarDumper::dump($curlOpts, 10, true);
        if ($this->client->enableProfiling)
            $profileToken = 'ext.riiak.transport.http.curl.processRequest(' . \CVarDumper::dumpAsString($this->readableCurlOpts($curlOpts)) . ')';

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
        try {
            if ($this->client->enableProfiling)
                Yii::beginProfile($profileToken, 'ext.riiak.transport.http.curl.processRequest');

            /**
             * Run the request
             */
            curl_exec($ch);
            $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
            curl_close($ch);

            if ($this->client->enableProfiling)
                Yii::endProfile($profileToken, 'ext.riiak.transport.http.curl.processRequest');

            /**
             * Return curl response.
             */
            return $this->processResponse($httpCode, $responseHeadersIO, $responseBodyIO);
        } catch (Exception $e) {
            curl_close($ch);
            Yii::log($e->getMessage(), CLogger::LEVEL_ERROR, 'ext.riiak.transport.http.curl.processRequest');
            return NULL;
        }
    }

    /**
     * Get (fetch) multiple objects
     *
     * @param array $urls
     * @param array $requestHeaders
     * @param string $content
     * @return array
     */
    public function multiGet(array $urls, array $requestHeaders = array(), $content = '') {
        /**
         * Init multi-curl
         */
        $mh = curl_multi_init();
        $curlOpts = $this->buildCurlOpts('GET', '', $requestHeaders, $content);

        Yii::trace('Executing HTTP Multi GET: ' . \CVarDumper::dumpAsString($urls) . ($content ? ' with content "' . $content . '"' : ''), 'ext.Transport.Http.Curl');
        if ($this->client->enableProfiling)
            $profileToken = 'ext.riiak.transport.http.curl.multiGet(' . \CVarDumper::dumpAsString($this->readableCurlOpts($curlOpts)) . ')';

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

        if ($this->client->enableProfiling)
            Yii::beginProfile($profileToken, 'ext.riiak.transport.http.curl.multiGet');

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
                         * Process response
                         */
                        $results[$url] = $this->processResponse($httpCode, $responses[$url]['responseHeadersIO'], $responses[$url]['responseBodyIO']);
                    } catch (Exception $e) {
                        Yii::log($e->getMessage(), CLogger::LEVEL_ERROR, 'ext.riiak.transport.http.curl.multiGet');
                        $results[$url] = null;
                    }

                    curl_multi_remove_handle($mh, $ch);
                    curl_close($ch);
                }
            }
        }

        if ($this->client->enableProfiling)
            Yii::endProfile($profileToken, 'ext.riiak.transport.http.curl.multiGet');

        curl_multi_close($mh);
        return $results;
    }

    /**
     * Remove bulk of empty keys from Riak response.
     *
     * @todo This function needs dire help. Should only be used for processing keys from request
     *
     * @param array $response
     * @param array $params
     * @return String  List of keys in JSON format.
     */
    public function getStreamedBucketKeys(array $response, array $params) {
        /**
         * Check if keys!=stream then return same response body.
         */
        if (!array_key_exists('keys', $params) || $params['keys'] != 'stream')
            return $response['body'];

        /**
         * Replace all blank array keys.
         */
        $response['body'] = str_replace('{"keys":[]}', '', $response['body']);

        /**
         * Declare required variables
         */
        $arrInput = array();
        $arrOutput = array();
        $strKeys = '';

        /**
         *  Convert input string into array
         *  for example :
         *  Input string is {"keys":[]}{"keys":["admin"]}{"keys":[]}{"keys":["test"]} etc.
         *  then output will be {"keys":[]},{"keys":["admin"]},{"keys":[]},{"keys":["test"]}
         */
        $arrInput = explode('#,#', str_replace('}{', '}#,#{', $response['body']));

        /**
         * Prepare loop to process input array.
         */
        foreach ($arrInput as $index => $value) {
            /**
             *  Decode input string '"keys":["admin"]' into array.
             */
            $data = (array) CJSON::decode($value);

            /**
             *  Check for keys count is greater than 0.
             */
            if (array_key_exists('keys', $data) && 1 < count($data['keys']))
                $strKeys .= implode('#,#', $data['keys']) . '#,#';
            else if (array_key_exists('keys', $data) && 0 < count($data['keys']))
                $strKeys .= $data['keys'][0] . '#,#';
        }

        /**
         * Return list of keys as JSON string.
         */
        $arrOutput['keys'] = explode('#,#', substr($strKeys, 0, strlen($strKeys) - 3));
        return CJSON::encode($arrOutput);
    }

}