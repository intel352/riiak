<?php

namespace riiak\transport\http;

use \CComponent,
    \CJSON,
    \Exception,
    \Yii;

/**
 * Contains transport layer actions of Riak
 * @package http
 */
class Curl extends \riiak\transport\Http {

    /**
     * Builds a CURL URL to access Riak API
     *
     * @param string $method
     * @param string $url
     * @param array $requestHeaders
     * @param string $obj
     * @return string
     */
    public function buildCurlOpts($method, $url, array $requestHeaders = array(), $obj = '') {
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
    public function readableCurlOpts(array $curlOpts) {
        $constants = get_defined_constants(true);
        $curlConstants = array_flip(array_intersect_key(array_flip($constants['curl']), $curlOpts));
        return array_map(function($const)use($curlOpts) {
                            return $curlOpts[$const];
                        }, $curlConstants);
    }

    /**
     * Method to process HTTP request using CURL.
     * 
     * @param \riiak\Riiak $client
     * @param string $method
     * @param string $url
     * @param array $requestHeaders
     * @param object $obj
     * @return array 
     */
    public function processRequest(\riiak\Riiak $client, $method, $url, array $requestHeaders = array(), $obj = '') {
        /**
         * Init curl
         */
        $ch = curl_init();
        $curlOpts = $this->buildCurlOpts($method, $url, $requestHeaders, $obj);

        if ($client->enableProfiling)
            $profileToken = 'ext.Transport.Curl.httpRequest(' . \CVarDumper::dumpAsString($this->readableCurlOpts($curlOpts)) . ')';

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
            if ($client->enableProfiling)
                Yii::beginProfile($profileToken, 'ext.Transport.Curl.httpRequest');

            /**
             * Run the request
             */
            curl_exec($ch);
            $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
            curl_close($ch);

            if ($client->enableProfiling)
                Yii::endProfile($profileToken, 'ext.Transport.Curl.httpRequest');

            /**
             * Prepare curl response
             */
            $responseData = array('http_code' => $httpCode, 'headerData' => $responseHeadersIO, 'body' => $responseBodyIO);
            /**
             * Return curl response.
             */
            return $responseData;
        } catch (Exception $e) {
            curl_close($ch);
            error_log('Error: ' . $e->getMessage());
            return NULL;
        }
    }

    /**
     * Method to remove bulk of empty keys from riak response.
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
         *  then output wiil be {"keys":[]},{"keys":["admin"]},{"keys":[]},{"keys":["test"]}
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
             *  Check for keys count is greate than 0.
             */
            if (array_key_exists('keys', $data) && 1 < count($data['keys'])) {
                $strKeys .= implode('#,#', $data['keys']) . '#,#';
            } else if (array_key_exists('keys', $data) && 0 < count($data['keys'])) {
                $strKeys .= $data['keys'][0] . '#,#';
            }
        }
        /**
         * Return list of keys as JSON string.
         */
        $arrOutput["keys"] = explode('#,#', substr($strKeys, 0, strlen($strKeys) - 3));
        return CJSON::encode($arrOutput);
    }

}