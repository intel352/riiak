<?php

/**
 * Utility functions used by Riiak library.
 * @package RiiakUtils
 */
class RiiakUtils extends CComponent {
    
    /**
     * Builds URL to connect to Riak server
     *
     * @param Riiak $client
     * @return string
     */
    public static function buildUrl(Riiak $client) {
        return 'http'.($client->ssl?'s':'').'://' . $client->host . ':' . $client->port;
    }

    /**
     * Builds a REST URL to access Riak API
     *
     * @param Riiak $client
     * @param RiiakBucket $bucket
     * @param string $key
     * @param string $spec
     * @param array $params
     * @return string 
     */
    public static function buildRestPath(Riiak $client,RiiakBucket $bucket=NULL, $key=NULL, $spec=NULL,array $params=NULL) {
        /**
         * Build http[s]://hostname:port/prefix[/bucket]
         */
        $path = self::buildUrl($client) . '/' . $client->prefix;

        /**
         * Add bucket
         */
        if (!is_null($bucket) && $bucket instanceof RiiakBucket)
            $path .= '/' . urlencode($bucket->name);

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
     * Executes HTTP request, returns named array(headers, body) of request, or null on error
     *
     * @param string $method GET|POST|PUT|DELETE
     * @param string $url
     * @param array $requestHeaders
     * @param string $obj
     * @return array|null
     */
    public static function httpRequest($method, $url,array $requestHeaders = array(), $obj = '') {
        /**
         * Init curl
         */
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_HTTPHEADER, $requestHeaders);
        
        switch($method) {
            case 'GET':
                curl_setopt($ch, CURLOPT_HTTPGET, 1);
                break;
            case 'POST':
                curl_setopt($ch, CURLOPT_POST, 1);
                curl_setopt($ch, CURLOPT_POSTFIELDS, $obj);
                break;
            /**
             * PUT/DELETE both declare CUSTOMREQUEST, thus no break after PUT
             */
            case 'PUT':
                curl_setopt($ch, CURLOPT_POSTFIELDS, $obj);
            case 'DELETE':
                curl_setopt($ch, CURLOPT_CUSTOMREQUEST, $method);
                break;
        }
        
        /**
         * Capture response headers
         */
        curl_setopt($ch, CURLOPT_HEADERFUNCTION,
            function($ch, $data) use(&$responseHeadersIO){
                $responseHeadersIO.=$data;
                return strlen($data);
            });

        /**
         * Capture response body
         */
        curl_setopt($ch, CURLOPT_WRITEFUNCTION,
            function($ch, $data) use(&$responseBodyIO){
                $responseBodyIO.=$data;
                return strlen($data);
            });

        try {
            /**
             * Run the request
             */
            curl_exec($ch);
            $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
            curl_close($ch);

            /**
             * Get headers
             */
            $parsedHeaders = RiiakUtils::parseHttpHeaders($responseHeadersIO);
            $responseHeaders = array_merge(array('http_code' => $httpCode),  array_change_key_case($parsedHeaders, CASE_LOWER));

            /**
             * Return headers/body array
             */
            return array('headers'=>$responseHeaders, 'body'=>$responseBodyIO);
        } catch (Exception $e) {
            curl_close($ch);
            error_log('Error: ' . $e->getMessage());
            return NULL;
        }
    }

    /**
     * Parse HTTP header string into an assoc array
     *
     * @param string $headers
     * @return array
     */
    static function parseHttpHeaders($headers) {
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