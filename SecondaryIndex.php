<?php

namespace riiak;

use \CComponent,
    \CJSON,
    \Exception,
    \Yii,
    \CLogger;

/**
 * The SecondaryIndex object allows you to perform all
 * Secondary index(2i) implementation operations on Riak.
 * @package riiak
 */
class SecondaryIndex extends Backend {

    /**
     * @var array List of Search criterias
     */
    public $search;

    /**
     * Holds Bucket name
     *
     * @var string
     */
    public $bucket;

    /**
     * Equal operator constant
     *
     * @var string
     */
    public $secIndexEqual = '';

    /**
     * Binary column key suffix
     *
     * @var string Default: '_bin'
     */
    public $secIndexBinary = '_bin';

    /**
     * Interger column key suffix
     *
     * @var string Default: '_int'
     */
    public $secIndexInteger = '_int';

    /**
     * Set search criteria
     * Encode search criteria using CJSON::encode and
     * return encoded string
     *
     * @param array $filter
     * @return string
     */
    public function setFilter(array $filter) {
        return $this->keyFilterAnd($filter);
    }

    /**
     * Prepare all key filters for secondary index(2i)
     * operations, throws an exception in case failure of any filter operation
     *
     * @param array $filter
     */
    public function keyFilterAnd(array $filter) {
        try{
            $arrSearchCriteria = array();
            $intIndex = 0;
            /**
             * Check if filter array is empty
             */
            if(!empty($filter)) {
                /**
                 * Prepare loop to process each filter
                 */
                foreach($filter as $key => $value) {
                    /**
                     * Set search column details in filter
                     */
                    $arrSearchCriteria[$intIndex]['column']   = $value['column'];
                    $arrSearchCriteria[$intIndex]['keyword']  = $value['keyword'];
                    $valueType = gettype($value['keyword']);
                    $arrSearchCriteria[$intIndex]['operator'] = $this->secIndexEqual;

                    /**
                     * Check type of filter input
                     */
                    if($valueType != 'string')
                       $arrSearchCriteria[$intIndex]['type']  = $this->secIndexInteger;
                    else
                       $arrSearchCriteria[$intIndex]['type']  = $this->secIndexBinary;

                    $intIndex++;
                }
            }
            $this->search = CJSON::encode($arrSearchCriteria);
        } catch (Exception $e) {
             Yii::log($e->getMessage(), CLogger::LEVEL_ERROR, 'ext.riiak.SecondaryIndex');
             throw new Exception(Yii::t('Riiak', 'Failed to add search criteria.'), (int) $e->getCode(), $e->errorInfo);
        }
    }

    /**
     *
     * @param array $filter
     */
    public function keyFilterOr(array $filter) {

    }

    /**
     * Run Secondary Index operation on Riak,
     * Call transport layer methods for Riak connection, handle response and
     * return result-set.
     */
    public function run() {
        $arrSearchCriteria = array();

        /**
         * Get search criteria
         */
        $arrSearchCriteria = CJSON::decode($this->search);

        /**
         * Construct URL
         */
        $url = $this->client->transport->buildSIRestPath($this->bucket, null, $arrSearchCriteria);
        $response = $this->client->transport->processRequest('GET', $url);
        return CJSON::decode($response['body']);
    }

    /**
     * Run Secondary Index multiget operation on Riak,
     * Call transport layer methods for Riak connection, handle response and
     * return result-set.
     */
    public function multiCriteriaSearch(){
        $arrSearchCriteria = array();

        /**
         * Get search criteria
         */
        $arrSearchCriteria = CJSON::decode($this->search);

        /**
         * Construct URLs
         */
        $arrSearchCriteria = array_combine(array_map(array('self', 'buildSIReloadUrl'), $arrSearchCriteria, array_fill(0, count($arrSearchCriteria), $this)), $arrSearchCriteria);
        $responses = $this->client->transport->multiGet(array_keys($arrSearchCriteria));
        $arrIntersect = array();

        /**
         * Prepare loop to handle multiple responses
         */
        $index = 0;
        foreach($responses as $key => $response) {
            $arrKeys = CJSON::decode($response['body']);

            if(0 === $index)
                $arrIntersect = $arrKeys['keys'];

            $arrIntersect = array_intersect($arrIntersect, $arrKeys['keys']);
            $index++;
        }

        /**
         * Return list of keys which satisfies search criteria
         */
        $arrOutput['keys'] = $arrIntersect;
        return $arrOutput;
    }

    /**
     * Construct URL for Riak operation
     *
     * @param array $params
     * @param \riiak\SecondaryIndex $object
     * @return string
     */
    protected static function buildSIReloadUrl($params, $object) {
        $params = array($params);
        return $object->client->transport->buildSIRestPath($object->bucket, null, $params);
    }

    /**
     * Prepare list of input keys for Map/Reduce query.
     *
     * @param array $arrkeys
     * @param string $strBucket
     * @return array
     */
    public function prepareInputKeys($arrKeys = array(), $strBucket = ''){
        $arrOutputKeys = array();
        foreach($arrKeys as $index => $value){
            $arrOutputKeys[] = array(
                'key' => $value,
                'container' => $strBucket                
            );
        }
        return $arrOutputKeys;
    }
}