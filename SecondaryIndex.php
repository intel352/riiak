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
     * Instance of Secondary index module
     * 
     * @var modules\secondayindex\Index 
     */
    public $_secondaryIndex;
    
    /**
     * Construct a new Object
     *
     * @param \riiak\Riiak $client A Riiak object
     */
    public function __construct(Riiak $client){
        if(!is_object($this->_secondaryIndex))
                $this->_secondaryIndex = new \ext\activedocument\modules\secondayindex\Index();
        
        parent::__construct($client);
    }
    
    /**
     * Get list of keys which satisfies search criteria
     * Call transport layer methods for Riak connection, handle response
     * and return list of resultant keys.
     * 
     * @param \ext\activedocument\Criteria $objCriteria 
     * @return array
     */
    public function getKeys(\ext\activedocument\Criteria $objCriteria) {
        
        /**
         * Array to hold list of resultant keys
         */
        $arrKeys = array();
        /**
         * Set bucket name
         */
        $this->bucket = $objCriteria->container;
        /**
         * Set search filters (search criteria)
         */
        $this->setfilter($objCriteria->search);
        /**
         * Process request
         */
        if(1 == count($objCriteria->search))
            $arrKeys = $this->run(null);
        else
            $arrKeys = $this->multiCriteriaSearch(null);
        /**
         * Get unique key set
         */
        $arrKeys = array_unique($arrKeys['keys']);
        $arrValues = array();
        foreach($arrKeys as $key => $value){
            $arrValues[] = $value;
        }
        $arrResponse['keys'] = $arrValues;
        /**
         * Return Riak response
         */
        return $arrResponse;
    }
    
    /**
     * Set search criteria
     * Encode search criteria using CJSON::encode and 
     * return encoded string
     * 
     * @param array $filter 
     * @return string
     */
    public function setfilter(array $filter) {
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
                     * Check for indexing is present on the column or not.
                     */
                    if(!$this->_secondaryIndex->checkColumnIndex($this->bucket, $value['column']))
                        continue;
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
                    if($valueType != 'string') {
                       $arrSearchCriteria[$intIndex]['type']  = $this->secIndexInteger;
                    } else {
                       $arrSearchCriteria[$intIndex]['type']  = $this->secIndexBinary;
                    }
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
     * 
     * @param string $timeout 
     */
    public function run($timeout = null) {
        $arrSearchCriteria = array();
        /**
         * Get search criteria
         */
        $arrSearchCriteria = CJSON::decode($this->search);
        /**
         * Construct URL
         */
        $url = $this->client->_transport->buildSIRestPath($this->bucket, null, $arrSearchCriteria);
        $response = $this->client->_transport->processRequest('GET', $url);
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
        $responses = $this->client->_transport->multiget(array_keys($arrSearchCriteria));
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
     * @param \riak\SecondaryIndex $object
     * @return string 
     */
    protected static function buildSIReloadUrl($params, $object) {
        $params = array($params);
        return $object->client->_transport->buildSIRestPath($object->bucket, null, $params);
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
                'container' => $strBucket,
                'data' => ''
            );
        }
        return $arrOutputKeys;
    }
}