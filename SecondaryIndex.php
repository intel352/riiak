<?php

namespace riiak;

use \CComponent,
    \CJSON,
    \Exception,
    \Yii,
    \CLogger;

/**
 * Secondary indexes implementation class
 * 
 * @package riiak
 * @todo Working on dynamically set and get search criteria functionality
 */
class SecondaryIndex extends Backend {
    /**
     *
     * @var array Array to save Search criteria
     */
    protected $search;
    /**
     *
     * @var string Bucket name
     */
    protected $bucket;
    
    /**
     * Secondary Index equal operator
     * 
     * @var string  
     */
    public $secIndexEqual = '';
    
    /**
     * Secondary index binary key suffix
     * 
     * @var string
     */
    public $secIndexBinary = '_bin';
    
    /**
     * Secondary index interger key suffix
     * 
     * @var string 
     */
    public $secIndexInteger = '_int';
    
    /**
     * Method to get list of keys using secondary index
     * 
     * @param object $objCriteria 
     * @return array
     */
    public function getKeys(\ext\activedocument\Criteria $objCriteria) {
        $arrKeys = array();
        /**
         * Get bucket
         */
        $this->bucket = $objCriteria->container;
        /**
         * Set search filters
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
        return $arrResponse;
    }
    
    /**
     * Set search criteria
     * 
     * @param array $filter 
     * @return string
     */
    public function setfilter(array $filter) {
        return $this->keyFilterAnd($filter);
    }
    
    /**
     * Prepare all key filters
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
                     * Set column details
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
     * 
     * @param type $objCriteria 
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
    
    public function multiCriteriaSearch(){
        $arrSearchCriteria = array();
        /**
         * Get search criteria
         */
        $arrSearchCriteria = CJSON::decode($this->search);
        $arrSearchCriteria = array_combine(array_map(array('self', 'buildSIReloadUrl'), $arrSearchCriteria, array_fill(0, count($arrSearchCriteria), $this)), $arrSearchCriteria);
        $responses = $this->client->_transport->multiget(array_keys($arrSearchCriteria));
        $arrIntersect = array();
        /**
         * Prepare loop handle multiple responses
         */
        foreach($responses as $key => $response) {
            $arrKeys = CJSON::decode($response['body']);
            
            if(0 >= count($arrIntersect))
                $arrIntersect = $arrKeys['keys'];
            
            $arrIntersect = array_intersect($arrIntersect, $arrKeys['keys']);
        }
        /**
         * Return list of keys which satisfies criteria
         */
        $arrOutput['keys'] = $arrIntersect;
        return $arrOutput;
    }
    
    /**
     *
     * @param array $params
     * @param object $object
     * @return string 
     */
    protected static function buildSIReloadUrl($params, $object) {
        $params = array($params);
        return $object->client->_transport->buildSIRestPath($object->bucket, null, $params);
    }
    
}