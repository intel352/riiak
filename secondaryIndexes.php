<?php

namespace riiak;

use \CComponent,
    \CJSON,
    \Exception,
    \Yii;

/**
 * Secondary indexes implementation class
 * 
 * @package riiak
 * @todo Working on dynamically set and get search criteria functionality
 */
class SecondaryIndexes extends Backend {
    /**
     *
     * @var array 
     */
    protected $search;
    /**
     *
     * @var string 
     */
    protected $bucket;
    
    /**
     * Secondary Index equal operator
     */
    public $SIndexEqual = '';
    
    /**
     * Secondary Index equal operator
     */
    public $SIndexBinary = '_bin';
    
    /**
     * Secondary Index equal operator
     */
    public $SIndexInteger = '_int';
    
    /**
     * Method to get keys using secondary index
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
        $arrKeys = $this->run(null);
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
     * Method to prepare key filters
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
                    $arrSearchCriteria[$intIndex]['operator'] = $this->SIndexEqual;
                    /**
                     * Check type of filter input
                     */
                    if($valueType != 'string') {
                       $arrSearchCriteria[$intIndex]['type']  = $this->SIndexInteger;
                    } else {
                       $arrSearchCriteria[$intIndex]['type']  = $this->SIndexBinary;
                    }
                    $intIndex++;
                }
            }
            $this->search = CJSON::encode($arrSearchCriteria);
        } catch (Exception $e) {
            error_log('Error: ' . $e->getMessage());
            return NULL;
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
}