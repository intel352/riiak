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
     * Method to get keys using secondary index
     * 
     * @param object $objCriteria 
     */
    public function getKeys(\ext\activedocument\Criteria $objCriteria){
        $arrKeys = array();
        $this->bucket = $objCriteria->container;
        $this->filter($objCriteria->search);
        $arrKeys = $this->run(null);
        return $arrKeys;
    }
    
    /**
     * Set search criteria
     * 
     * @param array $filter 
     */
    public function filter(array $filter){
        $this->search = CJSON::encode($filter);
    }
    
    /**
     *
     * @param array $filter 
     */
    public function keyFilterAnd(array $filter){
        
    }
    /**
     *
     * @param array $filter 
     */
    public function keyFilterOr(array $filter){
        
    }
    /**
     * 
     * @param type $objCriteria 
     */
    public function run($timeout = null){
        $arrSearchCriteria = array();
        /**
         * Get search criteria
         */
        $arrSearchCriteria = CJSON::decode($this->search);
        /**
         * @todo Working on moving URL generation logic from secondaryIndex class to transport layer class.
         */
        $url = $this->client->_transport->buildUrl($this->client) . '/' . $this->client->bucketPrefix . '/'  . $this->bucket . '/' . $this->client->SIPrefix . '/' .  $arrSearchCriteria[0]['column'] . '_bin/eq/' . $arrSearchCriteria[0]['keyword'];
        $response = $this->client->_transport->processRequest('GET', $url);
        return CJSON::decode($response['body']);
    }
}