<?php

namespace riiak;

use \CComponent,
    \CJSON,
    \Exception,
    \Yii;

/**
 * The Object holds meta information about a Riak object, plus the
 * object's data.
 * @package riiak
 *
 * Magic properties
 *
 * @property string $contentType
 * @property mixed $data
 * @property bool $exists
 * @property array[string]string $meta
 * @property array[string][int]string|int $indexes
 * @property array[string]string $autoIndexes
 * @property array[int]string $siblings
 *
 * @property-read int $status Status code of response
 * @property-read bool $hasSiblings
 * @property-read int $siblingCount
 * @property-read array[int]Link $links
 * @property-read MapReduce $mapReduce
 * @property-read string $vclock
 */
class Object extends CComponent {

    /**
     * Client instance
     *
     * @var Riiak
     */
    public $client;

    /**
     * Bucket
     *
     * @var Bucket
     */
    public $bucket;

    /**
     * Key
     *
     * @var string
     */
    public $key;

    /**
     * Whether or not to treat object as json
     *
     * @var bool
     */
    public $jsonize = true;

    /**
     * @var array
     */
    public $headers = array();

    /**
     * Array of Links
     *
     * @var array[int]Link
     */
    protected $_links = array();

    /**
     * Array of vtags
     *
     * @var array[int]string
     */
    protected $_siblings = array();

    /**
     * Whether the object exists
     *
     * @var bool
     */
    protected $_exists = false;

    /**
     * If constructed by newBinary|getBinary, returns string.
     * If not a string, will be JSON encoded when stored
     *
     * @var mixed
     */
    protected $_data;

    /**
     * @var array[string]string
     */
    protected $_meta = array();

    /**
     * @var array[string][int]string|int
     */
    protected $_indexes = array();

    /**
     * @var array[string]string
     */
    protected $_autoIndexes = array();

    /**
     * Construct a new Object
     *
     * @param Riiak $client A Riiak object
     * @param Bucket $bucket A Bucket object
     * @param string $key Optional - If empty, generated upon store()
     */
    public function __construct(Riiak $client, Bucket $bucket, $key = null) {
        $this->client = $client;
        $this->bucket = $bucket;
        $this->key = $key;
    }

    /**
     * Returns HTTP status of last operation
     *
     * @return int
     */
    public function getStatus() {
        return $this->headers['http_code'];
    }

    /**
     * Returns the object's content type
     *
     * @return string
     */
    public function getContentType() {
        return $this->headers['content-type'];
    }

    /**
     * Set the object's content type
     *
     * @param string $contentType The new content type
     * @return Object
     */
    public function setContentType($contentType) {
        $this->headers['content-type'] = $contentType;
        return $this;
    }

    /**
     * Returns the object's data
     *
     * @return mixed
     */
    public function getData() {
        return $this->_data;
    }

    /**
     * Set the object's data
     *
     * @param mixed $data The new data value
     * @return Object
     */
    public function setData($data) {
        $this->_data = $data;
        return $this;
    }

    /**
     * Get whether the object exists
     *
     * @return bool
     */
    public function getExists() {
        return $this->_exists;
    }

    /**
     * Set whether the object exists
     *
     * @param bool $bool
     * @return Object
     */
    public function setExists($bool) {
        $this->_exists = (bool) $bool;
        return $this;
    }

    /**
     * Add a link to a Object
     *
     * @param Link|Object $obj Either Object or Link
     * @param string $tag optional link tag. Default: bucket name. Ignored for Link
     * @return Object
     */
    public function addLink($obj, $tag = null) {
        if ($obj instanceof Link)
            $newlink = $obj;
        else
            $newlink = new Link($obj->bucket->name, $obj->key, $tag);

        $this->removeLink($newlink);
        $this->_links[] = $newlink;

        return $this;
    }

    /**
     * Remove a link to a Object
     *
     * @param Link|Object $obj Either Object or Link
     * @param string $tag optional link tag. Default: bucket name. Ignored for Link
     * @return Object
     */
    public function removeLink($obj, $tag = null) {
        if ($obj instanceof Link)
            $oldlink = $obj;
        else
            $oldlink = new Link($obj->bucket->name, $obj->key, $tag);

        foreach ($this->_links as $k => $link)
            if (!$link->isEqual($oldlink))
                unset($this->_links[$k]);

        return $this;
    }

    /**
     * Return an array of Link objects
     *
     * @return array
     */
    public function getLinks() {
        /**
         * Set the clients before returning
         */
        foreach ($this->_links as $link)
            $link->client = $this->client;

        return $this->_links;
    }

    /** @section Indexes */

    /**
     * @param array $array
     * @param string $name
     * @param string|int $value
     * @param 'int'|'bin' $type optional
     * @return Object
     */
    protected function _addIndex(array &$array, $name, $value, $type = null) {
        $index = strtolower($name . ($type !== null ? '_' . $type : ''));
        if (!isset($array[$index]))
            $array[$index] = array();

        /**
         * Riak de-dupes, but _addIndex is also used for autoIndex management
         */
        if (!in_array($value, $array[$index]))
            $array[$index][] = $value;

        return $this;
    }

    /**
     * @param array $array
     * @param string $name
     * @param array|string|int $value
     * @param 'int'|'bin' $type optional
     * @return Object
     */
    protected function _setIndex(array &$array, $name, $value, $type = null) {
        $index = strtolower($name . ($type !== null ? '_' . $type : ''));

        $array[$index] = $value;

        return $this;
    }

    /**
     * @param array $array
     * @param string $name
     * @param 'int'|'bin' $type optional
     * @return bool
     */
    protected function _hasIndex(array &$array, $name, $type = null) {
        $index = strtolower($name . ($type !== null ? '_' . $type : ''));
        return isset($array[$index]);
    }

    /**
     * @param array $array
     * @param string $name
     * @param 'int'|'bin' $type optional
     * @return array|string|int
     */
    protected function _getIndex(array &$array, $name, $type = null) {
        $index = strtolower($name . ($type !== null ? '_' . $type : ''));

        if (!isset($array[$index]))
            return null;

        return $array[$index];
    }

    /**
     * @param array $array
     * @param string $name
     * @param 'int'|'bin' $type optional
     * @param string|int $value optional Explicit value to remove
     * @return Object
     */
    protected function _removeIndex(array &$array, $name, $type = null, $value = null) {
        $index = strtolower($name . ($type !== null ? '_' . $type : ''));

        if (isset($array[$index]))
            if ($value !== null) {
                if (is_array($array[$index]) && false !== ($position = array_search($value, $array[$index])))
                    unset($array[$index][$position]);
            }else{
                unset($array[$index]);
            }

        return $this;
    }

    /**
     * @param array $array
     * @param string $name
     * @param 'int'|'bin' $type optional
     * @return Object
     */
    protected function _removeAllIndexes(array &$array, $name = null, $type = null) {
        if ($name === null)
            $array = array();
        else if ($type !== null)
            unset($array[strtolower($name . '_' . $type)]);
        else {
            $name = strtolower($name);
            unset($array[$name . '_int']);
            unset($array[$name . '_bin']);
        }

        return $this;
    }

    /**
     * Adds a secondary index to the object
     * This will create the index if it does not exist, or will
     * append an additional value if the index already exists and
     * does not contain the provided value.
     *
     * @param string $name
     * @param 'int'|'bin' $type optional
     * @param string|int $explicitValue optional If provided, uses this
     * value explicitly.  If not provided, this will search the object's
     * data for a data field named $name, and use it's value.
     * @return $this
     */
    public function addIndex($name, $type = null, $explicitValue = null) {
        if ($explicitValue === null)
            return $this->addAutoIndex($name, $type);

        return $this->_addIndex($this->_indexes, $name, $explicitValue, $type);
    }

    /**
     * Sets a given index to a specific value or set of values
     *
     * @param string $name
     * @param array|string|int $value
     * @param 'int'|'bin' $type optional
     * @return $this
     */
    public function setIndex($name, $value, $type = null) {
        return $this->_setIndex($this->_indexes, $name, (array) $value, $type);
    }

    /**
     * @param string $name
     * @param 'int'|'bin' $type optional
     * @return bool
     */
    public function hasIndex($name, $type = null) {
        return $this->_hasIndex($this->_indexes, $name, $type);
    }

    /**
     * Gets the current values for the identified index
     * Note, the NULL value has special meaning - when the object is
     * ->store()d, this value will be replaced with the current value
     * the value of the field matching $indexName from the object's data
     *
     * @param string $name
     * @param 'int'|'bin' $type optional
     *
     * @return array
     */
    public function getIndex($name, $type = null) {
        return $this->_getIndex($this->_indexes, $name, $type);
    }

    /**
     * @param array[string][int]string|int $value
     * @return Object
     */
    public function setIndexes(array $value) {
        $this->_indexes = $value;
        return $this;
    }

    /**
     * @return array[string][int]string|int
     */
    public function getIndexes() {
        return $this->_indexes;
    }

    /**
     * Removes a specific value from a given index
     *
     * @param string $name
     * @param 'int'|'bin' $type optional
     * @param string|int $explicitValue optional
     * @return $this
     */
    public function removeIndex($name, $type = null, $explicitValue = null) {
        return $this->_removeIndex($this->_indexes, $name, $type, $explicitValue);
    }

    /**
     * Bulk index removal
     * If $indexName and $indexType are provided, all values for the
     * identified index are removed.
     * If just $indexName is provided, all values for all types of
     * the identified index are removed
     * If neither is provided, all indexes are removed from the object
     *
     * Note that this function will NOT affect auto indexes
     *
     * @param string $name optional
     * @param 'int'|'bin' $type optional
     *
     * @return $this
     */
    public function removeAllIndexes($name = null, $type = null) {
        return $this->_removeAllIndexes($this->_indexes, $name, $type);
    }

    /** @section Auto Indexes */

    /**
     * Adds an automatic secondary index to the object
     * The value of an automatic secondary index is determined at
     * time of ->store() by looking for an $fieldName key
     * in the object's data.
     *
     * @param string $name
     * @param 'int'|'bin' $type optional
     *
     * @return $this
     */
    public function addAutoIndex($name, $type = null) {
        return $this->_setIndex($this->_autoIndexes, $name, $name, $type);
    }

    /**
     * Returns whether the object has a given auto index
     *
     * @param string $name
     * @param 'int'|'bin' $type optional
     *
     * @return boolean
     */
    public function hasAutoIndex($name, $type = null) {
        return $this->_hasIndex($this->_autoIndexes, $name, $type);
    }

    /**
     * @param array[string]string $value
     * @return Object
     */
    public function setAutoIndexes(array $value) {
        $this->_autoIndexes = $value;
        return $this;
    }

    /**
     * @return array[string]string
     */
    public function getAutoIndexes() {
        return $this->_autoIndexes;
    }

    /**
     * Removes a given auto index from the object
     *
     * @param string $name
     * @param 'int'|'bin' $type optional
     *
     * @return $this
     */
    public function removeAutoIndex($name, $type = null) {
        return $this->_removeIndex($this->_autoIndexes, $name, $type);
    }

    /**
     * Removes all auto indexes
     * If $fieldName is not provided, all auto indexes on the
     * object are stripped, otherwise just indexes on the given field
     * are stripped.
     * If $indexType is not provided, all types of index for the
     * given field are stripped, otherwise just a given type is stripped.
     *
     * @param string $name
     * @param 'int'|'bin' $type optional
     *
     * @return $this
     */
    public function removeAllAutoIndexes($name = null, $type = null) {
        return $this->_removeAllIndexes($this->_autoIndexes, $name, $type);
    }

    /** @section Meta Data */

    /**
     * Gets a given metadata value
     * Returns null if no metadata value with the given name exists
     *
     * @param string $metaName
     *
     * @return string|null
     */
    public function getMetaValue($metaName) {
        $metaName = strtolower($metaName);
        if (isset($this->_meta[$metaName]))
            return $this->_meta[$metaName];
        return null;
    }

    /**
     * Sets a given metadata value, overwriting an existing
     * value with the same name if it exists.
     * @param string $metaName
     * @param string $value
     * @return $this
     */
    public function setMetaValue($metaName, $value) {
        $this->_meta[strtolower($metaName)] = $value;
        return $this;
    }

    /**
     * Removes a given metadata value
     * @param string $metaName
     * @return $this
     */
    public function removeMetaValue($metaName) {
        unset($this->_meta[strtolower($metaName)]);
        return $this;
    }

    /**
     * Gets all metadata values
     * @return array[string]string
     */
    public function getMeta() {
        return $this->_meta;
    }

    /**
     * @param array $value
     * @return Object
     */
    public function setMeta(array $value) {
        $this->_meta = $value;
        return $this;
    }

    /**
     * Strips all metadata values
     * @return $this
     */
    public function removeMeta() {
        $this->_meta = array();
        return $this;
    }

    /**
     * Store the object in Riak. Upon completion, object could contain new
     * metadata, and possibly new data if Riak contains a newer version of
     * the object according to the object's vector clock.
     *
     * @param int $w optional W-Value: X partitions must respond before returning
     * @param int $dw optional DW-Value: X partitions must confirm write before returning
     * @return Object
     */
    public function store($w = null, $dw = null) {
        /**
         * Use defaults if not specified
         */
        $w = $this->bucket->getW($w);
        $dw = $this->bucket->getDW($w);

        /**
         * Construct the URL
         */
        $params = array('returnbody' => 'true', 'w' => $w, 'dw' => $dw);
        $url = $this->client->transport->buildBucketKeyPath($this->bucket, $this->key, null, $params);

        /**
         * Construct the headers
         */
        $headers = array('Accept: text/plain, */*; q=0.5',
            'Content-Type: ' . $this->getContentType(),
            'X-Riak-ClientId: ' . $this->client->clientId);

        /**
         * Add the vclock if it exists
         */
        if (!empty($this->vclock))
            $headers[] = 'X-Riak-Vclock: ' . $this->vclock;

        /**
         * Add the Links
         */
        foreach ($this->_links as $link)
            $headers[] = 'Link: ' . $link->toLinkHeader($this->client);

        /**
         * Add the auto indexes
         */
        if (is_array($this->_autoIndexes) && !empty($this->_autoIndexes)) {
            if (!is_array($this->data))
                throw new Exception('Auto index feature requires that "$this->data" be an array.');

            $collisions = array();
            foreach ($this->_autoIndexes as $index => $fieldName) {
                $value = null;
                // look up the value
                if (isset($this->data[$fieldName])) {
                    $value = $this->data[$fieldName];
                    $headers[] = 'X-Riak-Index-' . $index . ': ' . urlencode($value);

                    // look for value collisions with normal indexes
                    if (isset($this->_indexes[$index]))
                        if (false !== array_search($value, $this->_indexes[$index]))
                            $collisions[$index] = $value;
                }
            }

            $this->_meta['client-autoindex'] = count($this->_autoIndexes) > 0 ? CJSON::encode($this->_autoIndexes) : null;
            $this->_meta['client-autoindexcollision'] = count($collisions) > 0 ? CJSON::encode($collisions) : null;
        }

        /**
         * Add the indexes
         */
        foreach ($this->_indexes as $index => $values)
            if (is_array($values))
                $headers[] = 'X-Riak-Index-' . $index . ': ' . implode(', ', array_map('urlencode', $values));

        /**
         * Add the metadata
         */
        foreach ($this->_meta as $metaName => $metaValue)
            if ($metaValue !== null)
                $headers[] = 'X-Riak-Meta-' . $metaName . ': ' . $metaValue;

        if ($this->jsonize)
            $content = CJSON::encode($this->data);
        else
            $content = $this->data;

        /**
         * Run the operation
         */
        if ($this->key) {
            Yii::trace('Storing object with key "' . $this->key . '" in bucket "' . $this->bucket->name . '"', 'ext.riiak.Object');
            $response = $this->client->transport->putObject($this->bucket, $headers, $content, $url);
        } else {
            Yii::trace('Storing new object in bucket "' . $this->bucket->name . '"', 'ext.riiak.Object');
            $response = $this->client->transport->post($url, $headers, $content);
        }

        return self::populateResponse($this, $response, 'storeObject');
    }

    /**
     * Reload the object from Riak. When this operation completes, the object
     * could contain new metadata and a new value, if the object was updated
     * in Riak since it was last retrieved.
     *
     * @param int $r optional R-Value: X partitions must respond before returning
     * @return Object
     */
    public function reload($r = null) {
        /**
         * Do the request
         */
        $params = array('r' => $this->bucket->getR($r));
        Yii::trace('Reloading object "' . $this->key . '" from bucket "' . $this->bucket->name . '"', 'ext.riiak.Object');
        $response = $this->client->transport->getObject($this->bucket, $params, $this->key, null);
        return self::populateResponse($this, $response);
    }

    /**
     * @param Riiak $client
     * @param array $objects
     * @param int $r optional
     * @return array[string]Object
     */
    public static function reloadMulti(Riiak $client, array $objects, $r = null) {
        Yii::trace('Reloading multiple objects', 'ext.riiak.Object');
        $objects = array_combine(array_map(array('self', 'buildReloadUrl'), $objects, array_fill(0, count($objects), $r)), $objects);

        /**
         * Get (fetch) multiple objects
         */
        $responses = $client->transport->multiGet(array_keys($objects));
        array_walk($objects, function(&$object, $url)use(&$responses) {
                    Object::populateResponse($object, $responses[$url]);
                });
        return $objects;
    }

    /**
     * @param Object $object
     * @param int $r optional
     * @return string
     */
    protected static function buildReloadUrl(Object $object, $r = null) {
        $params = array('r' => $object->bucket->getR($r));
        return $object->client->transport->buildBucketKeyPath($object->bucket, $object->key, null, $params);
    }

    /**
     * @param Object $object
     * @param array $response
     * @return Object
     */
    public static function populateResponse(Object &$object, $response, $action='fetchObject') {
        $object->client->transport->populate($object, $object->bucket, $response, $action);

        /**
         * Parse the index and metadata headers
         */
        foreach ($object->headers as $key => $val) {
            if (preg_match('~^x-riak-([^-]+)-(.+)$~', $key, $matches)) {
                switch ($matches[1]) {
                    case 'index':
                        $index = substr($matches[2], 0, strrpos($matches[2], '_'));
                        $type = substr($matches[2], strlen($index) + 1);
                        $object->setIndex($index, array_map('urldecode', explode(', ', $val)), $type);
                        break;
                    case 'meta':
                        $object->setMetaValue($matches[2], $val);
                        break;
                }
            }
        }

        /**
         * If there are siblings, load the data for the first one by default
         */
        if ($object->getHasSiblings()) {
            $sibling = $object->getSibling(0);
            $object->data = $sibling->data;
        }

        /**
         * Look for auto indexes and deindex explicit values if appropriate
         */
        if (isset($object->meta['client-autoindex'])) {
            /**
             * dereference the autoindexes
             */
            $object->autoIndexes = CJSON::decode($object->meta['client-autoindex']);
            $collisions = isset($object->meta['client-autoindexcollision']) ? CJSON::decode($object->meta['client-autoindexcollision']) : array();

            if (is_array($object->autoIndexes) && is_array($object->data))
                foreach ($object->autoIndexes as $index => $fieldName) {
                    $value = null;

                    if (isset($object->data[$fieldName])) {
                        $value = $object->data[$fieldName];
                        /**
                         * Only strip this value if not explicit index
                         * @todo review logic
                         */
                        if (!(isset($collisions[$index]) && $collisions[$index] === $value))
                            if ($value !== null)
                                $object->removeIndex($index, null, $value);
                    }
                }
        }

        return $object;
    }

    /**
     * Delete this object from Riak
     *
     * @param int $dw optional DW-Value: X partitions must delete object before returning
     * @return Object
     */
    public function delete($dw = null) {
        /**
         * Use defaults if not specified
         */
        $dw = $this->bucket->getDW($dw);

        /**
         * Construct the URL
         */
        $url = $this->client->transport->buildBucketKeyPath($this->bucket, $this->key, null, array('dw' => $dw));

        /**
         * Run the operation
         */
        Yii::trace('Deleting object "' . $this->key . '" from bucket "' . $this->bucket->name . '"', 'ext.riiak.Object');
        $response = $this->client->transport->delete($url);
        $this->client->transport->populate($this, $this->bucket, $response, 'deleteObject');

        return $this;
    }

    /**
     * Reset this object
     *
     * @return Object
     */
    public function clear() {
        $this->headers = array();
        $this->_links = array();
        $this->_data = null;
        $this->_exists = false;
        $this->_siblings = array();
        $this->_indexes = array();
        $this->_autoIndexes = array();
        $this->_meta = array();
        return $this;
    }

    /**
     * Get the vclock of this object
     *
     * @return string|null
     */
    public function getVclock() {
        if (array_key_exists('x-riak-vclock', $this->headers))
            return $this->headers['x-riak-vclock'];
        return null;
    }

    /**
     * Populate object links
     *
     * @param string $linkHeaders
     * @return Object
     */
    public function populateLinks($linkHeaders) {
        $linkHeaders = explode(',', trim($linkHeaders));
        foreach ($linkHeaders as $linkHeader)
            if (preg_match('/\<\/([^\/]+)\/([^\/]+)\/([^\/]+)\>; ?riaktag="([^"]+)"/', trim($linkHeader), $matches))
                $this->_links[] = new Link(urldecode($matches[2]), urldecode($matches[3]), urldecode($matches[4]));

        return $this;
    }

    /**
     * Return true if this object has siblings
     *
     * @return bool
     */
    public function getHasSiblings() {
        return ($this->getSiblingCount() > 0);
    }

    /**
     * Get the number of siblings that this object contains
     *
     * @return int
     */
    public function getSiblingCount() {
        return count($this->_siblings);
    }

    /**
     * Retrieve a sibling by sibling number
     *
     * @param int $i Sibling number
     * @param int $r R-Value: X partitions must respond before returning
     * @return Object
     */
    public function getSibling($i, $r = null) {
        /**
         * Use defaults if not specified
         */
        $r = $this->bucket->getR($r);

        /**
         * Run the request
         */
        $vtag = $this->_siblings[$i];
        $params = array('r' => $r, 'vtag' => $vtag);

        Yii::trace('Fetching sibling "' . $i . '" of object "' . $this->key . '" from bucket "' . $this->bucket->name . '"', 'ext.riiak.Object');
        $response = $this->client->transport->getObject($this->bucket, $params, $this->key, null);

        /**
         * Respond with a new object
         */
        $obj = new Object($this->client, $this->bucket, $this->key);
        $obj->jsonize = $this->jsonize;
        return self::populateResponse($obj, $response, 'getSibling');
    }

    /**
     * Retrieve an array of siblings
     *
     * @todo It's possible to fetch multiple siblings in 1 request. That should be implemented here.
     * @link http://wiki.basho.com/HTTP-Fetch-Object.html#Get-all-siblings-in-one-request
     *
     * @param int $r R-Value: X partitions must respond before returning
     * @return array[int]Object
     */
    public function getSiblings($r = null) {
        $a = array();
        for ($i = 0; $i < $this->getSiblingCount(); $i++)
            $a[] = $this->getSibling($i, $r);

        return $a;
    }

    /**
     * Specify sibling vtags
     *
     * @param array[int]string $siblings
     * @return Object
     */
    public function setSiblings(array $siblings) {
        $this->_siblings = $siblings;
        return $this;
    }

    /**
     * Returns a MapReduce instance
     *
     * @param bool $reset Whether to create a new MapReduce instance
     * @return MapReduce
     */
    public function getMapReduce($reset = false) {
        return $this->client->getMapReduce($reset);
    }

    /**
     * Returns a SecondaryIndex instance
     *
     * @param bool $reset Whether to create a new SecondaryIndex instance
     * @return SecondaryIndex
     */
    public function getSecondaryIndex($reset = false) {
        return $this->client->getSecondaryIndex($reset);
    }

}
