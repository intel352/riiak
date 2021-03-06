![Basho Riak](http://wiki.basho.com/images/riaklogo.png)

# Riak PHP Client for Yii #
This is a PHP client for Riak, built as an extension for Yii Framework.  
Ported from Basho's official PHP client.

## Helpful Links ##
Yii Framework Riiak extension page  
<http://www.yiiframework.com/extension/riiak/>

Yii Framework Riiak discussion thread  
<http://www.yiiframework.com/forum/index.php?/topic/20090-riiak/>

## Documentation ##
API documentation for this library can be found at  
<https://bitbucket.org/intel352/riiak/wiki>

Documentation for use of Riak clients in general can be found at  
<http://wiki.basho.com/Client-Libraries.html>

## Repositories ##

The official source code for this client can be retrieved from  
<https://bitbucket.org/intel352/riiak>

Riak can be obtained pre-built from  
<http://downloads.basho.com/riak/>

or as source from  
<http://github.com/basho/riak/>

## Requirements ##
* Yii 1.1+ (Yii 1.1.7+ recommended, lower releases should be compatible, but are untested)
* PHP 5.3+ (Riiak library is namespaced, which is a PHP 5.3 feature)

## Installation ##
Clone this repository to fetch the latest version of this client

    hg clone https://bitbucket.org/intel352/riiak

## Quick start ##
This quick example assumes that you have a local riak cluster running on port 8098,
and have installed the Riiak extension into protected/extensions/riiak/

###config/main.php:###

    // ...
    'aliases'=>array(
        'riiak'=>'ext.riiak',
    ),
    // ...
    'components' => array(
        // ...
        'riiak'=>array(
            'class'=>'\riiak\Riiak',
        ),
        // ...
    ),
    // ...


###controller:###

    # Load Riiak component
    $client = Yii::app()->getComponent('riiak');

    # Choose a bucket name
    $bucket = $client->bucket('test');

    # Supply a key under which to store your data
    $person = $bucket->newObject('riak_developer_1', array(
        'name' => 'John Smith',
        'age' => 28,
        'company' => 'Facebook'
    ));

    # Save the object to Riak
    $person->store();

    # Fetch the object
    $person = $bucket->get('riak_developer_1');

    # Update the object
    $person->data['company'] = 'Google';
    $person->store();

## Connecting ##
Connect to a Riak server, as shown above, returns a [Riiak](https://bitbucket.org/intel352/riiak/wiki/class/Riiak) client instance

## Using Buckets ##
To select a bucket, use the \\riiak\\Riiak::bucket() method

    # Choose a bucket name
    $bucket = $client->bucket('test');

or using the \\riiak\\Bucket constructor

    # Create a bucket
    $bucket = new \riiak\Bucket($client, 'test');

If a bucket by this name does not already exist, a new one will be created for you when you store your first key.
This method returns a [Bucket](https://bitbucket.org/intel352/riiak/wiki/class/Bucket)

## Creating Objects ##
Objects can be created using the \\riiak\\Bucket::newObject() method

    # Create an object for future storage and populate it with some data
    $person = $bucket->newObject('riak_developer_1');

or using the \\riiak\\Object constructor

    # Create an object for future storage
    $person = new \riiak\Object($client, $bucket, 'riak_developer_1');

Both methods return a [Object](https://bitbucket.org/intel352/riiak/wiki/class/Object)

## Setting Object Values ##
Object data can be set using the \\riiak\\Object::setData() method

    # Populate object with some data
    $person->setData(array(
        'name' => 'John Smith',
        'age' => 28,
        'company' => 'Facebook'
    ));

or you may modify the object's data property using Yii's magic setter functionality

    # Populate object with some data
    $person->data = array(
        'name' => 'John Smith',
        'age' => 28,
        'company' => 'Facebook'
    );

This method returns a [Object](https://bitbucket.org/intel352/riiak/wiki/class/Object)

## Storing Objects ##
Objects can be stored using the \\riiak\\Object::store() method

    # Save the object to Riak
    $person->store();

This method returns a [Object](https://bitbucket.org/intel352/riiak/wiki/class/Object)

## Chaining ##
For methods like newObject(), setData() and store() which return objects of a similar class (in this case Object), chaining can be used to perform multiple operations in a single statement.

    # Create, set, and store an object
    $data = array(
    	'name' => 'John Smith',
    	'age' => 28,
    	'company' => 'Facebook'
    );
    $bucket->newObject('riak_developer_1')->setData($data)->store();

## Fetching Objects ##
Objects can be retrieved from a bucket using the \\riiak\\Bucket::get() method

    # Save the object to Riak
    $person = $bucket->get('riak_developer_1');

This method returns a [Object](https://bitbucket.org/intel352/riiak/wiki/class/Object)

## Modifying Objects ##
Objects can be modified using the \\riiak\\Object::store() method

    # Update the object
    $person = $bucket->get('riak_developer_1');
    $person->data['company'] = 'Google';
    $person->store();

This method returns a [Object](https://bitbucket.org/intel352/riiak/wiki/class/Object)

## Deleting Objects ##
Objects can be deleted using the \\riiak\\Object::delete() method

    # Update the object
    $person = $bucket->get('riak_developer_1');
    $person->delete();

This method returns a [Object](https://bitbucket.org/intel352/riiak/wiki/class/Object)

## Adding a Link ##
Links can be added using \\riiak\\Object::addLink()

    # Add a link from John to Dave
    $john = $bucket->get('riak_developer_1');
    $dave = $bucket->get('riak_developer_2');
    $john->addLink($dave, 'friend')->store();

This method returns a [Object](https://bitbucket.org/intel352/riiak/wiki/class/Object)

## Removing a Link ##
Links can be removed using \\riiak\\Object::removeLink()

    # Remove the link from John to Dave
    $john = $bucket->get('riak_developer_1');
    $dave = $bucket->get('riak_developer_2');
    $john->removeLink($dave, 'friend')->store();

This method returns a [Object](https://bitbucket.org/intel352/riiak/wiki/class/Object)

## Retrieving Links ##
An object's links can be retrieved using \\riiak\\Object::getLinks()

    # Retrieve all of John's links
    $john = $bucket->get('riak_developer_1');
    $links = $john->getLinks();

OR using Yii's magic getter

    $links = $john->links;

This method returns an array of [Link](https://bitbucket.org/intel352/riiak/wiki/class/Link)s

## Linkwalking ##
Linkwalking can be done using the \\riiak\\Object::link() method

    # Retrieve all of John's friends
    $john = $bucket->get('riak_developer_1');
    $friends = $john->link($bucket->name, 'friend')->run();

This method returns an array of [Link](https://bitbucket.org/intel352/riiak/wiki/class/Link)s

## Dereferencing Links ##
Links can be dereferenced to the linked object using the \\riiak\\Link::get() method

    # Retrieve all of John's friends
    $john = $bucket->get('riak_developer_1');
    $dave = $bucket->get('riak_developer_2');
    $john->addLink($dave, 'friend')->store();
    $friends = $john->link($bucket->name, 'friend')->run();
    $dave = $friends[0]->get();

This method returns a [Object](https://bitbucket.org/intel352/riiak/wiki/class/Object)

## Fetching Data With Map/Reduce ##
Data can be fetched by Map and Reduce using the \\riiak\\Riiak::getMapReduce() method

    # Fetch a sorted list of all keys in a bucket
    $result = $client->getMapReduce() // or $client->mapReduce using magic getter
        ->addBucket($bucket->name) // addBucket expects string, bucket name
    	->map('function (v) { return [v.key]; }')
    	->reduce('Riak.reduceSort')
    	->run();

This method returns an array of data representing the result of the Map/Reduce functions.

*More examples of Map/Reduce can be found in unit_tests.php (@todo - not existing yet)*

## Using Search ##
Searches can be executed using the \\riiak\\MapReduce::search() method

    # Create some test data
    $bucket = $client->bucket('searchbucket');
    $bucket->newObject('one', array('foo'=>'one', 'bar'=>'red'))->store();
    $bucket->newObject('two', array('foo'=>'two', 'bar'=>'green'))->store();

    # Execute a search for all objects with matching properties
    $results = $client->mapReduce->search('searchbucket', 'foo:one OR foo:two')->run();

This method will return null unless executed against a Riak Search cluster.

## Get Riak server configuration details ##
    $client->configuration property returns all Riak configuration details, like nodename, riak_search_version, ssl_version, mem_total, mem_allocated etc information.

## Check Riak server supports secondary indexes(2i) ##
    $client->getIsSecondaryIndexSupport() method returns boolean value either Riak supports secondary index functionality or not.

## Check Riak server supports multi-backend ##
    $client->getIsMultiBackendSupport() method returns boolean value either Riak supports multi-backend or not.

## Meta Data ##
You can provide meta data on objects using \\riiak\\Object::getMetaValue() and \\riiak\\Object::setMetaValue()

    # Set some new meta data
    $object->setMetaValue('some-meta', 'some-value');

    # Get some meta data (returns null if not found)
    $object->getMetaValue('some-meta');

    # Set all meta data (an array keyed by meta name)
    $object->setMeta(array('metakey'=>'metaval'));
    $object->meta = array('metakey'=>'metaval');

    # Get all meta data (an array keyed by meta name)
    $meta = $object->getMeta();
    $meta = $object->meta;

Remove existing metadata

    # Remove a single value
    $object->removeMeta('some-meta');

    # Remove all meta data
    $object->removeAllMeta();

## Adding Secondary Indexes ##
Secondary indexes can be added using the \\riiak\\Object::addIndex() and \\riiak\\Object::addAutoIndex() methods.

Auto indexes are kept fresh with the associated field automatically, so if you read an object, modify its data, and write it back, the auto index will reflect the new value from the object.  Traditional indexes are fixed and must be manually managed.  *NOTE* that auto indexes are a function of the Riak PHP client, and are not part of native Riak functionality.  Other clients writing the same object must manage the index manually.

    # Create some test data
    $bucket = $client->bucket('indextest');
    $bucket
      ->newObject('one', array('some_field'=>1, 'bar'=>'red'))
      # the 3rd parameter for addIndex() is an index value that we're supplying
      ->addIndex('index_name', 'int', 1)
      ->addIndex('index_name', 'int', 2)
      ->addIndex('text_index', 'bin', 'apple')
      # a shortcut to addAutoIndex() is to call addIndex() without 3rd parameter
      ->addAutoIndex('some_field', 'int')
      ->addAutoIndex('bar', 'bin')
      ->store();

You can remove a specific value from an index, all values from an index, or all indexes:

    # Remove just a single value
    $object->removeIndex('index_name', 'int', 2);

    # Remove all values from an index
    $object->removeAllIndexes('index_name', 'int');

    # Remove all index types for a given index name
    $object->removeAllIndexes('index_name');

    # Remove all indexes
    $object->removeAllIndexes();

Likewise you can remove auto indexes:

    # Just the 'foo' index
    $object->removeAutoIndex('foo', 'int');

    # All auto indexes
    $object->removeAllAutoIndexes('foo', 'int');

    # All auto indexes
    $object->removeAllAutoIndexes();

Mass load indexes, or just replace an existing index:

    $object->setIndex('index_name', 'int', array(1, 2, 3));
    $object->setIndex('text_index', 'bin', 'foo');

## Querying Secondary Indexes ##
Secondary indexes can be queried using the \\riiak\\Bucket::indexSearch() method.  This returns an array of \\riiak\\Link objects.

    # Exact Match
    $results = $bucket->indexSearch('index_name', 'int', 1);
    foreach ($results as $link) {
        echo 'Key: '.$link->getKey().'<br/>';
        $object = $link->get();
    }

    # Range Search
    $results = $bucket->indexSearch('index_name', 'int', 1, 10);

Duplicate entries may be found in a ranged index search if a given index has multiple values that fall within the range.  You can request that these duplicates be eliminated in the result.

    $results = $bucket->indexSearch('index_name', 'int', 1, 10, true);