<img src="http://www.basho.com/images/riaklogo.png">

# Riak PHP Client for Yii #
This is a PHP client for Riak, built as an extension for Yii Framework.
Ported from Basho's official PHP client.

## Helpful Links ##
Yii Framework Riiak extension page<br/>
<http://www.yiiframework.com/extension/riiak/>

Yii Framework Riiak discussion thread<br/>
<http://www.yiiframework.com/forum/index.php?/topic/20090-riiak/>

## Documentation ##
API documentation for this library can be found at<br/>
<https://bitbucket.org/intel352/riiak/wiki>

Documentation for use of Riak clients in general can be found at<br/>
<https://wiki.basho.com/display/RIAK/Client+Libraries>

## Repositories ##

The official source code for this client can be retrieved from<br/>
<https://bitbucket.org/intel352/riiak>

Riak can be obtained pre-built from<br/>
<http://downloads.basho.com/riak/>

or as source from<br/>
<http://github.com/basho/riak/>

## Installation ##
Clone this repository to fetch the latest version of this client

    hg clone https://bitbucket.org/intel352/riiak

## Quick start ##
This quick example assumes that you have a local riak cluster running on port 8098,
and have installed the Riiak extension into protected/extensions/riiak/

    Yii::import('ext.riiak.*');

    # Connect to Riak
    $client = new Riiak('127.0.0.1', 8098);

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
Connect to a Riak server by specifying the address or hostname and port:

    # Connect to Riak
    $client = new Riiak('127.0.0.1', 8098);

This method returns a [Riiak](https://bitbucket.org/intel352/riiak/wiki/class/Riiak) client instance

## Using Buckets ##
To select a bucket, use the Riiak::bucket() method

    # Choose a bucket name
    $bucket = $client->bucket('test');

or using the RiiakBucket() constructor

    # Create a bucket
    $bucket = new RiiakBucket($client, 'test');

If a bucket by this name does not already exist, a new one will be created for you when you store your first key.
This method returns a [RiiakBucket](https://bitbucket.org/intel352/riiak/wiki/class/RiiakBucket)

## Creating Objects ##
Objects can be created using the RiiakBucket::newObject() method

    # Create an object for future storage and populate it with some data
    $person = $bucket->newObject('riak_developer_1');

or using the RiiakObject() constructor

    # Create an object for future storage
    $person = new RiiakObject($client, $bucket, 'riak_developer_1');

Both methods return a [RiiakObject](https://bitbucket.org/intel352/riiak/wiki/class/RiiakObject)

## Setting Object Values ##
Object data can be set using the RiiakObject::setData() method

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

This method returns a [RiiakObject](https://bitbucket.org/intel352/riiak/wiki/class/RiiakObject)

## Storing Objects ##
Objects can be stored using the RiiakObject::store() method

    # Save the object to Riak
    $person->store();

This method returns a [RiiakObject](https://bitbucket.org/intel352/riiak/wiki/class/RiiakObject)

## Chaining ##
For methods like newObject(), setData() and store() which return objects of a similar class (in this case RiiakObject), chaining can be used to perform multiple operations in a single statement.

    # Create, set, and store an object
    $data = array(
    	'name' => 'John Smith',
    	'age' => 28,
    	'company' => 'Facebook'
    );
    $bucket->newObject('riak_developer_1')->setData($data)->store();

## Fetching Objects ##
Objects can be retrieved from a bucket using the RiiakBucket::get() method

    # Save the object to Riak
    $person = $bucket->get('riak_developer_1');

This method returns a [RiiakObject](https://bitbucket.org/intel352/riiak/wiki/class/RiiakObject)

## Modifying Objects ##
Objects can be modified using the RiiakObject::store() method

    # Update the object
    $person = $bucket->get('riak_developer_1');
    $person->data['company'] = 'Google';
    $person->store();

This method returns a [RiiakObject](https://bitbucket.org/intel352/riiak/wiki/class/RiiakObject)

## Deleting Objects ##
Objects can be deleted using the RiiakObject::delete() method

    # Update the object
    $person = $bucket->get('riak_developer_1');
    $person->delete();

This method returns a [RiiakObject](https://bitbucket.org/intel352/riiak/wiki/class/RiiakObject)

## Adding a Link ##
Links can be added using RiiakObject::addLink()

    # Add a link from John to Dave
    $john = $bucket->get('riak_developer_1');
    $dave = $bucket->get('riak_developer_2');
    $john->addLink($dave, 'friend')->store();

This method returns a [RiiakObject](https://bitbucket.org/intel352/riiak/wiki/class/RiiakObject)

## Removing a Link ##
Links can be removed using RiiakObject::removeLink()

    # Remove the link from John to Dave
    $john = $bucket->get('riak_developer_1');
    $dave = $bucket->get('riak_developer_2');
    $john->removeLink($dave, 'friend')->store();

This method returns a [RiiakObject](https://bitbucket.org/intel352/riiak/wiki/class/RiiakObject)

## Retrieving Links ##
An object's links can be retrieved using RiiakObject::getLinks()

    # Retrieve all of John's links
    $john = $bucket->get('riak_developer_1');
    $links = $john->getLinks();

OR using Yii's magic getter

    $links = $john->links;

This method returns an array of [RiiakLink](https://bitbucket.org/intel352/riiak/wiki/class/RiiakLink)s

## Linkwalking ##
Linkwalking can be done using the RiiakObject::link() method

    # Retrieve all of John's friends
    $john = $bucket->get('riak_developer_1');
    $friends = $john->link($bucket->name, 'friend')->run();

This method returns an array of [RiiakLink](https://bitbucket.org/intel352/riiak/wiki/class/RiiakLink)s

## Dereferencing Links ##
RiiakLinks can be dereferenced to the linked object using the RiiakLink::get() method

    # Retrieve all of John's friends
    $john = $bucket->get('riak_developer_1');
    $dave = $bucket->get('riak_developer_2');
    $john->addLink($dave, 'friend')->store();
    $friends = $john->link($bucket->name, 'friend')->run();
    $dave = $friends[0]->get();

This method returns a [RiiakObject](https://bitbucket.org/intel352/riiak/wiki/class/RiiakObject)

## Fetching Data With Map/Reduce ##
Data can be fetched by Map and Reduce using the Riiak::getMapReduce() method

    # Fetch a sorted list of all keys in a bucket
    $result = $client->getMapReduce() // or $client->mapReduce using magic getter
        ->addBucket($bucket->name) // addBucket expects string, bucket name
    	->map('function (v) { return [v.key]; }')
    	->reduce('Riak.reduceSort')
    	->run();

This method returns an array of data representing the result of the Map/Reduce functions.

*More examples of Map/Reduce can be found in unit_tests.php (@todo - not existing yet)*

## Using Search ##
Searches can be executed using the Riiak::search() method

    # Create some test data
    $bucket = $client->bucket('searchbucket');
    $bucket->newObject('one', array('foo'=>'one', 'bar'=>'red'))->store();
    $bucket->newObject('two', array('foo'=>'two', 'bar'=>'green'))->store();

    # Execute a search for all objects with matching properties
    $results = $client->search('searchbucket', 'foo:one OR foo:two')->run();

This method will return null unless executed against a Riak Search cluster.
