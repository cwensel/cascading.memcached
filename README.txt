
Welcome

 This is the Cascading.Memcached module.

 It provides support for reading/writing data to/from an memcached
 cluster when bound to a Cascading data processing flow.

 Cascading is a feature rich API for defining and executing complex,
 scale-free, and fault tolerant data processing workflows on a Hadoop
 cluster. It can be found at the following location:

   http://www.cascading.org/

Building

 This release requires at least Cascading 1.1.x. Hadoop 0.19.x,
 and the related memcached release.

 To build a jar,

 > ant -Dcascading.home=... -Dhadoop.home=... jar

 To test,

 > ant -Dcascading.home=... -Dhadoop.home=... test

where "..." is the install path of each of the dependencies.

Using

  The cascading-memcached.jar file should be added to the "lib"
  directory of your Hadoop application jar file along with all
  Cascading dependencies.