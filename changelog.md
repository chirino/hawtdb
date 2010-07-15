![HawtDB][logo]
===============

[logo]: http://hawtdb.fusesource.org/images/project-logo.png "HawtDB"

[HawtDB 1.2][1_2], un-released
-------------------------------------------
[1_2]: http://hawtdb.fusesource.org/maven/1.2

* Upgrade to hawtbuf 1.1, allows us to test if the codec can estimate serialized size of key/values. if it can't deferred encoding/decoding is disabled.
* Index getPage method renamed to getIndexLocation
* Improved IndexFactory APIs: create and open methods default to using the root page. 
* added a setFreeRanges and getFreeRanges to the allocator interface.
* Updated the predicates and vistor interfaces so that they are passed the Comparator configured on the index.  see #6 Documented custom comparators and also index iteration
* Fixes #6 Allow configuring a custom comparators on BTree indexes

[HawtDB 1.1][1_1], released 2010-07-01
-------------------------------------------
[1_1]: http://hawtdb.fusesource.org/maven/1.1

* Fixing BTree node next linking.. It was possible that a next link would not properly get set in some conditions during a node removal.
* You can add get callbacks when a commit gets flushed to disk.
* Changed the way the journal was handling callback based write completed notifications.  They are now delivered in batch form to a single JournalListener.  This reduces thread contention and increases throughput.
* Moved the built in predicate implementations into a Predicates class.
* Added close method to the Transaction interface.  Implementation now asserts it is no longer used after a close.
* Making the appender's max write batch size configurable.
* Revamped how Update and DefferedUpdates track shadow pages.  A little easier to follow now. - changed the interface to PagedAccessor so that instead of removing the linked pages, it just needs to report what the linked pages are.
* Got rid of the WriteKey wrapper class, updated logging.
* Better looking printStrucuture BTree method
* Added a few Logging classes to reduce the number of places we need to update if in case we decided to switch logging APIs.
* Fixing free page allocation bug when using deferred updates.
* Javadoc improvements
* Expose a config property to control the read cache size.
* Reworked how snapshot tracking was being done.   Fixes errors that occurred during heavy concurrent access.
* Added a non-blocking flush method to the TxPageFile 
* Read cache was not getting updated when a update batch was performed.  Cached entries that were updated and flushed to disk continued returning stale data.
* Fixed an recovery edge cases
* Don't start the thread from the thread factory. that causes illegal state exceptions
* Fixed journal bug where getting next location could return a the current location
* Renamed EncoderDecoder to PagedAccessor
* The util.buffer package has moved into it's own project at http://github.com/chirino/hawtbuf
* Fixes #4 : Errors occur when you re-open an empty data file.
* Extracted a SortedIndex interface from the Index interface to non sorted indexes having to deal with that leaky abstraction. 
* added a free() method to the Paged for symmetry with the alloc() method. 
* Improved website documentation

[HawtDB 1.0][1_0], released 2010-04-12
-------------------------------------------
[1_0]: http://hawtdb.fusesource.org/maven/1.0

* Initial release

