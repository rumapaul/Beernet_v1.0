		BEERNET - pbeer-to-pbeer

Beernet is a pbeer-to-pbeer system that provides replicated storage with
transactional updates to build distributed systems with strong consistency. 

This is version 1.0 of Beernet, which has extended version 0.9 to make the system Reversible and Phase-Aware.

The API of beernet can be found in directory doc/


License
-------

Beernet is released under the Beerware License (see file LICENSE) 


Building Beernet
----------------

Beernet sources are organized in directories src and tools. After installing, compiled files are stored in directories lib and bin. See 'Organization of the main project directory for more info'.

To compile and install run

make
make install

For more options on building run

make help 


Organization of the main project directory
------------------------------------------

The sources group includes:

src/ sources of Beernet components. Here is the meat. It becomes 'lib' after
running 'make install'

tools/ sources of the command line tools that allow non-oz programmers to use
Beernet. It becomes 'bin' after running 'make install'

The compiled directories are:

doc/ for documentation

lib/ for the compiled Beernet components

bin/ for Beernet tools. 

These three directories are not under versioning control. They just have the
skeleton of the released directories.

And remember that a beer a day keeps the doctor away.
