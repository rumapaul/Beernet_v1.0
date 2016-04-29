/*-------------------------------------------------------------------------
 *
 * Pbeer.oz
 *
 *    Source of the command line utility pbeer. It execute the correspondent
 *    program given on the arguments.
 *
 * LICENSE
 *
 *    Beernet is released under the Beerware License (see file LICENSE) 
 * 
 * IDENTIFICATION 
 *
 *    Author: Boriss Mejias <boriss.mejias@uclouvain.be>
 *
 *    Last change: $Revision: 403 $ $Author: boriss $
 *
 *    $Date: 2011-05-19 21:45:21 +0200 (Thu, 19 May 2011) $
 *
 * NOTES
 *    
 *    This is NOT a beernet component. It is a utility to connect to a running
 *    pbeer in a given network to executes some operations. Available
 *    operations are:
 *
 *    Key/value pair operations
 *       put
 *       get
 *       delete
 *
 *    Key/value-sets operations
 *       add
 *       readSet
 *       remove
 *
 *    Key/value pair operations with replication
 *       write
 *       read
 *    
 *    Lookup operations
 *       lookup
 *       lookupHash
 *
 *    Killing a pbeer
 *       kill
 *
 *-------------------------------------------------------------------------
 */

functor
import
   Application
   Property
   System
   BaseArgs       at '../lib/tools/BaseArgs.ozf'
   PbeerBaseArgs  at '../lib/tools/PbeerBaseArgs.ozf'
   %% for key/value-set operations
   Add            at '../lib/tools/Add.ozf'
   CreateSet      at '../lib/tools/CreateSet.ozf'
   DestroySet     at '../lib/tools/DestroySet.ozf'
   Remove         at '../lib/tools/Remove.ozf'
   ReadSet        at '../lib/tools/ReadSet.ozf'
   %% for kay/value pairs operations
   Delete         at '../lib/tools/Delete.ozf'
   Get            at '../lib/tools/Get.ozf'
   Put            at '../lib/tools/Put.ozf'
   %% lookup operations
   Lookup         at '../lib/tools/Lookup.ozf'
   LookupHash     at '../lib/tools/LookupHash.ozf'
   %% transactions - read/write operations with replication
   Write          at '../lib/tools/Write.ozf'
   Read           at '../lib/tools/Read.ozf'
   Erase          at '../lib/tools/Erase.ozf'
   %% killing pbeers
   Kill           at '../lib/tools/Kill.ozf'
define

   Say = System.showInfo
   Args
   
   proc {HelpMessage}
      {Say "Usage: "#{Property.get 'application.url'}#" <subcommand> [options]"}
      {Say ""}
      {Say '#'("Type '"#{Property.get 'application.url'}#" help <subcommand>' "
               "for help on a specific subcommand.")}
      {Say ""}
      {Say "Available subcommands with an example of use:"}
      {Say ""}
      {Say "   Key/value pair operations"}
      {Say "\tput\t-k key -v value"}
      {Say "\tget\t-k key"}
      {Say "\tdelete\t-k key"}
      {Say ""}
      {Say "   Key/value-sets operations"}
      {Say "\tcreateSet\t-k key [--secret secret] [--msecret mastersecret]"}
      {Say "\tdestroySet\t-k key [--msecret mastersecret]"}
      {Say "\tadd\t-k key -v any_value"}
      {Say "\treadSet\t-k key"}
      {Say "\tremove\t-k key -v any_value"}
      {Say ""}
      {Say "   Key/value pair operations with replication"}
      {Say "\twrite\t-k key -v value [--secret secret]"}
      {Say "\tread\t-k key -v value"}
      {Say "\terase\t-k key [--secret secret]"}
      {Say ""}
      {Say "   Lookup operations"}
      {Say "\tlookup\t-k key"}
      {Say "\tlookupHash --hashkey 666"}
      {Say ""}
   end

   proc {ThisHelpRun _/*Args*/}
      {HelpMessage}
      {Application.exit 0}
   end

   proc {ErrorMsg Msg}
      {Say "ERROR: "#Msg}
      {Say ""}
      {HelpMessage}
      {Application.exit 0}
   end

   /*
    * Use these lines if you want to use the SetsCommon functor to parametrize
    * these three oprations
   %% Key/Value-Set operations
   Add    = rec(defArgs:nil run:proc {$ Args} {SetsCommon.run Args add} end)
   Remove = rec(defArgs:nil run:proc {$ Args} {SetsCommon.run Args remove} end)
   ReadSet= rec(defArgs:nil run:proc {$ Args} {SetsCommon.run Args readSet} end)
   */

   Subcommands = subcmds(
                         %% Key/Value sets
                         add:       Add
                         createSet: CreateSet
                         destroySet:DestroySet
                         remove:    Remove
                         readSet:   ReadSet
                         %% Basic DHT
                         delete:    Delete
                         put:       Put
                         get:       Get
                         %% Lookup operations
                         lookup:    Lookup
                         lookupHash:LookupHash
                         %% Transactions
                         write:     Write
                         read:      Read
                         erase:     Erase
                         %% Killing pbeers
                         kill:      Kill
                         %% global help
                         help:      rec(defArgs:nil
                                        run:ThisHelpRun))
in

   {Property.put 'print.width' 1000}
   {Property.put 'print.depth' 1000}

   Args = {PbeerBaseArgs.getArgs record}

   %% Help message
   if Args.help then
      {HelpMessage}
      {Application.exit 0}
   end

   {BaseArgs.runSubCommand Args Subcommands ErrorMsg}
end
