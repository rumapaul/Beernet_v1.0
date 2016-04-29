/*-------------------------------------------------------------------------
 *
 * SimpleDB.oz
 *
 *    SimpleDB provides basic storage operations for items identified with two
 *    keys.
 *
 * LICENSE
 *
 *    Beernet is released under the Beerware License (see file LICENSE) 
 * 
 * IDENTIFICATION 
 *
 *    Author: Boriss Mejias <boriss.mejias@uclouvain.be>
 *            Ruma Paul <ruma.paul@uclouvain.be>
 *            
 *
 *    Last change: $Revision: 420 $ $Author: ruma $
 *
 *    $Date: 2016-04-12 20:24:05 +0200 (Tues, 12 April 2016) $
 *
 * NOTES
 *      
 *    Operations provided by SimpleDB are:
 *
 *       put(key1 key2 value)
 *       get(key1 key2)
 *       delete(key1 key2)
 *
 *-------------------------------------------------------------------------
 */

functor
import
   Component   at '../corecomp/Component.ozf'
   Constants   at '../commons/Constants.ozf'
   BootTime at 'x-oz://boot/Time'
  
export
   New
define

   NO_VALUE = Constants.noValue  % To be used inside the component as constant
   
   fun {New}
      DB
      Self

      proc {Delete delete(Key1 Key2)}
         KeyDict
      in
         KeyDict = {Dictionary.condGet DB Key1 unit}
         if KeyDict \= unit then
            {Dictionary.remove KeyDict Key2}
         end
      end

      proc {Get get(Key1 Key2 Val)}
         KeyDict
      in
         KeyDict = {Dictionary.condGet DB Key1 unit}
         if KeyDict == unit then
            Val = NO_VALUE
         else
            Val = {Dictionary.condGet KeyDict Key2 NO_VALUE}
         end
      end

      proc {Put put(Key1 Key2 Val)}
         KeyDict
      in
         KeyDict = {Dictionary.condGet DB Key1 unit}
         if KeyDict \= unit then
            {Dictionary.put KeyDict Key2 Val}
         else
            NewDict = {Dictionary.new}
         in
            {Dictionary.put DB Key1 NewDict}
            {Dictionary.put NewDict Key2 Val}
         end
      end

      %% Dump Keys within a range of keys into a list of entries.
      %% The resulting list is a list of lists.
      proc {DumpRange dumpRange(From To ?Result)}
         fun {DumpLoop Entries}
            case Entries
            of (Key#Dict)|MoreEntries then
               if From < To andthen Key >= From andthen Key =< To then
                  (Key#{Dictionary.entries Dict})|{DumpLoop MoreEntries}
               elseif Key >= From orelse Key =< To then
		  (Key#{Dictionary.entries Dict})|{DumpLoop MoreEntries}
               else
                  {DumpLoop MoreEntries}
               end
            [] nil then
               nil
            end
         end
      in
         Result = {DumpLoop {Dictionary.entries DB}}
      end

      %% Insert a list of list into the dictionary of dictionaries.
      proc {Insert insert(Entries Result)}
         proc {InsertLoop Key1 Items}
            case Items
            of (Key2#Val)|MoreItems then
               {Put put(Key1 Key2 Val)}
               {InsertLoop Key1 MoreItems}
            [] nil then
               skip
            end
         end
      in
         case Entries
         of (Key#Items)|MoreEntries then
            {InsertLoop Key Items}
            {Insert insert(MoreEntries Result)}
         [] nil then
            skip
         end
         Result = unit
      end

      %% Insert a list of list into the dictionary of dictionaries.
      proc {InsertNewest insertNewest(Entries Result)}
         proc {InsertLoop Key1 Items}
            case Items
            of (Key2#Val)|MoreItems then
               CurrentVal
               in
	       %if {Not Val.locked} then
               	   {Get get(Key1 Key2 CurrentVal)}
               	   if CurrentVal == NO_VALUE orelse CurrentVal.version < Val.version then
                  	{Put put(Key1 Key2 Val)}
		   end
               %end
               {InsertLoop Key1 MoreItems}
            [] nil then
               skip
            end
         end
      in
         case Entries
         of (Key#Items)|MoreEntries then
            {InsertLoop Key Items}
            {InsertNewest insertNewest(MoreEntries Result)}
         [] nil then
            skip
         end
         Result = unit
      end

      %% Insert a list of list into the dictionary of dictionaries.
      %% Backup Data: Not mine, keeping my pred's data on request
      proc {InsertBackUpData insertBackUpData(Entries Result)}
	proc {InsertLoop Key1 Items}
            case Items
            of (Key2#Val)|MoreItems then
               CurrentVal
               in
	       CurrentVal = {Record.adjoin Val item(tag:backup timestamp:{BootTime.getReferenceTime})}
               {Put put(Key1 Key2 CurrentVal)}
               {InsertLoop Key1 MoreItems}
            [] nil then
               skip
            end
         end
      in
         case Entries
         of (Key#Items)|MoreEntries then
            {InsertLoop Key Items}
            {InsertBackUpData insertBackUpData(MoreEntries Result)}
         [] nil then
            skip
         end
         Result = unit
      end

      Events = events(
                     %% basic operations
                     delete:     Delete
                     get:        Get
                     put:        Put
                     %% administration
                     dumpRange:  DumpRange
                     insert:     Insert
                     insertNewest:	InsertNewest
                     insertBackUpData:  InsertBackUpData
                     )
   in
      Self = {Component.newTrigger Events}
      DB = {Dictionary.new}
      Self
   end

end

