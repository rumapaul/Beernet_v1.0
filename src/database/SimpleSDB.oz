/*-------------------------------------------------------------------------
 *
 * SimpleSDB.oz
 *
 *    SimpleSDB provides basic storage operations for items identified with two
 *    keys and a secret. The secret is used for put and delete, but not for
 *    read. If the secret matches, the result is bound to success. If it
 *    doesn't match, it is bound to error(bad_secret)
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
 *    Contributors: Xavier de Coster, Matthieu Ghilain.
 *
 *    Last change: $Revision: 10 $ $Author: ruma $
 *
 *    $Date: $
 *
 * NOTES
 *      
 *    Operations provided by SimpleSDB are:
 *
 *       put(key1 key2 value secret result)
 *
 *       get(key1 key2 result)
 *
 *       delete(key1 key2 secret result)
 *
 *-------------------------------------------------------------------------
 */

functor
import
   Constants   at '../commons/Constants.ozf'
   Component   at '../corecomp/Component.ozf'
export
   New
define

   NO_VALUE = Constants.noValue  % To be used inside the component as constant
   SUCCESS  = Constants.success  % Correct secret, or new item created 
   ERROR    = Constants.badSecret % Incorrect secret
   
   %%To use tuples instead of records
   SECRET   = 1
   VALUE    = 2

   fun {New}
      DB
      Self

      proc {Delete delete(Key1 Key2 Secret Result)}
         KeyDict
      in
         KeyDict = {Dictionary.condGet DB Key1 unit}
         if KeyDict \= unit then
            Item = {Dictionary.condGet KeyDict Key2 unit}
         in
            if Item \= unit then
               if Item.SECRET == Secret then 
                  {Dictionary.remove KeyDict Key2}
                  Result = SUCCESS
               else
                  Result = ERROR
               end
            else %% No item using Key1/Key2. Nothing to be done.
               Result = NO_VALUE
            end
         else %% No key using Key1. Nothing to be done.
            Result = NO_VALUE
         end
      end

      proc {Get get(Key1 Key2 Result)}
         KeyDict
      in
         KeyDict = {Dictionary.condGet DB Key1 unit}
         if KeyDict == unit then
            Result = NO_VALUE
         else
            Item = {Dictionary.condGet KeyDict Key2 item(SECRET:unit
                                                         VALUE:NO_VALUE)}
         in
            Result = Item.VALUE
         end
      end

      proc {Put put(Key1 Key2 Val Secret Result)}
         KeyDict
      in
         KeyDict = {Dictionary.condGet DB Key1 unit}
         if KeyDict \= unit then
            Item = {Dictionary.condGet KeyDict Key2 unit}
         in
            if Item \= unit then
               if Item.SECRET == Secret then 
                  {Dictionary.put KeyDict Key2 item(SECRET:Secret VALUE:Val)}
                  Result = SUCCESS
               else
                  Result = ERROR
               end
            else %% New item, first used of Key1/Key2
               {Dictionary.put KeyDict Key2 item(SECRET:Secret VALUE:Val)}
               Result = SUCCESS
            end
         else %% New item, first used of Key1
            NewDict = {Dictionary.new}
         in
            {Dictionary.put DB Key1 NewDict}
            {Dictionary.put NewDict Key2 item(SECRET:Secret VALUE:Val)}
            Result = SUCCESS
         end
      end

      %% Dump Keys within a range of keys into a list of entries.
      %% The resulting list is a list of lists.
      proc {DumpRange dumpRange(From To Result)}
         fun {DumpLoop Entries}
            case Entries
            of (Key#Dict)|MoreEntries then
               if Key >= From andthen Key =< To then
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
      %% Returns a list of results of inserting the values
      proc {Insert insert(Entries ?Result)}
         fun {InsertLoop Key1 Items}
            case Items
            of (Key2#item(SECRET:Secret VALUE:Val))|MoreItems then
               ThisRes
            in
               {Put put(Key1 Key2 Val Secret ThisRes)}
               result(Key1 Key2 ThisRes)|{InsertLoop Key1 MoreItems}
            [] nil then
               nil
            end
         end
      in
         case Entries
         of (Key#Items)|MoreEntries then
            LoopRes NextRes
         in
            LoopRes = {InsertLoop Key Items}
            Result = LoopRes|NextRes
            {Insert insert(MoreEntries NextRes)}
         [] nil then
            Result = nil
         end
      end

      Events = events(
                     %% basic operations
                     delete:     Delete
                     get:        Get
                     put:        Put
                     %% administration
                     dumpRange:  DumpRange
                     insert:     Insert
                     )
   in
      Self = {Component.newTrigger Events}
      DB = {Dictionary.new}
      Self
   end

end

