/*-------------------------------------------------------------------------
 *
 * DBManager.oz
 *
 *    This components creates and provides access to different instances of
 *    simple databases.
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
 *    Last change: $Revision: 405 $ $Author: ruma $
 *
 *    $Date: 2014-11-05 13:50:21 +0200 (Wed, 05 Nov 2014) $
 *
 *-------------------------------------------------------------------------
 */

functor
import
   Component   at '../corecomp/Component.ozf'
   SimpleSDB   at '../database/SimpleSDB.ozf'
   SimpleDB    at '../database/SimpleDB.ozf'
export
   New
define

   fun {New}
      Self
      Suicide
      %Listener
      DBs
      DBMakers = dbs(basic:SimpleDB secrets:SimpleSDB)

      proc {Create create(name:DBid type:Type db:?NewDB)}
         NODB
      in
         NODB = {Name.new}
         if NODB == {Dictionary.condGet DBs DBid NODB} then
            NewDB = {DBMakers.Type.new}
            DBs.DBid := NewDB
         else
            NewDB = error(name_in_use)
         end
      end

      proc {Get get(name:DBid db:?TheDB)}
         TheDB = {Dictionary.condGet DBs DBid error(no_db)}
      end

      proc {GetCreate getCreate(name:DBid type:Type db:?NewDB)}
         NewDB = {Dictionary.condGet DBs DBid {DBMakers.Type.new}}
         DBs.DBid := NewDB
      end

      proc {SignalDestroy Event}
        AllEntries = {Dictionary.entries DBs}
        in 
	{List.forAll AllEntries
            proc {$ _#I}
	      {I Event} 
            end}
        {Suicide}
      end

      Events = events(
                     %% Key/Value pairs
                     create:     Create
                     get:        Get
                     getCreate:  GetCreate
                     signalDestroy: SignalDestroy
                     )
   in
      local
         FullComponent
      in
         FullComponent  = {Component.new Events}
         Self     = FullComponent.trigger
         Suicide  = FullComponent.killer
      end

      DBs      = {Dictionary.new}
      Self
   end

end
