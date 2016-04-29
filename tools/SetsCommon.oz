/*-------------------------------------------------------------------------
 *
 * SetsCommon.oz
 *
 *    Common run procedure for the three operations associated to key/value
 *    sets, presented as pbeer subcommands. 
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
 *-------------------------------------------------------------------------
 */

functor
import
   Application
   System
   PbeerBaseArgs  at 'PbeerBaseArgs.ozf'
   PbeerCommon    at 'PbeerCommon.ozf'
export
   DefArgs
   CapOrKey
   GetCapOrKey
   GetPbeer
   Run
define
   DefArgs  = nil
   GetPbeer = PbeerCommon.getPbeer

   fun {GetCapOrKey CapFile Key}
      TheKey
   in
      TheKey#_/*PrintKey*/ = {PbeerCommon.getCapOrKey CapFile Key}
      TheKey
   end

   fun {CapOrKey CapFile Key}
      TheKey
   in
      TheKey#_/*PrintKey*/ = {PbeerCommon.capOrKey CapFile Key}
      TheKey
   end

   proc {Run Args Op}
      Pbeer
      Key
      MyPort
      MyStream
   in
      if Args.help then
         Use
      in
         if Op == readSet then
            Use = [key cap ring store] 
         else
            Use = [key value cap ring store]
         end 
         {PbeerBaseArgs.helpMessage Use nil Op}
         {Application.exit 0}
      end
      Pbeer = {PbeerCommon.getPbeer Args.store Args.ring}
      MyPort = {Port.new MyStream}
      if Op == readSet then
         Result
      in
         Key = {GetCapOrKey Args.cap Args.key}
         {Pbeer readSet(Key Result)}
         for I in 1..{Record.width Result} do
            {Wait Result.I}
            {System.show Result.I}
         end
      else
         Key = {CapOrKey Args.cap Args.key}
         {Pbeer Op(Key Args.value MyPort)}
         {System.showInfo MyStream.1}
      end
      {Application.exit 0}
   end
end
