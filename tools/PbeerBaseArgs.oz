/*-------------------------------------------------------------------------
 *
 * PbeerBaseArgs.oz
 *
 *    Base arguments to launch the pbeer application. Help message associated
 *    to base arguments is also provided here.
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
   Property
   System
   BaseArgs at 'BaseArgs.ozf'
export
   BuildDefArgs
   Defaults
   HelpMessage
   GetArgs
   GetDefault
define

   TRANS_PROT  = paxos
   RING_NAME   = eldorado
   STORE_TKET  = 'mordor.tket'
   KEY         = key
   VALUE       = value
   HASH_KEY    = 666
   CAP_FILE    = nocap
   NO_SECRET   = public

   Say         = System.showInfo

   proc {HelpMessage Use ExtraText SubCmd}
      Help = rec(
         ring: "  -r, --ring\tRing name (default: "#RING_NAME#")"
         key:  "  -k, --key\tKey of the item (default: "#KEY#")"
         value:"  -v, --value\tValue of the item (default: "#VALUE#")"
         hashkey:'#'("      --hashkey\tUse a hash key. Bypass hash function "
                     "(default: " HASH_KEY ")")
         cap:'#'("  -c, --cap\tFilename to store/retrieve a capability key "
                 "(default: " CAP_FILE ")")
         secret:'#'("      --secret\tProtect updates with secret "
                 "(default: " NO_SECRET ")")
         secretval:'#'("       --secretval Protect value in a set with secret "
                 "(default: " NO_SECRET ")")
         msecret:'#'("     --msecret\tProtect the set with master secret "
                 "(default: " NO_SECRET ")")
         protocol:'#'("  -p, --protocol Transactional protocol to be used "
                      "(default: " TRANS_PROT ")")
         store:"  -s, --store\tTicket to the store (default: "#STORE_TKET#")"
         )
   in
      {Say "Usage: "#{Property.get 'application.url'}#" "#SubCmd#" [option]"}
      {Say ""}
      {Say "Options:"}
      for Item in Use do
         {Say Help.Item}
      end
      for Line in ExtraText do
         {Say Line}
      end
   end

   Defaults = record(
               cap(single        char:&c  type:atom   default:CAP_FILE)
               hashkey(single             type:int    default:HASH_KEY)
               key(single        char:&k  type:atom   default:KEY)
               msecret(single             type:atom   default:NO_SECRET)
               protocol(single   char:&p  type:atom   default:TRANS_PROT)
               ring(single       char:&r  type:atom   default:RING_NAME)
               secret(single              type:atom   default:NO_SECRET)
               secretval(single           type:atom   default:NO_SECRET)
               store(single      char:&s  type:atom   default:STORE_TKET)
               value(single      char:&v  type:atom   default:VALUE)
               help(single       char:[&? &h]         default:false)
               )

   fun {BuildDefArgs MoreArgs}
      {BaseArgs.mergeArgs MoreArgs Defaults}
   end

   fun {GetArgs MoreArgs}
      DefArgs
   in
      DefArgs  = {BuildDefArgs MoreArgs}
      try
         {Application.getArgs DefArgs}
      catch _ then
         {Say 'Unrecognised arguments'}
         optRec(help:true)
      end
   end

   fun {GetDefault Arg}
      NArgs
      fun {Loop I}
         if I =< NArgs then
            if {Label Defaults.I} == Arg then
               Defaults.I.default
            else
               {Loop I+1}
            end
         else
            none
         end
      end
   in
      NArgs = {Width Defaults}
      {Loop 1}
   end

end

