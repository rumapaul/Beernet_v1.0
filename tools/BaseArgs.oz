/*-------------------------------------------------------------------------
 *
 * BaseArgs.oz
 *
 *    Base arguments to launch a beernet ring. Help message associated to base
 *    arguments is also provided here.
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
   OS
   Property
   System
export
   BuildDefArgs
   Defaults
   HelpMessage
   GetArgs
   GetDefault
   MergeArgs
   RunSubCommand
define

   ACHEL_TKET  = 'achel.tket'
   LOG_FILE    = 'usetime'
   LOG_PATH    = {OS.getEnv 'PWD'}
   LOG_TKET    = 'logger.tket'
   LOG_SITE    = 'localhost'
   NODE_PATH   = {OS.getEnv 'PWD'}
   DIST_MODE   = localhost
   TRANS_PROT  = paxos
   N_TRANS     = 10
   N_READS     = 100
   OZ_PATH     = default
   READ_BUFF   = 20
   READ_ONLY   = all
   RING_NAME   = eldorado
   RING_SIZE   = 16
   DEF_SITES   = 1
   SCRP_FIRST  = firstPbeer
   SCRP_ANY    = anyPbeer
   STORE_PATH  = {OS.getEnv 'PWD'}
   STORE_TKET  = 'mordor.tket'
   STORE_SITE  = 'localhost'

   Say         = System.showInfo

   proc {HelpMessage ExtraText}
      proc {Loop Lines}
         case Lines
         of Line|MoreLines then
            {Say Line}
            {Loop MoreLines}
         [] nil then
            skip
         end
      end
   in
      {Say "Usage: "#{Property.get 'application.url'}#" [option]"}
      {Say ""}
      {Say "Options:"}
      {Say "  -r, --ring\tRing name (default: "#RING_NAME#")"}
      {Say "      --size\tExpected network size (default: "#RING_SIZE#")"}
      {Say '#'("      --sites\tAmount of machines to be used (default: "
               DEF_SITES ")")}
      {Say '#'("  -p, --protocol Transactional protocol to be used (default: "
               TRANS_PROT ")")}
      {Say '#'("  -t, --trans\tAmount of transactions to be run (default: "
               N_TRANS ")")}
      {Say "      --reads\tAmount of reads per peer (default: "#N_READS#")"}
      {Say "      --readbuff Buffer of reads (default: "#READ_BUFF#")"}
      {Say "      --readonly Run only this test (default: "#READ_ONLY#")"}
      {Say "  -s, --store\tTicket to the store (default: "#STORE_TKET#")"}
      {Say "      --storepath Store's ticket path (default: "#STORE_PATH#")"}
      {Say "      --storesite Store's site (default: "#STORE_SITE#")"}
      {Say "      --logger\tTicket to the logger (default: "#LOG_TKET#")"}
      {Say "      --logpath\tPath to logger's ticket (default: "#LOG_PATH#")"}
      {Say "      --logsite\tLogger's site (default: "#LOG_SITE#")"}
      {Say "      --logfile\tFile to log stats (default uses current time)"}
      {Say "      --nodepath Path to node's scripts (default: "#NODE_PATH#")"}
      {Say "      --ozpath\tPath to ozengine (default: "#OZ_PATH#")"}
      {Say "  -d, --dist\tDistributed mode (default: "#DIST_MODE#")"}
      {Say "  -a, --achel\tStop notification point (default: "#ACHEL_TKET#")"}
      {Say ""}

      {Loop ExtraText}

      {Say ""}
      {Say "  -h, --help\tThis help"}
   end

   Defaults = record(
               achel(single      char:&a  type:atom   default:ACHEL_TKET)
               dist(single       char:&d  type:atom   default:DIST_MODE)
               logfile(single             type:atom   default:LOG_FILE)
               logger(single              type:atom   default:LOG_TKET)
               logpath(single             type:atom   default:LOG_PATH)
               logsite(single             type:atom   default:LOG_SITE)
               nodepath(single            type:atom   default:NODE_PATH)
               ozpath(single              type:atom   default:OZ_PATH)
               protocol(single   char:&p  type:atom   default:TRANS_PROT)
               reads(single               type:int    default:N_READS)
               readbuff(single            type:int    default:READ_BUFF)
               readonly(single            type:atom   default:READ_ONLY)
               ring(single       char:&r  type:atom   default:RING_NAME)
               scrpfirst(single           type:atom   default:SCRP_FIRST)
               scrpany(single             type:atom   default:SCRP_ANY)
               sites(single               type:int    default:DEF_SITES)
               size(single                type:int    default:RING_SIZE)
               store(single      char:&s  type:atom   default:STORE_TKET)
               storepath(single           type:atom   default:STORE_PATH)
               storesite(single           type:atom   default:STORE_SITE)
               trans(single      char:&t  type:int    default:N_TRANS)
               help(single       char:[&? &h]         default:false)
               )

   fun {BuildDefArgs MoreArgs}
      {MergeArgs MoreArgs Defaults}
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

   fun {TupleToList Tup}
      fun {Loop I Acc}
         if I == 0 then
            Acc
         else
            {Loop I-1 Tup.I|Acc}
         end
      end
   in
      {Loop {Record.width Tup} nil}
   end

   fun {MergeArgs Args1 Args2}
      {List.toTuple record
                    {List.append {TupleToList Args1} {TupleToList Args2}}}
   end
   
   proc {RunSubCommand Args SubCmds ErrorMsg}
      case Args.1
      of SubCommand|MoreArgs then
         case SubCommand
         of "help" then
            case MoreArgs
            of SubCommand|nil then
               SubCmd = {String.toAtom SubCommand}
            in
               try
                  {SubCmds.SubCmd.run optRec(help:true)}
               catch _ then
                  {ErrorMsg "Wrong subcommand."}
               end
            else
               {ErrorMsg "Wrong invocation."}
            end
         else
            SubCmd
            Run
         in
            SubCmd = {String.toAtom SubCommand}
            Run = try
                     SubCmds.SubCmd
                  catch _ then
                     error(run:proc {$ _}
                                  {ErrorMsg SubCmd#" is a wrong subcommand."}
                               end)
                  end
            {Run.run Args}
         end
      else
         {ErrorMsg "Wrong invocation."}
      end
   end

end


