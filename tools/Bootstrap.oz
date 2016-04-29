/*-------------------------------------------------------------------------
 *
 * Bootstrap.oz
 *
 *    This module launches a network with the help of mordor, the place to
 *    bring them all, and the place to find them. This module is invoked from
 *    the beernet command line interface.
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
   Connection
   OS
   Pickle
   System
   BaseArgs at 'BaseArgs.ozf'
   Clansman at 'Clansman.ozf'
   TextFile at '../utils/TextFile.ozf'
export
   DefArgs
   GetDate
   Run
define
   DIST_USER   = {String.toAtom {OS.getEnv 'USER'}}
   DIST_NODES  = nodefault

   Say = System.showInfo

   DefArgs = record(
                     distuser(single   type:atom   default:DIST_USER)
                     distnodes(single  type:atom   default:DIST_NODES)
                   )
                  
   fun {NodeCall Args}
      '#'("./node --ring " Args.ring
                " --protocol " Args.protocol
                " --store " Args.store
                " --logger " Args.logger
                )
   end

   fun {MasterCall Args}
      '#'({NodeCall Args}
                " --master"
                " --size " Args.size
                )
   end

   proc {CreateScript Name Call Args}
      Flag Script
   in
      Script = {New TextFile.textFile init(name:Name
                                           flags:[write create truncate text])}
      {Script putS("#!/bin/sh\n")}
      if Args.ozpath \= {BaseArgs.getDefault ozpath} then
         {Script putS("export PATH="#Args.ozpath#":$PATH")}
      end
      {Script putS("cd "#Args.nodepath)}
      if Args.dist == internet then
         {Script putS('#'("scp " Args.distuser "@" Args.storesite ":"
                           Args.storepath Args.store " ."))}
         {Script putS('#'("scp " Args.distuser "@" Args.logsite ":"
                           Args.logpath Args.logger " ."))}
      end
      {Script putS("linux32 "#{Call Args})}
      {Script close}
      {OS.system "chmod +x "#Name Flag}
      {Wait Flag}
   end

   %% This procedure launches one unix process per pbeer
   proc {LaunchLocalNodes Args}
      {Delay 1666}
      {Say "Launching the master node"}
      {OS.system "linux32 "#{MasterCall Args}#" &" _}
      {Delay 1666}
      for I in 1..(Args.size-1) do
         {OS.system "linux32 "#{NodeCall Args}#" &" _}
         {Delay 100}
      end
   end

   %% This procedure launches all pbeers as lightweight threads on the current
   %% unix processor running the whole network
   proc {LaunchSimNodes Args}
      {Delay 1666}
      {Say "Launching the master node"}
      {Clansman.run {Record.adjoin Args args(master:true busy:false)}}
      {Delay 1666}
      for I in 1..(Args.size-1) do
         {Clansman.run {Record.adjoin Args args(master:false busy:false)}}
         {Delay 100}
      end
   end

   %% This is the most complicated procedure. It launches different unix
   %% processes on every site involved in the experiment. It balances the
   %% load as fair as possible among the sites. A pbeer is launched every half
   %% a second.
   proc {LaunchSharedDiskNodes Args}
      AllSites
      NodeScript
      RunScript
      proc {LaunchMasterScript Site User}
         %% Launch master script
         {LaunchRemoteScript Site User}
         %% And switch to any pbeer for the next calls
         RunScript   := LaunchRemoteScript
         NodeScript  := Args.scrpany
         {Delay 1666}
      end
      proc {LaunchRemoteScript Site User}
         SshCall = '#'("ssh -t -l " User " " Site " sh "
                        Args.nodepath "/" @NodeScript)
         Flag
      in
         {OS.system SshCall#" || echo \"failed\" &" Flag}
         {Wait Flag}
         {Delay 500}
      end
      proc {Loop Nodes I}
         if I =< Args.size then 
            case Nodes
            of Node|MoreNodes then
               {@RunScript Node Args.distuser}
               if I mod Args.sites == 0 then
                  {Loop AllSites I+1}
               else
                  {Loop MoreNodes I+1}
               end
            [] nil then
               {Loop AllSites I}
            end
         else
            {Say "All pbeers launched"}
         end
      end
   in
      {CreateScript Args.scrpfirst MasterCall Args}
      {CreateScript Args.scrpany NodeCall Args}
      AllSites    = {TextFile.read Args.distnodes}
      NodeScript  = {NewCell Args.scrpfirst}
      RunScript   = {NewCell LaunchMasterScript}
      {Loop AllSites 1}
   end

   fun {GetDate}
      GmTime Year Month Day Hour Min
   in
      GmTime   = {OS.gmTime}
      Year     = GmTime.year+1900
      Month    = if GmTime.mon < 9 then "0"#GmTime.mon+1 else GmTime.mon+1 end
      Day      = if GmTime.mDay < 10 then "0"#GmTime.mDay else GmTime.mDay end
      Hour     = if GmTime.hour < 10 then "0"#GmTime.hour else GmTime.hour end
      Min      = if GmTime.min < 10 then "0"#GmTime.min else GmTime.min end
      '#'(Year Month Day "-" Hour Min)
   end

   fun {StopService Args}
      AllSites
      StopStream
      %% Connect to all sites, and kill all mozart processes
      proc {KillSites Nodes I}
         case Nodes
         of Node|MoreNodes then
            if I =< Args.sites then
               {OS.system '#'("ssh -t -l " Args.distuser " " Node 
                              " killall -9 emulator.exe &") _}
                              %" sh /usr/bin/killall -9 emulator.exe &") _}
               {KillSites MoreNodes I+1}
            end
         [] nil then
            {KillSites AllSites I}
         end
      end
   in
      thread
         Mordor
      in
         for Msg in StopStream do
            %% Waits for the stop message
            case Msg
            of stop(Ack) then
               MyAck
            in
               Ack = unit
               {Say "Going to kill everything in 3 seconds..."}
               {Delay 3000}
               if Args.dist == cluster then
                  AllSites = {TextFile.read Args.distnodes}
                  {KillSites AllSites 1}
               end
               Mordor = {Connection.take
                           {Pickle.load Args.storepath#'/'#Args.store}}
               {Send Mordor theonering(MyAck)}
               {Wait MyAck}
               {Application.exit 0}
            end
         end
      end
      %% Return the stop port
      {NewPort StopStream}
   end

   proc {Run Args}
      %% Help message
      if Args.help then
         HelpMsg = ['#'("      Options for the distribution mode 'cluster'")
                    '#'("      --distuser  User name to connect to cluster "
                        "(default: " DIST_USER ")")
                    '#'("      --distnodes File with list of cluster node "
                        "names (default: " DIST_NODES ")")]
      in
         {BaseArgs.helpMessage HelpMsg}
         {Application.exit 0}
      end
      %% Launch stop service on ticke Args.achel
      {Pickle.save {Connection.offerUnlimited {StopService Args}} Args.achel}

      {Say "Lauching Mordor Store"}
      {OS.system '#'("linux32 ./mordor --ticket " Args.storepath '/' 
                      Args.store " &") _}
      {Delay 1666}

      case Args.dist
      of cluster then
         {LaunchSharedDiskNodes Args}
      [] localhost then
         {LaunchLocalNodes Args}
      [] sim then
         {LaunchSimNodes Args}
      else
         {Say " *** WARNING! *** "}
         {Say "Wrong distribution mode. Running as localhost"}
         {LaunchLocalNodes Args}
      end
   end
end


