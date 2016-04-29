/*-------------------------------------------------------------------------
 *
 * Mordor.oz
 *
 *    This is a small store server that allows pbeers to store their addresses
 *    and ring identifiers. The 'mordor' comes obviously from "The Lord of the
 *    Rings", because its an application to bring them all.
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
   Pickle
   Property
   System
   Random   at '../lib/utils/Random.ozf'
define

   MORDOR_TKET = 'mordor.tket'

   Args
   Say      = System.showInfo
   SayNothing = proc {$ _} skip end
   AccPts   = {Dictionary.new}
   Pbeers   = {Dictionary.new}
   Flags    = {Dictionary.new}
   Logs     = {Dictionary.new}
   Gate
   Stream

   fun {RetrievePbeer Dict Key Id}
      Elems Max Val 
      fun {Find Id Items}
         case Items
         of Item|MoreItems then
            ItemId = {Item getId($)}
         in
            if ItemId == Id then
               Item
            else
               {Find Id MoreItems}
            end
         [] nil then
            none
         end
      end
   in
      Val = {Dictionary.condGet Dict Key empty}
      if Val == empty then
         none
      else
         ring(Elems Max) = Val
         {Find Id {Dictionary.items Elems}}
      end
   end

   fun {RetrieveElement Dict Key}
      Elems Max N Val
   in
      Val = {Dictionary.condGet Dict Key empty}
      if Val == empty then
         none
      else
         ring(Elems Max) = Val
         N = {Random.urandInt 1 Max}
         %{Say "Some one requested an access point for ring "#Key}
         Elems.N
      end
   end

   proc {AddElement Dict Key Elem Out}
      Elems Max Val
   in
      Val = {Dictionary.condGet Dict Key empty}
      if Val == empty then
         RingDict
      in
         RingDict = {Dictionary.new}
         RingDict.1 := Elem
         Dict.Key := ring(RingDict 1)
         {Out "New ring "#Key#" created"}
      else
         ring(Elems Max) = Val
         Elems.(Max+1) := Elem 
         Dict.Key := ring(Elems Max+1)
         %{Out "New element stored for ring "#Key}
      end
   end

   proc {RunService Msgs}
      case Msgs
      of Msg|MoreMsgs then
         case Msg
         of getAccessPoint(Ring Ref) then
            Ref = {RetrieveElement AccPts Ring}
         [] registerAccessPoint(Ring PbeerRef) then
            {AddElement AccPts Ring PbeerRef SayNothing}
         [] installFlag(Ring Flag) then
            Flags.Ring  := Flag
         [] retrieveFlag(Ring MyFlag) then
            MyFlag = Flags.Ring
         [] bindFlag(Ring) then
            {Say "Binding flag "#Ring}
            Flags.Ring = mordor
         [] bindFlagWith(Ring Var) then
            Flags.Ring = Var
         [] registerLogger(Ring Logger) then
            Logs.Ring := Logger
         [] retrieveLogger(Ring MyLogger) then
            MyLogger = Logs.Ring
         [] getPbeer(Ring Ref) then
            Ref = {RetrieveElement Pbeers Ring}
         [] getPbeer(Ring Id Ref) then
            Ref = {RetrievePbeer Pbeers Ring Id}
         [] registerPbeer(Ring PbeerRef) then
            {AddElement Pbeers Ring PbeerRef Say}
         [] theonering(Ack) then
            Ack = unit
            {Say "Destroying Mordor"}
            {Application.exit 0}

         else
            {Say "Message Not Understood "#Msg}
         end
         {RunService MoreMsgs}
      else
         {Say "Something went wrong with the communication."}
         {Say "Shutting down the service..."}
         {Application.exit 0}
      end
   end

   proc {HelpMessage}
      {Say "Usage: "#{Property.get 'application.url'}#" [option]"}
      {Say ""}
      {Say "Options:"}
      {Say "  -t, --ticket\tTicket to mordor (default: "#MORDOR_TKET#")"}
      {Say "  -h, --help\tThis help"}
   end

   proc {WelcomeMessage}
      {Say "--- Welcome to Mordor - The place to bind them ---"}
      {Say ""}
      {Say "Possible messages:"}
      {Say "   getAccessPoint(ring name, unbound reference)"}
      {Say "   registerAccessPoint(ring name, pbeer reference)"}
      {Say ""}
      {Say "Waiting for messages"}
   end

in

   {Property.put 'print.width' 1000}
   {Property.put 'print.depth' 1000}

   %% Defining input arguments
   Args = try
             {Application.getArgs
              record(
                     ticket(single char:&t type:atom default:MORDOR_TKET)
                     help(single char:[&? &h] default:false)
                     )}

          catch _ then
             {Say 'Unrecognised arguments'}
             optRec(help:true)
          end

   %% Help message
   if Args.help then
      {HelpMessage}
      {Application.exit 0}
   end
 
   Gate = {NewPort Stream}

   thread
      {RunService Stream}
   end

   {Pickle.save {Connection.offerUnlimited Gate} Args.ticket}

   {WelcomeMessage}
end

