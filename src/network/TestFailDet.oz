functor
import
   Connection
   Pickle
   Network  at 'Network.ozf'
define
   PbeersIds = [foo flets bar]
   Pbeers
   ComLayer

   fun {GetPbeers L}
      case L
      of H|T then
         {Connection.take {Pickle.load H}}|{GetPbeers T}
      [] nil then
         nil
      end
   end

   proc {ConnectPbeers L}
      proc {ConnectPbeer Pbeer L}
         case L
         of H|T then
            {ComLayer sendTo(H connectTo(Pbeer))}
            {ConnectPbeer Pbeer T}
         [] nil then
            skip
         end
      end
   in
      case L
      of H|T then
         {ConnectPbeer H T}
         {ConnectPbeers T}
      [] nil then
         skip
      end
   end

in
   ComLayer = {Network.new}
   Pbeers = {GetPbeers PbeersIds}
   {ConnectPbeers Pbeers}
end


