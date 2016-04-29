functor
export
   Parse
define
   SEPARATOR = &&  
   %% Translate a String into an Oz entity
   fun {StringToValue Str Default}
      if {String.isFloat Str} then
         {String.toFloat Str}
      elseif {String.isInt Str} then
         {String.toInt Str}
      elseif Default == atom then
         {String.toAtom Str}
      else
         Str
      end
   end

   %% Get a tuple from the string. Tuple values are also strings
   fun {GetOp Str}
      Op Args Rest
   in
      {String.token Str &( Op Rest}
      Args = {String.tokens {String.token Rest &) $ _} SEPARATOR}
      {List.toTuple {String.toAtom Op} Args}
   end

   %% Translate strings into records
   fun {Parse Str}
      case {GetOp Str}
      %% DHT operations
      of put(Key Val Secret) then
         put(k:{StringToValue Key atom}
             v:{StringToValue Val string}
             s:{StringToValue Secret atom})
      [] get(Key) then
         get(k:{StringToValue Key atom})
      [] delete(Key Secret) then
         delete(k:{StringToValue Key atom} s:{StringToValue Secret atom})
      %% Transactional items
      [] write(Secret Key Val) then
         write(k:{StringToValue Key atom}
               v:{StringToValue Val string}
               s:{StringToValue Secret atom})
      [] read(Key) then
         read(k:{StringToValue Key atom})
      [] destroy(Key Secret) then
         destroy(k:{StringToValue Key atom} s:{StringToValue Secret atom})
      %% Sets
      [] createset(MasterSec Secret Key) then
         createSet(k:{StringToValue Key atom}
                   ms:{StringToValue MasterSec atom}
                   s:{StringToValue Secret atom})
      [] deleteset(Secret Key) then
         destroySet(k:{StringToValue Key atom} s:{StringToValue Secret atom})
      [] add(Secret Key ValSecret Val) then
         add(k:{StringToValue Key atom} v:{StringToValue Val string}
             s:{StringToValue Secret atom} vs:{StringToValue ValSecret atom})
      [] remove(Secret Key ValSecret Val) then
         remove(k:{StringToValue Key atom} v:{StringToValue Val string}
                s:{StringToValue Secret atom} vs:{StringToValue ValSecret atom})
      %% error("ill formed string")
      else
         error(Str)
      end
   end

end
