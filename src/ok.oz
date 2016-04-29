declare
A = "arising thunder"
B = 'in the fight'
C = a(qua)

{System.show A#B#C}
{System.show {Record.is C}}
{System.showInfo {Value.toVirtualString A 100 100}#B#{Value.toVirtualString C 100 100}}

declare
proc {Blabla Text}
   fun {CleanString Text}
      if {Record.is Text}
         {Value.toVirtualString Text 100 100}
      else
         Text
      end
   end
   fun {CleanFullString Text}
      case Text
      of T#MoreText then
         {CleanString Text}#{CleanFullString MoreText}
      else
         {CleanString Text}
      end
   end
in
   {System.showInfo {CleanFullString Text}}
end
