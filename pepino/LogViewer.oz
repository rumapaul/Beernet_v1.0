%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Log Viewer
%% Graphical representation of a logged session
%%
%% By Donatien Grolaux, 2007
%% Contributors: Boriss Mejias, 2008
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% structure of messages :
%% in(Message_id Receiver_id Receiver_thread_id Sender_id Message)
%% out(Message_id Sender_id Sender_thread_id Receiver_id Message)
%% next(Site_id Side_thread_id)
%% event(Site_id Side_thread_id Message)
%%
%% the rendering try to associate an 'in' message to all the 'out' messages corresponding to this 'in', based on the ids of the site and the thread. 'next' explicitly cut this association. 'event' annotates a site with a message
%%
%% example
%% out(1 1 1 2 join(1))
%% in(1 2 1 1 join(1))
%% out(2 2 1 1 accept)
%% in(2 1 1 2 accept)
%%
%% will draw:
%% site1:->site2,join(1)
%% site2:site1->,join(1);->site1,accept % these are grouped together
%% site1:site2->,accept

%declare

functor

import
   Tk
   Open
   Compiler
   System

export
   ReadLog
   WriteLog
   LogMerger
   DrawLog
   InteractiveLogViewer

define

   %Show=System.show
   
   ColWidth=100
   LineWidth=16
   Weight=100.
   MaxSpeed=16.0
   MinSpeed=2.0
   MaxDistance=500.0
   Attraction = {NewCell 0.3}
   Repulsion  = {NewCell 0.1}

   class TextFile from Open.text Open.file end
   fun {Head Xs} Xs.1 end
   fun {Tail Xs} Xs.2 end
   fun {Distance A#B C#D} AC=A-C BD=B-D in {Max {Sqrt AC*AC + BD*BD} 1.} end

   fun{ReadLog FileName}
      Out
      S={NewCell Out}
      FN
      DummyPort={NewPort _}
      DummyChunk={Chunk.new c}
      DummyName={NewName}
   
      fun{Replace Str}
         case Str
         of &<|&P|&o|&r|&t|&>|Ls then
            &D|&u|&m|&m|&y|&P|&o|&r|&t|{Replace Ls}
         [] &<|&C|&h|&u|&n|&k|&>|Ls then
            &D|&u|&m|&m|&y|&C|&h|&u|&n|&k|{Replace Ls}
         [] &<|&N|&>|Ls then
            &D|&u|&m|&m|&y|&N|&a|&m|&e|{Replace Ls}      
         [] Lx|Ls then
            Lx|{Replace Ls}
         else nil end
      end
      thread
         try
            {New TextFile init(name:FileName)}=FN
            proc{Loop}
               if {FN atEnd($)} then
                  raise atEnd end
               end
               Str1={FN getS($)}
               Str={Replace Str1}
            in
               try
                  M={Compiler.evalExpression Str env('DummyPort':DummyPort 'DummyChunk':DummyChunk 'DummyName':DummyName) _ $}
                  O N
               in
                  {Exchange S O N}
                  O=M|N
               catch _ then 
                  {System.showInfo "Ignored line "#Str1}
               end
               {Loop}
            end
         in
            {Loop}
         catch _ then
            try {Access S}=nil catch _ then skip end
            thread try {FN close} catch _ then skip end end
         end
      end
   in
      Out
   end

   fun{LogMerger Pt}
      %% Pt is a port where nodes can send messages NodeId#LogEvent
      %% this function returns a stream so that:
      %% 1) messages received from the same NodeId appear in the same order
      %% 2) 'in'(...) messages are received after their corresponding out(...) message
      %%
      %% the strategy is to create a stream per NodeId.
      %% messages are pulled from non sleeping streams only (in random order)
      %% when an 'in' is received and the out has not yet been sent, then that stream is slept
      %% when an out is received, the stream sleep on the corresponding 'in' is waken up
      Streams={Dictionary.new}
      fun{AddToStream NodeId LogEvent}
         %% add an element to the stream of NodeId
         %% returns true if a new stream was created, false otherwise
         Ret
         R={Dictionary.condGet Streams NodeId unit}
         First Last
         Ret=if R==unit then
                First={Cell.new _}
                Last={Cell.new @First}
                {Dictionary.put Streams NodeId First#Last#{NewCell true}}
                true
             else
                First#Last#_=R
                false
             end
         O N
      in
         O=Last:=N
         O=LogEvent|N
         Ret
      end
      fun{IsStreamAwake NodeId}
         %% returns true if the stream of NodeId is awaken, false otherwise
         if {Dictionary.member Streams NodeId} then
            {Access {Dictionary.get Streams NodeId}.3}
         else true end
      end
      proc{SleepStream NodeId}
         %% sleeps the stream of NodeId
         {Assign {Dictionary.get Streams NodeId}.3 false}
      end
      proc{AwakenStream NodeId}
         %% awakens the stream of NodeId
         {Assign {Dictionary.get Streams NodeId}.3 true}
      end
      fun{BindOnIncoming NodeId}
         {Access {Dictionary.get Streams NodeId}.1}
      end
      fun{GetMessage NodeId}
         %% returns the pending message on the stream of NodeId
         {Access {Dictionary.get Streams NodeId}.1}.1
      end
      fun{PullMessage NodeId}
         %% drops the pending message on the stream of NodeId and returns it
         Msgs={Dictionary.get Streams NodeId}.1
         R
      in
         R=@Msgs.1
         Msgs:=@Msgs.2
         R
      end
      fun{GetStreamList}
         %% returns the list of existing streams
         {Dictionary.keys Streams}
      end
      Out
      OutPort={Port.new Out}
      Mb={NewCell _}

      OutIndex={Dictionary.new}
      proc{SetOut Source Target V}
         D1={Dictionary.condGet OutIndex Source unit}
         D
         if D1==unit then
            D={Dictionary.new}
            {Dictionary.put OutIndex Source D}
         else
            D=D1
         end
      in
         {Dictionary.put D Target V}
      end
      fun{GetOut Source Target}
         D1={Dictionary.condGet OutIndex Source unit}
         D
         if D1==unit then
            D={Dictionary.new}
            {Dictionary.put OutIndex Source D}
         else
            D=D1
         end
      in
         {Dictionary.condGet D Target ~1}
      end
      StreamLock={Lock.new}
   in
      thread
         {ForAll {Port.new $ Pt}
          proc{$ NodeId#Msg}
             lock StreamLock then
                if {AddToStream NodeId Msg} then
                   O
                in
                   O = Mb := _
                   O = unit
                end
             end
          end}
      end
      thread
         proc{Loop}
            %% waits for a message on an awaken stream
            Active R
            lock StreamLock then
               Active={List.filter {GetStreamList} IsStreamAwake}
               R={Record.adjoinAt
                  {List.toRecord r {List.map Active fun{$ NodeId} NodeId#{BindOnIncoming NodeId} end}}
                  mb @Mb}
            end
            Which={Record.waitOr R}
         in
            lock StreamLock then
               case Which of mb then skip
               [] NodeId then
                  Msg={GetMessage NodeId}
               in
                  case Msg
                  of out(MId SenderId _ ReceiverId ...) then
                     {SetOut SenderId ReceiverId MId}
                     %% checks if a stream should be awakened
                     if {Not {IsStreamAwake ReceiverId}} then
                        case {GetMessage ReceiverId} of 'in'(MId2 !ReceiverId _ !SenderId ...) then
                           if MId>=MId2 then
%           {Browse awakening#MId#SenderId#ReceiverId#{GetOut SenderId ReceiverId}}
                              {AwakenStream ReceiverId}
                           end
                        else skip end
                     end
                     {Port.send OutPort {PullMessage NodeId}}
                  [] 'in'(MId ReceiverId _ SenderId ...) then
                     if ReceiverId\=SenderId andthen {GetOut SenderId ReceiverId}<MId then %% message not yet sent
%          {Browse sleeping#MId#SenderId#ReceiverId#{GetOut SenderId ReceiverId}}
                        {SleepStream NodeId}
                     else
                        {Port.send OutPort {PullMessage NodeId}}
                     end
                  else
                     {Port.send OutPort {PullMessage NodeId}}
                  end
               end
            end
            {Loop}
         end
      in
         {Loop}
      end
      Out
   end

   fun{InteractiveLogViewer Log}  %% Log is a stream or a list
      Window={New Tk.toplevel tkInit(delete:proc{$} {@OnClose Window} end)}    
      
      {Tk.send wm(title Window "PEPINO")}
%      {Tk.send wm(state Window zoomed)}
      PanedWindow={New {Tk.newWidgetClass noCommand panedwindow} tkInit(parent:Window orient:horizontal showhandle:true sashrelief:raised opaqueresize:true)}

	
      
      ControlFrame = {New Tk.frame tkInit(parent:Window)}
      
      LogFrame     = {New Tk.frame tkInit(parent:PanedWindow relief:raised)}
      GraphCanvas  = {New Tk.canvas tkInit(parent:PanedWindow bg:white relief:raised)}
      
      LogCanvas={New Tk.canvas tkInit(parent:LogFrame bg:white)}
      Title={New Tk.canvas tkInit(parent:LogFrame height:LineWidth*2 bg:white)}
      ScrollH={New Tk.scrollbar tkInit(parent:LogFrame orient:horizontal)}
      ScrollV={New Tk.scrollbar tkInit(parent:LogFrame orient:vertical)}
      {Window tkBind(event:'<MouseWheel>' args:[int('D')]
                     action:proc{$ D}
                               {LogCanvas tk(yview scroll (D div ~120) units)}
                            end)}

      
      {Tk.send grid(rowconfigure Window 0 weight:1)}
      {Tk.send grid(columnconfigure Window 0 weight:1)}
      {Tk.send grid(configure PanedWindow column:0 row:0 sticky:nswe)}
      {Tk.send grid(configure ControlFrame column:0 row:1 sticky:swe)}

      %% Trying to group the Transactional panel related widget on this area
      %% +----------------------+
      %% |TransWindow           |
      %% |+--------------------+|
      %% || CodeFrame          ||
      %% |+--------------------+|
      %% |+--------------------+|
      %% || DebugFrame         ||
      %% |+--------------------+|
      %% |+--------------------+|
      %% || OutputFrame        ||
      %% |+--------------------+|
      %% +----------------------+
      %%
      TransWindow  = {New {Tk.newWidgetClass noCommand panedwindow}
                      tkInit(parent:PanedWindow
                             orient:vertical
                             sashrelief:raised
                             showhandle:true
                             opaqueresize:false)}
      CodeWindow   = {New {Tk.newWidgetClass noCommand panedwindow}
                      tkInit(parent:TransWindow
                             orient:vertical
                             sashrelief:raised
                             showhandle:false
                             opaqueresize:false)}
      DebugFrame   = {New Tk.frame tkInit(parent:TransWindow)}
      OutputWindow = {New {Tk.newWidgetClass noCommand panedwindow}
                      tkInit(parent:TransWindow
                             orient:vertical
                             sashrelief:raised
                             showhandle:false
                             opaqueresize:false)}

      TransLabel   = {New Tk.label tkInit(parent:CodeWindow text:"Transaction")}
      CodeArea     = {New Tk.text
                      tkInit(parent:CodeWindow
                             height:12
                             width:40)}
      {CodeArea tk(insert 'end' '#'("proc {Trans Obj}\n"
                                    "   V\n"
                                    "in\n"
                                    "   {Obj write(aachen p2p)}\n"
                                    "   {Obj read(aachen V)}\n"
                                    "   {Say V}\n"
                                    "   {Obj commit}\n"
                                    "end"))}
      %% This part goes bellow the code area
      CodeButFrame = {New Tk.frame tkInit(parent:CodeWindow)}
      OutcomeLabel = {New Tk.label tkInit(parent:CodeButFrame text:"Outcome: ")}
      OutcomeArea  = {New Tk.text tkInit(parent:CodeButFrame height:1 width:9)}      
      RunButton    = {New Tk.button tkInit(parent:CodeButFrame
                                           text:"Run"
                                           %relief:raised
                                           action:proc{$}
                                                     Str
                                                  in
                                                     {System.show 'Trying to recover something'}
                                                     Str = {CodeArea tkReturnString(get p(1 0) 'end' $)}
                                                     {Wait Str}
                                                     {System.show Str}
                                                     {@OnRunTrans Str}
                                                  end)}
      {Tk.send pack(side:left OutcomeLabel OutcomeArea)}
      {Tk.send pack(side:right RunButton)}
      {CodeWindow tk(add TransLabel CodeArea CodeButFrame)}

      %% Buttons for the debug frame
      BreakPointButton = {New Tk.button tkInit(parent:DebugFrame
                                               text:"BreakPoint"
                                               relief:raised
                                               action:proc{$}
                                                         {@OnBreakPoint unit}
                                                      end)}
      ResumeButton     = {New Tk.button tkInit(parent:DebugFrame
                                               text:"Resume TM"
                                               relief:raised
                                               action:proc{$}
                                                         {@OnResume unit}
                                                      end)}
      CrashButton      = {New Tk.button tkInit(parent:DebugFrame
                                               text:"Crash TM!"
                                               relief:raised
                                               action:proc{$}
                                                         {@OnCrashTM unit}
                                                      end)}
      {Tk.send pack(side:left BreakPointButton ResumeButton CrashButton)}      

      %% The output frame
      OutputLabel = {New Tk.label tkInit(parent:OutputWindow text:"Output")}
      OutputArea  = {New Tk.text
                     tkInit(parent:OutputWindow
                            height:7
                            width:40)}
      {OutputWindow tk(add OutputLabel OutputArea)}
      
      {TransWindow tk(add CodeWindow DebugFrame OutputWindow)}
      %% End initial setting Transactional panel
      
      {PanedWindow tk(add LogFrame GraphCanvas TransWindow)}

      StopButton={New Tk.button tkInit(parent:ControlFrame text:"[]" relief:sunken action:CO#stop)}
      FrameButton={New Tk.button tkInit(parent:ControlFrame text:"|>" relief:raised action:CO#oneFrame)}
      PlayButton={New Tk.button tkInit(parent:ControlFrame text:">" relief:raised action:CO#play(speed:250))}
      FFButton={New Tk.button tkInit(parent:ControlFrame text:">>" relief:raised action:CO#play(speed:100))}
      FFFButton={New Tk.button tkInit(parent:ControlFrame text:">>>" relief:raised action:CO#play(speed:50))}
      ToEndButton={New Tk.button tkInit(parent:ControlFrame text:">|" relief:raised action:CO#runToEnd)}
      IndexVar={New Tk.variable tkInit(0)}
      IndexText={New Tk.entry tkInit(parent:ControlFrame textvariable:IndexVar)}
      {IndexText tkBind(event:'<Return>'
                        action:proc{$}
                                  V1=try {IndexVar tkReturnInt($)} catch _ then ~1 end
                                  V=if {Int.is V1} then V1 else ~1 end
                               in
                                  {CO goto(V)}
                               end)}
      Label={New Tk.label tkInit(parent:ControlFrame text:"Message: ")}
      PatternVar={New Tk.variable tkInit("")}
      PatternText={New Tk.entry tkInit(parent:ControlFrame
                                       textvariable:PatternVar)} 
      {PatternText tkBind(event:'<Return>'
                          action:proc{$}
                                    Str = {PatternVar tkReturn($)}
                                 in
                                    {Wait Str}
                                    %{System.show Str}
                                    {@OnEnter Str}
                                 end)}
      ColorFrame={New Tk.frame tkInit(parent:ControlFrame)}
      ColorButtons={Dictionary.new}
      CurrentAttractorColor={NewCell black}

      {Tk.send pack(side:left StopButton FrameButton PlayButton FFButton FFFButton ToEndButton IndexText Label)}
      {Tk.send pack(side:left fill:x expand:true PatternText)}
      {Tk.send pack(side:right fill:x ColorFrame)}

      {Tk.send grid(rowconfigure LogFrame 1 weight:1)}
      {Tk.send grid(columnconfigure LogFrame 0 weight:1)}
      {Tk.send grid(configure Title column:0 row:0 sticky:nwe)}
      {Tk.send grid(configure LogCanvas column:0 row:1 sticky:nswe)}
      {Tk.send grid(configure ScrollH column:0 row:2 sticky:we)}
      {Tk.send grid(configure ScrollV column:1 row:0 rowspan:2 sticky:ns)}
      {Tk.defineUserCmd xscroll
       proc{$ L}
          {LogCanvas tk(xview b(L))}
          {Title tk(xview b(L))}
       end [list(atom)] _}
      {Tk.send v('proc xscroll2 args {\n' #
                 '   eval xscroll {$args}\n' #
                 '}')}
      {LogCanvas tk(configure xscrollcommand:s(ScrollH set))}
      {ScrollH tk(configure command:xscroll2)}
      {Tk.addYScrollbar LogCanvas ScrollV}

      {PanedWindow tk(sash place 0 300 0)}
      
      NodeCount={NewCell 0}
      LineIdx={NewCell ~1}
      NodeDict={Dictionary.new}
      OriginIdx={Dictionary.new}
      Conf={NewCell c}

      MaxX={NewCell 100.0}
      MaxY={NewCell 100.0}
      GraphNodes={Dictionary.new}
      GraphEdges={Dictionary.new}
      GraphAttractors={Dictionary.new}
      Attractors={NewCell c}
%      Repulsors={NewCell c}
      Active = {NewCell _}   % whether the nodes are active in the window
      {GraphCanvas tkBind(event:"<Enter>" action:proc {$} @Active = unit end)}
      {GraphCanvas tkBind(event:"<Leave>" action:proc {$} unit = Active := _ end)}
      {GraphCanvas tkBind(event:"<Configure>" action:proc{$}
                                                        {Assign MaxX {Tk.returnFloat winfo(width GraphCanvas)}}
                                                        {Assign MaxY {Tk.returnFloat winfo(height GraphCanvas)}}
%                    @Active = unit
%                    thread
%                       {Delay 100}
%                       O N
%                    in
%                       {Exchange Active O N}
%                       if {IsFree O} then N=O end
%                    end
                                                     end)}
      OnParse      = {NewCell proc{$ _} skip end}
      OnClick      = {NewCell proc{$ _} skip end}
      OnEnter      = {NewCell proc{$ _} skip end}
      OnRunTrans   = {NewCell proc{$ _} skip end}  
      OnBreakPoint = {NewCell proc{$ _} skip end}
      OnResume     = {NewCell proc{$ _} skip end}
      OnCrashTM    = {NewCell proc{$ _} skip end}
      OnClose      = {NewCell proc{$ C} {C tkClose} end}
      fun{IncLineIdx}
         O N
      in
         {Exchange LineIdx O N}
         N=O+1
         {LogCanvas tk(configure scrollregion:q(~ColWidth 0
                                                {Access NodeCount}*ColWidth (N+3)*LineWidth))}
         {Title tk(configure scrollregion:q(~ColWidth 0
                                            {Access NodeCount}*ColWidth LineWidth*2))}
         {LogCanvas tk(yview moveto 1.0)}
         N
      end
      PI=3.14159264
      fun{Complement L}
         D={Dictionary.clone GraphNodes}
      in
         {ForAll L proc{$ K} {Dictionary.remove D K} end}
         {Dictionary.keys D}
      end
      proc{AddColor C}
         O N
      in
         {Dictionary.condExchange ColorButtons C unit O N}
         if O==unit then
            N={New Tk.button tkInit(parent:ColorFrame text:C
                                    action:proc{$}
                                              {ForAll {Dictionary.items ColorButtons}
                                               proc{$ L}
                                                  {L tk(configure relief:if L==N then sunken else raised end)}
                                               end}
                                              {SetAttractorColor C}
                                           end
                                    bg:C)}
            {Tk.send pack(side:left N)}
            if C==@CurrentAttractorColor then
               {N tk(configure relief:sunken)}
            end
         else
            N=O
         end
      end
      proc{AddNode Id}
         if {Not {Dictionary.member NodeDict Id}} then
            fun{Loop N}
               %% divide the circle in sections
               %% N>=0 & N<2 =>angle=0+(n*pi)
               %% N>=2 & N<4 => angle=pi/2+((n-2)*pi)
               %% N>=4 & N<8 => angle=pi/4+((n-4)*(pi/2))
               %% N>=8 & N<16 => angle=pi/8+((n-8)*(pi/4))
               fun{ILoop C Delta Seg}
                  if N>=C andthen N<(C*2) then
                     Delta+{Int.toFloat (N-C)}*Seg
                  else
                     {ILoop C*2 Delta/2.0 Seg/2.0}
                  end
               end
            in
               if N==0 then 0.0 elseif N==1 then PI
               else
                  {ILoop 2 PI/2.0 PI}
               end
            end
            Col={Length {Dictionary.keys NodeDict}}+1
            T={New Tk.canvasTag tkInit(parent:GraphCanvas)}
            T2={New Tk.canvasTag tkInit(parent:GraphCanvas)}
            T3={New Tk.canvasTag tkInit(parent:GraphCanvas)}
       
            Angle={Loop @NodeCount}
       
            W={Max 50.0 {Min @MaxX @MaxY}/2.0-20.0}
            X=@MaxX/2.0+W*{Cos Angle}
            Y=@MaxY/2.0+W*{Sin Angle}
       
%      {GraphCanvas tk(create text 100 100+10*Col text:{Value.toVirtualString node(id:Id x:X y:Y angle:Angle w:W maxx:@MaxX maxy:@MaxY) 1000 1000})}
%      OI={GraphCanvas tkReturnInt(create(oval X-10. Y-10. X+10. Y+10. fill:white tags:T) $)}
            TI={GraphCanvas tkReturnInt(create(text X Y text:Id tags:T) $)}
            [X1 Y1 X2 Y2]={GraphCanvas tkReturnListInt(bbox(TI) $)}
            BI={GraphCanvas tkReturnInt(create(rect X1-2 Y1-2 X2+2 Y2+2 fill:white outline:black tags:T) $)}
            {GraphCanvas tk('raise' TI)}
            {T tkBind(event:'<Enter>' action:proc{$}
                                                unit = Active := _
                                                {GraphCanvas tk('raise' TI)}
                                                {GraphCanvas tk(lower BI TI)}
                                                {GraphCanvas tk(itemconfigure T2 width:3)}
                                                {GraphCanvas tk(itemconfigure T3 width:3 stipple:gray50)}
                                             end)}
            {T tkBind(event:'<Leave>' action:proc{$}
                                                @Active=unit
                                                {GraphCanvas tk(lower TI)}
                                                {GraphCanvas tk(lower BI TI)}
                                                {GraphCanvas tk(itemconfigure T2 width:1)}
                                                {GraphCanvas tk(itemconfigure T3 width:1 stipple:'')}
                                             end)}
            {T tkBind(event:'<3>'
                      args:[int(x) int(y)]
                      action:proc{$ X Y}
                                {@OnClick node(Id
                                               tag:T
                                               canvas:GraphCanvas
                                               x:X
                                               y:Y)}
                             end)}


            Dragging = {NewCell false}
            DragTo   = {NewCell nil}
            {T tkBind(event:  "<Button-1>"
                      args:   [float(x) float(y)]
                      action: proc {$ X Y} DragTo := X#Y Dragging := true @Active = unit end)}
            {T tkBind(event:  "<Motion>"
                      args:   [float(x) float(y)]
                      action: proc {$ X Y}
                                 if @Dragging then DragTo := X#Y end
                              end)}
            {T tkBind(event:  "<ButtonRelease-1>"
                      action: proc {$} Dragging := false end)}
         in
            {Dictionary.put NodeDict Id unit#0#Col}
            {Title tk(create text ColWidth*Col-(ColWidth div 2) LineWidth text:Id)}
            {LogCanvas tk(create line ColWidth*Col-(ColWidth div 2) LineWidth
                          ColWidth*Col-(ColWidth div 2) 100000)}
            NodeCount:=@NodeCount+1
            {Dictionary.put GraphNodes Id n(t:T
                                            'from':T2
                                            to:T3
                                            box:BI
                                            text:TI
                                            c:(X#Y)|_)}
            thread
               %% constant update node position
               proc{Loop OX#OY CheckAtt Att Rep}
                  {Delay 100}
                  {Wait @Active}
                  Old=GraphNodes.Id
                  NCheckAtt NAtt NRep
                  X1#Y1=if @Dragging then
                           NAtt=Att
                           NRep=Rep
                           NCheckAtt=CheckAtt            
                           @DragTo
                        else
                           %% list of attractors is GraphAttractors.Id
                           %% list of repulsors is {Complement Id|GraphAttractors.Id}
                           if @Attractors==CheckAtt then
                              NAtt=Att
                              NRep=Rep
                              NCheckAtt=CheckAtt
                           else
                              NCheckAtt=@Attractors
                              if {CondSelect NCheckAtt Id nil}==nil then
                                 NAtt=nil
                                 NRep=nil %% unconnected nodes don't count
%            {Map {List.filter {Dictionary.keys GraphNodes}
%                  fun{$ K} K\=Id andthen {CondSelect NCheckAtt K nil}\=nil end}
%                  fun{$ K}
%                {Dictionary.get GraphNodes K}.c
%                  end}
                              else
                                 NAtt={Map NCheckAtt.Id
                                       fun{$ K}
                                          {Dictionary.get GraphNodes K}.c
                                       end}
                                 NRep={Map {List.filter
                                            {Complement Id|NCheckAtt.Id}
                                            fun{$ K}
%                     {CondSelect @Repulsors K false}\=false
                                               {CondSelect @Attractors K nil}\=nil
                                            end}
                                       fun{$ K}
                                          {Dictionary.get GraphNodes K}.c
                                       end}
%            {Show Id#NCheckAtt.Id#{List.filter
%                  {Complement Id|NCheckAtt.Id}
%                  fun{$ K} {CondSelect @Attractors K nil}\=nil end}#@Attractors}
                              end
                           end
                           Moves1={Map NAtt
                                   fun{$ X}
                                      A#B={Head X}
                                      D = {Distance A#B OX#OY}
                                      F = (@Attraction * (D-Weight) / D)
                                   in
                                      (F*(A-OX))#(F*(B-OY))
                                   end}
                           Moves2={Map NRep
                                   fun{$ X}
                                      A#B={Head X}
                                      D = {Distance A#B OX#OY}
                                      F=(if D>MaxDistance then
                                            0.0
                                         else
                                            @Repulsion * Weight * Weight / (D*D)
                                         end)
                                   in
                                      (F*(OX-A))#(F*(OY-B))
                                   end}
                        in
                           {FoldL {Append Moves1 Moves2}
                            fun {$ A#B C#D} (A+C)#(B+D) end OX#OY}
                        end
                  X2#Y2={Min {Max 10.0 X1} {Access MaxX}-10.0}#{Min {Max 10.0 Y1} {Access MaxY}-10.0}
                  ND={Distance OX#OY X2#Y2}
                  XN#YN=if @Dragging then
                           X2#Y2 %% no speed limit for the user
                        elseif ND<MinSpeed then OX#OY
                        elseif ND<MaxSpeed then X2#Y2 else
                           (OX+(X2-OX)*MaxSpeed/ND)#(OY+(Y2-OY)*MaxSpeed/ND)
                        end
                  N
               in
                  {GraphCanvas tk(move T XN-OX YN-OY)}
                  Old.c.2=(XN#YN)|N
                  {Dictionary.put GraphNodes Id
                   {Record.adjoinAt Old c (XN#YN)|N}}
%       if @Dragging then
%          {Loop XN#YN CheckAtt Att Rep}
%       else
                  {Loop XN#YN NCheckAtt {List.map NAtt Tail} {List.map NRep Tail}}
%       end
               end
            in
               {Loop X#Y c nil nil}
%         {Loop X#Y nil nil {Map {List.filter {Dictionary.keys GraphNodes}
%                  fun{$ K} K\=Id end}
%             fun{$ K}
%                  {Dictionary.get GraphNodes K}.c
%             end}}
            end
         end
      end
      proc{AddEdge From To Color}
         {AddNode From}
         {AddNode To}
         {AddColor Color}
         D1={Dictionary.condGet GraphEdges From unit}
         D2=if D1==unit then
               {Dictionary.put GraphEdges From D2}
               {Dictionary.new}
            else D1 end
         D3={Dictionary.condGet D2 To unit}
         D=if D3==unit then
              {Dictionary.put D2 To D}
              {Dictionary.new}
           else D3 end
         Tag={New Tk.canvasTag tkInit(parent:GraphCanvas)}
         LI={GraphCanvas tkReturnInt(create(line 0. 0. 0. 0.
                                            tags:q(Tag {Dictionary.get GraphNodes From}.'from' {Dictionary.get GraphNodes To}.'to')
                                            arrow:last fill:Color smooth:true) $)}
         {Dictionary.put D LI e(c:Color
                                tag:Tag
                                t:F)} %% when F is bound, edge is removed
         {Tag tkBind(event:'<Enter>' action:proc{$}
                                               unit = Active := _
                                               {GraphCanvas tk(itemconfigure LI width:3)}
                                            end)}
         {Tag tkBind(event:'<Leave>' action:proc{$}
                                               @Active=unit
                                               {GraphCanvas tk(itemconfigure LI width:1)}
                                            end)}
         {Tag tkBind(event:'<3>'
                     args:[int(x) int(y)]
                     action:proc{$ X Y}
                               {@OnClick edge(From To
                                              tag:Tag
                                              canvas:GraphCanvas
                                              x:X y:Y)}
                            end)}
         F

         Dist={Length {Dictionary.keys D}}
      in
         if Color==@CurrentAttractorColor then
            {AddAttractor From To}
         end
         thread
            proc{Loop C1 C2 OFrom OTo}
               Which={Record.waitOr c(C1 C2 F)}
            in
               if Which==3 then
                  {Dictionary.remove D LI}
                  {GraphCanvas tk(delete LI)}
               else
                  NFrom
                  NTo
                  NC1 NC2
                  if Which==1 then
                     NTo=OTo
                     NC2=C2
                     case C1 of X#Y|Ls then
                        NFrom=X#Y
                        NC1=Ls
                     else
                        F=unit
                        NC1=_
                        NFrom=unit
                     end
                  else
                     NFrom=OFrom
                     NC1=C1
                     case C2 of X#Y|Ls then
                        NTo=X#Y
                        NC2=Ls
                     else
                        F=unit
                        NC2=_
                        NTo=unit
                     end
                  end
               in
                  if NFrom\=unit andthen NTo\=unit then
                     %% set coords
                     X1#Y1=NFrom
                     X2#Y2=NTo
                     D = {Distance X1#Y1 X2#Y2}
                     Xa#Ya=X1#Y1
                     Xb#Yb=X2+(X1-X2)*10./D#Y2+(Y1-Y2)*10./D
                     Xc#Yc=((Xb+Xa)/2.0)#((Yb+Ya)/2.0)
                     Xd#Yd=(~Yb+Ya)#(Xb-Xa)
%          {GraphCanvas tk(delete ttfg1)}
%          {GraphCanvas tk(create line Xa Ya (Xa+Xd) (Ya+Yd) fill:red tags:ttfg1)}
%          Xe#Ye=(Xc+(((D/5.0)*{Int.toFloat Dist})*Xd)/N)#(Yc+(((D/5.0)*{Int.toFloat Dist})*Yd)/N)
                     Ecart={Min D/6.0 20.0}*{Int.toFloat Dist}
                     Xe#Ye=(Xc+(Ecart*Xd)/D)#(Yc+(Ecart*Yd)/D)
                  in
                     {GraphCanvas tk(coords LI Xa Ya Xe Ye Xb Yb)}
                  end
                  {Loop NC1 NC2 NFrom NTo}
               end
            end
         in
            {Loop GraphNodes.From.c GraphNodes.To.c unit unit}
         end
      end
      proc{RemoveEdge From To Color}
         D1={Dictionary.condGet GraphEdges From unit}
         D2=if D1==unit then
               {Dictionary.put GraphEdges From D2}
               {Dictionary.new}
            else D1 end
         D3={Dictionary.condGet D2 To unit}
         D=if D3==unit then
              {Dictionary.put D2 To D}
              {Dictionary.new}
           else D3 end
      in
         if Color==@CurrentAttractorColor then
            {RemoveAttractor From To}
         end
         {ForAll {Dictionary.entries D}
          proc{$ _/*Id*/#E}
             if E.c==Color then
                E.t=unit
             end
          end}
      end
      proc{SetAttractorColor C}
         CurrentAttractorColor:=C
         {Dictionary.removeAll GraphAttractors}
         {ForAll {Dictionary.entries GraphEdges}
          proc{$ From#D}
             {ForAll {Dictionary.entries D}
              proc{$ To#DD}
                 {ForAll {Dictionary.items DD}
                  proc{$ E}
                     if E.c==C then
                        D1={Dictionary.condGet GraphAttractors From unit}
                        D2=if D1==unit then
                              {Dictionary.put GraphAttractors From D2}
                              {Dictionary.new}
                           else D1 end
                        Old={Dictionary.condGet D2 To 0}
                     in
                        {Dictionary.put D2 To Old+1}
                     end
                  end}
              end}
          end}
     
         {UpdateAttractors}
      end
      proc{RemoveAllOutEdges From}
         D1={Dictionary.condGet GraphEdges From unit}
         D2=if D1==unit then
               {Dictionary.put GraphEdges From D2}
               {Dictionary.new}
            else D1 end
      in
         {ForAll {Dictionary.entries D2}
          proc{$ _#D}
             {ForAll {Dictionary.items D}
              proc{$ E} E.t=unit end}
          end}
         {Dictionary.remove GraphAttractors From}
         {UpdateAttractors}
      end
      proc{UpdateAttractors}
         Nodes={Dictionary.keys GraphNodes}
         A={List.toRecord g {List.map Nodes fun{$ K} K#{Dictionary.new} end}}
      in
%   Repulsors:=c
         {ForAll {Dictionary.entries GraphAttractors}
          proc{$ From#ToDict}
             {ForAll {Dictionary.keys ToDict}
              proc{$ To}
                 if To\=From then
                    A.From.To:=unit
                    A.To.From:=unit
%         if {CondSelect @Repulsors To false}==false then
%            Repulsors:={Record.adjoinAt @Repulsors To true}
%         end
                 end
              end}
          end}
%   {Show updAttr#{List.toRecord c {List.map {Dictionary.entries GraphAttractors} fun{$ Id#D} Id#{Dictionary.entries D} end}}}
         Attractors:={Record.map A Dictionary.keys}
      end
      proc{AddAttractor From To}
         {AddNode From}
         {AddNode To}
         D1={Dictionary.condGet GraphAttractors From unit}
         D2=if D1==unit then
               {Dictionary.put GraphAttractors From D2}
               {Dictionary.new}
            else D1 end
         Old={Dictionary.condGet D2 To 0}
      in
         {Dictionary.put D2 To Old+1}
         if Old==0 then
            {UpdateAttractors}
         end
      end
      proc{RemoveAttractor From To}
         D1={Dictionary.condGet GraphAttractors From unit}
         D2=if D1==unit then
               {Dictionary.put GraphAttractors From D2}
               {Dictionary.new}
            else D1 end
         Old={Dictionary.condGet D2 To ~1}
      in
         if Old>1 then
            {Dictionary.put D2 To Old-1}
         elseif Old>0 then
            {Dictionary.remove D2 To}
            {UpdateAttractors}
         end
      end
      proc{StoreIdx K1 K2 K3 V}
         D1={Dictionary.condGet OriginIdx K1 unit}
         D1x=if D1==unit then
                D={Dictionary.new}
                {Dictionary.put OriginIdx K1 D}
             in
                D
             else D1 end
         D2={Dictionary.condGet D1x K2 unit}
         D2x=if D2==unit then
                D={Dictionary.new}
                {Dictionary.put D1x K2 D}
             in
                D
             else D2 end
      in
         {Dictionary.put D2x K3 V}
      end
      fun{GetIdx K1 K2 K3}
         try
            D={Dictionary.get {Dictionary.get OriginIdx K1} K2}
            V={Dictionary.get D K3}
         in
            {Dictionary.remove D K3}
            V
         catch _ then 
            {IncLineIdx}
         end
      end
      FirstArrowId
      TagDict={Dictionary.new}
      fun{GetTags S R}
         M1={Min S R}
         M2={Max S R}
         D1=if {Dictionary.member TagDict M1} then
               {Dictionary.get TagDict M1}
            else
               D={Dictionary.new}
            in
               {Dictionary.put TagDict M1 D}
               D
            end
      in
         if {Dictionary.member D1 M2} then
            {Dictionary.get D1 M2}
         else
            T={New Tk.canvasTag tkInit(parent:LogCanvas)}#{New Tk.canvasTag tkInit(parent:LogCanvas)}#{New Tk.canvasTag tkInit(parent:LogCanvas)}
            proc{Raise} % T.1 is text, T.2 is arrow, T.3 is blackbox
               {LogCanvas tk('raise' T.2)}
               {LogCanvas tk('raise' T.3)}
               {LogCanvas tk('raise' T.1)}
               {LogCanvas tk(itemconfigure T.2 width:3)}
               {LogCanvas tk(itemconfigure T.3 outline:black fill:white)}
            end
            proc{Lower}
               {LogCanvas tk(lower T.2)}
               {LogCanvas tk('raise' T.3 FirstArrowId)}
               {LogCanvas tk(itemconfigure T.2 width:1)}
               {LogCanvas tk(itemconfigure T.3 fill:white outline:white)}
            end
         in
            {T.1 tkBind(event:"<Enter>" action:Raise)}
            {T.1 tkBind(event:"<Leave>" action:Lower)}
            {T.2 tkBind(event:"<Enter>" action:Raise)}
            {T.2 tkBind(event:"<Leave>" action:Lower)}
            {Dictionary.put D1 M2 T}
            T
         end
      end
      Tick={NewCell _}
      SetIndex={NewName}
      proc{Loop I N}
         {Wait @Tick}
         {Tk.send update(idletasks)}
         {CO SetIndex(I)}
         case N of Nx|Ns then
            case Nx
            of 'in'(Message_id Receiver_id Receiver_thread_id Sender_id Message ...) then
               {AddNode Sender_id}
               {AddNode Receiver_id}
               Color={CondSelect Nx color gray}
            in
               if {CondSelect @Conf Color true} then
                  OrgLine={GetIdx Sender_id Receiver_id Message_id}
                  DestLine={IncLineIdx}
                  Node={Dictionary.get NodeDict Receiver_id}
                  {Dictionary.put NodeDict Receiver_id Receiver_thread_id#DestLine#Node.3}
                  OrgCol={Dictionary.get NodeDict Sender_id}.3
                  DestCol=Node.3
                  %% draw an arrow between (orgline,orgcol) and (destline,destcol)
                  Tags={GetTags Sender_id Receiver_id}
                  AId={LogCanvas tkReturnInt(create(line OrgCol*ColWidth-(ColWidth div 2) (OrgLine+2)*LineWidth
                                                    DestCol*ColWidth-(ColWidth div 2) (DestLine+2)*LineWidth
                                                    fill:{CondSelect Nx color gray}
                                                    arrow:last
                                                    tags:Tags.2
                                                    arrowshape:q(10 10 5)) $)}
                  if {IsFree FirstArrowId} then
                     {Wait AId} FirstArrowId=AId
                  end
                  TId={LogCanvas tkReturnInt(create(text
                                                    (OrgCol*ColWidth-(ColWidth div 2)+DestCol*ColWidth-(ColWidth div 2)) div 2
                                                    (((OrgLine+2)*LineWidth+(DestLine+2)*LineWidth) div 2)-(LineWidth div 3)
                                                    fill:{CondSelect Nx color gray}
                                                    tags:Tags.1
                                                    text:{Value.toVirtualString Message 10000 10000}) $)}
                  [X1 Y1 X2 Y2]={LogCanvas tkReturnListInt(bbox(TId) $)}
                  BId={LogCanvas tkReturnInt(create(rect X1-2 Y1-2 X2+2 Y2+2 fill:white outline:white tags:Tags.3) $)}
               in
                  {LogCanvas tk(lower AId)}
                  {LogCanvas tk('raise' BId FirstArrowId)}
                  {LogCanvas tk('raise' TId)}
               end
            [] out(Message_id Sender_id Sender_thread_id Receiver_id ...) then
               {AddNode Sender_id}
               {AddNode Receiver_id}
               Color={CondSelect Nx color gray}
            in
               if {CondSelect @Conf Color true} then
                  %% outgoing message
                  %% this message should not be drawed yet, instead we remember where we are
                  %% so that can draw it when it is received
                  %% where we are is either the next available line,
                  %% or the continuation of where this node already is (same thread_id)
                  Line
                  Node={Dictionary.get NodeDict Sender_id}
               in
                  if Node.1==Sender_thread_id then
                     Line=Node.2
                  else
                     Line={IncLineIdx}
                  end
                  {Dictionary.put NodeDict Sender_id Sender_thread_id#Line#Node.3}
                  {StoreIdx Sender_id Receiver_id Message_id Line}
                  %%      {Dictionary.put OriginIdx Message_id Line}
               end
            [] next(Site_id ...) then
               {AddNode Site_id}
               Node={Dictionary.get NodeDict Site_id}
            in
               {Dictionary.put NodeDict Site_id unit#0#Node.3}
            [] comment(Message ...) then
               Color={CondSelect Nx color black}
               Line={IncLineIdx}
               Tags={GetTags ~1 ~1}
               TId={LogCanvas tkReturnInt(create(text
                                                 (ColWidth div 2) (Line+2)*LineWidth
                                                 anchor:w
                                                 fill:Color
                                                 tags:Tags.1
                                                 text:I#": "#{Value.toVirtualString Message 10000 10000}) $)}
               /*
               LId={LogCanvas tkReturnInt(create(line
                                                 0 (Line+2)*LineWidth
                                                 10000 (Line+2)*LineWidth
                                                 tags:Tags.2
                                                 fill:Color) $)}
               [X1 Y1 X2 Y2]={LogCanvas tkReturnListInt(bbox(TId) $)}
               BId={LogCanvas tkReturnInt(create(rect X1-2 Y1-2 X2+2 Y2+2 fill:white tags:Tags.3 outline:white) $)}
               */
            in
               {LogCanvas tk('raise' TId)}   
            [] event(Site_id Message ...) then
               {AddNode Site_id}
               Color={CondSelect Nx color gray}
            in
               if {CondSelect @Conf Color true} then
                  Tags={GetTags Site_id Site_id}
                  Node={Dictionary.get NodeDict Site_id}
                  Line={IncLineIdx}
                  Col=Node.3
                  {Dictionary.put NodeDict Site_id {CondSelect Nx thid unit}#Line#Col}
                  TId={LogCanvas tkReturnInt(create(text
                                                    Col*ColWidth-(ColWidth div 2)
                                                    (Line+2)*LineWidth
                                                    fill:{CondSelect Nx color red}
                                                    tags:Tags.1
                                                    text:{Value.toVirtualString Message 10000 10000}) $)}
%                  [X1 Y1 X2 Y2]={LogCanvas tkReturnListInt(bbox(TId) $)}
%                  BId={LogCanvas tkReturnInt(create(rect X1-2 Y1-2 X2+2 Y2+2 fill:white tags:Tags.3 outline:white) $)}
%       ECmd={VirtualString.toAtom e#TId}
%       LCmd={VirtualString.toAtom l#TId}
%       {Tk.defineUserCmd ECmd
%        proc{$}
%           {LogCanvas tk('raise' TId)}
%           {LogCanvas tk(lower BId TId)}
%           {LogCanvas tk(itemconfigure BId outline:black fill:white)}
%        end nil _}
%       {Tk.defineUserCmd LCmd
%        proc{$}
%           {LogCanvas tk(itemconfigure BId fill:white outline:white)}
%        end nil _}
               in
                  {LogCanvas tk('raise' TId)}     
%       {LogCanvas tk(bind TId '<Enter>' ECmd)}
%       {LogCanvas tk(bind TId '<Leave>' LCmd)}
               end
            else
               {System.show ignored#Nx}
            end
            {@OnParse Nx}
            {Loop I+1 Ns}
         else 
            Line={IncLineIdx}
         in
            {LogCanvas tk(create line 0 (Line+2)*LineWidth 100000 (Line+2)*LineWidth)}
            {CO stop}
         end
      end
      thread
         {Loop 1 Log}
      end
      Init={NewName}


      ButtonList=[StopButton FrameButton PlayButton FFButton FFFButton ToEndButton]

      proc{SinkButton B}
         {ForAll ButtonList
          proc{$ N}
             if N==B then
                {B tk(configure relief:sunken)}
             else
                {N tk(configure relief:raised)}
             end
          end}
      end

      class Controller
         prop locking
         attr
            PlayThId
            Goto
            Match
            Index
            outputline
         meth !Init
            PlayThId:=unit
            Goto:=~1
            Match:=fun{$ _} false end
            {self onParse(proc{$ _} skip end)}
            Index:=0
            outputline := 0
         end
         meth Change(B)
            try {Thread.terminate @PlayThId} catch _ then skip end
            Goto:=~1
            {SinkButton B}
         end
         meth normal
            {Tk.send wm(state Window normal)}
         end
         meth minimize
            {Tk.send wm(state Window iconic)}
         end
         meth maximize
            {Tk.send wm(state Window zoomed)}
         end
         meth runToEnd
            {self Change(ToEndButton)}
            @Tick=unit
         end
         meth stop
            O N
         in
            {self Change(StopButton)}
            O=Tick:=N
            if {IsFree O} then
               N=O
            end
         end
         meth oneFrame
            {self Change(StopButton)}
            unit=Tick:=_
         end
         meth onParse(P)
            OnParse:=proc{$ E}
                        {P E}
                        if {@Match E} then
                           {self stop}
                        end
                     end
         end
         meth onClick(P)
            OnClick:=P
         end
         meth onEnter(P)
            OnEnter:=P
         end
         meth onRunTrans(P)
            OnRunTrans:=P
         end
         meth onBreakPoint(P)
            OnBreakPoint:=P
         end
         meth onResume(P)
            OnResume:=P
         end
         meth onCrashTM(P)
            OnCrashTM:=P
         end
         meth onClose(P)
            OnClose:=P
         end
         meth say(S)
            Old New
         in
            Old = outputline := New
            New = Old + 1
            try
               {OutputArea tk(insert p(New 1) S#"\n")}
            catch _ then
               {OutputArea tk(insert p(New 1) "error(KEY NOT FOUND)\n")}
            end
         end
         meth setOutcome(OC)
            {OutcomeArea tk(delete p(1 0) p(1 9))}
            {OutcomeArea tk(insert p(1 1) OC)}
         end
         meth addEdge(F T C)
            {AddEdge F T C}
         end
         meth removeEdge(F T C)
            {RemoveEdge F T C}
         end
         meth removeAllOutEdges(F)
            {RemoveAllOutEdges F}
         end
         meth addAttractor(F T)
            lock
               {AddAttractor F T}
            end
         end
         meth removeAttractor(F T)
            lock
               {RemoveAttractor F T}
            end
         end
         meth setAttractorColor(C)
            lock
               {SetAttractorColor C}
            end
         end
         meth getNodeInfo(F $)
            R={Dictionary.condGet GraphNodes F unit}
         in
            if R\=unit then
               node(id:F
                    canvas:GraphCanvas
                    box:R.box
                    text:R.text
                    tag:R.t
                    c:R.c)
            else R end
         end
         meth play(speed:Speed<=100)
            N
         in
            {self Change(if Speed>=250 then PlayButton
                         elseif Speed>=100 then FFButton
                         else FFFButton end)}
            PlayThId:=N
            thread
               proc{Loop}
                  unit=Tick:=_
                  {Delay Speed}
                  {Loop}
               end
            in
               N={Thread.this}
               {Loop}
            end
            {Wait N}
         end
         meth display(...)=M
            Conf:=M
         end
         meth goto(V)
            if V<@Index then
               {Tk.send bell}
               {IndexVar tkSet(@Index)}
            else
               {self Change(FFFButton)}
               Goto:=V
               @Tick=unit
            end
         end
         meth gotomatch(P)
            {self Change(FFFButton)}
            Match:=P
            @Tick=unit
         end
         meth !SetIndex(I)
            Index:=I
            {IndexVar tkSet(I)}
            if I==@Goto then
               {self stop}
            end
         end
      end
      CO={New Controller Init}
   in
      CO
   end

   proc{DrawLog Log Conf}
      O={InteractiveLogViewer Log}
   in
      {O {Record.adjoin Conf display}}
      {O runToEnd}
   end
      
   
%    proc{DrawLog Log Conf} %% Log is a stream or a list
%       Window={New Tk.toplevel tkInit}
%       {Tk.send wm(title Window "Log Viewer")}
%       Canvas={New Tk.canvas tkInit(parent:Window bg:white)}
%       Title={New Tk.canvas tkInit(parent:Window height:LineWidth*2 bg:white)}
%       ScrollH={New Tk.scrollbar tkInit(parent:Window orient:horizontal)}
%       ScrollV={New Tk.scrollbar tkInit(parent:Window orient:vertical)}
%       {Tk.send grid(rowconfigure Window 1 weight:1)}
%       {Tk.send grid(columnconfigure Window 0 weight:1)}
%       {Tk.send grid(configure Title column:0 row:0 sticky:nwe)}
%       {Tk.send grid(configure Canvas column:0 row:1 sticky:nswe)}
%       {Tk.send grid(configure ScrollH column:0 row:2 sticky:we)}
%       {Tk.send grid(configure ScrollV column:1 row:0 rowspan:2 sticky:ns)}
%       {Tk.defineUserCmd xscroll
%        proc{$ L1 L2}
%    {Canvas tk(xview L1 L2)}
%    {Title tk(xview L1 L2)}
%        end [string string] _}
% %   {Tk.addXScrollbar Canvas ScrollH}
%       {Canvas tk(configure xscrollcommand:s(ScrollH set))}
%       {ScrollH tk(configure command:xscroll)}
%       {Tk.addYScrollbar Canvas ScrollV}
%       NodeCount={NewCell 0}
%       LineIdx={NewCell ~1}
%       NodeDict={Dictionary.new}
%       OriginIdx={Dictionary.new}
%       fun{IncLineIdx}
%   O N
%       in
%   {Exchange LineIdx O N}
%   N=O+1
%   {Canvas tk(configure scrollregion:q(0 0
%                   {Access NodeCount}*ColWidth (N+3)*LineWidth))}
%   {Title tk(configure scrollregion:q(0 0
%                  {Access NodeCount}*ColWidth LineWidth*2))}
%   N
%       end
%       proc{AddNode Id}
%   if {Not {Dictionary.member NodeDict Id}} then
%      Col={Length {Dictionary.keys NodeDict}}+1
%   in
%      {Dictionary.put NodeDict Id unit#0#Col}
%      {Title tk(create text ColWidth*Col-(ColWidth div 2) LineWidth text:Id)}
%      {Canvas tk(create line ColWidth*Col-(ColWidth div 2) LineWidth
%            ColWidth*Col-(ColWidth div 2) 100000)}
%      {Assign NodeCount {Access NodeCount}+1}
%   end
%       end
%       proc{StoreIdx K1 K2 K3 V}
%   D1={Dictionary.condGet OriginIdx K1 unit}
%   D1x=if D1==unit then
%     D={Dictionary.new}
%     {Dictionary.put OriginIdx K1 D}
%       in
%     D
%       else D1 end
%   D2={Dictionary.condGet D1x K2 unit}
%   D2x=if D2==unit then
%     D={Dictionary.new}
%     {Dictionary.put D1x K2 D}
%       in
%     D
%       else D2 end
%       in
%   {Dictionary.put D2x K3 V}
%       end
%       fun{GetIdx K1 K2 K3}
%   try
%      D={Dictionary.get {Dictionary.get OriginIdx K1} K2}
%      V={Dictionary.get D K3}
%   in
%      {Dictionary.remove D K3}
%      V
%   catch _ then 
%      {IncLineIdx}
%   end
%       end
%       FirstArrowId
%       TagDict={Dictionary.new}
%       fun{GetTags S R}
%   M1={Min S R}
%   M2={Max S R}
%   D1=if {Dictionary.member TagDict M1} then
%         {Dictionary.get TagDict M1}
%      else
%         D={Dictionary.new}
%      in
%         {Dictionary.put TagDict M1 D}
%         D
%      end
%       in
%   if {Dictionary.member D1 M2} then
%      {Dictionary.get D1 M2}
%   else
%      T={New Tk.canvasTag tkInit(parent:Canvas)}#{New Tk.canvasTag tkInit(parent:Canvas)}#{New Tk.canvasTag tkInit(parent:Canvas)}
%      proc{Raise} % T.1 is text, T.2 is arrow, T.3 is blackbox
%         {Canvas tk('raise' T.2)}
%         {Canvas tk('raise' T.3)}
%         {Canvas tk('raise' T.1)}
% %       {Canvas tk(lower T.3 T.1)}
% %       {Canvas tk(lower T.2 T.1)}
%         {Canvas tk(itemconfigure T.2 width:3)}
%         {Canvas tk(itemconfigure T.3 outline:black fill:white)}
%      end
%      proc{Lower}
%         {Canvas tk(lower T.2)}
%         {Canvas tk('raise' T.3 FirstArrowId)}
%         {Canvas tk(itemconfigure T.2 width:1)}
%         {Canvas tk(itemconfigure T.3 fill:white outline:white)}
%      end
%   in
%      {T.1 tkBind(event:"<Enter>" action:Raise)}
%      {T.1 tkBind(event:"<Leave>" action:Lower)}
%      {T.2 tkBind(event:"<Enter>" action:Raise)}
%      {T.2 tkBind(event:"<Leave>" action:Lower)}
%      {Dictionary.put D1 M2 T}
%      T
%   end
%       end
%       proc{Loop N}
%   case N of Nx|Ns then
%      case Nx
%      of 'in'(Message_id Receiver_id Receiver_thread_id Sender_id Message ...) then
%         Color={CondSelect Nx color gray}
%      in
%         if {CondSelect Conf Color true} then
%       {AddNode Sender_id}
%       {AddNode Receiver_id}
% %       OrgLine1={Dictionary.condGet OriginIdx Message_id unit}
% %       OrgLine=if OrgLine1==unit then {IncLineIdx} else OrgLine1 end
% %       {Dictionary.remove OriginIdx Message_id}
%       OrgLine={GetIdx Sender_id Receiver_id Message_id}
%       DestLine={IncLineIdx}
%       Node={Dictionary.get NodeDict Receiver_id}
%       {Dictionary.put NodeDict Receiver_id Receiver_thread_id#DestLine#Node.3}
%       OrgCol={Dictionary.get NodeDict Sender_id}.3
%       DestCol=Node.3
%       %% draw an arrow between (orgline,orgcol) and (destline,destcol)
%       Tags={GetTags Sender_id Receiver_id}
%       AId={Canvas tkReturnInt(create(line OrgCol*ColWidth-(ColWidth div 2) (OrgLine+2)*LineWidth
%                  DestCol*ColWidth-(ColWidth div 2) (DestLine+2)*LineWidth
%                  fill:{CondSelect Nx color gray}
%                  arrow:last
%                  tags:Tags.2
%                  arrowshape:q(10 10 5)) $)}
%       if {IsFree FirstArrowId} then
%          {Wait AId} FirstArrowId=AId
%       end
%       TId={Canvas tkReturnInt(create(text
%                  (OrgCol*ColWidth-(ColWidth div 2)+DestCol*ColWidth-(ColWidth div 2)) div 2
%                  (((OrgLine+2)*LineWidth+(DestLine+2)*LineWidth) div 2)-(LineWidth div 3)
%                  fill:{CondSelect Nx color gray}
%                  tags:Tags.1
%                  text:{Value.toVirtualString Message 10000 10000}) $)}
%       [X1 Y1 X2 Y2]={Canvas tkReturnListInt(bbox(TId) $)}
%       BId={Canvas tkReturnInt(create(rect X1-2 Y1-2 X2+2 Y2+2 fill:white outline:white tags:Tags.3) $)}
% %          ECmd={VirtualString.toAtom e#AId}
% %          LCmd={VirtualString.toAtom l#AId}
% %          {Tk.defineUserCmd ECmd
% %      proc{$}
% %         {Canvas tk('raise' TId)}
% %         {Canvas tk(lower BId TId)}
% %         {Canvas tk(lower AId BId)}
% %         {Canvas tk(itemconfigure AId width:3)}
% %         {Canvas tk(itemconfigure BId outline:'#D0D0D0' fill:'#D0D0D0')}
% %      end nil _}
% %          {Tk.defineUserCmd LCmd
% %      proc{$}
% %         {Canvas tk(lower AId)}
% %         {Canvas tk('raise' BId FirstArrowId)}
% %         {Canvas tk(itemconfigure AId width:1)}
% %         {Canvas tk(itemconfigure BId fill:white outline:white)}
% %      end nil _}
%         in
%       {Canvas tk(lower AId)}
%       {Canvas tk('raise' BId FirstArrowId)}
%       {Canvas tk('raise' TId)}
% %          {Canvas tk(bind AId '<Enter>' ECmd)}
% %          {Canvas tk(bind AId '<Leave>' LCmd)}
% %          {Canvas tk(bind TId '<Enter>' ECmd)}
% %          {Canvas tk(bind TId '<Leave>' LCmd)}
%         end
%      [] out(Message_id Sender_id Sender_thread_id Receiver_id ...) then
%         Color={CondSelect Nx color gray}
%      in
%         if {CondSelect Conf Color true} then
%       %% outgoing message
%       %% this message should not be drawed yet, instead we remember where we are
%       %% so that can draw it when it is received
%       %% where we are is either the next available line,
%       %% or the continuation of where this node already is (same thread_id)
%       {AddNode Sender_id}
%       {AddNode Receiver_id}
%       Line
%       Node={Dictionary.get NodeDict Sender_id}
%         in
%       if Node.1==Sender_thread_id then
%          Line=Node.2
%       else
%          Line={IncLineIdx}
%       end
%       {Dictionary.put NodeDict Sender_id Sender_thread_id#Line#Node.3}
%       {StoreIdx Sender_id Receiver_id Message_id Line}
%       %%      {Dictionary.put OriginIdx Message_id Line}
%         end
%      [] next(Site_id ...) then
%         {AddNode Site_id}
%         Node={Dictionary.get NodeDict Site_id}
%      in
%         {Dictionary.put NodeDict Site_id unit#0#Node.3}
% %    [] event(Site_id Side_thread_id Message) then
% %       {AddNode Site_id}
% %       Node={Dictionary.get NodeDict Site_id}
% %       Line={IncLineIdx}
% %       Col=Node.3
% %       {Dictionary.put NodeDict Site_id Side_thread_id#Line#Col}
% %       TId={Canvas tkReturnInt(create(text
% %                  Col*ColWidth-(ColWidth div 2)
% %                  (Line+2)*LineWidth
% %                  text:{Value.toVirtualString Message 10000 10000}) $)}
% %       [X1 Y1 X2 Y2]={Canvas tkReturnListInt(bbox(TId) $)}
% %       BId={Canvas tkReturnInt(create(rect X1 Y1 X2 Y2 fill:white outline:white) $)}
% %    in
% %       {Canvas tk('raise' TId)}
%      [] event(Site_id Message ...) then
%         Color={CondSelect Nx color gray}
%      in
%         if {CondSelect Conf Color true} then
%       {AddNode Site_id}
%       Node={Dictionary.get NodeDict Site_id}
%       Line={IncLineIdx}
%       Col=Node.3
%       {Dictionary.put NodeDict Site_id {CondSelect Nx thid unit}#Line#Col}
%       TId={Canvas tkReturnInt(create(text
%                  Col*ColWidth-(ColWidth div 2)
%                  (Line+2)*LineWidth
%                  fill:{CondSelect Nx color red}
%                  text:{Value.toVirtualString Message 10000 10000}) $)}
%       [X1 Y1 X2 Y2]={Canvas tkReturnListInt(bbox(TId) $)}
%       BId={Canvas tkReturnInt(create(rect X1-2 Y1-2 X2+2 Y2+2 fill:white outline:white) $)}
%       ECmd={VirtualString.toAtom e#TId}
%       LCmd={VirtualString.toAtom l#TId}
%       {Tk.defineUserCmd ECmd
%        proc{$}
%           {Canvas tk('raise' TId)}
%           {Canvas tk(lower BId TId)}
%           {Canvas tk(itemconfigure BId outline:black fill:white)}
%        end nil _}
%       {Tk.defineUserCmd LCmd
%        proc{$}
%           {Canvas tk(itemconfigure BId fill:white outline:white)}
%        end nil _}
%         in
%       {Canvas tk('raise' TId)}     
%       {Canvas tk(bind TId '<Enter>' ECmd)}
%       {Canvas tk(bind TId '<Leave>' LCmd)}
%         end
%      else
%         {System.show ignored#Nx}
%      end
%      {Loop Ns}
%   else 
%      Line={IncLineIdx}
%   in
%      {Canvas tk(create line 0 (Line+2)*LineWidth 100000 (Line+2)*LineWidth)}
%   end
%       end
%    in
%       {Loop Log}
%    end

   fun{WriteLog Log FileName}
      Close
      CloseSync
      proc{Loop L}
         {WaitOr L Close}
         if {IsDet L} then
            {FN putS({Value.toVirtualString L.1 1000 1000})}
            {Loop L.2}
         else
            %% close file
            try {FN close} catch _ then skip end
            CloseSync=unit
         end
      end
      FN
   in
      FN={New TextFile init(name:FileName
                            flags:[write create truncate text])}
      thread
         {Loop Log}
      end
      proc{$} Close=unit {Wait CloseSync} end
   end

end

%{System.show {Thread.this}}
%{System.show {Pickle.pack {NewName}}}

 %Log1=[out(1 1 0 2)
  %     out(2 1 0 2)
   %    'in'(1 1 0 2 m21)
    %   'in'(2 1 0 2 m22)]

 %Log2=['in'(1 2 0 1 m11)
  %     'out'(1 2 0 1)
   %    'out'(2 2 0 1)
    %   'in'(2 2 0 1 m12)]

 %Log={Multiplex Log1 Log2}
 %{Show Log}
 %{DrawLog Log c}
