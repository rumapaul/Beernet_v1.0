%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Graph Viewer
%% Graphical representation of a graph
%%
%% By Raphal Collet, modified by Donatien Grolaux
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

functor

import
   QTk at 'x-oz://system/wp/QTk.ozf'

export
   DrawGraph

define

   fun lazy {Step I S} I|{Step I+S S} end
   fun {Head Xs} Xs.1 end
   fun {Tail Xs} Xs.2 end
   fun {Distance A#B C#D} AC=A-C BD=B-D in {Max {Sqrt AC*AC + BD*BD} 1.} end

% constants
   W = 100.
   Attraction = {NewCell 0.3}
   Repulsion  = {NewCell 0.1}
   MaxX={NewCell 100.0}
   MaxY={NewCell 100.0}

   fun {DrawGraph G1 Title}
      %% expected G record is a tuple
      EventProc={NewCell proc{$ _} skip end}
      MapId={List.toRecord c
	     {List.mapInd {Record.toListInd G1} fun{$ I K#_} I#K end}}
      IMapId={List.toRecord c
	      {List.mapInd {Record.toListInd G1} fun{$ I K#_} K#I end}}
      G={List.toRecord c
	 {List.map {Record.toListInd G1} fun{$ K#V}
					    (IMapId.K)#{List.map V fun{$ E} IMapId.E end}
					 end}}
      %CEdge={NewCell c}
      Canvas
      Desc = td(title:Title canvas(handle:Canvas glue:nswe bg:white
		       width:800 height:800))
      {{QTk.build Desc} show}

      Active = {NewCell _}   % whether the nodes are active in the window
      {Canvas bind(event:"<Enter>" action:proc {$} @Active = unit end)}
      {Canvas bind(event:"<Leave>" action:proc {$} unit = Active := _ end)}
      {Canvas bind(event:"<Configure>" action:proc{$}
						 {Assign MaxX {Int.toFloat {Canvas winfo(width:$)}}}
						 {Assign MaxY {Int.toFloat {Canvas winfo(height:$)}}}
						 @Active = unit
						 thread
						    {Delay 100}
						    O N
						 in
						    {Exchange Active O N}
						    if {IsFree O} then N=O end
						 end
					      end)}

   % creates a new node, and returns the list of its positions
      fun {NewNode Label X#Y Attractors Repulsors}
	 T = {Canvas newTag($)}
	 fun {MoveNode X#Y Attractors Repulsors}
	    {Delay 50}
	    {Wait @Active}
	    Xn#Yn = if @Dragging then @DragTo else
		       Moves1 = {Map {Map Attractors Head}
				 fun {$ A#B}
				    D = {Distance A#B X#Y}
				    F = @Attraction * (D-W) / D
				 in
				    '#'(F*(A-X) F*(B-Y))
				 end}
		       Moves2 = {Map {Map Repulsors Head}
				 fun {$ A#B}
				    D = {Distance A#B X#Y}
				    F=if D>300.0 then
					 0.0
				      else
					 @Repulsion * W*W / (D*D)
				      end
				 in
				    '#'(F*(X-A) F*(Y-B))
				 end}
		    in
		       {FoldL {Append Moves1 Moves2}
			fun {$ A#B C#D} (A+C)#(B+D) end X#Y}
		    end
	    X1#Y1={Min {Max 10.0 Xn} {Access MaxX}-10.0}#{Min {Max 10.0 Yn} {Access MaxY}-10.0}
	 in
%	 {T move(X1-X Y1-Y)}
	    {Canvas tk(move T X1-X Y1-Y)}
	    X1#Y1 | {MoveNode X1#Y1 {Map Attractors Tail} {Map Repulsors Tail}}
	 end
	 Dragging = {NewCell false}
	 DragTo   = {NewCell nil}
      in
	 {Canvas create(oval X-10. Y-10. X+10. Y+10. fill:lightgreen tags:T)}
	 {Canvas create(text X Y text:MapId.Label tags:T)}
	 {T bind(event:  "<Button-1>"
		 args:   [float(x) float(y)]
		 action: proc {$ X Y} DragTo := X#Y Dragging := true end)}
	 {T bind(event:  "<Motion>"
		 args:   [float(x) float(y)]
		 action: proc {$ X Y}
			    if @Dragging then DragTo := X#Y end
			 end)}
	 {T bind(event:  "<ButtonRelease-1>"
		 action: proc {$} Dragging := false end)}
	 {T bind(event: "<3>"
		 args: [int(x) int(y)]
		 action: proc{$ X Y}
			    {@EventProc rightClickNode(id:MapId.Label
							     canvas:Canvas
							     tag:T
							     x:X y:Y)}
			 end)}
	 thread X#Y | {MoveNode X#Y Attractors Repulsors} end
      end

   % maintains the edge between nodes A and B
 %      proc {NewEdge A B}
% 	 T = {Canvas newTag($)}
%       in
% 	 {Canvas create(line 0. 0. 0. 0. arrow:last tags:T smooth:true)} {T lower}
% 	 {T bind(event: "<3>"
% 		 args: [float(x) float(y)]
% 		 action: proc{$ X Y}
% 			    {@EventProc rightClickEdge('from':MapId.A
% 							     to:MapId.B
% 							     canvas:Canvas
% 							     tag:T
% 							     x:X y:Y)}
% 			 end)}
% 	 thread
% 	    for X1#Y1 in Node.A  X2#Y2 in Node.B do
% 	       D = {Distance X1#Y1 X2#Y2}
% 	       Xa#Ya=X1#Y1
% 	       Xb#Yb=X2+(X1-X2)*10./D#Y2+(Y1-Y2)*10./D
% 	       Xc#Yc=((Xb+Xa)/2.0)#((Yb+Ya)/2.0)
% 	       Xd#Yd=(~Yb+Ya)#(Xb-Xa)
% 	       N={Float.sqrt (Xd-Xa)*(Xd-Xa)+(Yd-Ya)*(Yd-Ya)}
% 	       Xe#Ye=(Xc+70.0*Xd/N)#(Yc+70.0*Yd/N)
% 	    in
% %	    {T setCoords(X1 Y1 X2+(X1-X2)*10./D Y2+(Y1-Y2)*10./D)}
% 	       {Canvas tk(coords T Xa Ya Xe Ye Xb Yb)}
% 	    end
% 	 end
%       end

      proc{NewEdge A B}
	 {NewCEdge A B black}
      end
      
      CEdgeCount={NewCell c}

      proc{NewCEdge A B Color}
	 O N
	 T = {Canvas newTag($)}
	 {Exchange CEdgeCount O N}
	 R1=if {HasFeature O A} then
	       O.A
	    else
	       c
	    end
	 R=if {HasFeature R1 B} then
	      R1.B+1
	   else 1 end
	 N={Record.adjoinAt O A
	    {Record.adjoinAt R1 B R}}
      in
	 {Canvas create(line 0. 0. 0. 0. arrow:last tags:T fill:Color smooth:true)} {T lower}
	 {T bind(event: "<3>"
		 args: [int(x) int(y)]
		 action: proc{$ X Y}
			    {@EventProc rightClickEdge('from':MapId.A
							      to:MapId.B
							      canvas:Canvas
							      color:Color
							      tag:T
							      x:X y:Y)}
			 end)}
	 {T bind(event: "<Enter>"
		 action: proc{$}
			    {T set(width:3)}
			 end)}
	 {T bind(event: "<Leave>"
		 action: proc{$}
			    {T set(width:1)}
			 end)}
	 thread
	    for X1#Y1 in Node.A  X2#Y2 in Node.B do
	       D = {Distance X1#Y1 X2#Y2}
	       Xa#Ya=X1#Y1
	       Xb#Yb=X2+(X1-X2)*10./D#Y2+(Y1-Y2)*10./D
	       Xc#Yc=((Xb+Xa)/2.0)#((Yb+Ya)/2.0)
	       Xd#Yd=(~Yb+Ya)#(Xb-Xa)
	       N={Float.sqrt (Xd-Xa)*(Xd-Xa)+(Yd-Ya)*(Yd-Ya)}
	       Xe#Ye=(Xc+((70.0*{Int.toFloat (R)})*Xd)/N)#(Yc+((70.0*{Int.toFloat (R)})*Yd)/N)
	    in
%	    {T setCoords(X1 Y1 X2+(X1-X2)*10./D Y2+(Y1-Y2)*10./D)}
	       {Canvas tk(coords T Xa Ya Xe Ye Xb Yb)}
	    end
	 end	 
      end

      N = {Width G}
      fun {Complement Is} % complement of a list w.r.t. 1..N
	 D = {NewDictionary} in
	 for I in 1..N do D.I := I end
	 for I in Is do {Dictionary.remove D I} end
	 {Dictionary.items D}
      end

      Node = {Record.mapInd {NormalizedGraph G}
	      fun {$ I Attr}
		 Angle = 6.28 * {IntToFloat I}/{IntToFloat N}
	      in
		 {NewNode I
		  '#'(400. + W*{Cos Angle} 400. + W*{Sin Angle})
		  {Map Attr fun {$ J} Node.J end}
		  {Map {Complement I|Attr} fun {$ J} Node.J end}}
	      end}
      %AddNode
      %RemoveNode
      %AddEdge
      proc{AddCEdge From To Color}
	 {NewCEdge IMapId.From IMapId.To Color}
      end
      proc{OnEvent P}
	 EventProc:=P
      end
   in
      for I in 1..N do
	 for J in G.I do {NewEdge I J} end
      end

      graph(%addNode:AddNode
	    %removeNode:RemoveNode
	    %addEdge:AddEdge
	    onEvent:OnEvent
	    addCEdge:AddCEdge)
   end

% returns the non-directed graph corresponding to G
   fun {NormalizedGraph G}
      N = {Width G}
      A = {Record.map G fun {$ _} {NewDictionary} end}
   in
      for I in 1..N do
	 for J in G.I do   % add edges in both directions
	    A.I.J := unit   A.J.I := unit
	 end
	 {Dictionary.remove A.I I}   % remove loops
      end
      {Record.map A Dictionary.keys}
   end
end

% {DrawGraph graph([2 3] [3 4] [4 5] [5 1] [1 2])}

% % Petersen's graph: the edges of the inner cycle should be "stronger"
% {DrawGraph
%  graph(1:[2 6] 2:[3 7] 3:[4 8] 4:[5 9] 5:[1 10]
%        6:[7] 7:[8] 8:[9] 9:[10] 10:[6])}

% % a "hairy" graph
% {DrawGraph
%  graph([2 6 7 8] [3 9 10 11] [4 12 13 14] [5 15 16 17] [1 18 19 20]
%        nil nil nil nil nil nil nil nil nil nil nil nil nil nil nil)}

% {DrawGraph
%  graph(1:[2 3 4] 2:[5 6] 3:[7 8] 4:nil 5:nil 6:[9] 7:[9] 8:nil 9:nil)}
%R=graph([3 2] [4 1] [4 1] [2 3])
%{DrawGraph graph([3 2] [4 1] [4 1] [2 3])}
%R=graph([3 2] [1] [4 1] [3])
%{DrawGraph R}
